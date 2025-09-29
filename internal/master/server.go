package master

import (
	"alpha-autosell-bot/internal/account"
	"alpha-autosell-bot/internal/auth"
	"alpha-autosell-bot/internal/binance"
	"alpha-autosell-bot/internal/task"
	"alpha-autosell-bot/price"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"alpha-autosell-bot/internal/common"
	"alpha-autosell-bot/internal/proto"
	"alpha-autosell-bot/pkg/network"
	"alpha-autosell-bot/pkg/redis"

	redisv8 "github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
)

// Server 主控端服务器
type Server struct {
	proto.UnimplementedTradingServiceServer

	config          *common.Config
	grpcServer      *grpc.Server
	httpServer      *http.Server
	redisClient     *redis.Client
	mongoAuthManager *auth.MongoAuthManager // 添加MongoDB授权管理器
	useMongoAuth    bool                    // 是否使用MongoDB授权

	// 节点管理
	nodes       map[string]*NodeInfo
	nodesMutex  sync.RWMutex
	nodeCounter int // 节点计数器，用于自动命名

	// 通用账号管理
	accountManager *account.UniversalAccountManager

	// 通用任务管理
	taskManager *task.UniversalTaskManager

	// 统计管理
	statsManager *binance.StatsManager

	// Flash Trade 管理（保留 - Web 端需要）
	flashTasks map[string]*FlashTaskInfo
	flashMutex sync.RWMutex

	// 自动卖出结果管理
	autoSellResults map[string]interface{}
	mu              sync.RWMutex

	// 指令确认跟踪
	commandAcks map[string]map[string]bool // commandID -> nodeID -> acked
	ackMutex    sync.RWMutex
	
	// 网络连接管理
	connManager *network.ConnectionManager

	// 价格客户端
	priceClient interface{}
}

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID        string                 `json:"node_id"`
	Address       string                 `json:"address"`
	Status        string                 `json:"status"` // online, offline, busy
	LastHeartbeat time.Time              `json:"last_heartbeat"`
	ActiveTasks   []string               `json:"active_tasks"`
	Stats         map[string]interface{} `json:"stats"`         // 节点统计信息
	Metadata      map[string]string      `json:"metadata"`      // 节点元数据
	FriendlyName  string                 `json:"friendly_name"` // 友好名称，如 "节点1", "节点2"
	Username      string                 `json:"username"`      // 用户名
}

// NullableTime 可为空的时间类型
type NullableTime struct {
	time.Time
}

// UnmarshalJSON 自定义JSON解析，处理空字符串
func (nt *NullableTime) UnmarshalJSON(data []byte) error {
	str := string(data)
	if str == `""` || str == "null" {
		nt.Time = time.Now()
		return nil
	}

	var t time.Time
	err := t.UnmarshalJSON(data)
	if err != nil {
		nt.Time = time.Now()
		return nil
	}
	nt.Time = t
	return nil
}

// FlashTaskInfo Flash任务信息
type FlashTaskInfo struct {
	TaskID         string                 `json:"task_id"`
	TokenAddress   string                 `json:"token_address"`
	USDTAmount     float64                `json:"usdt_amount"`
	BaseAsset      string                 `json:"base_asset"`
	TargetVolume   float64                `json:"target_volume"`
	AutoLoop       bool                   `json:"auto_loop"`
	PricePrecision int                    `json:"price_precision"`
	ChainID        string                 `json:"chain_id"`        // 🔧 区块链ID
	PriceMode      string                 `json:"price_mode"`      // 🔧 价格模式
	SpeedMode      string                 `json:"speed_mode"`      // 🚀 新增：速度模式
	Status         string                 `json:"status"` // created, running, completed, failed
	StartTime      time.Time              `json:"start_time"`
	EndTime        *time.Time             `json:"end_time,omitempty"`
	TargetNodes    []string               `json:"target_nodes"`    // 目标节点列表
	TargetAccounts []string               `json:"target_accounts"` // 目标账号列表
	Results        map[string]interface{} `json:"results"`         // 执行结果
}

// NewServer 创建主控端服务器
func NewServer(config *common.Config) (*Server, error) {
	// 创建Redis客户端
	redisClient := redis.NewClient(redis.Config{
		Addr:     config.Redis.Addr,
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})

	// 创建通用账号管理器（使用 Redis）
	// 获取原生 Redis 客户端
	nativeRedisClient := redisClient.GetNativeClient()
	accountManager := account.NewUniversalAccountManagerWithRedis(nativeRedisClient)

	// 创建通用任务管理器
	taskManager := task.NewUniversalTaskManager(accountManager)

	// 注册自动卖出任务执行器
	autoSellExecutor := task.NewAutoSellExecutor(redisClient.GetNativeClient())
	// 注册被控端节点（这里需要根据实际情况配置）
	autoSellExecutor.RegisterSlaveNode("alpha_autosell_node_1", "localhost:8081")
	taskManager.RegisterExecutor(task.TaskTypeAutoSell, autoSellExecutor)

	// 创建统计管理器
	statsManager := binance.NewStatsManager()
	
	// 创建MongoDB授权管理器（如果配置了MongoDB）
	var mongoAuthManager *auth.MongoAuthManager
	useMongoAuth := false
	
	// 检查是否配置了MongoDB
	if config.MongoDB != nil && config.MongoDB.Enabled && config.MongoDB.URI != "" {
		var err error
		mongoAuthManager, err = auth.NewMongoAuthManager()
		if err != nil {
			log.Printf("⚠️ 创建MongoDB授权管理器失败: %v，将使用Redis授权", err)
		} else {
			useMongoAuth = true
			log.Println("✅ MongoDB授权管理器创建成功")
		}
	}

	server := &Server{
		config:          config,
		redisClient:     redisClient,
		mongoAuthManager: mongoAuthManager,
		useMongoAuth:    useMongoAuth,
		nodes:           make(map[string]*NodeInfo),
		nodeCounter:     0,
		accountManager:  accountManager,
		taskManager:     taskManager,
		statsManager:    statsManager,
		flashTasks:      make(map[string]*FlashTaskInfo),
		commandAcks:     make(map[string]map[string]bool),
	}

	// 启动指令确认监听
	go server.listenForCommandAcks()

	return server, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	// 初始化节点管理
	s.nodes = make(map[string]*NodeInfo)
	s.nodeCounter = 0

	// 初始化Flash Trade管理
	s.flashTasks = make(map[string]*FlashTaskInfo)
	s.autoSellResults = make(map[string]interface{})
	
	// 初始化命令确认跟踪
	s.commandAcks = make(map[string]map[string]bool)

	// 启动gRPC服务器
	addr := s.config.GetGRPCAddr()
	log.Printf("🚀 启动gRPC服务器: %s", addr)
	
	// 创建网络监听器
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("无法监听端口 %s: %v", addr, err)
	}
	
	// 创建gRPC服务器
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     2 * time.Minute,
			MaxConnectionAge:      5 * time.Minute,
			MaxConnectionAgeGrace: 30 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
		grpc.MaxRecvMsgSize(s.config.GRPC.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(s.config.GRPC.MaxSendMsgSize),
	)
	
	// 注册服务
	proto.RegisterTradingServiceServer(s.grpcServer, s)
	
	// 启动gRPC服务器
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC服务器错误: %v", err)
		}
	}()
	
	// 启动HTTP服务器
	httpAddr := s.config.GetServerAddr()
	log.Printf("🌐 启动HTTP服务器: %s", httpAddr)
	go func() {
		// 创建路由器
		mux := http.NewServeMux()
		
		// 静态文件服务
		fs := http.FileServer(http.Dir("web"))
		mux.Handle("/", fs)
		
		// 账号状态页面特殊处理
		mux.HandleFunc("/account-status", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "web/account_status.html")
		})
		
		// 节点页面特殊处理
		mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "web/nodes.html")
		})
		
		// API路由
		mux.HandleFunc("/api/v1/flash-trade/realtime-stats", s.handleRealtimeStats)
		mux.HandleFunc("/api/v1/nodes", s.handleGetNodes)
		
		// 创建HTTP服务器
		s.httpServer = &http.Server{
			Addr:    httpAddr,
			Handler: mux,
		}
		
		// 启动HTTP服务器
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ HTTP服务器错误: %v", err)
		}
	}()

	// 注册任务执行器
	s.taskManager.RegisterExecutor("flash_trade", &FlashTradeExecutor{server: s})
	// 注意：auto_sell 执行器在 NewServer 中单独注册
	s.taskManager.RegisterExecutor("batch_trade", &BatchTradeExecutor{server: s})
	log.Println("✅ 任务执行器注册完成")

	// 加载持久化数据
	accounts := s.accountManager.GetAllAccounts()
	log.Printf("✅ 数据加载成功，共 %d 个账号", len(accounts))

	// 连接价格客户端
	if err := s.connectToPriceClient(); err != nil {
		log.Printf("⚠️  连接价格客户端失败: %v", err)
	} else {
		log.Println("✅ 价格客户端连接成功")
	}

	// 启动 Flash Trade 统计订阅
	go s.startFlashTradeStatsSubscription()

	// 启动自动卖出结果订阅
	go s.startAutoSellResultsSubscription()

	// 启动账号状态监控订阅
	go s.startAccountStatusSubscription()

	// 从Redis恢复节点信息
	go s.loadNodesFromRedis()
	
	// 启动节点状态监控
	go s.startNodeStatusMonitor()
	
	log.Printf("✅ 主控端服务器启动完成")
	return nil
}

// Stop 停止主控端服务器
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
	}
	
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(ctx)
	}
	
	if s.redisClient != nil {
		s.redisClient.Close()
	}
	
	// 关闭MongoDB连接
	if s.mongoAuthManager != nil {
		s.mongoAuthManager.Close()
		log.Println("MongoDB授权管理器已关闭")
	}
	
	log.Println("主控端服务器已停止")
}

// GetNodes 获取所有节点
func (s *Server) GetNodes() map[string]*NodeInfo {
	s.nodesMutex.RLock()
	defer s.nodesMutex.RUnlock()

	result := make(map[string]*NodeInfo)
	for k, v := range s.nodes {
		result[k] = v
	}
	return result
}

// GetAccountManager 获取账号管理器
func (s *Server) GetAccountManager() *account.UniversalAccountManager {
	return s.accountManager
}

// GetRedisClient 获取Redis客户端
func (s *Server) GetRedisClient() *redis.Client {
	return s.redisClient
}

// GetTaskManager 获取任务管理器
func (s *Server) GetTaskManager() *task.UniversalTaskManager {
	return s.taskManager
}

// connectToPriceClient 连接价格客户端
func (s *Server) connectToPriceClient() error {
	// 这里应该连接到价格服务
	// 暂时返回 nil 表示成功
	return nil
}

// executeFlashTrade 执行 Flash Trade 任务
func (s *Server) executeFlashTrade(ctx context.Context, taskData interface{}) error {
	log.Printf("🔥 执行 Flash Trade 任务")
	// 这里应该实现具体的 Flash Trade 逻辑
	// 暂时返回 nil 表示成功
	return nil
}

// executeAutoSell 执行自动卖出任务
func (s *Server) executeAutoSell(ctx context.Context, taskData interface{}) error {
	log.Printf("💰 执行自动卖出任务")
	// 这里应该实现具体的自动卖出逻辑
	// 暂时返回 nil 表示成功
	return nil
}

// executeBatchTrade 执行批量交易任务
func (s *Server) executeBatchTrade(ctx context.Context, taskData interface{}) error {
	log.Printf("📦 执行批量交易任务")
	// 这里应该实现具体的批量交易逻辑
	// 暂时返回 nil 表示成功
	return nil
}

// ===== 任务执行器实现 =====

// FlashTradeExecutor Flash Trade 任务执行器
type FlashTradeExecutor struct {
	server *Server
}

func (e *FlashTradeExecutor) Execute(universalTask *task.UniversalTask, accountManager *account.UniversalAccountManager) error {
	log.Printf("📡 主控端分发 Flash Trade 任务到被控端: %s", universalTask.ID)

	// 解析任务参数
	params := universalTask.Parameters
	tokenAddress, _ := params["token_address"].(string)
	usdtAmount, _ := params["usdt_amount"].(float64)
	baseAsset, _ := params["base_asset"].(string)
	targetVolume, _ := params["target_volume"].(float64)
	autoLoop, _ := params["auto_loop"].(bool) // 启用自动循环参数
	pricePrecision, _ := params["price_precision"].(float64)
	chainID, _ := params["chain_id"].(string)       // 🔧 新增：解析链ID
	priceMode, _ := params["price_mode"].(string)   // 🔧 新增：解析价格模式

	// 验证必要参数
	if tokenAddress == "" {
		return fmt.Errorf("token_address 参数不能为空")
	}
	if usdtAmount <= 0 {
		return fmt.Errorf("usdt_amount 必须大于 0")
	}
	if targetVolume <= 0 {
		return fmt.Errorf("target_volume 必须大于 0")
	}

	// 获取目标账号列表
	targetAccounts := universalTask.TargetAccounts
	if len(targetAccounts) == 0 {
		// 如果没有指定账号，使用所有活跃账号
		allAccounts := accountManager.GetAllAccounts()
		for id, account := range allAccounts {
			if account.Status == "active" {
				targetAccounts = append(targetAccounts, id)
			}
		}
	}

	if len(targetAccounts) == 0 {
		return fmt.Errorf("没有可用的目标账号")
	}

	log.Printf("🎯 Flash Trade 目标: %d 个账号, 代币: %s, 金额: %.2f USDT",
		len(targetAccounts), tokenAddress, usdtAmount)

	// 更新任务状态为运行中
	universalTask.Status = task.TaskStatusRunning
	now := time.Now()
	universalTask.StartedAt = &now

	// 创建 Flash Trade 任务信息，与现有的 Flash Trade API 兼容
	flashTask := &FlashTaskInfo{
		TaskID:         universalTask.ID,
		TokenAddress:   tokenAddress,
		USDTAmount:     usdtAmount,
		BaseAsset:      baseAsset,
		TargetVolume:   targetVolume,
		AutoLoop:       false, // 通用任务不支持自动循环
		PricePrecision: int(pricePrecision),
		Status:         "running",
		StartTime:      now,
		TargetNodes:    universalTask.TargetNodes,
		TargetAccounts: targetAccounts,
		Results:        make(map[string]interface{}),
	}

	// 保存到 Flash 任务列表
	e.server.flashMutex.Lock()
	e.server.flashTasks[universalTask.ID] = flashTask
	e.server.flashMutex.Unlock()

	// 详细的执行逻辑，保存每个账号的结果
	successCount := 0
	errorCount := 0
	accountResults := make(map[string]interface{}) // 保存每个账号的详细结果

	// 通过 gRPC 分发任务给被控端执行
	log.Printf("📡 开始分发 Flash Trade 任务给被控端")

	// 如果指定了目标节点，只发送给指定节点
	if len(universalTask.TargetNodes) > 0 {
		for _, nodeID := range universalTask.TargetNodes {
			// 获取该节点的账号列表，验证账号归属
			nodeAccounts := e.server.GetAccountManager().GetNodeAccounts(nodeID)

			// 过滤出属于该节点的目标账号
			var validAccountsForNode []string
			if len(targetAccounts) > 0 {
				// 如果指定了目标账号，过滤出属于该节点的账号
				for _, targetAccount := range targetAccounts {
					for _, nodeAccount := range nodeAccounts {
						if nodeAccount.ID == targetAccount {
							validAccountsForNode = append(validAccountsForNode, targetAccount)
							break
						}
					}
				}
			} else {
				// 如果没有指定目标账号，使用该节点的所有活跃账号
				for _, nodeAccount := range nodeAccounts {
					if nodeAccount.Status == "active" {
						validAccountsForNode = append(validAccountsForNode, nodeAccount.ID)
					}
				}
			}

			log.Printf("📤 节点 %s: 目标账号 %v, 节点账号数 %d, 有效账号 %v", nodeID, targetAccounts, len(nodeAccounts), validAccountsForNode)

			// 为每个有效账号创建单独的请求
			if len(validAccountsForNode) > 0 {
				for _, accountID := range validAccountsForNode {
					grpcReq := &proto.FlashTradeRequest{
						NodeId:         nodeID,
						AccountId:      accountID,
						TokenAddress:   tokenAddress,
						UsdtAmount:     usdtAmount,
						BaseAsset:      baseAsset,
						TargetVolume:   targetVolume,
						AutoLoop:       autoLoop,
						PricePrecision: int32(pricePrecision),
						ChainId:        chainID,    // 🔧 新增：传递链ID
						PriceMode:      priceMode,  // 🔧 新增：传递价格模式
					}

					log.Printf("📤 发送 Flash Trade 任务到节点 %s, 账号: %s", nodeID, accountID)

					resp, err := e.server.StartFlashTrade(context.Background(), grpcReq)
					if err != nil {
						log.Printf("❌ 节点 %s 账号 %s Flash Trade 启动失败: %v", nodeID, accountID, err)
						errorCount++
					} else if resp.Success {
						log.Printf("✅ 节点 %s 账号 %s Flash Trade 启动成功: %s", nodeID, accountID, resp.Message)
						successCount++
					} else {
						log.Printf("⚠️ 节点 %s 账号 %s Flash Trade 启动失败: %s", nodeID, accountID, resp.Message)
						errorCount++
					}
				}
			} else {
				// 如果没有有效账号，发送不指定账号的请求（使用该节点的所有账号）
				grpcReq := &proto.FlashTradeRequest{
					NodeId:         nodeID,
					TokenAddress:   tokenAddress,
					UsdtAmount:     usdtAmount,
					BaseAsset:      baseAsset,
					TargetVolume:   targetVolume,
					AutoLoop:       autoLoop,
					PricePrecision: int32(pricePrecision),
					ChainId:        chainID,    // 🔧 新增：传递链ID
					PriceMode:      priceMode,  // 🔧 新增：传递价格模式
				}

				log.Printf("📤 发送 Flash Trade 任务到节点 %s (所有账号)", nodeID)

				resp, err := e.server.StartFlashTrade(context.Background(), grpcReq)
				if err != nil {
					log.Printf("❌ 节点 %s Flash Trade 启动失败: %v", nodeID, err)
					errorCount++
				} else if resp.Success {
					log.Printf("✅ 节点 %s Flash Trade 启动成功: %s", nodeID, resp.Message)
					successCount++
				} else {
					log.Printf("⚠️ 节点 %s Flash Trade 启动失败: %s", nodeID, resp.Message)
					errorCount++
				}
			}
		}
	} else {
		// 如果没有指定节点，发送给所有在线节点
		if len(targetAccounts) > 0 {
			// 如果指定了账号，需要找到每个账号所属的节点
			accountNodeMap := make(map[string]string) // accountID -> nodeID

			// 获取所有账号的节点分配信息
			allAccounts := e.server.GetAccountManager().GetAllAccounts()
			for _, account := range allAccounts {
				if account.AssignedNode != "" {
					accountNodeMap[account.ID] = account.AssignedNode
				}
			}

			// 按节点分组账号
			nodeAccountsMap := make(map[string][]string) // nodeID -> []accountID
			for _, accountID := range targetAccounts {
				if nodeID, exists := accountNodeMap[accountID]; exists {
					nodeAccountsMap[nodeID] = append(nodeAccountsMap[nodeID], accountID)
				}
			}

			log.Printf("📤 按节点分发账号: %v", nodeAccountsMap)

			// 为每个节点的每个账号发送请求
			for nodeID, accountIDs := range nodeAccountsMap {
				for _, accountID := range accountIDs {
					grpcReq := &proto.FlashTradeRequest{
						NodeId:         nodeID,
						AccountId:      accountID,
						TokenAddress:   tokenAddress,
						UsdtAmount:     usdtAmount,
						BaseAsset:      baseAsset,
						TargetVolume:   targetVolume,
						AutoLoop:       autoLoop,
						PricePrecision: int32(pricePrecision),
					}

					log.Printf("📤 发送 Flash Trade 任务到节点 %s, 账号: %s", nodeID, accountID)

					resp, err := e.server.StartFlashTrade(context.Background(), grpcReq)
					if err != nil {
						log.Printf("❌ 节点 %s 账号 %s Flash Trade 启动失败: %v", nodeID, accountID, err)
						errorCount++
					} else if resp.Success {
						log.Printf("✅ 节点 %s 账号 %s Flash Trade 启动成功: %s", nodeID, accountID, resp.Message)
						successCount++
					} else {
						log.Printf("⚠️ 节点 %s 账号 %s Flash Trade 启动失败: %s", nodeID, accountID, resp.Message)
						errorCount++
					}
				}
			}
		} else {
			// 如果没有指定账号，发送给所有在线节点（使用各节点的所有账号）
			log.Printf("📤 发送 Flash Trade 任务到所有在线节点 (所有账号)")

			// 获取所有在线节点
			allNodes := e.server.GetNodes()
			onlineNodes := make([]string, 0)
			for nodeID, nodeInfo := range allNodes {
				if nodeInfo.Status == "online" {
					onlineNodes = append(onlineNodes, nodeID)
				}
			}

			log.Printf("📡 找到 %d 个在线节点: %v", len(onlineNodes), onlineNodes)

			// 为每个在线节点发送任务
			for _, nodeID := range onlineNodes {
				grpcReq := &proto.FlashTradeRequest{
					NodeId:         nodeID, // 🔧 修复：为每个节点指定NodeId
					TokenAddress:   tokenAddress,
					UsdtAmount:     usdtAmount,
					BaseAsset:      baseAsset,
					TargetVolume:   targetVolume,
					AutoLoop:       autoLoop,
					PricePrecision: int32(pricePrecision),
					ChainId:        chainID,    // 🔧 新增：传递链ID
					PriceMode:      priceMode,  // 🔧 新增：传递价格模式
				}

				log.Printf("📤 发送 Flash Trade 任务到节点 %s (所有账号)", nodeID)
				log.Printf("   参数: 代币=%s, 金额=%.2f, 目标量=%.2f", tokenAddress, usdtAmount, targetVolume)

				resp, err := e.server.StartFlashTrade(context.Background(), grpcReq)
				if err != nil {
					log.Printf("❌ 节点 %s Flash Trade 启动失败: %v", nodeID, err)
					errorCount++
				} else if resp.Success {
					log.Printf("✅ 节点 %s Flash Trade 启动成功: %s", nodeID, resp.Message)
					log.Printf("   任务ID: %s, 影响节点: %v", resp.TaskId, resp.AffectedNodes)
					successCount++
				} else {
					log.Printf("⚠️ 节点 %s Flash Trade 启动失败: %s", nodeID, resp.Message)
					errorCount++
				}
			}
		}
	}

	// 记录分发结果
	for _, accountID := range targetAccounts {
		accountResults[accountID] = map[string]interface{}{
			"success":          successCount > 0,
			"message":          "任务已分发到被控端执行",
			"distributed_time": time.Now().Unix(),
			"target_nodes":     universalTask.TargetNodes,
		}
	}

	// 更新最终状态
	if errorCount == 0 {
		universalTask.Status = task.TaskStatusCompleted
	} else if successCount == 0 {
		universalTask.Status = task.TaskStatusFailed
	} else {
		universalTask.Status = task.TaskStatusCompleted // 部分成功也算完成
	}

	// 设置完成时间
	completedAt := time.Now()
	universalTask.CompletedAt = &completedAt

	// 更新 Flash 任务状态
	e.server.flashMutex.Lock()
	if flashTask, exists := e.server.flashTasks[universalTask.ID]; exists {
		if errorCount == 0 {
			flashTask.Status = "completed"
		} else if successCount == 0 {
			flashTask.Status = "failed"
		} else {
			flashTask.Status = "partial_success"
		}
		flashTask.EndTime = &completedAt
		flashTask.Results = accountResults
	}
	e.server.flashMutex.Unlock()

	// 记录详细结果
	universalTask.Results["success_count"] = successCount
	universalTask.Results["error_count"] = errorCount
	universalTask.Results["total_accounts"] = len(targetAccounts)
	universalTask.Results["success_rate"] = float64(successCount) / float64(len(targetAccounts)) * 100
	universalTask.Results["account_results"] = accountResults // 每个账号的详细结果

	// 计算总利润
	totalProfit := 0.0
	for _, result := range accountResults {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if success, ok := resultMap["success"].(bool); ok && success {
				if profit, ok := resultMap["profit"].(float64); ok {
					totalProfit += profit
				}
			}
		}
	}
	universalTask.Results["total_profit"] = totalProfit

	// 添加任务参数到结果中
	universalTask.Results["task_parameters"] = map[string]interface{}{
		"token_address":   tokenAddress,
		"usdt_amount":     usdtAmount,
		"base_asset":      baseAsset,
		"target_volume":   targetVolume,
		"price_precision": pricePrecision,
	}

	log.Printf("🎉 Flash Trade 任务完成: %s, 成功: %d, 失败: %d",
		universalTask.ID, successCount, errorCount)

	return nil
}

func (e *FlashTradeExecutor) Validate(parameters map[string]interface{}) error {
	// 验证 token_address
	if tokenAddress, ok := parameters["token_address"].(string); !ok || tokenAddress == "" {
		return fmt.Errorf("token_address 参数必须是非空字符串")
	}

	// 验证 usdt_amount
	if usdtAmount, ok := parameters["usdt_amount"].(float64); !ok || usdtAmount <= 0 {
		return fmt.Errorf("usdt_amount 参数必须是大于 0 的数字")
	}

	// 验证 target_volume
	if targetVolume, ok := parameters["target_volume"].(float64); !ok || targetVolume <= 0 {
		return fmt.Errorf("target_volume 参数必须是大于 0 的数字")
	}

	// 验证 price_precision
	if pricePrecision, ok := parameters["price_precision"].(float64); ok {
		if pricePrecision < 0 || pricePrecision > 18 {
			return fmt.Errorf("price_precision 参数必须在 0-18 之间")
		}
	}

	return nil
}

func (e *FlashTradeExecutor) GetDefaultParameters() map[string]interface{} {
	return map[string]interface{}{
		"token_address":   "",
		"usdt_amount":     100.0,
		"base_asset":      "ALPHA_251",
		"target_volume":   1000.0,
		"auto_loop":       false,
		"price_precision": 8.0,
	}
}

// FlashTradeResult Flash Trade 结果结构体（基于根目录实现）
type FlashTradeResult struct {
	Success     bool    `json:"success"`
	Message     string  `json:"message"`
	BuyPrice    float64 `json:"buy_price"`
	SellPrice   float64 `json:"sell_price"`
	TokenAmount float64 `json:"token_amount"`
	Profit      float64 `json:"profit"`
	ExecuteTime int64   `json:"execute_time_ms"`
}

// executeFlashTradeBasedOnRoot 基于根目录 flash_trade.go 的实现（已废弃，现在在被控端执行）
// 保留此方法以防需要本地测试，但正常情况下不会被调用
func (e *FlashTradeExecutor) executeFlashTradeBasedOnRoot(accountID string, account *account.UniversalAccount, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, pricePrecision int) FlashTradeResult {
	log.Printf("⚡ 开始执行账号 %s 的 Flash Trade (基于根目录实现)", accountID)

	startTime := time.Now()

	// 1. 验证最小交易金额
	minTradeAmount := 3.0
	if usdtAmount < minTradeAmount {
		return FlashTradeResult{
			Success: false,
			Message: fmt.Sprintf("交易金额过小，最小需要%.1f USDT，当前: %.2f USDT", minTradeAmount, usdtAmount),
		}
	}

	// 2. 设置默认值
	if baseAsset == "" {
		baseAsset = "ALPHA_251"
	}
	if pricePrecision == 0 {
		pricePrecision = 8
	}

	// 3. 验证能否获取实时价格（使用内部价格服务）
	currentPrice, err := e.getCurrentTokenPrice(tokenAddress, pricePrecision)
	if err != nil {
		return FlashTradeResult{
			Success: false,
			Message: "实时价格获取失败: " + err.Error(),
		}
	}

	log.Printf("🔍 [%s] 获取到的原始价格: %.*f (Token: %s)", accountID, pricePrecision, currentPrice, tokenAddress)

	// 4. 买单价格加万一提高成交率
	buyPrice := currentPrice * 1.0001 // 加万分之1
	buyPrice = e.adjustPricePrecision(buyPrice, pricePrecision)

	// 5. 计算数量和金额，确保符合币安要求
	// 🎲 添加±5%随机浮动，避免固定买入量被风控识别
	randomFactor := 0.95 + rand.Float64()*0.1 // 0.95 到 1.05 之间的随机数
	adjustedUSDTAmount := usdtAmount * randomFactor

	log.Printf("🎲 [%s] Master买入量随机化: 原始%.6f USDT → 调整%.6f USDT (浮动%.2f%%)",
		accountID, usdtAmount, adjustedUSDTAmount, (randomFactor-1)*100)

	idealTokenAmount := adjustedUSDTAmount / buyPrice
	tokenAmount := float64(int(idealTokenAmount)) // 数量必须取整
	calculatedAmount := buyPrice * tokenAmount
	exactAmount := math.Round(calculatedAmount*100000000) / 100000000 // 8位小数精度

	// 🔧 记录实际使用的USDT金额用于统计
	actualUSDTUsed := exactAmount

	// 6. 检查计算后的金额是否为0
	if exactAmount <= 0 || tokenAmount <= 0 {
		return FlashTradeResult{
			Success: false,
			Message: "计算后金额为0，无法下单",
		}
	}

	// 7. 检查资金账户USDT余额
	fundingBalance, err := e.getFundingAccountBalance(account.Csrftoken, account.Cookie)
	if err != nil {
		log.Printf("❌ [%s] 查询资金账户余额失败: %v", accountID, err)
		if strings.Contains(err.Error(), "请检查是否已登录") {
			return FlashTradeResult{
				Success: false,
				Message: "认证失效，请重新登录",
			}
		}
	} else {
		log.Printf("💰 [%s] 资金账户余额: %.8f USDT，需要金额: %.8f USDT", accountID, fundingBalance, exactAmount)

		if fundingBalance < exactAmount {
			if fundingBalance < 1.0 {
				return FlashTradeResult{
					Success: false,
					Message: "资金账户余额过少",
				}
			}

			// 使用实际余额的95%
			adjustedAmount := fundingBalance * 0.95
			adjustedTokenAmount := math.Floor(adjustedAmount / buyPrice)
			adjustedExactAmount := buyPrice * adjustedTokenAmount
			adjustedExactAmount = math.Round(adjustedExactAmount*100000000) / 100000000

			if adjustedTokenAmount >= 1 && adjustedExactAmount > 0 {
				exactAmount = adjustedExactAmount
				tokenAmount = adjustedTokenAmount
			} else {
				return FlashTradeResult{
					Success: false,
					Message: "调整后金额仍不足，无法下单",
				}
			}
		}
	}

	// 8. 执行买入操作（带重试机制）
	buyOrderID, buySuccess := e.placeBuyOrderWithRetry(account, baseAsset, buyPrice, tokenAmount, exactAmount)
	if !buySuccess {
		if buyOrderID == "INSUFFICIENT_BALANCE" {
			return FlashTradeResult{
				Success: false,
				Message: "USDT余额不足",
			}
		}

		// 买入失败，尝试重新获取价格重试
		log.Printf("🔄 [%s] 买入失败，尝试重新获取价格重试", accountID)
		return e.retryBuyWithNewPrice(account, tokenAddress, usdtAmount, baseAsset, targetVolume, pricePrecision, startTime)
	}

	// 9. 等待买入确认
	buyConfirmed := e.waitForBuyConfirmation(buyOrderID, account.Csrftoken, account.Cookie)
	if !buyConfirmed {
		// 取消订单
		e.cancelOrder(buyOrderID, baseAsset+"USDT", account.Csrftoken, account.Cookie)
		return FlashTradeResult{
			Success: false,
			Message: "买入超时",
		}
	}

	log.Printf("✅ [%s] 买入确认成功", accountID)

	// 🔧 新增：Master节点买入成功后立即统计买入交易额（使用实际金额）
	buyInCost := actualUSDTUsed // 🎲 使用实际花费的USDT金额，确保统计准确
	if buyInCost > 0 && tokenAmount > 0 {
		// 生成唯一交易ID防止重复统计
		tradeID := e.generateTradeID(accountID, tokenAddress, buyPrice, tokenAmount)
		// 只统计买入交易额，损益为0（因为还没卖出）
		e.server.statsManager.UpdateAccountStatsWithID(accountID, buyInCost, 0, tradeID)
		log.Printf("📊 [%s] Master买入成功立即统计 - 实际买单金额: %.6f USDT (原始%.6f, 浮动%.2f%%) (ID:%s)",
			accountID, buyInCost, usdtAmount, (buyInCost/usdtAmount-1)*100, tradeID[:8])
	}

	// 10. 执行智能卖出
	sellResult := e.smartSellWithRetry(account, tokenAddress, baseAsset, buyPrice, tokenAmount, pricePrecision, startTime)

	return sellResult
}

// getCurrentTokenPrice 获取代币当前价格（基于 flash_trade.go 和 price 包实现）
func (e *FlashTradeExecutor) getCurrentTokenPrice(tokenAddress string, pricePrecision int) (float64, error) {
	// 使用真实的 price 包获取价格
	log.Printf("🔍 [价格获取] 开始获取代币价格: %s, 精度: %d", tokenAddress, pricePrecision)

	// 调用 price 包的 GetTokenPriceWithPrecision 方法
	price, err := price.GetTokenPriceWithPrecision(tokenAddress, "56", pricePrecision)
	if err != nil {
		log.Printf("❌ [价格获取] 获取价格失败: %v", err)
		return 0, fmt.Errorf("获取代币价格失败: %v", err)
	}

	log.Printf("✅ [价格获取] 价格获取成功: %.*f", pricePrecision, price)
	return price, nil
}

// adjustPricePrecision 调整价格精度
func (e *FlashTradeExecutor) adjustPricePrecision(price float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(price*multiplier) / multiplier
}

// generateTradeID 生成唯一交易ID
func (e *FlashTradeExecutor) generateTradeID(accountID, tokenAddress string, buyPrice, tokenAmount float64) string {
	// 使用账户ID、代币地址、买入价格、代币数量和时间戳生成唯一ID
	data := fmt.Sprintf("%s_%s_%.12f_%.0f_%d",
		accountID, tokenAddress, buyPrice, tokenAmount, time.Now().UnixNano())

	// 使用SHA256生成哈希
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)[:16] // 取前16位作为交易ID
}

// WalletGroupResponse 钱包组响应结构（基于 flash_trade.go）
type WalletGroupResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    []struct {
		WalletGroupType string `json:"walletGroupType"`
		WalletGroupName string `json:"walletGroupName"`
		TotalBalance    string `json:"totalBalance"`
		WalletList      []struct {
			AccountType string `json:"accountType"`
			WalletName  string `json:"walletName"`
			Balance     string `json:"balance"`
		} `json:"walletList"`
	} `json:"data"`
	Success bool `json:"success"`
}

// getFundingAccountBalance 获取资金账户余额（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) getFundingAccountBalance(csrftoken, cookie string) (float64, error) {
	// API调用频率限制
	e.waitForAPIRateLimit()

	log.Printf("🔍 查询资金账户USDT余额")

	req, err := http.NewRequest("GET", "https://www.binance.com/bapi/asset/v3/private/asset-service/wallet/wallet-group?quoteAsset=USDT&needAlphaAsset=true&needEuFuture=true", nil)
	if err != nil {
		return 0, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("accept", "*/*")
	req.Header.Set("clienttype", "web")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)
	req.Header.Set("lang", "zh-CN")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("读取响应失败: %v", err)
	}

	var walletResp WalletGroupResponse
	if err := json.Unmarshal(body, &walletResp); err != nil {
		return 0, fmt.Errorf("解析响应失败: %v", err)
	}

	if walletResp.Code != "000000" {
		return 0, fmt.Errorf("API返回错误: %s", walletResp.Message)
	}

	// 查找资金账户（CARD类型）
	for _, group := range walletResp.Data {
		for _, wallet := range group.WalletList {
			if wallet.AccountType == "CARD" && wallet.WalletName == "资金账户" {
				balance, _ := strconv.ParseFloat(wallet.Balance, 64)
				log.Printf("✅ 找到资金账户USDT余额: %.8f", balance)
				return balance, nil
			}
		}
	}

	return 0, fmt.Errorf("未找到资金账户余额")
}

// OrderRequest 订单请求结构体（基于 flash_trade.go）
type OrderRequest struct {
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	Type        string `json:"type"`
	Quantity    string `json:"quantity"`
	Price       string `json:"price"`
	TimeInForce string `json:"timeInForce"`
	Csrftoken   string `json:"-"`
	Cookie      string `json:"-"`
}

// placeBuyOrder 下买单（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) placeBuyOrder(account *account.UniversalAccount, baseAsset string, price, quantity, amount float64) (string, bool) {
	symbol := baseAsset + "USDT"

	order := OrderRequest{
		Symbol:      symbol,
		Side:        "BUY",
		Type:        "LIMIT",
		Quantity:    fmt.Sprintf("%.0f", quantity), // 数量必须取整
		Price:       fmt.Sprintf("%.12f", price),
		TimeInForce: "GTC",
		Csrftoken:   account.Csrftoken,
		Cookie:      account.Cookie,
	}

	return e.placeOrderWithRetry(order, account.ID)
}

// placeBuyOrderWithRetry 下买单带重试机制（增强版）
func (e *FlashTradeExecutor) placeBuyOrderWithRetry(account *account.UniversalAccount, baseAsset string, price, quantity, amount float64) (string, bool) {
	maxRetries := 8 // 🔧 增加重试次数

	for attempt := 1; attempt <= maxRetries; attempt++ {
		orderID, success := e.placeBuyOrder(account, baseAsset, price, quantity, amount)

		if success {
			if attempt > 1 {
				log.Printf("✅ [%s] 买入重试成功 (第%d次尝试): %s", account.ID, attempt, orderID)
			}
			return orderID, true
		}

		// 特殊错误不重试
		if orderID == "INSUFFICIENT_BALANCE" || orderID == "AUTH_FAILED" {
			log.Printf("❌ [%s] 买入失败，不重试: %s", account.ID, orderID)
			return orderID, false
		}

		if attempt < maxRetries {
			// 🔧 优化延迟策略：前3次快速重试，后续递增延迟
			var delay time.Duration
			if attempt <= 3 {
				delay = 200 * time.Millisecond // 前3次快速重试
			} else {
				delay = time.Duration(attempt*300) * time.Millisecond // 后续递增延迟
			}
			log.Printf("⚠️ [%s] 买入失败，%v后重试 %d/%d", account.ID, delay, attempt, maxRetries)
			time.Sleep(delay)
		}
	}

	log.Printf("❌ [%s] 买入失败，已重试%d次", account.ID, maxRetries)
	return "", false
}

// retryBuyWithNewPrice 重新获取价格重试买入（增强版：支持多次重试）
func (e *FlashTradeExecutor) retryBuyWithNewPrice(account *account.UniversalAccount, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, pricePrecision int, startTime time.Time) FlashTradeResult {
	return e.retryBuyWithNewPriceAttempt(account, tokenAddress, usdtAmount, baseAsset, targetVolume, pricePrecision, startTime, 1)
}

// retryBuyWithNewPriceAttempt 重新获取价格重试买入（带重试计数）
func (e *FlashTradeExecutor) retryBuyWithNewPriceAttempt(account *account.UniversalAccount, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, pricePrecision int, startTime time.Time, retryAttempt int) FlashTradeResult {
	maxBuyRetries := 3 // 🔧 新增：最大买入重试次数

	log.Printf("🔄 [%s] 重新获取价格重试买入 (第%d/%d次)", account.ID, retryAttempt, maxBuyRetries)

	// 重新获取当前价格
	currentPrice, err := e.getCurrentTokenPrice(tokenAddress, pricePrecision)
	if err != nil {
		// 价格获取失败，如果还有重试次数则继续重试
		if retryAttempt < maxBuyRetries {
			log.Printf("⚠️ [%s] 价格获取失败，等待5秒后重试 (%d/%d): %v", account.ID, retryAttempt, maxBuyRetries, err)
			time.Sleep(5 * time.Second)
			return e.retryBuyWithNewPriceAttempt(account, tokenAddress, usdtAmount, baseAsset, targetVolume, pricePrecision, startTime, retryAttempt+1)
		}
		return FlashTradeResult{
			Success: false,
			Message: "重新获取价格失败: " + err.Error(),
		}
	}

	log.Printf("🔍 [%s] 重新获取到的价格: %.*f", account.ID, pricePrecision, currentPrice)

	// 买单价格加万一提高成交率
	buyPrice := currentPrice * 1.0001
	buyPrice = e.adjustPricePrecision(buyPrice, pricePrecision)

	// 重新计算数量和金额
	// 🎲 重试时也添加随机浮动
	retryRandomFactor := 0.95 + rand.Float64()*0.1
	adjustedRetryUSDTAmount := usdtAmount * retryRandomFactor

	log.Printf("🎲 [%s] Master重试买入量随机化: 原始%.6f USDT → 调整%.6f USDT (浮动%.2f%%)",
		account.ID, usdtAmount, adjustedRetryUSDTAmount, (retryRandomFactor-1)*100)

	idealTokenAmount := adjustedRetryUSDTAmount / buyPrice
	tokenAmount := float64(int(idealTokenAmount))
	calculatedAmount := buyPrice * tokenAmount
	exactAmount := math.Round(calculatedAmount*100000000) / 100000000

	// 🔧 记录重试时实际使用的USDT金额
	retryActualUSDTUsed := exactAmount

	if exactAmount <= 0 || tokenAmount <= 0 {
		return FlashTradeResult{
			Success: false,
			Message: "重新计算后金额为0，无法下单",
		}
	}

	// 重试买入
	buyOrderID, buySuccess := e.placeBuyOrderWithRetry(account, baseAsset, buyPrice, tokenAmount, exactAmount)
	if !buySuccess {
		return FlashTradeResult{
			Success: false,
			Message: "重新获取价格后买入仍然失败",
		}
	}

	// 等待买入确认
	buyConfirmed := e.waitForBuyConfirmation(buyOrderID, account.Csrftoken, account.Cookie)
	if !buyConfirmed {
		e.cancelOrder(buyOrderID, baseAsset+"USDT", account.Csrftoken, account.Cookie)

		// 🔧 新增：如果还有重试次数，继续重试
		if retryAttempt < maxBuyRetries {
			log.Printf("🔄 [%s] 重试买入超时，等待10秒后进行下一次重试 (%d/%d)", account.ID, retryAttempt, maxBuyRetries)
			time.Sleep(10 * time.Second)
			return e.retryBuyWithNewPriceAttempt(account, tokenAddress, usdtAmount, baseAsset, targetVolume, pricePrecision, startTime, retryAttempt+1)
		}

		return FlashTradeResult{
			Success: false,
			Message: fmt.Sprintf("重试买入超时，已重试%d次", maxBuyRetries),
		}
	}

	log.Printf("✅ [%s] 重试买入确认成功", account.ID)

	// 🔧 新增：Master节点重试买入成功后立即统计买入交易额（使用实际金额）
	buyInCost := retryActualUSDTUsed // 🎲 使用重试时实际花费的USDT金额
	if buyInCost > 0 && tokenAmount > 0 {
		// 生成唯一交易ID防止重复统计
		tradeID := e.generateTradeID(account.ID, tokenAddress, buyPrice, tokenAmount)
		// 只统计买入交易额，损益为0（因为还没卖出）
		e.server.statsManager.UpdateAccountStatsWithID(account.ID, buyInCost, 0, tradeID)
		log.Printf("📊 [%s] Master重试买入成功立即统计 - 实际买单金额: %.6f USDT (原始%.6f, 浮动%.2f%%) (ID:%s)",
			account.ID, buyInCost, usdtAmount, (buyInCost/usdtAmount-1)*100, tradeID[:8])
	}

	// 执行智能卖出
	return e.smartSellWithRetry(account, tokenAddress, baseAsset, buyPrice, tokenAmount, pricePrecision, startTime)
}

// placeOrderWithRetry 下单带重试机制（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) placeOrderWithRetry(order OrderRequest, accountID string) (string, bool) {
	maxRetries := 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		orderID, success := e.placeOrderWithID(order)

		if success {
			if attempt > 1 {
				log.Printf("✅ [%s] 订单重试成功 (第%d次尝试): %s", accountID, attempt, orderID)
			}
			return orderID, true
		}

		// 特殊错误不重试
		if orderID == "INSUFFICIENT_BALANCE" || orderID == "AUTH_FAILED" {
			log.Printf("❌ [%s] 订单失败，不重试: %s", accountID, orderID)
			return orderID, false
		}

		if attempt < maxRetries {
			log.Printf("⚠️ [%s] 订单失败，重试 %d/%d", accountID, attempt, maxRetries)
			time.Sleep(50 * time.Millisecond) // 50ms间隔重试
		}
	}

	log.Printf("❌ [%s] 订单失败，已重试%d次", accountID, maxRetries)
	return "", false
}

// placeOrderWithID 下单并返回订单ID（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) placeOrderWithID(order OrderRequest) (string, bool) {
	// API调用频率限制
	e.waitForAPIRateLimit()

	jsonData, _ := json.Marshal(order)

	req, err := http.NewRequest("POST", "https://www.binance.com/bapi/asset/v1/private/alpha-trade/order/place", bytes.NewReader(jsonData))
	if err != nil {
		return "", false
	}

	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("clienttype", "web")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", order.Csrftoken)
	req.Header.Set("cookie", order.Cookie)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// 解析响应获取订单ID
	var orderResp map[string]interface{}
	if err := json.Unmarshal(body, &orderResp); err != nil {
		return "", resp.StatusCode == 200
	}

	if resp.StatusCode == 200 {
		// 检查是否有错误码
		if code, exists := orderResp["code"]; exists {
			if code != "000000" && code != 0 {
				// 特殊错误处理
				if code == "481020" {
					// 余额不足直接返回特殊标识，避免无意义重试
					return "INSUFFICIENT_BALANCE", false
				}

				// 检查认证失效
				if strings.Contains(fmt.Sprintf("%v", orderResp["message"]), "请检查是否已登录") ||
					strings.Contains(fmt.Sprintf("%v", orderResp["message"]), "请求失败") {
					log.Printf("🚨 认证失效，停止下单")
					return "AUTH_FAILED", false
				}

				return "", false
			}
		}

		// 尝试多种方式获取订单ID
		orderID := e.extractOrderID(orderResp)
		if orderID != "" {
			return orderID, true
		}
		return "unknown", true
	}

	return "", false
}

// extractOrderID 从响应中提取订单ID（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) extractOrderID(orderResp map[string]interface{}) string {
	// 方式1: data字段直接是订单ID (数字)
	if orderID, ok := orderResp["data"].(float64); ok {
		result := fmt.Sprintf("%.0f", orderID)
		return result
	}

	// 方式2: data字段直接是订单ID (字符串)
	if orderID, ok := orderResp["data"].(string); ok {
		return orderID
	}

	return ""
}

// waitForBuyConfirmation 等待买入确认（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) waitForBuyConfirmation(orderID, csrftoken, cookie string) bool {
	// 等待买入确认，循环检查历史订单
	buyTime := time.Now().UnixMilli()

	for i := 0; i < 20; i++ { // 1秒，每50ms检查一次
		time.Sleep(50 * time.Millisecond)

		if e.checkBuyOrderConfirmed(orderID, buyTime, csrftoken, cookie) {
			return true
		}
	}
	return false
}

// checkBuyOrderConfirmed 检查指定买单是否已确认成交（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) checkBuyOrderConfirmed(buyOrderID string, buyTime int64, csrftoken, cookie string) bool {
	// API调用频率限制
	e.waitForAPIRateLimit()

	now := time.Now().UnixMilli()
	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=20&startTime=%d&endTime=%d",
		buyTime-5000, now) // 查询买入时间前后的订单

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}

	// 检查响应状态
	if code, exists := response["code"]; exists && code != "000000" && code != 0 {
		return false
	}

	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return false
	}

	rows, ok := data["rows"].([]interface{})
	if !ok {
		return false
	}

	// 查找对应的买单
	for _, row := range rows {
		order, ok := row.(map[string]interface{})
		if !ok {
			continue
		}

		// 检查订单ID
		if orderIDFloat, ok := order["orderId"].(float64); ok {
			if fmt.Sprintf("%.0f", orderIDFloat) == buyOrderID {
				// 检查订单状态
				if status, ok := order["status"].(string); ok && status == "FILLED" {
					return true
				}
			}
		}
	}

	return false
}

// cancelOrder 取消订单（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) cancelOrder(orderID, symbol, csrftoken, cookie string) bool {
	// API调用频率限制
	e.waitForAPIRateLimit()

	payload := fmt.Sprintf(`{"orderId":"%s","symbol":"%s"}`, orderID, symbol)

	req, err := http.NewRequest("POST", "https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/cancel", strings.NewReader(payload))
	if err != nil {
		return false
	}

	req.Header.Set("clienttype", "web")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}

	// 检查响应码
	if code, exists := response["code"]; exists {
		if code == "000000" || code == 0 {
			// 确认订单真的被取消了
			return e.checkOrderCanceled(orderID, csrftoken, cookie)
		}
	}

	return false
}

// checkOrderCanceled 确认订单是否真的被取消了（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) checkOrderCanceled(orderID, csrftoken, cookie string) bool {
	// 等待一小段时间让取消操作生效
	time.Sleep(100 * time.Millisecond)

	// 查询订单状态，如果订单不存在或状态为CANCELED则认为取消成功
	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=20&startTime=%d&endTime=%d",
		time.Now().UnixMilli()-300000, time.Now().UnixMilli()) // 查询最近5分钟的订单

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	req.Header.Set("accept", "*/*")
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}

	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return true // 如果查询失败，假设取消成功
	}

	rows, ok := data["rows"].([]interface{})
	if !ok {
		return true
	}

	// 查找对应的订单
	for _, row := range rows {
		order, ok := row.(map[string]interface{})
		if !ok {
			continue
		}

		if orderIDFloat, ok := order["orderId"].(float64); ok {
			if fmt.Sprintf("%.0f", orderIDFloat) == orderID {
				if status, ok := order["status"].(string); ok {
					return status == "CANCELED" || status == "EXPIRED"
				}
			}
		}
	}

	return true // 如果找不到订单，认为已被取消
}

// waitForAPIRateLimit 等待API调用频率限制（基于 flash_trade.go 实现）
var (
	lastAPICall     time.Time
	apiCallInterval = 300 * time.Millisecond // 🚨 风控优化：API调用间隔300ms，避免风控
	apiCallMutex    sync.Mutex
)

func (e *FlashTradeExecutor) waitForAPIRateLimit() {
	apiCallMutex.Lock()
	defer apiCallMutex.Unlock()

	timeSinceLastCall := time.Since(lastAPICall)
	if timeSinceLastCall < apiCallInterval {
		time.Sleep(apiCallInterval - timeSinceLastCall)
	}
	lastAPICall = time.Now()
}

// smartSellWithRetry 智能卖出重试机制（基于 flash_trade.go 高级实现）
func (e *FlashTradeExecutor) smartSellWithRetry(account *account.UniversalAccount, tokenAddress, baseAsset string, buyPrice, tokenAmount float64, pricePrecision int, startTime time.Time) FlashTradeResult {
	maxSellRetries := 5

	for sellAttempt := 1; sellAttempt <= maxSellRetries; sellAttempt++ {
		log.Printf("🔄 [%s] 开始第%d次卖出尝试", account.ID, sellAttempt)

		// 获取当前市场价格
		currentMarketPrice, err := e.getCurrentTokenPrice(tokenAddress, pricePrecision)
		if err != nil {
			log.Printf("❌ [%s] 获取当前价格失败: %v", account.ID, err)
			if sellAttempt < maxSellRetries {
				time.Sleep(time.Duration(sellAttempt*200) * time.Millisecond)
				continue
			}
			return FlashTradeResult{
				Success: false,
				Message: "获取当前价格失败",
			}
		}
		currentMarketPrice = e.adjustPricePrecision(currentMarketPrice, pricePrecision)

		var sellPrice float64
		var strategy string

		// 智能卖出策略
		if currentMarketPrice > buyPrice {
			// 情况1: 当前价格 > 买入价格，按当前价格减万1卖出
			sellPrice = currentMarketPrice * 0.9999 // 减万1
			sellPrice = e.adjustPricePrecision(sellPrice, pricePrecision)
			profit := (sellPrice - buyPrice) / buyPrice * 10000
			strategy = fmt.Sprintf("市场价-万1, 利润: %.2f万分", profit)
		} else if currentMarketPrice < buyPrice {
			// 情况2: 当前价格 < 买入价格，判断磨损
			loss := (buyPrice - currentMarketPrice) / buyPrice
			lossWanFen := loss * 10000

			if loss > 0.005 { // 磨损 > 千5
				// 挂千5磨损价格
				sellPrice = buyPrice * 0.995 // 千5磨损
				sellPrice = e.adjustPricePrecision(sellPrice, pricePrecision)
				strategy = fmt.Sprintf("千5挂单, 磨损: 50万分 (市场磨损: %.2f万分)", lossWanFen)
			} else {
				// 磨损 ≤ 千5，直接按当前价格卖
				sellPrice = currentMarketPrice
				strategy = fmt.Sprintf("市场价卖出, 磨损: %.2f万分", lossWanFen)
			}
		} else {
			// 情况3: 当前价格 = 买入价格，按万一磨损卖出
			sellPrice = buyPrice * 0.9999 // 万一磨损
			sellPrice = e.adjustPricePrecision(sellPrice, pricePrecision)
			strategy = "万一磨损, 磨损: 1万分"
		}

		log.Printf("💰 [%s] 卖出策略 (尝试%d): %s, 价格: %.12f", account.ID, sellAttempt, strategy, sellPrice)

		// 执行卖出
		sellOrderID, sellSuccess := e.placeSellOrderWithRetry(account, baseAsset, sellPrice, tokenAmount)
		if sellSuccess {
			// 等待卖出确认
			sellConfirmed := e.waitForSellConfirmation(sellOrderID, account.Csrftoken, account.Cookie)
			if sellConfirmed {
				// 计算利润
				profit := (sellPrice - buyPrice) * tokenAmount
				executeTime := time.Since(startTime).Milliseconds()

				log.Printf("🎉 [%s] Flash Trade 完成: 买入价 %.8f, 卖出价 %.8f, 利润 %.8f",
					account.ID, buyPrice, sellPrice, profit)

				return FlashTradeResult{
					Success:     true,
					Message:     "Flash Trade 完成",
					BuyPrice:    buyPrice,
					SellPrice:   sellPrice,
					TokenAmount: tokenAmount,
					Profit:      profit,
					ExecuteTime: executeTime,
				}
			} else {
				// 卖出确认失败，取消订单并重试
				log.Printf("⚠️ [%s] 卖出确认失败，取消订单并重试", account.ID)
				e.cancelOrder(sellOrderID, baseAsset+"USDT", account.Csrftoken, account.Cookie)
			}
		}

		// 如果不是最后一次尝试，等待一段时间再重试
		if sellAttempt < maxSellRetries {
			waitTime := time.Duration(sellAttempt*500) * time.Millisecond
			log.Printf("⏳ [%s] 卖出失败，等待%v后重试", account.ID, waitTime)
			time.Sleep(waitTime)
		}
	}

	// 所有卖出尝试都失败了
	return FlashTradeResult{
		Success: false,
		Message: fmt.Sprintf("卖出失败，已重试%d次", maxSellRetries),
	}
}

// placeSellOrder 下卖单（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) placeSellOrder(account *account.UniversalAccount, baseAsset string, price, quantity float64) (string, bool) {
	symbol := baseAsset + "USDT"

	order := OrderRequest{
		Symbol:      symbol,
		Side:        "SELL",
		Type:        "LIMIT",
		Quantity:    fmt.Sprintf("%.0f", quantity), // 数量必须取整
		Price:       fmt.Sprintf("%.12f", price),
		TimeInForce: "GTC",
		Csrftoken:   account.Csrftoken,
		Cookie:      account.Cookie,
	}

	return e.placeOrderWithRetry(order, account.ID)
}

// placeSellOrderWithRetry 下卖单带重试机制（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) placeSellOrderWithRetry(account *account.UniversalAccount, baseAsset string, price, quantity float64) (string, bool) {
	maxRetries := 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		orderID, success := e.placeSellOrder(account, baseAsset, price, quantity)

		if success {
			if attempt > 1 {
				log.Printf("✅ [%s] 卖出重试成功 (第%d次尝试): %s", account.ID, attempt, orderID)
			}
			return orderID, true
		}

		// 特殊错误不重试
		if orderID == "AUTH_FAILED" {
			log.Printf("❌ [%s] 卖出失败，不重试: %s", account.ID, orderID)
			return orderID, false
		}

		if attempt < maxRetries {
			log.Printf("⚠️ [%s] 卖出失败，重试 %d/%d", account.ID, attempt, maxRetries)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond) // 递增延迟
		}
	}

	log.Printf("❌ [%s] 卖出失败，已重试%d次", account.ID, maxRetries)
	return "", false
}

// waitForSellConfirmation 等待卖出确认（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) waitForSellConfirmation(orderID, csrftoken, cookie string) bool {
	// 等待卖出确认，循环检查历史订单
	sellTime := time.Now().UnixMilli()

	for i := 0; i < 20; i++ { // 1秒，每50ms检查一次
		time.Sleep(50 * time.Millisecond)

		if e.checkSellOrderConfirmed(orderID, sellTime, csrftoken, cookie) {
			return true
		}
	}
	return false
}

// checkSellOrderConfirmed 检查指定卖单是否已确认成交（基于 flash_trade.go 实现）
func (e *FlashTradeExecutor) checkSellOrderConfirmed(sellOrderID string, sellTime int64, csrftoken, cookie string) bool {
	// API调用频率限制
	e.waitForAPIRateLimit()

	now := time.Now().UnixMilli()
	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=20&startTime=%d&endTime=%d",
		sellTime-5000, now) // 查询卖出时间前后的订单

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}

	// 检查响应状态
	if code, exists := response["code"]; exists && code != "000000" && code != 0 {
		return false
	}

	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return false
	}

	rows, ok := data["rows"].([]interface{})
	if !ok {
		return false
	}

	// 查找对应的卖单
	for _, row := range rows {
		order, ok := row.(map[string]interface{})
		if !ok {
			continue
		}

		// 检查订单ID
		if orderIDFloat, ok := order["orderId"].(float64); ok {
			if fmt.Sprintf("%.0f", orderIDFloat) == sellOrderID {
				// 检查订单状态
				if status, ok := order["status"].(string); ok && status == "FILLED" {
					return true
				}
			}
		}
	}

	return false
}

// 注意：executeRealFlashTrade 方法已删除，现在通过被控端调用 flash_trade.go 接口

// 注意：AutoSellExecutor 已移动到 internal/task/auto_sell_executor.go

// BatchTradeExecutor 批量交易任务执行器
type BatchTradeExecutor struct {
	server *Server
}

func (e *BatchTradeExecutor) Execute(task *task.UniversalTask, accountManager *account.UniversalAccountManager) error {
	log.Printf("📦 执行批量交易任务: %s", task.ID)
	// 这里应该实现具体的批量交易逻辑
	return nil
}

func (e *BatchTradeExecutor) Validate(parameters map[string]interface{}) error {
	// 验证批量交易参数
	return nil
}

func (e *BatchTradeExecutor) GetDefaultParameters() map[string]interface{} {
	return map[string]interface{}{
		"batch_size":    10,
		"delay_between": 1000,
		"max_retries":   3,
	}
}

// syncAccountsToNode 同步账号数据到节点
func (s *Server) syncAccountsToNode(nodeID string) {
	// 检查节点是否已授权
	if !s.isNodeAuthorized(nodeID) {
		log.Printf("⚠️ 节点 %s 未授权，跳过账号同步", nodeID)
		return
	}

	log.Printf("🔄 开始同步账号数据到已授权节点: %s", nodeID)

	// 获取分配给该节点的账号
	accounts := s.accountManager.GetNodeAccounts(nodeID)
	if len(accounts) == 0 {
		log.Printf("📭 节点 %s 暂无分配的账号", nodeID)
		return
	}

	// 通过 Redis 发送账号同步命令
	for _, account := range accounts {
		command := map[string]interface{}{
			"command_type": "sync_account",
			"command_id":   fmt.Sprintf("sync_%s_%d", account.ID, time.Now().Unix()),
			"target_node":  nodeID,
			"payload": map[string]interface{}{
				"action":  "add",
				"account": account,
			},
		}

		if err := s.redisClient.PublishCommand("account_sync", command); err != nil {
			log.Printf("❌ 发送账号同步命令失败: %v", err)
		} else {
			log.Printf("📤 已发送账号 %s 同步命令到节点 %s", account.ID, nodeID)
		}
	}
}

// SyncAccountToNode 同步单个账号到指定节点
func (s *Server) SyncAccountToNode(accountID, nodeID string) {
	// 检查节点是否已授权
	if !s.isNodeAuthorized(nodeID) {
		log.Printf("⚠️ 节点 %s 未授权，跳过账号同步", nodeID)
		return
	}

	// 获取账号信息
	account, err := s.accountManager.GetAccount(accountID)
	if err != nil {
		log.Printf("❌ 获取账号信息失败 %s: %v", accountID, err)
		return
	}

	log.Printf("🔄 开始同步账号 %s 到节点 %s", accountID, nodeID)

	// 通过 Redis 发送账号同步命令
	command := map[string]interface{}{
		"command_type": "sync_account",
		"command_id":   fmt.Sprintf("sync_%s_%d", accountID, time.Now().Unix()),
		"target_node":  nodeID,
		"payload": map[string]interface{}{
			"action":  "update",
			"account": account,
		},
	}

	if err := s.redisClient.PublishCommand("account_sync", command); err != nil {
		log.Printf("❌ 发送账号同步命令失败: %v", err)
	} else {
		log.Printf("📤 已发送账号 %s 更新同步命令到节点 %s", accountID, nodeID)
	}
}

// RemoveAccountFromNode 从指定节点删除账号
func (s *Server) RemoveAccountFromNode(accountID, nodeID string) {
	// 检查节点是否已授权
	if !s.isNodeAuthorized(nodeID) {
		log.Printf("⚠️ 节点 %s 未授权，跳过账号删除", nodeID)
		return
	}

	log.Printf("🗑️ 开始从节点 %s 删除账号 %s", nodeID, accountID)

	// 通过 Redis 发送账号删除命令
	command := map[string]interface{}{
		"command_type": "sync_account",
		"command_id":   fmt.Sprintf("remove_%s_%d", accountID, time.Now().Unix()),
		"target_node":  nodeID,
		"payload": map[string]interface{}{
			"action":     "delete",
			"account_id": accountID,
		},
	}

	if err := s.redisClient.PublishCommand("account_sync", command); err != nil {
		log.Printf("❌ 发送账号删除命令失败: %v", err)
	} else {
		log.Printf("📤 已发送账号 %s 删除命令到节点 %s", accountID, nodeID)
	}
}

// isNodeAuthorized 检查节点是否已授权（内部方法）
func (s *Server) isNodeAuthorized(nodeID string) bool {
	// 如果使用MongoDB授权
	if s.useMongoAuth && s.mongoAuthManager != nil {
		// 首先检查node_auth集合
		if s.mongoAuthManager.IsNodeAuthorized(nodeID) {
			return true
		}
		
		// 如果node_auth集合中未授权，检查nodes集合
		return s.checkNodesCollectionAuth(nodeID)
	}
	
	// 否则使用Redis授权
	ctx := context.Background()
	key := fmt.Sprintf("node_auth:%s", nodeID)

	result, err := s.redisClient.GetNativeClient().Get(ctx, key).Result()
	if err != nil {
		return false // 默认未授权
	}

	return result == "authorized"
}

// checkNodesCollectionAuth 检查nodes集合中的授权状态
func (s *Server) checkNodesCollectionAuth(nodeID string) bool {
	if s.mongoAuthManager == nil || s.mongoAuthManager.GetClient() == nil {
		log.Printf("🔍 主控端授权检查: 节点ID=%s, MongoDB客户端未初始化", nodeID)
		return false
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取nodes集合
	collection := s.mongoAuthManager.GetClient().Database("alpha").Collection("nodes")
	
	// 查询节点信息
	var nodeInfo struct {
		IsAuthorized int `bson:"is_authorized"`
	}
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&nodeInfo)
	if err != nil {
		log.Printf("🔍 主控端授权检查: 节点ID=%s, 查询失败: %v", nodeID, err)
		return false
	}
	
	// 输出授权状态
	authorized := nodeInfo.IsAuthorized == 1
	log.Printf("🔍 主控端授权检查: 节点ID=%s, 授权状态=%v (is_authorized=%d)", 
		nodeID, authorized, nodeInfo.IsAuthorized)
	
	// 返回授权状态
	return authorized
}

// IsNodeAuthorized 检查节点是否已授权（公开方法）
func (s *Server) IsNodeAuthorized(nodeID string) bool {
	return s.isNodeAuthorized(nodeID)
}

// AuthorizeNode 授权节点
func (s *Server) AuthorizeNode(nodeID string) error {
	// 如果使用MongoDB授权
	if s.useMongoAuth && s.mongoAuthManager != nil {
		// 创建节点元数据
		metadata := map[string]interface{}{
			"authorized_time": time.Now().Format(time.RFC3339),
		}
		
		// 调用MongoDB授权管理器
		err := s.mongoAuthManager.AuthorizeNode(nodeID, metadata)
		if err != nil {
			return fmt.Errorf("MongoDB授权节点失败: %v", err)
		}
		
		log.Printf("✅ 节点 %s 已通过MongoDB授权", nodeID)
	} else {
		// 使用Redis授权
		ctx := context.Background()
		key := fmt.Sprintf("node_auth:%s", nodeID)

		err := s.redisClient.GetNativeClient().Set(ctx, key, "authorized", 0).Err()
		if err != nil {
			return fmt.Errorf("授权节点失败: %v", err)
		}

		log.Printf("✅ 节点 %s 已通过Redis授权", nodeID)
	}

	// 授权后立即同步账号数据
	go s.syncAccountsToNode(nodeID)

	return nil
}

// RevokeNodeAuthorization 撤销节点授权
func (s *Server) RevokeNodeAuthorization(nodeID string) error {
	// 如果使用MongoDB授权
	if s.useMongoAuth && s.mongoAuthManager != nil {
		// 调用MongoDB授权管理器
		err := s.mongoAuthManager.RevokeNodeAuthorization(nodeID)
		if err != nil {
			return fmt.Errorf("MongoDB撤销节点授权失败: %v", err)
		}
		
		log.Printf("⚠️ 节点 %s 授权已通过MongoDB撤销", nodeID)
	} else {
		// 使用Redis授权
		ctx := context.Background()
		key := fmt.Sprintf("node_auth:%s", nodeID)

		err := s.redisClient.GetNativeClient().Del(ctx, key).Err()
		if err != nil {
			return fmt.Errorf("撤销节点授权失败: %v", err)
		}

		log.Printf("⚠️ 节点 %s 授权已通过Redis撤销", nodeID)
	}

	// 撤销授权后，清理该节点的所有账号
	go s.cleanupNodeAccounts(nodeID)

	return nil
}

// GetAuthorizedNodes 获取所有已授权的节点
func (s *Server) GetAuthorizedNodes() ([]string, error) {
	// 如果使用MongoDB授权
	if s.useMongoAuth && s.mongoAuthManager != nil {
		// 调用MongoDB授权管理器
		nodes, err := s.mongoAuthManager.GetAuthorizedNodes()
		if err != nil {
			return nil, fmt.Errorf("MongoDB获取授权节点失败: %v", err)
		}
		
		// 提取节点ID
		var nodeIDs []string
		for _, node := range nodes {
			nodeIDs = append(nodeIDs, node.NodeID)
		}
		
		return nodeIDs, nil
	}
	
	// 使用Redis授权
	ctx := context.Background()
	pattern := "node_auth:*"

	keys, err := s.redisClient.GetNativeClient().Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("获取授权节点失败: %v", err)
	}

	var authorizedNodes []string
	for _, key := range keys {
		nodeID := strings.TrimPrefix(key, "node_auth:")
		
		// 检查是否已授权
		result, err := s.redisClient.GetNativeClient().Get(ctx, key).Result()
		if err == nil && result == "authorized" {
			authorizedNodes = append(authorizedNodes, nodeID)
		}
	}

	return authorizedNodes, nil
}

// cleanupNodeAccounts 清理节点的所有账号
func (s *Server) cleanupNodeAccounts(nodeID string) {
	log.Printf("🧹 开始清理节点 %s 的账号数据", nodeID)

	// 获取分配给该节点的所有账号
	accounts := s.accountManager.GetNodeAccounts(nodeID)
	if len(accounts) == 0 {
		log.Printf("📭 节点 %s 没有分配的账号", nodeID)
		return
	}

	log.Printf("🗑️ 从节点 %s 清理 %d 个账号", nodeID, len(accounts))

	// 从节点删除所有账号
	for _, account := range accounts {
		// 发送删除命令到节点
		s.RemoveAccountFromNode(account.ID, nodeID)

		// 从账号管理器中取消节点分配
		account.AssignedNode = ""
		s.accountManager.UpdateAccount(account.ID, account)
	}

	log.Printf("✅ 节点 %s 的账号清理完成", nodeID)
}

// ===== gRPC 服务实现 =====

// RegisterNode 注册节点 (gRPC)
func (s *Server) RegisterNode(ctx context.Context, req *proto.NodeRegisterRequest) (*proto.NodeRegisterResponse, error) {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	nodeID := req.NodeId
	if nodeID == "" {
		return &proto.NodeRegisterResponse{
			Success: false,
			Message: "节点ID不能为空",
		}, nil
	}

	// 🔧 修复：先尝试从Redis恢复节点信息
	var friendlyName string
	var nodeInfo *NodeInfo

	// 尝试从Redis加载已保存的节点信息
	if existingNode, err := s.loadNodeFromRedis(nodeID); err == nil && existingNode != nil {
		// 找到已保存的节点信息，恢复友好名称
		friendlyName = existingNode.FriendlyName
		log.Printf("🔄 恢复节点 %s 的保存名称: %s", nodeID, friendlyName)

		// 更新节点信息，保持原有的友好名称
		nodeInfo = &NodeInfo{
			NodeID:        nodeID,
			Address:       req.HttpAddr,
			Status:        "online",
			LastHeartbeat: time.Now(),
			ActiveTasks:   []string{},
			Metadata:      req.Metadata,
			FriendlyName:  friendlyName, // 使用恢复的名称
		}
	} else {
		// 没有找到已保存的信息，生成新的友好名称
		s.nodeCounter++
		friendlyName = fmt.Sprintf("节点%d", s.nodeCounter)
		log.Printf("🆕 为新节点 %s 生成名称: %s", nodeID, friendlyName)

		// 提取用户名
		username := "unknown"
		if req.Metadata != nil {
			if u, ok := req.Metadata["username"]; ok && u != "" {
				username = u
			}
		}

		// 创建新的节点信息
		nodeInfo = &NodeInfo{
			NodeID:        nodeID,
			Address:       req.HttpAddr,
			Status:        "online",
			LastHeartbeat: time.Now(),
			ActiveTasks:   []string{},
			Metadata:      req.Metadata,
			FriendlyName:  friendlyName,
			Username:      username,
		}
	}

	s.nodes[nodeID] = nodeInfo

	// 保存到Redis
	if err := s.saveNodeToRedis(nodeID, nodeInfo); err != nil {
		log.Printf("⚠️ 保存节点信息到Redis失败: %v", err)
	}

	log.Printf("✅ gRPC 节点注册成功: %s (%s), 地址: %s", nodeID, friendlyName, req.HttpAddr)
	log.Printf("🔍 节点信息: %+v", nodeInfo)

	// 同步账号数据到新节点
	go s.syncAccountsToNode(nodeID)

	return &proto.NodeRegisterResponse{
		Success:        true,
		Message:        "节点注册成功",
		AssignedNodeId: friendlyName,
	}, nil
}

// Heartbeat 心跳检测 (gRPC)
func (s *Server) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	nodeID := req.NodeId
	if node, exists := s.nodes[nodeID]; exists {
		wasOffline := node.Status == "offline"
		now := time.Now()

		// 🔧 优化：更新心跳时间
		node.LastHeartbeat = now

		// 🔧 优化：只有在真正离线时才改变状态，避免频繁状态切换
		if wasOffline {
			node.Status = "online"
			log.Printf("✅ 节点 %s (%s) 已重新上线", nodeID, node.FriendlyName)

			// 保存状态变更到Redis
			if err := s.saveNodeToRedis(nodeID, node); err != nil {
				log.Printf("⚠️ 保存节点在线状态到Redis失败: %v", err)
			}
		} else if node.Status != "online" {
			// 确保状态为在线
			node.Status = "online"
		}

		return &proto.HeartbeatResponse{
			Success: true,
			Message: "心跳正常",
		}, nil
	}

	// 🔧 优化：未注册的节点，提供更详细的错误信息
	log.Printf("⚠️ 收到未注册节点的心跳: %s", nodeID)
	return &proto.HeartbeatResponse{
		Success: false,
		Message: fmt.Sprintf("节点 %s 未注册，请先注册节点", nodeID),
	}, nil
}

// StartFlashTrade 启动Flash Trade (gRPC)
func (s *Server) StartFlashTrade(ctx context.Context, req *proto.FlashTradeRequest) (*proto.FlashTradeResponse, error) {
	// 创建Flash Trade任务
	taskID := fmt.Sprintf("flash-trade-%d", time.Now().UnixNano())

	flashTask := &FlashTaskInfo{
		TaskID:         taskID,
		TokenAddress:   req.TokenAddress,
		USDTAmount:     req.UsdtAmount,
		BaseAsset:      req.BaseAsset,
		TargetVolume:   req.TargetVolume,
		AutoLoop:       req.AutoLoop,
		PricePrecision: int(req.PricePrecision),
		ChainID:        req.ChainId,        // 🔧 支持多链
		PriceMode:      req.PriceMode,      // 🔧 支持价格模式
		// SpeedMode字段已移除
		Status:         "created",
		StartTime:      time.Now(),
		TargetNodes:    []string{req.NodeId},
		TargetAccounts: []string{req.AccountId},
		Results:        make(map[string]interface{}),
	}

	s.flashMutex.Lock()
	s.flashTasks[taskID] = flashTask
	s.flashMutex.Unlock()

	log.Printf("🔥 Flash Trade 任务创建: %s", taskID)

	// 🔧 修复：正确处理目标账号列表
	var targetAccounts []string
	if req.AccountId != "" {
		targetAccounts = []string{req.AccountId}
	} else {
		targetAccounts = []string{} // 空数组表示所有账号
	}

	// 发送命令给被控端（按照被控端期望的格式）
	command := map[string]interface{}{
		"command_id":   taskID,
		"command_type": "flash_trade",
		"target_node":  req.NodeId,
		"payload": map[string]interface{}{
			"action":          "start",
			"task_id":         taskID,
			"token_address":   req.TokenAddress,
			"usdt_amount":     req.UsdtAmount,
			"base_asset":      req.BaseAsset,
			"target_volume":   req.TargetVolume,
			"auto_loop":       req.AutoLoop,
			"price_precision": req.PricePrecision,
			"chain_id":        req.ChainId,    // 🔧 新增：传递链ID
			"price_mode":      req.PriceMode,  // 🔧 新增：传递价格模式
			"target_accounts": targetAccounts, // 正确的账号列表
		},
	}

	// 🔧 优化：带确认机制的指令下发
	log.Printf("🔧 [调试] 准备发送Flash Trade命令到Redis频道 'flash_commands':")
	log.Printf("   命令ID: %s", taskID)
	log.Printf("   目标节点: %s", req.NodeId)
	log.Printf("   命令内容: %+v", command)

	// 发送指令到Redis
	if err := s.redisClient.PublishCommand("flash_commands", command); err != nil {
		log.Printf("❌ 发送 Flash Trade 命令失败: %v", err)
		return &proto.FlashTradeResponse{
			Success: false,
			Message: "发送命令失败: " + err.Error(),
		}, err
	}

	log.Printf("📤 Flash Trade 命令已发送到被控端: %s (频道: flash_commands)", taskID)

	// 🔧 新增：等待被控端确认接收（可选，提高可靠性）
	if req.NodeId != "" {
		// 指定节点时，等待确认
		go s.waitForFlashCommandAck(taskID, req.NodeId, 10*time.Second)
	} else {
		// 广播时，等待所有在线节点确认
		go s.waitForFlashCommandAckFromAllNodes(taskID, 10*time.Second)
	}

	return &proto.FlashTradeResponse{
		Success:          true,
		Message:          "Flash Trade 启动成功，命令已发送",
		TaskId:           taskID,
		AffectedNodes:    []string{req.NodeId},
		AffectedAccounts: []string{req.AccountId},
	}, nil
}

// StopFlashTrade 停止Flash Trade (gRPC)
func (s *Server) StopFlashTrade(ctx context.Context, req *proto.StopFlashTradeRequest) (*proto.StopFlashTradeResponse, error) {
	s.flashMutex.Lock()
	defer s.flashMutex.Unlock()

	if task, exists := s.flashTasks[req.TaskId]; exists {
		task.Status = "stopped"
		now := time.Now()
		task.EndTime = &now

		log.Printf("🛑 Flash Trade 任务停止: %s", req.TaskId)

		return &proto.StopFlashTradeResponse{
			Success:      true,
			Message:      "Flash Trade 停止成功",
			StoppedCount: 1,
		}, nil
	}

	return &proto.StopFlashTradeResponse{
		Success: false,
		Message: "任务不存在",
	}, nil
}

// GetFlashTradeStats 获取Flash Trade统计 (gRPC)
func (s *Server) GetFlashTradeStats(ctx context.Context, req *proto.FlashTradeStatsRequest) (*proto.FlashTradeStatsResponse, error) {
	// 返回基本统计信息
	return &proto.FlashTradeStatsResponse{
		Success:      true,
		GlobalStats:  &proto.FlashTradeGlobalStats{},
		AccountStats: []*proto.FlashTradeAccountStats{},
		NodeStats:    []*proto.FlashTradeNodeStats{},
	}, nil
}

// ===== 节点管理功能 =====

// RegisterNodeSimple 简化的节点注册
func (s *Server) RegisterNodeSimple(nodeID, address, friendlyName string, metadata map[string]string) error {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	if nodeID == "" {
		return fmt.Errorf("节点ID不能为空")
	}

	// 🔧 修复：先尝试从Redis恢复节点信息
	var finalFriendlyName string
	var nodeInfo *NodeInfo

	// 提取用户名
	username := "unknown"
	if metadata != nil {
		if u, ok := metadata["username"]; ok && u != "" {
			username = u
		}
	}

	// 尝试从Redis加载已保存的节点信息
	if existingNode, err := s.loadNodeFromRedis(nodeID); err == nil && existingNode != nil {
		// 找到已保存的节点信息，恢复友好名称
		finalFriendlyName = existingNode.FriendlyName
		log.Printf("🔄 恢复节点 %s 的保存名称: %s", nodeID, finalFriendlyName)

		// 更新节点信息，保持原有的友好名称
		nodeInfo = &NodeInfo{
			NodeID:        nodeID,
			Address:       address,
			Status:        "online",
			LastHeartbeat: time.Now(),
			ActiveTasks:   []string{},
			Metadata:      metadata,
			FriendlyName:  finalFriendlyName, // 使用恢复的名称
			Username:      username,          // 添加用户名
		}
	} else {
		// 没有找到已保存的信息，使用传入的名称或生成新名称
		if friendlyName == "" {
			s.nodeCounter++
			finalFriendlyName = fmt.Sprintf("节点%d", s.nodeCounter)
			log.Printf("🆕 为新节点 %s 生成名称: %s", nodeID, finalFriendlyName)
		} else {
			finalFriendlyName = friendlyName
			log.Printf("🆕 为新节点 %s 使用指定名称: %s", nodeID, finalFriendlyName)
		}

		// 创建新的节点信息
		nodeInfo = &NodeInfo{
			NodeID:        nodeID,
			Address:       address,
			Status:        "online",
			LastHeartbeat: time.Now(),
			ActiveTasks:   []string{},
			Metadata:      metadata,
			FriendlyName:  finalFriendlyName,
			Username:      username, // 添加用户名
		}
	}

	s.nodes[nodeID] = nodeInfo

	// 保存到Redis
	if err := s.saveNodeToRedis(nodeID, nodeInfo); err != nil {
		log.Printf("⚠️ 保存节点信息到Redis失败: %v", err)
	}

	log.Printf("✅ 节点注册成功: %s (%s)", nodeID, friendlyName)

	// 同步账号数据到新节点
	go s.syncAccountsToNode(nodeID)

	return nil
}

// UpdateNodeHeartbeat 更新节点心跳
func (s *Server) UpdateNodeHeartbeat(nodeID string) error {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	if node, exists := s.nodes[nodeID]; exists {
		node.LastHeartbeat = time.Now()
		node.Status = "online"
		return nil
	}

	return fmt.Errorf("节点未注册: %s", nodeID)
}

// UpdateNodeName 更新节点友好名称
func (s *Server) UpdateNodeName(nodeID, friendlyName string) error {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		return fmt.Errorf("节点不存在: %s", nodeID)
	}

	oldName := node.FriendlyName
	node.FriendlyName = friendlyName

	// 保存到Redis
	if err := s.saveNodeToRedis(nodeID, node); err != nil {
		log.Printf("⚠️ 保存节点信息到Redis失败: %v", err)
		// 不返回错误，因为内存中已经更新了
	}

	log.Printf("✅ 节点 %s 名称已更新: %s -> %s", nodeID, oldName, friendlyName)
	return nil
}

// saveNodeToRedis 保存节点信息到Redis
func (s *Server) saveNodeToRedis(nodeID string, node *NodeInfo) error {
	if s.redisClient == nil {
		return fmt.Errorf("Redis客户端未初始化")
	}

	ctx := context.Background()
	key := fmt.Sprintf("node_info:%s", nodeID)

	// 序列化节点信息
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %v", err)
	}

	// 保存到Redis，设置24小时过期时间
	if err := s.redisClient.GetNativeClient().Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("保存节点信息到Redis失败: %v", err)
	}

	return nil
}

// loadNodeFromRedis 从Redis加载节点信息
func (s *Server) loadNodeFromRedis(nodeID string) (*NodeInfo, error) {
	if s.redisClient == nil {
		return nil, fmt.Errorf("Redis客户端未初始化")
	}

	ctx := context.Background()
	key := fmt.Sprintf("node_info:%s", nodeID)

	// 从Redis获取数据
	data, err := s.redisClient.GetNativeClient().Get(ctx, key).Result()
	if err != nil {
		if err == redisv8.Nil {
			return nil, nil // 数据不存在
		}
		return nil, fmt.Errorf("从Redis获取节点信息失败: %v", err)
	}

	// 反序列化节点信息
	var node NodeInfo
	if err := json.Unmarshal([]byte(data), &node); err != nil {
		return nil, fmt.Errorf("反序列化节点信息失败: %v", err)
	}

	return &node, nil
}

// loadNodesFromRedis 从Redis加载所有节点信息
func (s *Server) loadNodesFromRedis() {
	if s.redisClient == nil {
		log.Printf("⚠️ Redis客户端未初始化，跳过节点信息恢复")
		return
	}

	ctx := context.Background()
	pattern := "node_info:*"

	// 获取所有节点信息键
	keys, err := s.redisClient.GetNativeClient().Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("❌ 获取节点信息键失败: %v", err)
		return
	}

	if len(keys) == 0 {
		log.Printf("📭 Redis中暂无节点信息")
		return
	}

	log.Printf("🔄 开始从Redis恢复节点信息，找到 %d 个节点", len(keys))

	loadedCount := 0
	for _, key := range keys {
		// 提取节点ID
		nodeID := strings.TrimPrefix(key, "node_info:")

		// 从Redis加载节点信息
		node, err := s.loadNodeFromRedis(nodeID)
		if err != nil {
			log.Printf("⚠️ 加载节点 %s 信息失败: %v", nodeID, err)
			continue
		}

		if node == nil {
			continue
		}

		// 检查节点是否已存在（避免覆盖在线节点）
		s.nodesMutex.Lock()
		if existingNode, exists := s.nodes[nodeID]; exists {
			// 如果现有节点是在线的，只更新友好名称
			if existingNode.Status == "online" {
				existingNode.FriendlyName = node.FriendlyName
				log.Printf("🔄 更新在线节点 %s 的友好名称: %s", nodeID, node.FriendlyName)
			}
		} else {
			// 节点不存在，添加为离线状态
			node.Status = "offline"
			s.nodes[nodeID] = node
			log.Printf("📥 恢复离线节点: %s (%s)", nodeID, node.FriendlyName)
			loadedCount++
		}
		s.nodesMutex.Unlock()
	}

	log.Printf("✅ 节点信息恢复完成，恢复了 %d 个节点", loadedCount)
}

// startNodeStatusMonitor 启动节点状态监控
func (s *Server) startNodeStatusMonitor() {
	log.Printf("🔍 启动节点状态监控服务")

	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkNodeStatus()
		}
	}
}

// checkNodeStatus 检查所有节点状态
func (s *Server) checkNodeStatus() {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	now := time.Now()
	// 🔧 优化：增加离线阈值到3分钟，减少误判
	offlineThreshold := 3 * time.Minute
	// 🔧 新增：警告阈值，2分钟开始警告但不标记离线
	warningThreshold := 2 * time.Minute

	for nodeID, node := range s.nodes {
		// 计算距离上次心跳的时间
		timeSinceLastHeartbeat := now.Sub(node.LastHeartbeat)

		// 🔧 优化：分级状态管理
		if timeSinceLastHeartbeat > offlineThreshold && node.Status == "online" {
			// 超过3分钟，标记为离线
			node.Status = "offline"
			log.Printf("❌ 节点 %s (%s) 已离线 - 距离上次心跳: %v",
				nodeID, node.FriendlyName, timeSinceLastHeartbeat.Round(time.Second))

			// 保存状态变更到Redis
			if err := s.saveNodeToRedis(nodeID, node); err != nil {
				log.Printf("⚠️ 保存节点离线状态到Redis失败: %v", err)
			}
		} else if timeSinceLastHeartbeat > warningThreshold && timeSinceLastHeartbeat <= offlineThreshold && node.Status == "online" {
			// 🔧 新增：2-3分钟之间，记录警告但不改变状态
			log.Printf("⚠️ 节点 %s (%s) 心跳延迟 - 距离上次心跳: %v (警告阈值)",
				nodeID, node.FriendlyName, timeSinceLastHeartbeat.Round(time.Second))
		}
	}
}

// startFlashTradeStatsSubscription 启动 Flash Trade 统计订阅
func (s *Server) startFlashTradeStatsSubscription() {
	log.Printf("📊 启动 Flash Trade 统计订阅")

	// 订阅 Flash Trade 统计频道
	pubsub := s.redisClient.SubscribeCommands("flash_trade_stats")

	// 启动订阅处理协程
	go func() {
		defer pubsub.Close()

		ch := pubsub.Channel()
		for msg := range ch {
			log.Printf("📨 收到 Flash Trade 统计消息: %s", msg.Payload)

			// 解析消息
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				log.Printf("❌ 解析 Flash Trade 统计消息失败: %v", err)
				log.Printf("   原始消息: %s", msg.Payload)
				continue
			}

			log.Printf("📊 解析后的数据: %+v", data)

			// 处理统计信息
			s.handleFlashTradeStats(data)
		}
	}()

	log.Printf("✅ Flash Trade 统计订阅启动成功")
}

// startAccountStatusSubscription 启动账号状态监控订阅
func (s *Server) startAccountStatusSubscription() {
	log.Printf("👥 启动账号状态监控订阅")

	// 订阅账号状态报告频道
	pubsub := s.redisClient.SubscribeCommands("account_status_reports")

	// 启动订阅处理协程
	go func() {
		defer pubsub.Close()

		ch := pubsub.Channel()
		for msg := range ch {
			// 解析消息
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				log.Printf("❌ 解析账号状态消息失败: %v", err)
				continue
			}

			// 处理账号状态信息
			s.handleAccountStatusReport(data)
		}
	}()

	log.Printf("✅ 账号状态监控订阅启动成功")
}

// startAutoSellResultsSubscription 启动自动卖出结果订阅
func (s *Server) startAutoSellResultsSubscription() {
	log.Printf("📊 启动自动卖出结果订阅")

	// 订阅自动卖出结果频道
	pubsub := s.redisClient.SubscribeCommands("autosell_results")

	// 启动订阅处理协程
	go func() {
		defer pubsub.Close()

		for msg := range pubsub.Channel() {
			var resultData map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &resultData); err != nil {
				log.Printf("❌ 解析自动卖出结果失败: %v", err)
				continue
			}

			// 处理自动卖出结果
			s.handleAutoSellResult(resultData)
		}
	}()

	log.Printf("✅ 自动卖出结果订阅启动成功")
}

// GetAutoSellResults 获取自动卖出结果
func (s *Server) GetAutoSellResults(nodeID, accountID string, limit int) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.autoSellResults == nil {
		return []interface{}{}
	}

	var results []interface{}
	count := 0

	// 按时间倒序遍历结果
	for _, result := range s.autoSellResults {
		if count >= limit {
			break
		}

		if resultMap, ok := result.(map[string]interface{}); ok {
			// 过滤条件
			if nodeID != "" {
				if resultNodeID, ok := resultMap["node_id"].(string); !ok || resultNodeID != nodeID {
					continue
				}
			}

			if accountID != "" {
				if resultAccountID, ok := resultMap["account_id"].(string); !ok || resultAccountID != accountID {
					continue
				}
			}

			results = append(results, result)
			count++
		}
	}

	return results
}

// GetAllFlashTradeTasks 获取所有Flash Trade任务
func (s *Server) GetAllFlashTradeTasks() map[string]*FlashTaskInfo {
	s.flashMutex.RLock()
	defer s.flashMutex.RUnlock()

	// 创建任务副本
	tasks := make(map[string]*FlashTaskInfo)
	for taskID, task := range s.flashTasks {
		tasks[taskID] = task
	}

	return tasks
}

// SendFlashTradeCommand 发送Flash Trade命令到指定节点
func (s *Server) SendFlashTradeCommand(nodeID, action string, params map[string]interface{}) error {
	if s.redisClient == nil {
		return fmt.Errorf("Redis客户端未初始化")
	}

	// 构建命令
	command := map[string]interface{}{
		"command_id":   fmt.Sprintf("flash_trade_%s_%d", action, time.Now().Unix()),
		"command_type": action,
		"target_node":  nodeID,
		"payload":      params,
		"timestamp":    time.Now().Unix(),
	}

	// 发送命令到Redis频道
	err := s.redisClient.PublishCommand("flash_trade_commands", command)
	if err != nil {
		return fmt.Errorf("发送命令失败: %v", err)
	}

	log.Printf("📤 Flash Trade命令已发送: 节点=%s, 动作=%s", nodeID, action)
	return nil
}

// handleAutoSellResult 处理自动卖出结果
func (s *Server) handleAutoSellResult(data map[string]interface{}) {
	nodeID, _ := data["node_id"].(string)
	accountID, _ := data["account_id"].(string)
	results, _ := data["results"].(map[string]interface{})
	timestamp, _ := data["timestamp"].(float64)

	log.Printf("📊 收到自动卖出结果: 节点=%s, 账号=%s", nodeID, accountID)

	// 存储自动卖出结果到内存中
	s.mu.Lock()
	if s.autoSellResults == nil {
		s.autoSellResults = make(map[string]interface{})
	}

	resultKey := fmt.Sprintf("%s_%s_%d", nodeID, accountID, int64(timestamp))
	s.autoSellResults[resultKey] = map[string]interface{}{
		"node_id":     nodeID,
		"account_id":  accountID,
		"timestamp":   timestamp,
		"results":     results,
		"task_data":   data["task_data"],
		"received_at": time.Now().Unix(),
	}
	s.mu.Unlock()

	// 记录详细信息
	if results != nil {
		status, _ := results["status"].(string)
		if status == "completed" {
			if orderID, ok := results["order_id"].(string); ok {
				if usdtAmount, ok := results["actual_usdt_amount"].(float64); ok {
					log.Printf("💰 自动卖出完成: 订单=%s, 获得USDT=%.6f", orderID, usdtAmount)
				}
			}
		} else if status == "failed" {
			if errorMsg, ok := results["sell_error"].(string); ok {
				log.Printf("❌ 自动卖出失败: %s", errorMsg)
			}
		}
	}
}

// handleFlashTradeStats 处理 Flash Trade 统计信息
func (s *Server) handleFlashTradeStats(data map[string]interface{}) {
	log.Printf("🔍 处理 Flash Trade 统计数据: %+v", data)

	nodeID, ok := data["node_id"].(string)
	if !ok {
		log.Printf("❌ Flash Trade 统计数据格式错误：缺少 node_id，数据: %+v", data)
		return
	}

	// 兼容多种timestamp类型
	var timestamp float64
	if ts, ok := data["timestamp"].(float64); ok {
		timestamp = ts
	} else if ts, ok := data["timestamp"].(int64); ok {
		timestamp = float64(ts)
	} else if ts, ok := data["timestamp"].(int); ok {
		timestamp = float64(ts)
	} else {
		log.Printf("❌ Flash Trade 统计数据格式错误：timestamp类型不支持，类型: %T，值: %v", data["timestamp"], data["timestamp"])
		return
	}

	flashStats, ok := data["flash_stats"].(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash Trade 统计数据格式错误：缺少 flash_stats，数据: %+v", data)
		return
	}

	// 解析 flash_trade.go 的统计格式 - 支持新旧两种格式
	var globalStats map[string]interface{}
	var accountStats map[string]interface{}

	// 尝试新格式（英文键名）
	if stats, exists := flashStats["global_stats"]; exists {
		if gs, ok := stats.(map[string]interface{}); ok {
			globalStats = gs
		}
	} else if stats, exists := flashStats["全局统计"]; exists {
		// 兼容旧格式（中文键名）
		if gs, ok := stats.(map[string]interface{}); ok {
			globalStats = gs
		}
	}

	if stats, exists := flashStats["account_stats"]; exists {
		if as, ok := stats.(map[string]interface{}); ok {
			accountStats = as
		}
	} else if stats, exists := flashStats["账户统计"]; exists {
		// 兼容旧格式（中文键名）
		if as, ok := stats.(map[string]interface{}); ok {
			accountStats = as
		}
	}

	log.Printf("📊 收到节点 %s 的 Flash Trade 统计 (时间戳: %.0f)", nodeID, timestamp)
	if globalStats != nil {
		// 支持新旧两种键名格式
		activeAccounts := globalStats["active_accounts"]
		if activeAccounts == nil {
			activeAccounts = globalStats["活跃账户数"]
		}
		currentVolume := globalStats["total_current_volume"]
		if currentVolume == nil {
			currentVolume = globalStats["总当前交易量"]
		}
		completionRate := globalStats["completion_rate"]
		if completionRate == nil {
			completionRate = globalStats["完成率"]
		}

		log.Printf("   全局统计: 活跃账户数=%v, 总当前交易量=%v, 完成率=%v",
			activeAccounts, currentVolume, completionRate)
	}

	// 更新节点的 Flash Trade 统计信息
	s.updateNodeFlashTradeStats(nodeID, globalStats, accountStats)
}

// updateNodeFlashTradeStats 更新节点的 Flash Trade 统计
func (s *Server) updateNodeFlashTradeStats(nodeID string, globalStats, accountStats map[string]interface{}) {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		log.Printf("⚠️ 节点 %s 不存在，无法更新 Flash Trade 统计", nodeID)
		return
	}

	// 更新节点的 Flash Trade 统计
	if node.Stats == nil {
		node.Stats = make(map[string]interface{})
	}

	// 存储按照 flash_trade.go 格式的统计数据
	node.Stats["flash_trade_global"] = globalStats
	node.Stats["flash_trade_accounts"] = accountStats
	node.Stats["last_flash_trade_update"] = time.Now().Unix()

	// 🔧 添加调试信息
	log.Printf("📊 存储节点 %s 的统计数据:", nodeID)
	log.Printf("   全局统计: %+v", globalStats)
	log.Printf("   账户统计: %+v", accountStats)

	// 🔧 修复：提取关键指标到节点统计中 - 支持新旧格式
	if globalStats != nil {
		// 尝试新格式（英文键名）
		if activeAccounts, ok := globalStats["active_accounts"]; ok {
			node.Stats["active_accounts"] = activeAccounts
		} else if activeAccounts, ok := globalStats["活跃账户数"]; ok {
			node.Stats["active_accounts"] = activeAccounts
		}

		if totalVolume, ok := globalStats["total_current_volume"]; ok {
			node.Stats["total_volume"] = totalVolume
		} else if totalVolume, ok := globalStats["总当前交易量"]; ok {
			node.Stats["total_volume"] = totalVolume
		}

		if completionRate, ok := globalStats["completion_rate"]; ok {
			node.Stats["completion_rate"] = completionRate
		} else if completionRate, ok := globalStats["完成率"]; ok {
			node.Stats["completion_rate"] = completionRate
		}

		if totalLoss, ok := globalStats["total_loss"]; ok {
			node.Stats["total_loss"] = totalLoss
		} else if totalLoss, ok := globalStats["总亏损"]; ok {
			node.Stats["total_loss"] = totalLoss
		}
	}

	log.Printf("✅ 节点 %s 的 Flash Trade 统计已更新", nodeID)
}

// GetNodesFlashTradeStats 获取所有节点的 Flash Trade 统计
func (s *Server) GetNodesFlashTradeStats(filterNodeID string) map[string]interface{} {
	s.nodesMutex.RLock()
	defer s.nodesMutex.RUnlock()

	result := make(map[string]interface{})
	now := time.Now()

	for nodeID, node := range s.nodes {
		// 如果指定了节点ID，只返回该节点的统计
		if filterNodeID != "" && nodeID != filterNodeID {
			continue
		}

		// 🔧 修复：只返回活跃节点的实时数据
		if node.Status != "online" {
			log.Printf("⏸️ 跳过非在线节点: %s (状态: %s)", nodeID, node.Status)
			continue
		}

		// 检查节点心跳时间，超过2分钟认为离线
		if now.Sub(node.LastHeartbeat) > 2*time.Minute {
			log.Printf("⏸️ 跳过心跳超时节点: %s (最后心跳: %v)", nodeID, node.LastHeartbeat)
			continue
		}

		if node.Stats != nil {
			// 检查Flash Trade统计数据的时效性
			lastUpdateTime := int64(0)
			if lastUpdate, exists := node.Stats["last_flash_trade_update"]; exists {
				if updateTime, ok := lastUpdate.(int64); ok {
					lastUpdateTime = updateTime
				}
			}

			// 如果统计数据超过30秒没有更新，认为不是实时数据
			if lastUpdateTime > 0 && now.Unix()-lastUpdateTime > 30 {
				log.Printf("⏸️ 跳过过期统计数据: %s (最后更新: %d秒前)", nodeID, now.Unix()-lastUpdateTime)
				continue
			}
			nodeStats := map[string]interface{}{
				"node_id":        nodeID,
				"status":         node.Status,
				"last_update":    node.Stats["last_flash_trade_update"],
				"last_heartbeat": node.LastHeartbeat.Unix(),
				"is_realtime":    true,                        // 标识这是实时数据
				"data_age":       now.Unix() - lastUpdateTime, // 数据年龄（秒）
			}

			// 添加全局统计
			if globalStats, exists := node.Stats["flash_trade_global"]; exists {
				nodeStats["全局统计"] = globalStats
			}

			// 添加账户统计
			if accountStats, exists := node.Stats["flash_trade_accounts"]; exists {
				nodeStats["账户统计"] = accountStats
				// 🔧 修复：为了兼容 account-status 页面，也添加 账户详情 字段
				nodeStats["账户详情"] = accountStats
			}

			// 添加提取的关键指标
			if activeAccounts, exists := node.Stats["active_accounts"]; exists {
				nodeStats["active_accounts"] = activeAccounts
			}
			if totalVolume, exists := node.Stats["total_volume"]; exists {
				nodeStats["total_volume"] = totalVolume
			}
			if completionRate, exists := node.Stats["completion_rate"]; exists {
				nodeStats["completion_rate"] = completionRate
			}
			if totalLoss, exists := node.Stats["total_loss"]; exists {
				nodeStats["total_loss"] = totalLoss
			}

			result[nodeID] = nodeStats
		}
	}

	return result
}

// handleAccountStatusReport 处理账号状态报告
func (s *Server) handleAccountStatusReport(data map[string]interface{}) {
	// 先尝试提取节点ID用于日志
	var logNodeID string
	if nodeID, ok := data["node_id"].(string); ok {
		logNodeID = nodeID
	} else {
		logNodeID = "未知节点"
	}

	// 记录收到的原始数据结构
	log.Printf("📥 收到节点 %s 的账号状态报告，数据字段: %v", logNodeID, getMapKeys(data))

	nodeID, ok := data["node_id"].(string)
	if !ok {
		log.Printf("❌ 账号状态数据格式错误：缺少 node_id")
		log.Printf("   可用字段: %v", getMapKeys(data))
		// 显示完整数据结构
		if jsonData, err := json.MarshalIndent(data, "   ", "  "); err == nil {
			log.Printf("   完整数据结构:\n%s", string(jsonData))
		}
		return
	}

	timestamp, ok := data["timestamp"].(float64)
	if !ok {
		log.Printf("❌ 节点 %s 账号状态数据格式错误：缺少 timestamp", nodeID)
		log.Printf("   可用字段: %v", getMapKeys(data))
		return
	}

	accountReports, ok := data["account_reports"].([]interface{})
	if !ok {
		log.Printf("❌ 节点 %s 账号状态数据格式错误：缺少 account_reports", nodeID)
		log.Printf("   可用字段: %v", getMapKeys(data))
		if reports, exists := data["account_reports"]; exists {
			log.Printf("   account_reports 字段类型: %T, 值: %v", reports, reports)
		} else {
			log.Printf("   完全缺少 account_reports 字段")
		}

		// 显示完整的数据结构用于调试
		if jsonData, err := json.MarshalIndent(data, "   ", "  "); err == nil {
			log.Printf("   完整数据结构:\n%s", string(jsonData))
		}
		return
	}

	summary, ok := data["summary"].(map[string]interface{})
	if !ok {
		log.Printf("❌ 节点 %s 账号状态数据格式错误：缺少 summary", nodeID)
		log.Printf("   可用字段: %v", getMapKeys(data))
		if summaryData, exists := data["summary"]; exists {
			log.Printf("   summary 字段类型: %T, 值: %v", summaryData, summaryData)
		} else {
			log.Printf("   完全缺少 summary 字段")
		}
		return
	}

	log.Printf("👥 收到节点 %s 的账号状态报告 (时间戳: %.0f)", nodeID, timestamp)
	log.Printf("   账号总数: %v, 活跃账号: %v, 过期账号: %v",
		summary["total_accounts"], summary["active_accounts"], summary["expired_accounts"])

	// 更新节点心跳（账号状态报告也算作活动证明）
	s.UpdateNodeHeartbeat(nodeID)

	// 更新节点的账号状态信息
	s.updateNodeAccountStatus(nodeID, accountReports, summary)
}

// updateNodeAccountStatus 更新节点的账号状态
func (s *Server) updateNodeAccountStatus(nodeID string, accountReports []interface{}, summary map[string]interface{}) {
	s.nodesMutex.Lock()
	defer s.nodesMutex.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		log.Printf("⚠️ 节点 %s 不存在，无法更新账号状态", nodeID)
		return
	}

	// 更新节点的账号状态
	if node.Stats == nil {
		node.Stats = make(map[string]interface{})
	}

	// 存储账号状态数据
	node.Stats["account_reports"] = accountReports
	node.Stats["account_summary"] = summary
	node.Stats["last_account_status_update"] = time.Now().Unix()

	// 提取关键指标到节点统计中
	if totalAccounts, ok := summary["total_accounts"]; ok {
		node.Stats["total_accounts"] = totalAccounts
	}
	if activeAccounts, ok := summary["active_accounts"]; ok {
		node.Stats["active_accounts"] = activeAccounts
	}
	if expiredAccounts, ok := summary["expired_accounts"]; ok {
		node.Stats["expired_accounts"] = expiredAccounts
	}

	log.Printf("✅ 节点 %s 的账号状态已更新", nodeID)
}

// getMapKeys 获取map的所有键（辅助调试函数）
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// GetNodesAccountStatus 获取所有节点的账号状态
func (s *Server) GetNodesAccountStatus(filterNodeID string) map[string]interface{} {
	s.nodesMutex.RLock()
	defer s.nodesMutex.RUnlock()

	result := make(map[string]interface{})

	for nodeID, node := range s.nodes {
		// 如果指定了节点ID过滤，只返回该节点的数据
		if filterNodeID != "" && nodeID != filterNodeID {
			continue
		}

		if node.Stats != nil {
			nodeStats := map[string]interface{}{
				"node_id":     nodeID,
				"status":      node.Status,
				"last_update": node.Stats["last_account_status_update"],
			}

			// 添加账号报告
			if accountReports, exists := node.Stats["account_reports"]; exists {
				nodeStats["account_reports"] = accountReports
			}

			// 添加账号摘要
			if accountSummary, exists := node.Stats["account_summary"]; exists {
				nodeStats["account_summary"] = accountSummary
			}

			// 添加提取的关键指标
			if totalAccounts, exists := node.Stats["total_accounts"]; exists {
				nodeStats["total_accounts"] = totalAccounts
			}
			if activeAccounts, exists := node.Stats["active_accounts"]; exists {
				nodeStats["active_accounts"] = activeAccounts
			}
			if expiredAccounts, exists := node.Stats["expired_accounts"]; exists {
				nodeStats["expired_accounts"] = expiredAccounts
			}

			result[nodeID] = nodeStats
		}
	}

	return result
}

// isTaskCanceled 检查任务是否被取消
func (e *FlashTradeExecutor) isTaskCanceled(taskID string) bool {
	taskObj, err := e.server.GetTaskManager().GetTask(taskID)
	if err != nil {
		return false
	}
	return taskObj.Status == task.TaskStatusCancelled
}

// KYCResponse KYC接口响应结构（基于 flash_trade.go）
type KYCResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		FillInfo struct {
			FirstName string `json:"firstName"`
		} `json:"fillInfo"`
	} `json:"data"`
	Success bool `json:"success"`
}

// TokenBalance 代币余额结构（基于 flash_trade.go）
type TokenBalance struct {
	TokenAddress string  `json:"tokenAddress"`
	Balance      float64 `json:"balance"`
	Symbol       string  `json:"symbol"`
}

// getUserFirstName 获取用户firstName（基于 flash_trade.go）
func (e *FlashTradeExecutor) getUserFirstName(csrftoken, cookie string) (string, error) {
	// API调用频率限制
	e.waitForAPIRateLimit()

	log.Printf("🔍 查询用户KYC信息获取firstName")

	req, err := http.NewRequest("POST", "https://www.binance.com/bapi/kyc/v2/private/certificate/user-kyc/current-kyc-status", strings.NewReader("{}"))
	if err != nil {
		return "", fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)
	req.Header.Set("clienttype", "web")
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("读取响应失败: %v", err)
	}

	var kycResp KYCResponse
	if err := json.Unmarshal(body, &kycResp); err != nil {
		return "", fmt.Errorf("解析响应失败: %v", err)
	}

	if !kycResp.Success || kycResp.Code != "000000" {
		return "", fmt.Errorf("KYC查询失败: %s", kycResp.Message)
	}

	firstName := kycResp.Data.FillInfo.FirstName
	if firstName == "" {
		return "", fmt.Errorf("firstName为空")
	}

	log.Printf("✅ 获取到用户firstName: %s", firstName)
	return firstName, nil
}

// getTokenBalance 查询指定token的余额（基于 flash_trade.go）
func (e *FlashTradeExecutor) getTokenBalance(tokenAddress, csrftoken, cookie string) (*TokenBalance, error) {
	// API调用频率限制
	e.waitForAPIRateLimit()

	log.Printf("🔍 查询token余额: %s", tokenAddress)

	req, err := http.NewRequest("GET", "https://www.binance.com/bapi/defi/v1/private/wallet-direct/cloud-wallet/alpha", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 解析响应并查找指定代币
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 这里需要根据实际的响应格式来解析代币余额
	// 暂时返回模拟数据
	return &TokenBalance{
		TokenAddress: tokenAddress,
		Balance:      0.0,
		Symbol:       "UNKNOWN",
	}, nil
}

// listenForCommandAcks 监听指令确认
func (s *Server) listenForCommandAcks() {
	if s.redisClient == nil {
		log.Printf("⚠️ Redis客户端不可用，跳过指令确认监听")
		return
	}

	pubsub := s.redisClient.SubscribeCommands("flash_command_acks")
	defer pubsub.Close()

	log.Printf("📥 开始监听指令确认频道: flash_command_acks")

	for {
		msg, err := pubsub.ReceiveMessage(context.Background())
		if err != nil {
			log.Printf("❌ 接收指令确认失败: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		s.handleCommandAck(msg.Payload)
	}
}

// handleCommandAck 处理指令确认
func (s *Server) handleCommandAck(payload string) {
	var ack struct {
		CommandID string `json:"command_id"`
		NodeID    string `json:"node_id"`
		Status    string `json:"status"`
		Message   string `json:"message"`
	}

	if err := json.Unmarshal([]byte(payload), &ack); err != nil {
		log.Printf("❌ 解析指令确认失败: %v", err)
		return
	}

	s.ackMutex.Lock()
	defer s.ackMutex.Unlock()

	if s.commandAcks[ack.CommandID] == nil {
		s.commandAcks[ack.CommandID] = make(map[string]bool)
	}

	s.commandAcks[ack.CommandID][ack.NodeID] = (ack.Status == "received")

	log.Printf("📨 收到指令确认: 命令=%s, 节点=%s, 状态=%s",
		ack.CommandID, ack.NodeID, ack.Status)
}

// waitForFlashCommandAck 等待指定节点的指令确认
func (s *Server) waitForFlashCommandAck(commandID, nodeID string, timeout time.Duration) {
	log.Printf("⏳ 等待节点 %s 确认指令 %s", nodeID, commandID)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-ticker.C:
			s.ackMutex.RLock()
			if acks, exists := s.commandAcks[commandID]; exists {
				if acked, nodeExists := acks[nodeID]; nodeExists && acked {
					s.ackMutex.RUnlock()
					log.Printf("✅ 节点 %s 已确认接收指令 %s", nodeID, commandID)
					return
				}
			}
			s.ackMutex.RUnlock()

		case <-timeoutTimer.C:
			log.Printf("⚠️ 节点 %s 确认指令 %s 超时", nodeID, commandID)
			return
		}
	}
}

// waitForFlashCommandAckFromAllNodes 等待所有在线节点的指令确认
func (s *Server) waitForFlashCommandAckFromAllNodes(commandID string, timeout time.Duration) {
	// 获取所有在线节点
	s.nodesMutex.RLock()
	var onlineNodes []string
	for nodeID, node := range s.nodes {
		if node.Status == "online" {
			onlineNodes = append(onlineNodes, nodeID)
		}
	}
	s.nodesMutex.RUnlock()

	if len(onlineNodes) == 0 {
		log.Printf("⚠️ 没有在线节点需要确认指令 %s", commandID)
		return
	}

	log.Printf("⏳ 等待 %d 个在线节点确认指令 %s: %v",
		len(onlineNodes), commandID, onlineNodes)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-ticker.C:
			s.ackMutex.RLock()
			acks, exists := s.commandAcks[commandID]
			if !exists {
				s.ackMutex.RUnlock()
				continue
			}

			// 检查所有在线节点是否都已确认
			allAcked := true
			ackedCount := 0
			for _, nodeID := range onlineNodes {
				if acked, ok := acks[nodeID]; ok && acked {
					ackedCount++
				} else {
					allAcked = false
				}
			}
			s.ackMutex.RUnlock()

			if allAcked {
				log.Printf("✅ 所有在线节点 (%d/%d) 已确认接收指令 %s",
					ackedCount, len(onlineNodes), commandID)
				return
			}

			if ackedCount > 0 {
				log.Printf("📊 指令 %s 确认进度: %d/%d 节点已确认",
					commandID, ackedCount, len(onlineNodes))
			}

		case <-timeoutTimer.C:
			s.ackMutex.RLock()
			acks, exists := s.commandAcks[commandID]
			ackedCount := 0
			if exists {
				for _, nodeID := range onlineNodes {
					if acked, ok := acks[nodeID]; ok && acked {
						ackedCount++
					}
				}
			}
			s.ackMutex.RUnlock()

			log.Printf("⚠️ 指令 %s 确认超时: %d/%d 节点已确认",
				commandID, ackedCount, len(onlineNodes))
			return
		}
	}
}

// getAccountAuthFromRedis 从Redis获取账号认证信息
func (s *Server) getAccountAuthFromRedis(accountID string) (*AccountAuth, error) {
	// 如果使用MongoDB授权
	if s.useMongoAuth && s.mongoAuthManager != nil {
		// 从MongoDB获取账号信息
		account, err := s.mongoAuthManager.GetAccountAuth(accountID)
		if err != nil {
			return nil, fmt.Errorf("从MongoDB获取账号认证信息失败: %v", err)
		}
		
		// 转换为AccountAuth格式
		return &AccountAuth{
			AccountID: account.AccountID,
			Csrftoken: account.Csrftoken,
			Cookie:    account.Cookie,
			LastUsed:  time.Now(),
		}, nil
	}

	// 否则从Redis获取
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 尝试从flash_trade_auth键获取
	key := fmt.Sprintf("flash_trade_auth:%s", accountID)
	authData, err := s.redisClient.GetNativeClient().Get(ctx, key).Result()
	if err != nil {
		if err == redisv8.Nil {
			// 尝试从account键获取
			key = fmt.Sprintf("account:%s", accountID)
			accountData, err := s.redisClient.GetNativeClient().Get(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("账号不存在: %s", accountID)
			}

			// 解析账号数据
			var accountInfo map[string]interface{}
			if err := json.Unmarshal([]byte(accountData), &accountInfo); err != nil {
				return nil, fmt.Errorf("解析账号数据失败: %v", err)
			}

			// 提取认证信息
			csrftoken, _ := accountInfo["csrftoken"].(string)
			cookie, _ := accountInfo["cookie"].(string)

			if csrftoken == "" || cookie == "" {
				return nil, fmt.Errorf("账号认证信息不完整")
			}

			return &AccountAuth{
				AccountID: accountID,
				Csrftoken: csrftoken,
				Cookie:    cookie,
				LastUsed:  time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("获取账号认证信息失败: %v", err)
	}

	// 解析flash_trade认证数据
	var auth AccountAuth
	if err := json.Unmarshal([]byte(authData), &auth); err != nil {
		return nil, fmt.Errorf("解析认证数据失败: %v", err)
	}

	return &auth, nil
}

// AccountAuth 账号认证信息
type AccountAuth struct {
	AccountID string    `json:"account_id"`
	Csrftoken string    `json:"csrftoken"`
	Cookie    string    `json:"cookie"`
	LastUsed  time.Time `json:"last_used"`
}

// handleRealtimeStats 处理实时统计请求
func (s *Server) handleRealtimeStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 获取所有节点的统计数据
	s.nodesMutex.RLock()
	nodes := s.nodes
	s.nodesMutex.RUnlock()

	// 汇总统计数据
	var totalAccounts, activeAccounts int
	var totalVolume, totalLoss float64
	var completionRate float64
	var accountDetails = make(map[string]interface{})

	for _, node := range nodes {
		// 从节点统计中提取数据
		if stats, exists := node.Stats["flash_trade_stats"]; exists {
			if statsMap, ok := stats.(map[string]interface{}); ok {
				// 提取活跃账户数
				if active, ok := statsMap["active_accounts"].(float64); ok {
					activeAccounts += int(active)
				}
				
				// 提取总账户数
				if total, ok := statsMap["total_accounts"].(float64); ok {
					totalAccounts += int(total)
				}
				
				// 提取交易量
				if volume, ok := statsMap["total_volume"].(float64); ok {
					totalVolume += volume
				}
				
				// 提取损失
				if loss, ok := statsMap["total_loss"].(float64); ok {
					totalLoss += loss
				}
			}
		}
		
		// 收集账户详情
		if accounts, exists := node.Stats["账户详情"]; exists {
			if accountsMap, ok := accounts.(map[string]interface{}); ok {
				for accountID, details := range accountsMap {
					accountDetails[accountID] = details
				}
			}
		}
	}

	// 计算完成率
	if totalVolume > 0 {
		completionRate = (totalVolume / 100000) * 100 // 假设目标是10万
	}

	// 构建响应
	response := map[string]interface{}{
		"success": true,
		"timestamp": time.Now().Format(time.RFC3339),
		"stats": map[string]interface{}{
			"total_accounts": totalAccounts,
			"active_accounts": activeAccounts,
			"total_volume": totalVolume,
			"total_loss": totalLoss,
			"completion_rate": completionRate,
			"loss_rate": func() float64 { if totalVolume > 0 { return (totalLoss / totalVolume) * 10000 } else { return 0 } }(), // 万分比
		},
		"accounts": accountDetails,
	}

	// 添加中文字段
	response["成功"] = true
	response["时间戳"] = time.Now().Format(time.RFC3339)
	response["统计"] = map[string]interface{}{
		"总账户数": totalAccounts,
		"活跃账户数": activeAccounts,
		"总交易量": totalVolume,
		"总损失": totalLoss,
		"完成率": completionRate,
		"损失率": func() float64 { if totalVolume > 0 { return (totalLoss / totalVolume) * 10000 } else { return 0 } }(), // 万分比
	}
	response["账户"] = accountDetails

	// 返回JSON响应
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("❌ 编码实时统计响应失败: %v", err)
		http.Error(w, "内部服务器错误", http.StatusInternalServerError)
	}
}

// handleGetNodes 处理获取节点信息的请求
func (s *Server) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 获取所有节点的统计数据
	s.nodesMutex.RLock()
	nodes := s.nodes
	s.nodesMutex.RUnlock()

	// 构建响应
	response := make(map[string]interface{})
	response["success"] = true
	response["nodes"] = nodes

	// 返回JSON响应
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("❌ 编码节点信息响应失败: %v", err)
		http.Error(w, "内部服务器错误", http.StatusInternalServerError)
	}
}

