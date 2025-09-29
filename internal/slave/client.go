package slave

import (
	"alpha-autosell-bot/internal/account"
	"alpha-autosell-bot/internal/auth"
	"alpha-autosell-bot/internal/binance"
	"alpha-autosell-bot/internal/common"
	"alpha-autosell-bot/internal/proto"
	"alpha-autosell-bot/pkg/redis"
	"alpha-autosell-bot/pkg/utils"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// AccountInfo Flash Trade 使用的账号信息结构
type AccountInfo struct {
	AccountID    string `json:"account_id"`
	Name         string `json:"name"`
	Csrftoken    string `json:"csrftoken"`
	Cookie       string `json:"cookie"`
	Status       string `json:"status"`
	Description  string `json:"description"`
	AssignedNode string `json:"assigned_node"` // 🔧 新增：分配的节点ID
}

// Command 简单的命令结构体
type Command struct {
	CommandID   string `json:"command_id"`
	CommandType string `json:"command_type"`
	TargetNode  string `json:"target_node"`
	Type        string `json:"type"`
	Payload     string `json:"payload"`
}

// Client 被控端客户端
type Client struct {
	config          *common.Config
	grpcConn        *grpc.ClientConn
	grpcClient      proto.TradingServiceClient
	redisClient     *redis.Client
	mongoAuthManager *auth.MongoAuthManager // 添加MongoDB授权管理器
	useMongoAuth    bool                    // 是否使用MongoDB授权
	accountManager  *AccountManager

	// 状态管理
	nodeID    string
	status    string
	startTime time.Time

	// 控制通道
	stopChan  chan struct{}
	isRunning bool
	runMutex  sync.RWMutex
}

// NewClient 创建新的被控端客户端
func NewClient(config *common.Config) (*Client, error) {
	// 生成节点ID（如果未指定）
	nodeID := config.Server.NodeID
	if nodeID == "" {
		// 生成随机节点ID
		nodeID = utils.GenerateNodeID("slave")
		log.Printf("🆔 生成节点ID: %s", nodeID)
	}

	// 创建Redis客户端
	redisClient := redis.NewClient(redis.Config{
		Addr:     config.Redis.Addr,
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})

	// 创建账号管理器
	accountManager := NewAccountManager(nodeID, "data/accounts.json")
	
	// 创建授权管理器（如果配置了数据库）
	var mongoAuthManager *auth.MongoAuthManager
	useMongoAuth := false
	
	// 检查是否配置了数据库
	if config.MongoDB != nil && config.MongoDB.Enabled && config.MongoDB.URI != "" {
		var err error
		mongoAuthManager, err = auth.NewMongoAuthManager()
		if err != nil {
			// 静默失败，不输出日志
		} else {
			useMongoAuth = true
		}
	}

	return &Client{
		config:          config,
		redisClient:     redisClient,
		mongoAuthManager: mongoAuthManager,
		useMongoAuth:    useMongoAuth,
		accountManager:  accountManager,
		nodeID:          nodeID,
		status:          "offline",
		startTime:       time.Now(),
		stopChan:        make(chan struct{}),
		isRunning:       false,
		runMutex:        sync.RWMutex{},
	}, nil
}

// Start 启动被控端客户端
func (c *Client) Start() error {
	c.runMutex.Lock()
	if c.isRunning {
		c.runMutex.Unlock()
		return fmt.Errorf("服务已在运行")
	}
	c.isRunning = true
	c.runMutex.Unlock()

	log.Printf("🚀 启动服务 - 节点ID: %s", c.nodeID)

	// 连接到主控端
	if err := c.connectToMaster(); err != nil {
		log.Printf("⚠️ 连接服务器失败: %v", err)
		// 不返回错误，允许程序继续运行
	}
	
	// 检查授权状态 - 无痕检查
	if !c.isAuthorized() {
		// 等待5秒
		time.Sleep(5 * time.Second)
		// 停止客户端
		c.Stop()
		// 退出程序
		os.Exit(1)
		return fmt.Errorf("节点未授权")
	}

	// 启动Redis命令处理
	go c.handleRedisCommands()

	// 启动心跳
	go c.startHeartbeat()

	// 启动账号状态上报
	go c.startAccountStatusReporting()

	// 启动 Flash Trade 统计上报
	go c.startFlashTradeStatsReporting()

	// 启动账号同步
	go c.startAccountSync()

	c.status = "online"
	log.Printf("✅ 服务启动成功")

	return nil
}

// Stop 停止被控端客户端
func (c *Client) Stop() {
	c.runMutex.Lock()
	defer c.runMutex.Unlock()

	if !c.isRunning {
		return
	}

	// 发送停止信号
	close(c.stopChan)
	c.isRunning = false

	// 关闭gRPC连接
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
	}

	// 关闭Redis连接
	if c.redisClient != nil {
		c.redisClient.Close()
	}
	
	// 关闭MongoDB连接
	if c.mongoAuthManager != nil {
		c.mongoAuthManager.Close()
		log.Println("MongoDB授权管理器已关闭")
	}

	log.Println("被控端客户端已停止")
}

// connectToMaster 连接到主控端
func (c *Client) connectToMaster() error {
	masterAddr := c.config.Server.MasterAddr
	if masterAddr == "" {
		masterAddr = "localhost:29090" // 默认主控端地址
	}
	log.Printf("🔗 连接主控端...")

	// 创建gRPC连接，不使用阻塞模式
	conn, err := grpc.Dial(masterAddr,
		grpc.WithInsecure(),
		grpc.WithTimeout(30*time.Second), // 增加连接超时时间
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second, // 60秒发送一次 keepalive
			Timeout:             10 * time.Second, // 10秒超时
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("gRPC连接失败: %v", err)
	}

	c.grpcConn = conn
	c.grpcClient = proto.NewTradingServiceClient(conn)

	// 注册节点
	if err := c.registerNode(); err != nil {
		return fmt.Errorf("节点注册失败: %v", err)
	}

	log.Printf("✅ 已连接到主控端")
	return nil
}

// registerNode 注册节点到主控端
func (c *Client) registerNode() error {
	// 首先尝试 gRPC 注册
	if err := c.registerNodeGRPC(); err != nil {
		log.Printf("⚠️ gRPC 注册失败，尝试 HTTP API 注册...")

		// 如果 gRPC 失败，尝试 HTTP API 注册
		if err := c.registerNodeHTTP(); err != nil {
			return fmt.Errorf("HTTP API 注册也失败: %v", err)
		}
	}

	c.status = "online"
	log.Printf("✅ 节点注册成功")

	// 检查节点授权状态
	go c.checkAuthorizationStatus()

	return nil
}

// registerNodeGRPC 通过 gRPC 注册节点
func (c *Client) registerNodeGRPC() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 增加超时时间到30秒
	defer cancel()

	// 获取用户名
	username := os.Getenv("USERNAME")
	if username == "" {
		username = os.Getenv("USER")
	}
	if username == "" {
		username = "unknown"
	}

	req := &proto.NodeRegisterRequest{
		NodeId:   c.nodeID,
		NodeType: "slave",
		HttpAddr: c.getLocalIPAddress(),
		Metadata: map[string]string{
			"version":    "1.0.0",
			"start_time": c.startTime.Format(time.RFC3339),
			"username":   username, // 添加用户名
		},
		Timestamp: time.Now().Unix(),
	}

	// 添加重试逻辑
	var err error
	var resp *proto.NodeRegisterResponse
	
	for retries := 0; retries < 3; retries++ {
		if retries > 0 {
			log.Printf("🔄 重试注册节点...")
			time.Sleep(time.Duration(retries) * 2 * time.Second)
		}
		
		resp, err = c.grpcClient.RegisterNode(ctx, req)
		if err == nil {
			break
		}
	}
	
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("gRPC 注册失败: %s", resp.Message)
	}

	return nil
}

// registerNodeHTTP 通过 HTTP API 注册节点
func (c *Client) registerNodeHTTP() error {
	// 构建主控端HTTP地址
	masterHTTPAddr := c.getMasterHTTPAddr()
	if masterHTTPAddr == "" {
		return fmt.Errorf("无法确定主控端HTTP地址")
	}

	// 获取用户名
	username := os.Getenv("USERNAME")
	if username == "" {
		username = os.Getenv("USER")
	}
	if username == "" {
		username = "unknown"
	}

	// 构建注册请求
	reqData := map[string]interface{}{
		"node_id":   c.nodeID,
		"node_type": "slave",
		"http_addr": c.getLocalIPAddress(),
		"metadata": map[string]string{
			"version":    "1.0.0",
			"start_time": c.startTime.Format(time.RFC3339),
			"username":   username,
		},
		"timestamp": time.Now().Unix(),
	}

	jsonData, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("序列化请求数据失败: %v", err)
	}

	// 发送注册请求
	url := fmt.Sprintf("%s/api/v1/nodes/register", masterHTTPAddr)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建HTTP请求失败: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送HTTP请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP API返回错误状态: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	var response struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("解析响应失败: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("HTTP API注册失败: %s", response.Message)
	}

	return nil
}

// getMasterHTTPAddr 获取主控端 HTTP 地址
func (c *Client) getMasterHTTPAddr() string {
	masterAddr := c.config.Server.MasterAddr
	if masterAddr == "" {
		masterAddr = "localhost:28080"
	}

	// 从 master_addr 中提取主机地址
	parts := strings.Split(masterAddr, ":")
	if len(parts) < 2 {
		// 如果没有端口，默认使用 localhost
		return "localhost:28080"
	}

	host := parts[0]

	// 将 gRPC 端口转换为 HTTP 端口
	// gRPC 端口 29090 -> HTTP 端口 28080
	// 保持端口差值为 1010
	if strings.Contains(masterAddr, ":29090") {
		return fmt.Sprintf("http://%s:28080", host)
	}

	// 对于其他端口，假设 HTTP 端口比 gRPC 端口小 1010
	if len(parts) >= 2 {
		grpcPort := parts[1]
		if grpcPort == "29090" {
			return fmt.Sprintf("http://%s:28080", host)
		}
		// 对于自定义端口，尝试减去 1010
		// 例如：gRPC 19090 -> HTTP 18080
		if len(grpcPort) >= 4 && grpcPort[len(grpcPort)-4:] == "9090" {
			httpPort := grpcPort[:len(grpcPort)-4] + "8080"
			return fmt.Sprintf("http://%s:%s", host, httpPort)
		}
	}

	// 默认返回 28080 端口
	return fmt.Sprintf("http://%s:28080", host)
}

// sendHTTPRegisterRequest 发送 HTTP 注册请求
func (c *Client) sendHTTPRegisterRequest(masterAddr string, data map[string]interface{}) error {

	// 序列化数据
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("序列化注册数据失败: %v", err)
	}

	// 构建请求 URL（masterAddr已经包含http://前缀）
	url := fmt.Sprintf("%s/api/v1/nodes/register", masterAddr)

	// 创建 HTTP 请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建 HTTP 请求失败: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送 HTTP 请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP 注册失败，状态码: %d", resp.StatusCode)
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("解析响应失败: %v", err)
	}

	// 检查注册结果
	if success, ok := result["success"].(bool); !ok || !success {
		message := "未知错误"
		if msg, ok := result["message"].(string); ok {
			message = msg
		}
		return fmt.Errorf("注册失败: %s", message)
	}

	log.Printf("✅ HTTP API 注册成功")
	return nil
}

// startHeartbeat 启动心跳
func (c *Client) startHeartbeat() {
	// 🔧 优化：减少心跳间隔到30秒，提高连接稳定性
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// 立即发送一次心跳
	c.sendHeartbeat()

	for {
		select {
		case <-ticker.C:
			c.sendHeartbeat()
		case <-c.stopChan:
			return
		}
	}
}

// sendHeartbeat 发送心跳
func (c *Client) sendHeartbeat() {
	if c.grpcClient == nil {
		log.Printf("⚠️ gRPC客户端未初始化，跳过心跳发送")
		return
	}

	// 🔧 优化：增加重试机制，最多重试3次
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second) // 增加超时时间

		req := &proto.HeartbeatRequest{
			NodeId:    c.nodeID,
			Timestamp: time.Now().Unix(),
		}

		resp, err := c.grpcClient.Heartbeat(ctx, req)
		cancel()

		if err != nil {
			if attempt < maxRetries {
				log.Printf("⚠️ 心跳发送失败 (尝试 %d/%d): %v，1秒后重试", attempt, maxRetries, err)
				time.Sleep(1 * time.Second)
				continue
			} else {
				log.Printf("❌ 心跳发送失败 (已重试%d次): %v", maxRetries, err)
				c.status = "offline"
				// 尝试重新连接
				go c.reconnectToMaster()
				return
			}
		}

		if resp.Success {
			c.status = "online"
			if attempt > 1 {
				log.Printf("✅ 心跳发送成功 (第%d次尝试)", attempt)
			}
			return
		} else {
			log.Printf("⚠️ 心跳响应失败: %s", resp.Message)
		}
	}
}

// 🔧 新增：重新连接到主控端
func (c *Client) reconnectToMaster() {
	log.Printf("🔄 尝试重新连接到主控端...")

	// 关闭旧连接
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
		c.grpcClient = nil
	}

	// 等待一段时间后重连
	time.Sleep(5 * time.Second)

	// 尝试重新连接
	if err := c.connectToMaster(); err != nil {
		log.Printf("❌ 重新连接主控端失败: %v", err)
		c.status = "offline"

		// 5分钟后再次尝试
		time.AfterFunc(5*time.Minute, func() {
			c.reconnectToMaster()
		})
	} else {
		log.Printf("✅ 重新连接主控端成功")
		c.status = "online"
	}
}

// handleRedisCommands 处理Redis命令
func (c *Client) handleRedisCommands() {
	// 检查Redis客户端是否可用
	if c.redisClient == nil {
		log.Printf("⚠️ Redis客户端不可用，跳过Redis命令处理")
		return
	}

	// 开始监听Redis命令

	pubsub := c.redisClient.SubscribeCommands("account_commands", "account_sync", "flash_commands", "flash_stats_request", "autosell_commands")
	defer pubsub.Close()

	for {
		select {
		case <-c.stopChan:
			return
		default:
		}

		for msg := range pubsub.Channel() {
			log.Printf("🔧 [调试] 收到Redis消息: 频道=%s, 内容长度=%d", msg.Channel, len(msg.Payload))

			switch msg.Channel {
			case "account_commands":
				c.handleAccountCommand(msg.Payload)
			case "account_sync":
				c.handleAccountSync(msg.Payload)
			case "flash_commands":
				c.handleFlashCommand(msg.Payload)
			case "flash_stats_request":
				c.handleFlashStatsRequest(msg.Payload)
			case "autosell_commands":
				log.Printf("🔧 [调试] 收到自动卖出命令，准备处理...")
				c.handleAutoSellCommand(msg.Payload)
			default:
				log.Printf("⚠️ 收到未知频道的消息: %s", msg.Channel)
			}
		}
	}
}

// GetNodeID 获取节点ID
func (c *Client) GetNodeID() string {
	return c.nodeID
}

// GetAccountManager 获取账号管理器
func (c *Client) GetAccountManager() *AccountManager {
	return c.accountManager
}

// GetStatus 获取状态
func (c *Client) GetStatus() string {
	return c.status
}

// handleAccountCommand 处理账号管理命令
func (c *Client) handleAccountCommand(payload string) {
	// 首先检查节点是否已授权
	if !c.isAuthorized() {
		log.Printf("⚠️ 节点未授权，拒绝处理账号管理命令")
		return
	}

	var command Command
	if err := json.Unmarshal([]byte(payload), &command); err != nil {
		log.Printf("❌ 解析账号管理命令失败: %v", err)
		return
	}

	// 检查是否是目标节点
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		return
	}

	log.Printf("📨 收到账号管理命令: %s", command.CommandID)

	if command.CommandType == "manage_account" {
		c.handleManageAccountCommand(command)
	}
}

// handleManageAccountCommand 处理账号管理命令
func (c *Client) handleManageAccountCommand(command Command) {
	payloadBytes, _ := json.Marshal(command.Payload)
	var req proto.AccountManageRequest
	if err := json.Unmarshal(payloadBytes, &req); err != nil {
		log.Printf("❌ 解析账号管理请求失败: %v", err)
		return
	}

	var message string

	switch req.Action {
	case "add":
		account := &Account{
			ID:          req.AccountId,
			Name:        req.Name,
			Csrftoken:   req.Csrftoken,
			Cookie:      req.Cookie,
			Description: req.Description,
		}
		if err := c.accountManager.AddAccount(account); err != nil {
			message = fmt.Sprintf("添加账号失败: %v", err)
		} else {
			message = "账号添加成功"
		}

	case "update":
		updates := &Account{
			Name:        req.Name,
			Csrftoken:   req.Csrftoken,
			Cookie:      req.Cookie,
			Description: req.Description,
		}
		if err := c.accountManager.UpdateAccount(req.AccountId, updates); err != nil {
			message = fmt.Sprintf("更新账号失败: %v", err)
		} else {
			message = "账号更新成功"
		}

	case "delete":
		if err := c.accountManager.DeleteAccount(req.AccountId); err != nil {
			message = fmt.Sprintf("删除账号失败: %v", err)
		} else {
			message = "账号删除成功"
		}

	case "get":
		accounts := c.accountManager.GetAllAccounts()
		message = fmt.Sprintf("获取到 %d 个账号", len(accounts))

	default:
		message = fmt.Sprintf("未知操作: %s", req.Action)
	}

	log.Printf("✅ 账号管理操作完成: %s - %s", req.Action, message)
}

// handleAccountSync 处理主控端的账号同步命令
func (c *Client) handleAccountSync(payload string) {
	// 首先检查节点是否已授权
	if !c.isAuthorized() {
		log.Printf("⚠️ 节点未授权，拒绝处理账号同步命令")
		return
	}

	var command binance.Command
	if err := json.Unmarshal([]byte(payload), &command); err != nil {
		log.Printf("❌ 解析账号同步命令失败: %v", err)
		return
	}

	// 检查是否是目标节点
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		return
	}

	log.Printf("📨 收到账号同步命令: %s", command.CommandID)

	if command.CommandType == "sync_account" {
		c.handleSyncAccountCommand(command)
	}
}

// handleSyncAccountCommand 处理账号同步命令
func (c *Client) handleSyncAccountCommand(command binance.Command) {
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ 账号同步命令格式错误")
		return
	}

	action, _ := payload["action"].(string)
	accountData, _ := payload["account"].(map[string]interface{})

	var message string

	switch action {
	case "add", "update":
		if accountData == nil {
			log.Printf("❌ 账号数据为空")
			return
		}

		account := &Account{
			ID:          getString(accountData, "id"),
			Name:        getString(accountData, "name"),
			Csrftoken:   getString(accountData, "csrftoken"),
			Cookie:      getString(accountData, "cookie"),
			Description: getString(accountData, "description"),
		}

		// 检查本地是否已有该账号
		if existingAccount, err := c.accountManager.GetAccount(account.ID); err == nil {
			// 本地已存在账号
			_ = existingAccount
		} else {
			// 本地不存在账号
		}

		if action == "add" {
			if err := c.accountManager.AddAccount(account); err != nil {
				message = fmt.Sprintf("同步添加账号失败: %v", err)
			} else {
				message = "账号同步添加成功"
			}
		} else {
			updates := &Account{
				Name:        account.Name,
				Csrftoken:   account.Csrftoken,
				Cookie:      account.Cookie,
				Description: account.Description,
			}
			if err := c.accountManager.UpdateAccount(account.ID, updates); err != nil {
				// 如果更新失败且是因为账号不存在，则尝试添加
				if strings.Contains(err.Error(), "账号不存在") {
					log.Printf("⚠️ 账号不存在，转为添加操作: %s", account.ID)
					if addErr := c.accountManager.AddAccount(account); addErr != nil {
						message = fmt.Sprintf("同步添加账号失败: %v", addErr)
					} else {
						message = "账号同步添加成功 (原为更新操作)"
					}
				} else {
					message = fmt.Sprintf("同步更新账号失败: %v", err)
				}
			} else {
				message = "账号同步更新成功"
			}
		}

	case "delete":
		accountID, _ := payload["account_id"].(string)
		if err := c.accountManager.DeleteAccount(accountID); err != nil {
			message = fmt.Sprintf("同步删除账号失败: %v", err)
		} else {
			message = "账号同步删除成功"
		}

	default:
		message = fmt.Sprintf("未知同步操作: %s", action)
	}

	log.Printf("✅ 账号同步操作完成: %s - %s", action, message)

	// 显示当前账号列表（调试用）
	c.logCurrentAccounts()
}

// logCurrentAccounts 记录当前账号列表（调试用）
func (c *Client) logCurrentAccounts() {
	accounts := c.accountManager.GetAllAccounts()
	if len(accounts) == 0 {
		log.Printf("当前节点无账号")
	} else {
		log.Printf("当前节点账号: %d个", len(accounts))
	}
}

// getString 从map中获取字符串值
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// handleFlashCommand 处理Flash Trade命令
func (c *Client) handleFlashCommand(payload string) {
	// 首先检查节点是否已授权
	if !c.isAuthorized() {
		log.Printf("⚠️ 节点未授权，拒绝处理Flash Trade命令")
		return
	}

	log.Printf("🔧 [调试] 收到Flash命令原始消息: %s", payload)

	var command binance.Command
	if err := json.Unmarshal([]byte(payload), &command); err != nil {
		log.Printf("❌ Flash命令解析失败: %v", err)
		log.Printf("   原始消息: %s", payload)
		return
	}

	log.Printf("🔧 [调试] Flash命令解析成功:")
	log.Printf("   命令ID: %s", command.CommandID)
	log.Printf("   命令类型: %s", command.CommandType)
	log.Printf("   目标节点: %s", command.TargetNode)
	log.Printf("   当前节点: %s", c.nodeID)

	// 检查是否是目标节点
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		log.Printf("🔄 Flash命令跳过: 目标节点=%s, 当前节点=%s", command.TargetNode, c.nodeID)
		return
	}

	log.Printf("📨 收到Flash命令: %s (类型: %s, 目标节点: %s)", command.CommandID, command.CommandType, command.TargetNode)

	// 将 Payload 转换为 map[string]interface{}
	payloadMap, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash命令Payload格式错误")
		return
	}

	action, ok := payloadMap["action"].(string)
	if !ok {
		log.Printf("❌ Flash命令缺少action字段")
		return
	}

	switch action {
	case "start":
		c.handleFlashStart(&command)
	case "stop":
		c.handleFlashStop(&command)
	case "pause":
		c.handleFlashPause(&command)
	case "resume":
		c.handleFlashResume(&command)
	default:
		log.Printf("❌ 未知的Flash命令action: %s", action)
	}
}

// handleFlashStart 处理Flash Trade启动命令
func (c *Client) handleFlashStart(command *binance.Command) {
	log.Printf("🚀 处理Flash Trade启动命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash启动命令Payload格式错误")
		return
	}

	// 解析参数
	taskID, _ := payload["task_id"].(string)
	tokenAddress, _ := payload["token_address"].(string)
	usdtAmount, _ := payload["usdt_amount"].(float64)
	baseAsset, _ := payload["base_asset"].(string)
	targetVolume, _ := payload["target_volume"].(float64)
	autoLoop, _ := payload["auto_loop"].(bool)
	pricePrecision, _ := payload["price_precision"].(float64)
	chainID, _ := payload["chain_id"].(string)       // 🔧 新增：解析链ID
	priceMode, _ := payload["price_mode"].(string)   // 🔧 新增：解析价格模式

	// 获取目标账号列表
	targetAccountsInterface, _ := payload["target_accounts"].([]interface{})
	var targetAccounts []string
	for _, acc := range targetAccountsInterface {
		if accStr, ok := acc.(string); ok {
			targetAccounts = append(targetAccounts, accStr)
		}
	}

	log.Printf("   任务ID: %s", taskID)
	log.Printf("   代币地址: %s", tokenAddress)
	log.Printf("   交易金额: %.2f USDT", usdtAmount)
	log.Printf("   基础资产: %s", baseAsset)
	log.Printf("   目标交易量: %.2f", targetVolume)
	log.Printf("   自动循环: %v", autoLoop)
	log.Printf("   价格精度: %.0f", pricePrecision)
	log.Printf("   链ID: %s", chainID)           // 🔧 新增：显示链ID
	log.Printf("   价格模式: %s", priceMode)      // 🔧 新增：显示价格模式
	log.Printf("   目标账号: %v", targetAccounts)

	// 🔧 新增：立即发送接收确认
	c.sendCommandAck(taskID, "received", "Flash Trade指令已接收")

	// 验证参数
	if taskID == "" || tokenAddress == "" || usdtAmount <= 0 {
		log.Printf("❌ Flash Trade参数无效")
		c.sendFlashResponse(taskID, "error", "参数无效")
		return
	}

	// 从 Redis 获取所有账号
	allAccounts, err := c.getAccountsFromRedis()
	if err != nil {
		log.Printf("❌ 从 Redis 获取账号失败: %v", err)
		c.sendFlashResponse(taskID, "error", "获取账号失败")
		return
	}

	if len(allAccounts) == 0 {
		log.Printf("⚠️ Redis 中没有可用账号")
		c.sendFlashResponse(taskID, "error", "Redis 中没有可用账号")
		return
	}

	// 从Redis获取到账号

	// 转换为 AccountInfo 格式，并过滤分配给当前节点的账号
	var accounts []*AccountInfo
	for _, acc := range allAccounts {
		// 🔧 修复：只处理分配给当前节点的账号
		if acc.AssignedNode != "" && acc.AssignedNode != c.nodeID {

			continue
		}

		// 如果账号没有分配节点，也跳过（避免重复执行）
		if acc.AssignedNode == "" {
			log.Printf("⚠️ 跳过未分配节点的账号: %s (%s)", acc.AccountID, acc.Name)
			continue
		}

		accounts = append(accounts, acc) // 🔧 修复：直接使用已转换的AccountInfo

		log.Printf("✅ 包含本节点账号: %s (%s)", acc.AccountID, acc.Name)
	}

	// 过滤目标账号
	var validAccounts []*AccountInfo
	if len(targetAccounts) > 0 {
		log.Printf("🎯 使用指定账号: %v", targetAccounts)
		// 使用指定账号
		for _, targetAcc := range targetAccounts {
			if targetAcc == "" {
				log.Printf("⚠️ 跳过空账号ID")
				continue
			}
			for _, acc := range accounts {
				if acc.AccountID == targetAcc && acc.Status == "active" {
					validAccounts = append(validAccounts, acc)
					log.Printf("✅ 找到目标账号: %s (%s)", acc.AccountID, acc.Name)
					break
				}
			}
		}
	} else {
		log.Printf("🌐 使用所有活跃账号")
		// 使用所有活跃账号
		for _, acc := range accounts {
			if acc.Status == "active" {
				validAccounts = append(validAccounts, acc)
				log.Printf("✅ 添加活跃账号: %s (%s)", acc.AccountID, acc.Name)
			} else {
				log.Printf("⏸️ 跳过非活跃账号: %s (%s) [状态: %s]", acc.AccountID, acc.Name, acc.Status)
			}
		}
	}

	if len(validAccounts) == 0 {
		log.Printf("⚠️ 没有找到有效的目标账号")
		c.sendFlashResponse(taskID, "error", "没有找到有效的目标账号")
		return
	}

	log.Printf("✅ 开始执行Flash Trade: %d个账号", len(validAccounts))

	// 为每个账号调用 flash_trade.go 的 /trade 接口
	successCount := 0
	for _, account := range validAccounts {
		log.Printf("🚀 [%s] 开始处理 Flash Trade 任务", account.AccountID)
		log.Printf("   账号状态: %s, Cookie长度: %d", account.Status, len(account.Cookie))

		// 验证账号信息完整性
		if account.Csrftoken == "" || account.Cookie == "" {
			log.Printf("❌ [%s] 账号信息不完整，跳过执行", account.AccountID)
			continue
		}

		// 调用 flash_trade.go 的 /trade 接口
		success := c.callFlashTradeAPIWithChain(account.AccountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, int(pricePrecision), chainID, priceMode, account.Csrftoken, account.Cookie)
		if success {
			log.Printf("✅ [%s] Flash Trade 调用成功", account.AccountID)
			successCount++
		} else {
			log.Printf("❌ [%s] Flash Trade 调用失败", account.AccountID)
		}
	}

	// 发送响应
	message := fmt.Sprintf("节点 %s 成功启动 %d/%d 个账号的Flash Trade", c.nodeID, successCount, len(validAccounts))
	c.sendFlashResponse(taskID, "started", message)
}

// handleFlashStop 处理Flash Trade停止命令
func (c *Client) handleFlashStop(command *binance.Command) {
	log.Printf("🛑 处理Flash Trade停止命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash停止命令Payload格式错误")
		return
	}

	taskID, _ := payload["task_id"].(string)
	accountID, _ := payload["account_id"].(string)

	log.Printf("   任务ID: %s", taskID)
	log.Printf("   账号ID: %s", accountID)

	stoppedCount := 0

	// 🔧 实现：调用 flash_trade.go 的停止接口
	if accountID != "" {
		// 停止指定账号的Flash Trade
		if c.stopFlashTradeForAccount(accountID) {
			stoppedCount = 1
			log.Printf("✅ 成功停止账号 %s 的Flash Trade", accountID)
		} else {
			log.Printf("❌ 停止账号 %s 的Flash Trade失败", accountID)
		}
	} else {
		// 停止所有Flash Trade（如果没有指定账号）
		stoppedCount = c.stopAllFlashTrade()
		log.Printf("✅ 成功停止 %d 个Flash Trade", stoppedCount)
	}

	// 发送响应
	message := fmt.Sprintf("节点 %s 停止了 %d 个Flash Trade", c.nodeID, stoppedCount)
	c.sendFlashResponse(taskID, "stopped", message)
}

// stopFlashTradeForAccount 停止指定账号的Flash Trade
func (c *Client) stopFlashTradeForAccount(accountID string) bool {
	// 调用 flash_trade.go 的停止接口
	url := "http://localhost:8080/stop"

	payload := map[string]interface{}{
		"account_id": accountID,
		"action":     "stop",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("❌ 序列化停止请求失败: %v", err)
		return false
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ 创建停止请求失败: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 发送停止请求失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Printf("✅ 成功停止账号 %s 的Flash Trade", accountID)
		return true
	} else {
		log.Printf("❌ 停止请求失败，状态码: %d", resp.StatusCode)
		return false
	}
}

// stopAllFlashTrade 停止所有Flash Trade
func (c *Client) stopAllFlashTrade() int {
	// 调用 flash_trade.go 的全局停止接口
	url := "http://localhost:8080/stop-all"

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Printf("❌ 创建全局停止请求失败: %v", err)
		return 0
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 发送全局停止请求失败: %v", err)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		// 这里可以解析响应获取实际停止的数量
		// 暂时返回1表示成功
		return 1
	} else {
		log.Printf("❌ 全局停止请求失败，状态码: %d", resp.StatusCode)
		return 0
	}
}

// handleFlashPause 处理Flash Trade暂停命令
func (c *Client) handleFlashPause(command *binance.Command) {
	log.Printf("⏸️ 处理Flash Trade暂停命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash暂停命令Payload格式错误")
		c.sendFlashResponse(command.CommandID, "failed", "Payload格式错误")
		return
	}

	// 获取任务ID（可选）
	_, _ = payload["task_id"].(string)

	// 获取账户ID（可选，如果指定则只暂停特定账户）
	accountID, _ := payload["account_id"].(string)

	var pausedCount int
	var message string

	if accountID != "" {
		// 暂停特定账户
		if c.pauseFlashTradeAccount(accountID) {
			pausedCount = 1
			message = fmt.Sprintf("账户 %s 已暂停", accountID)
			log.Printf("✅ Flash Trade账户已暂停: %s", accountID)
		} else {
			message = fmt.Sprintf("账户 %s 暂停失败", accountID)
			log.Printf("❌ Flash Trade账户暂停失败: %s", accountID)
		}
	} else {
		// 暂停所有账户
		pausedCount = c.pauseAllFlashTrade()
		message = fmt.Sprintf("已暂停 %d 个Flash Trade账户", pausedCount)
		log.Printf("✅ Flash Trade全部暂停完成，共暂停 %d 个账户", pausedCount)
	}

	// 发送响应
	responseMessage := fmt.Sprintf("节点 %s %s", c.nodeID, message)
	if pausedCount > 0 {
		c.sendFlashResponse(command.CommandID, "paused", responseMessage)
	} else {
		c.sendFlashResponse(command.CommandID, "failed", responseMessage)
	}
}

// handleFlashResume 处理Flash Trade恢复命令
func (c *Client) handleFlashResume(command *binance.Command) {
	log.Printf("▶️ 处理Flash Trade恢复命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ Flash恢复命令Payload格式错误")
		c.sendFlashResponse(command.CommandID, "failed", "Payload格式错误")
		return
	}

	// 获取任务ID（可选）
	_, _ = payload["task_id"].(string)

	// 获取账户ID（可选，如果指定则只恢复特定账户）
	accountID, _ := payload["account_id"].(string)

	var resumedCount int
	var message string

	if accountID != "" {
		// 恢复特定账户
		if c.resumeFlashTradeAccount(accountID) {
			resumedCount = 1
			message = fmt.Sprintf("账户 %s 已恢复", accountID)
			log.Printf("✅ Flash Trade账户已恢复: %s", accountID)
		} else {
			message = fmt.Sprintf("账户 %s 恢复失败", accountID)
			log.Printf("❌ Flash Trade账户恢复失败: %s", accountID)
		}
	} else {
		// 恢复所有账户
		resumedCount = c.resumeAllFlashTrade()
		message = fmt.Sprintf("已恢复 %d 个Flash Trade账户", resumedCount)
		log.Printf("✅ Flash Trade全部恢复完成，共恢复 %d 个账户", resumedCount)
	}

	// 发送响应
	responseMessage := fmt.Sprintf("节点 %s %s", c.nodeID, message)
	if resumedCount > 0 {
		c.sendFlashResponse(command.CommandID, "resumed", responseMessage)
	} else {
		c.sendFlashResponse(command.CommandID, "failed", responseMessage)
	}
}

// handleFlashStatsRequest 处理Flash Trade统计请求
func (c *Client) handleFlashStatsRequest(payload string) {
	// 首先检查节点是否已授权
	if !c.isAuthorized() {
		log.Printf("⚠️ 节点未授权，拒绝处理Flash Trade统计请求")
		return
	}

	var command binance.Command
	if err := json.Unmarshal([]byte(payload), &command); err != nil {
		log.Printf("❌ Flash统计请求解析失败: %v", err)
		return
	}

	// 检查是否是目标节点
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		return
	}

	log.Printf("📊 收到Flash统计请求: %s", command.CommandID)

	// 获取Flash Trade统计数据
	responseData := c.getFlashTradeStats()

	// 发送统计响应
	c.sendFlashStatsResponse(command.CommandID, responseData)
}

// handleAutoSellCommand 处理自动卖出命令
func (c *Client) handleAutoSellCommand(payload string) {
	// 首先检查节点是否已授权
	if !c.isAuthorized() {
		log.Printf("⚠️ 节点未授权，拒绝处理自动卖出命令")
		return
	}

	log.Printf("🔧 [调试] 收到自动卖出命令原始消息: %s", payload)

	var command binance.Command
	if err := json.Unmarshal([]byte(payload), &command); err != nil {
		log.Printf("❌ 自动卖出命令解析失败: %v", err)
		log.Printf("   原始消息: %s", payload)
		return
	}

	log.Printf("🔧 [调试] 自动卖出命令解析成功:")
	log.Printf("   命令ID: %s", command.CommandID)
	log.Printf("   命令类型: %s", command.CommandType)
	log.Printf("   目标节点: %s", command.TargetNode)
	log.Printf("   当前节点: %s", c.nodeID)

	// 检查是否是目标节点
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		log.Printf("🔄 自动卖出命令跳过: 目标节点=%s, 当前节点=%s", command.TargetNode, c.nodeID)
		return
	}

	log.Printf("📨 收到自动卖出命令: %s (类型: %s, 目标节点: %s)", command.CommandID, command.CommandType, command.TargetNode)

	// 将 Payload 转换为 map[string]interface{}
	payloadMap, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ 自动卖出命令Payload格式错误")
		return
	}

	action, ok := payloadMap["action"].(string)
	if !ok {
		log.Printf("❌ 自动卖出命令缺少action字段")
		return
	}

	switch action {
	case "start":
		c.handleAutoSellStart(&command)
	case "stop":
		c.handleAutoSellStop(&command)
	case "status":
		c.handleAutoSellStatus(&command)
	default:
		log.Printf("❌ 未知的自动卖出命令action: %s", action)
	}
}

// handleAutoSellStart 处理自动卖出启动命令
func (c *Client) handleAutoSellStart(command *binance.Command) {
	log.Printf("📨 收到自动卖出命令: %s (类型: %s, 目标节点: %s)", command.CommandID, command.CommandType, command.TargetNode)

	// 检查目标节点（与 Flash Trade 逻辑一致）
	if command.TargetNode != "" && command.TargetNode != c.nodeID {
		log.Printf("⚠️ 自动卖出命令目标节点 %s 与当前节点 %s 不匹配，跳过处理", command.TargetNode, c.nodeID)
		return
	}

	log.Printf("🚀 处理自动卖出启动命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ 自动卖出启动命令Payload格式错误")
		return
	}

	// 解析参数
	_, _ = payload["task_id"].(string) // taskID暂时不使用
	accountID, _ := payload["account_id"].(string)
	tokenAddress, _ := payload["token_address"].(string)
	monitorAmount, _ := payload["monitor_amount"].(float64)
	baseAsset, _ := payload["base_asset"].(string)

	// 验证必要参数
	if accountID == "" || tokenAddress == "" {
		log.Printf("❌ 自动卖出启动命令缺少必要参数: account_id=%s, token_address=%s", accountID, tokenAddress)
		c.sendAutoSellResponse(command.CommandID, "failed", "缺少必要参数")
		return
	}

	// 检查账号是否分配给当前节点
	if !c.isAccountAssignedToThisNode(accountID) {
		log.Printf("⚠️ 账号 %s 未分配给当前节点 %s，跳过处理", accountID, c.nodeID)
		c.sendAutoSellResponse(command.CommandID, "skipped", "账号未分配给当前节点")
		return
	}

	log.Printf("✅ 账号 %s 已分配给当前节点 %s，开始处理", accountID, c.nodeID)

	// 从Redis获取账号认证信息
	csrftoken, cookie, err := c.getAccountAuthFromRedis(accountID)
	if err != nil {
		log.Printf("❌ 获取账号 %s 认证信息失败: %v", accountID, err)
		c.sendAutoSellResponse(command.CommandID, "failed", fmt.Sprintf("获取账号认证信息失败: %v", err))
		return
	}

	// 设置默认值
	if baseAsset == "" {
		baseAsset = "ALPHA_251"
	}
	if monitorAmount <= 0 {
		monitorAmount = 1.0
	}

	log.Printf("💰 [%s] 启动自动卖出监控: 代币=%s, 监控数量=%.6f", accountID, tokenAddress, monitorAmount)

	// 调用 alpha_autosell.go 服务
	success := c.callAutoSellService("start", map[string]interface{}{
		"account_id":     accountID,
		"token_address":  tokenAddress,
		"monitor_amount": monitorAmount,
		"base_asset":     baseAsset,
		"csrftoken":      csrftoken,
		"cookie":         cookie,
	})

	if success {
		log.Printf("✅ [%s] 自动卖出监控启动成功: %s", accountID, tokenAddress)
		c.sendAutoSellResponse(command.CommandID, "started", fmt.Sprintf("账号 %s 的代币 %s 自动卖出监控已启动", accountID, tokenAddress))
	} else {
		log.Printf("❌ [%s] 自动卖出监控启动失败: %s", accountID, tokenAddress)
		c.sendAutoSellResponse(command.CommandID, "failed", "自动卖出监控启动失败")
	}
}

// handleAutoSellStop 处理自动卖出停止命令
func (c *Client) handleAutoSellStop(command *binance.Command) {
	log.Printf("🛑 处理自动卖出停止命令")

	// 将 Payload 转换为 map[string]interface{}
	payload, ok := command.Payload.(map[string]interface{})
	if !ok {
		log.Printf("❌ 自动卖出停止命令Payload格式错误")
		return
	}

	// 解析参数
	taskID, _ := payload["task_id"].(string)
	accountID, _ := payload["account_id"].(string)
	tokenAddress, _ := payload["token_address"].(string)

	log.Printf("💰 停止自动卖出监控: 任务ID=%s, 账号=%s, 代币=%s", taskID, accountID, tokenAddress)

	// 调用 alpha_autosell.go 服务
	success := c.callAutoSellService("stop", map[string]interface{}{
		"task_id":       taskID,
		"account_id":    accountID,
		"token_address": tokenAddress,
	})

	if success {
		log.Printf("✅ 自动卖出监控停止成功")
		c.sendAutoSellResponse(command.CommandID, "stopped", "自动卖出监控已停止")
	} else {
		log.Printf("❌ 自动卖出监控停止失败")
		c.sendAutoSellResponse(command.CommandID, "failed", "自动卖出监控停止失败")
	}
}

// handleAutoSellStatus 处理自动卖出状态查询命令
func (c *Client) handleAutoSellStatus(command *binance.Command) {
	log.Printf("📊 处理自动卖出状态查询命令")

	// 调用 alpha_autosell.go 服务获取状态
	statusData := c.getAutoSellStatus()

	// 发送状态响应
	c.sendAutoSellStatusResponse(command.CommandID, statusData)
}

// callAutoSellService 调用 alpha_autosell.go 服务
func (c *Client) callAutoSellService(action string, params map[string]interface{}) bool {
	var url string
	var method string
	var requestBody map[string]interface{}

	switch action {
	case "start":
		url = "http://localhost:8081/monitor"
		method = "POST"
		requestBody = params
	case "stop":
		url = "http://localhost:8081/stop"
		method = "POST"
		requestBody = params
	default:
		log.Printf("❌ 未知的自动卖出服务操作: %s", action)
		return false
	}

	// 序列化请求体
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("❌ 序列化自动卖出请求失败: %v", err)
		return false
	}

	// 创建HTTP请求
	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ 创建自动卖出HTTP请求失败: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求（自动卖出可能需要较长时间，增加超时）
	client := &http.Client{Timeout: 120 * time.Second} // 2分钟超时
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 调用自动卖出服务失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ 读取自动卖出服务响应失败: %v", err)
		return false
	}

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("❌ 解析自动卖出服务响应失败: %v", err)
		return false
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		log.Printf("✅ 自动卖出服务调用成功: %s", response["message"])

		// 如果响应包含交易结果数据，上报给主控端（参考 Flash Trade 实现）
		if data, ok := response["data"].(map[string]interface{}); ok {
			// 检查是否包含卖出结果
			if results, ok := response["results"].(map[string]interface{}); ok && len(results) > 0 {
				log.Printf("📊 检测到自动卖出结果，准备上报给主控端")
				go c.reportAutoSellResults(data, results)
			}
		} else {
			// 兼容旧格式：直接检查results字段
			if results, ok := response["results"].(map[string]interface{}); ok && len(results) > 0 {
				log.Printf("📊 检测到自动卖出结果（旧格式），准备上报给主控端")
				// 构建data字段
				data := map[string]interface{}{
					"account_id": params["account_id"],
					"task_id":    response["task_id"],
				}
				go c.reportAutoSellResults(data, results)
			}
		}

		return true
	} else {
		log.Printf("❌ 自动卖出服务调用失败: %s", response["message"])
		return false
	}
}

// getAutoSellStatus 获取自动卖出状态
func (c *Client) getAutoSellStatus() map[string]interface{} {
	// 调用 alpha_autosell.go 服务获取状态
	resp, err := http.Get("http://localhost:8081/status")
	if err != nil {
		log.Printf("❌ 获取自动卖出状态失败: %v", err)
		return map[string]interface{}{
			"error": "无法连接到自动卖出服务",
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ 读取自动卖出状态响应失败: %v", err)
		return map[string]interface{}{
			"error": "读取响应失败",
		}
	}

	var statusData map[string]interface{}
	if err := json.Unmarshal(body, &statusData); err != nil {
		log.Printf("❌ 解析自动卖出状态响应失败: %v", err)
		return map[string]interface{}{
			"error": "解析响应失败",
		}
	}

	return statusData
}

// sendAutoSellResponse 发送自动卖出响应
func (c *Client) sendAutoSellResponse(commandID, status, message string) {
	responseData := map[string]interface{}{
		"command_id": commandID,
		"node_id":    c.nodeID,
		"status":     status,
		"message":    message,
		"timestamp":  time.Now().Unix(),
	}

	if err := c.redisClient.PublishCommand("autosell_responses", responseData); err != nil {
		log.Printf("❌ 发送自动卖出响应失败: %v", err)
	} else {
		log.Printf("📤 自动卖出响应已发送: %s", status)
	}
}

// sendAutoSellStatusResponse 发送自动卖出状态响应
func (c *Client) sendAutoSellStatusResponse(commandID string, statusData map[string]interface{}) {
	responseData := map[string]interface{}{
		"command_id": commandID,
		"node_id":    c.nodeID,
		"status":     statusData,
		"timestamp":  time.Now().Unix(),
	}

	if err := c.redisClient.PublishCommand("autosell_status_responses", responseData); err != nil {
		log.Printf("❌ 发送自动卖出状态响应失败: %v", err)
	} else {
		log.Printf("📤 自动卖出状态响应已发送")
	}
}

// sendFlashResponse 发送Flash Trade响应
func (c *Client) sendFlashResponse(taskID, status, message string) {
	response := map[string]interface{}{
		"task_id":   taskID,
		"node_id":   c.nodeID,
		"status":    status,
		"message":   message,
		"timestamp": utils.GetCurrentTimestamp(),
	}

	if err := c.redisClient.PublishCommand("flash_responses", response); err != nil {
		log.Printf("❌ Flash响应发送失败: %v", err)
	} else {
		log.Printf("📤 Flash响应已发送: %s -> %s", taskID, status)
	}
}

// sendFlashStatsResponse 发送Flash Trade统计响应
func (c *Client) sendFlashStatsResponse(commandID string, stats map[string]interface{}) {
	// 构建响应数据
	responseData := map[string]interface{}{
		"node_id": c.nodeID,
		"stats":   stats,
	}

	// 构建响应命令
	response := binance.Command{
		CommandID:   commandID,
		CommandType: "flash_stats_response",
		TargetNode:  "",  // 不指定目标节点，发送给所有节点
		Payload:    responseData,
	}

	// 序列化响应
	responseBytes, err := json.Marshal(response)
	if err != nil {
		log.Printf("❌ 序列化Flash统计响应失败: %v", err)
		return
	}

	// 发布响应到Redis
	if c.redisClient != nil {
		if err := c.redisClient.PublishCommand("flash_stats_response", string(responseBytes)); err != nil {
			log.Printf("❌ 发布Flash统计响应失败: %v", err)
		} else {
			log.Printf("📤 已发送Flash统计响应")
		}
	}
}

// startAccountStatusReporting 启动账号状态上报
func (c *Client) startAccountStatusReporting() {
	ticker := time.NewTicker(10 * time.Second) // 每10秒上报一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.reportAccountStatus()
		case <-c.stopChan:
			return
		}
	}
}

// reportAccountStatus 上报账号状态
func (c *Client) reportAccountStatus() {
	// 检查Redis客户端是否可用
	if c.redisClient == nil {
		// 仅在调试模式下显示日志
		if os.Getenv("DEBUG") == "1" {
			log.Printf("⚠️ Redis客户端未初始化，跳过账号状态上报")
		}
		return
	}

	// 从Redis获取分配给当前节点的账号
	redisAccounts, err := c.getAccountsFromRedis()
	if err != nil {
		// 仅在调试模式下显示日志
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 从Redis获取账号失败: %v", err)
		}
		return
	}

	// 过滤出分配给当前节点的账号
	var nodeAccounts []*AccountInfo
	for _, acc := range redisAccounts {
		if acc.AssignedNode == c.nodeID {
			nodeAccounts = append(nodeAccounts, acc)
		}
	}

	if len(nodeAccounts) == 0 {
		// 仅在调试模式下显示日志
		if os.Getenv("DEBUG") == "1" {
			log.Printf("📭 节点暂无分配的账号，跳过状态上报")
		}
		return
	}

	// 获取Flash Trade统计数据（从 flash_trade.go 接口）
	flashStats := c.getFlashTradeStats()

	// 构建账号状态报告
	var accountReports []map[string]interface{}

	for _, account := range nodeAccounts {
		// 从Flash Trade统计中获取账号状态
		var isLooping bool
		var currentToken string
		var currentTaskID string
		var totalVolume, targetVolume, totalLoss, totalLossRate float64
		var tradeCount int
		var lastTradeTime, taskStartTime time.Time

		// 从flashStats中提取该账号的统计信息
		if flashStats != nil {
			if accountStats, exists := flashStats["accounts"].(map[string]interface{}); exists {
				if accountData, exists := accountStats[account.AccountID].(map[string]interface{}); exists {
					isLooping, _ = accountData["is_looping"].(bool)
					currentToken, _ = accountData["current_token"].(string)
					currentTaskID, _ = accountData["current_task_id"].(string)
					totalVolume, _ = accountData["total_volume"].(float64)
					targetVolume, _ = accountData["target_volume"].(float64)
					totalLoss, _ = accountData["total_loss"].(float64)
					totalLossRate, _ = accountData["total_loss_rate"].(float64)
					tradeCount, _ = accountData["trade_count"].(int)

					// 获取最后交易时间
					if lastTradeTimeUnix, ok := accountData["last_trade_time"].(float64); ok {
						lastTradeTime = time.Unix(int64(lastTradeTimeUnix), 0)
					}

					// 获取任务开始时间（默认为当前时间减去1小时）
					taskStartTime = time.Now().Add(-1 * time.Hour)
				}
			}
		}

		// 计算完成率
		var completionRate float64
		if targetVolume > 0 {
			completionRate = (totalVolume / targetVolume) * 100
		}

		// 默认过期时间为4.5天后
		expiresAt := time.Now().Add(4*24*time.Hour + 12*time.Hour).Unix()
		createdAt := time.Now().Add(-24 * time.Hour).Unix() // 假设创建于24小时前
		updatedAt := time.Now().Unix()                      // 当前时间

		report := map[string]interface{}{
			"account_id":      account.AccountID,
			"name":            account.Name,
			"status":          account.Status,
			"expires_at":      expiresAt,
			"is_looping":      isLooping,
			"current_token":   currentToken,
			"current_task_id": currentTaskID,
			"total_volume":    totalVolume,
			"target_volume":   targetVolume,
			"trade_count":     tradeCount,
			"total_loss":      totalLoss,
			"total_loss_rate": totalLossRate,
			"completion_rate": completionRate,
			"last_trade_time": lastTradeTime.Unix(),
			"task_start_time": taskStartTime.Unix(),
			"created_at":      createdAt,
			"updated_at":      updatedAt,
		}

		accountReports = append(accountReports, report)
	}

	// 计算活跃账号数量（正在循环的账号）
	activeCount := 0
	for _, account := range nodeAccounts {
		if flashStats != nil {
			if accountStats, exists := flashStats["accounts"].(map[string]interface{}); exists {
				if accountData, exists := accountStats[account.AccountID].(map[string]interface{}); exists {
					if isLooping, ok := accountData["is_looping"].(bool); ok && isLooping {
						activeCount++
					}
				}
			}
		}
	}

	// 构建完整的状态报告（匹配主控端期望的格式）
	statusReport := map[string]interface{}{
		"node_id":         c.nodeID,
		"timestamp":       float64(time.Now().Unix()),
		"account_reports": accountReports, // 主控端期望的字段名
		"summary": map[string]interface{}{
			"total_accounts":   len(nodeAccounts),
			"active_accounts":  activeCount,
			"expired_accounts": 0, // 从Redis获取的数据不会过期
		},
		"flash_stats": flashStats,
	}

	// 发送状态报告
	if err := c.redisClient.PublishCommand("account_status_reports", statusReport); err != nil {
		// 仅在调试模式下显示错误
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 账号状态上报失败: %v", err)
		}
	} else {
		// 仅在调试模式下显示成功信息
		if os.Getenv("DEBUG") == "1" {
			log.Printf("✅ 账号状态上报成功: %d个账号", len(accountReports))
		}
	}
}

// checkAuthorizationStatus 检查节点授权状态
func (c *Client) checkAuthorizationStatus() {
	// 无授权版本：直接返回，不进行授权检查
	log.Println("🔑 无授权版本：跳过授权检查")
	return
	
	// 以下是原始授权检查代码，已被禁用
	/*
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.isShuttingDown {
				return
			}

			// 检查授权状态
			req := &pb.CheckAuthRequest{
				NodeId: c.nodeID,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := c.masterClient.CheckAuth(ctx, req)
			cancel()

			if err != nil {
				log.Printf("❌ 授权检查失败: %v", err)
				continue
			}

			if !resp.IsAuthorized {
				log.Printf("⚠️ 节点未授权，将在30秒后退出")
				time.Sleep(30 * time.Second)
				log.Printf("🛑 节点未授权，程序退出")
				os.Exit(1)
			}
		case <-c.stopChan:
			return
		}
	}
	*/
}

// isAuthorized 检查节点是否已授权
func (c *Client) isAuthorized() bool {
	// 无授权版本：直接返回true，跳过授权检查
	log.Println("🔑 无授权版本：isAuthorized总是返回true")
	return true
	
	// 以下是原始授权检查代码，已被禁用
	/*
	// 只使用nodes集合进行授权检查
	if c.useMongoAuth && c.mongoAuthManager != nil {
		return c.checkNodesCollectionAuth()
	}
	
	// 授权系统未配置或不可用
	return false
	*/
}

// maskURI 掩盖URI中的敏感信息
func maskURI(uri string) string {
	if uri == "" {
		return "<empty>"
	}
	
	// 简单的掩盖方式，只显示URI的前10个和后10个字符
	if len(uri) > 20 {
		return uri[:10] + "..." + uri[len(uri)-10:]
	}
	
	return "<uri_masked>"
}

// checkNodesCollectionAuth 检查nodes集合中的授权状态
func (c *Client) checkNodesCollectionAuth() bool {
	if c.mongoAuthManager == nil || c.mongoAuthManager.GetClient() == nil {
		return false
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取nodes集合
	collection := c.mongoAuthManager.GetClient().Database("alpha").Collection("nodes")
	
	// 查询节点信息
	var nodeInfo struct {
		IsAuthorized int `bson:"is_authorized"`
	}
	
	// 执行查询
	err := collection.FindOne(ctx, bson.M{"node_id": c.nodeID}).Decode(&nodeInfo)
	if err != nil {
		return false
	}
	
	// 只检查is_authorized字段是否为1
	authorized := nodeInfo.IsAuthorized == 1
	
	return authorized
}

// registerNodeInMongoDB 在MongoDB中注册节点
func (c *Client) registerNodeInMongoDB() error {
	if c.mongoAuthManager == nil || c.mongoAuthManager.GetClient() == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取nodes集合
	collection := c.mongoAuthManager.GetClient().Database("alpha").Collection("nodes")
	
	// 获取节点信息
	hostname, _ := os.Hostname()
	ipAddress := getLocalIP()
	macAddress := utils.GetMACAddress()
	osInfo := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	
	// 创建节点信息
	nodeInfo := bson.M{
		"node_id":        c.nodeID,
		"user":           "",
		"binance_id":     "",
		"hostname":       hostname,
		"ip_address":     ipAddress,
		"mac_address":    macAddress,
		"os_info":        osInfo,
		"register_time":  time.Now(),
		"is_authorized":  0, // 初始未授权
		"last_heartbeat": time.Now(),
	}
	
	// 插入节点信息
	_, err := collection.InsertOne(ctx, nodeInfo)
	return err
}

// startAccountSync 启动账号同步
func (c *Client) startAccountSync() {
	// 立即执行一次同步
	c.syncAccountsFromMaster()

	// 每5分钟同步一次
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.syncAccountsFromMaster()
		case <-c.stopChan:
			return
		}
	}
}

// syncAccountsFromMaster 从主控端同步账号数据
func (c *Client) syncAccountsFromMaster() {
	// 构建主控端HTTP地址
	masterHTTPAddr := c.getMasterHTTPAddr()
	if masterHTTPAddr == "" {
		log.Printf("❌ 无法确定主控端HTTP地址")
		return
	}

	// 请求节点账号数据
	url := fmt.Sprintf("%s/api/v1/universal/accounts/node/%s", masterHTTPAddr, c.nodeID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		// 仅在调试模式下显示详细错误
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 请求主控端账号数据失败: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// 仅在调试模式下显示详细错误
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 主控端返回错误状态: %d", resp.StatusCode)
		}
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// 仅在调试模式下显示详细错误
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 读取响应失败: %v", err)
		}
		return
	}

	var response struct {
		Success  bool   `json:"success"`
		NodeID   string `json:"node_id"`
		Accounts []struct {
			ID           string `json:"id"`
			Name         string `json:"name"`
			Csrftoken    string `json:"csrftoken"`
			Cookie       string `json:"cookie"`
			Status       string `json:"status"`
			AssignedNode string `json:"assigned_node"`
		} `json:"accounts"`
		Count int `json:"count"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		// 仅在调试模式下显示详细错误
		if os.Getenv("DEBUG") == "1" {
			log.Printf("❌ 解析响应失败: %v", err)
		}
		return
	}

	// 同步账号到本地
	syncCount := 0
	for _, masterAccount := range response.Accounts {
		// 创建本地账号对象
		localAccount := &Account{
			ID:          masterAccount.ID,
			Name:        masterAccount.Name,
			Csrftoken:   masterAccount.Csrftoken,
			Cookie:      masterAccount.Cookie,
			Status:      masterAccount.Status,
			Description: fmt.Sprintf("从主控端同步"),
		}

		// 检查本地是否已存在
		if existingAccount, err := c.accountManager.GetAccount(masterAccount.ID); err == nil {
			// 更新现有账号
			if existingAccount.Csrftoken != masterAccount.Csrftoken ||
				existingAccount.Cookie != masterAccount.Cookie ||
				existingAccount.Status != masterAccount.Status {

				if err := c.accountManager.UpdateAccount(masterAccount.ID, localAccount); err != nil {
					// 仅在调试模式下显示详细错误
					if os.Getenv("DEBUG") == "1" {
						log.Printf("⚠️ 更新账号失败 %s: %v", masterAccount.ID, err)
					}
				} else {
					syncCount++
				}
			}
		} else {
			// 添加新账号
			if err := c.accountManager.AddAccount(localAccount); err != nil {
				// 仅在调试模式下显示详细错误
				if os.Getenv("DEBUG") == "1" {
					log.Printf("⚠️ 添加账号失败 %s: %v", masterAccount.ID, err)
				}
			} else {
				syncCount++
			}
		}
	}

	// 只在有账号同步时显示日志
	if syncCount > 0 {
		log.Printf("✅ 同步了 %d 个账号", syncCount)
	}
}

// getLocalIPAddress 获取本机真实IP地址
func (c *Client) getLocalIPAddress() string {
	// 方法1: 通过连接到主控端获取本机IP
	if realIP := c.getIPByConnectingToMaster(); realIP != "" {
		address := fmt.Sprintf("%s:%d", realIP, c.config.Server.Port)
		log.Printf("🌐 使用连接主控端获取的IP地址: %s", address)
		return address
	}

	// 方法2: 获取本机的外网IP
	if publicIP := c.getPublicIP(); publicIP != "" {
		address := fmt.Sprintf("%s:%d", publicIP, c.config.Server.Port)
		log.Printf("🌐 使用公网IP地址: %s", address)
		return address
	}

	// 方法3: 获取本机的内网IP
	if privateIP := c.getPrivateIP(); privateIP != "" {
		address := fmt.Sprintf("%s:%d", privateIP, c.config.Server.Port)
		log.Printf("🌐 使用内网IP地址: %s", address)
		return address
	}

	// 兜底: 使用localhost
	address := fmt.Sprintf("localhost:%d", c.config.Server.Port)
	log.Printf("⚠️ 无法获取真实IP，使用localhost: %s", address)
	return address
}

// getIPByConnectingToMaster 通过连接主控端获取本机IP
func (c *Client) getIPByConnectingToMaster() string {
	if c.config.Server.MasterAddr == "" {
		return ""
	}

	// 尝试连接到主控端
	conn, err := net.Dial("tcp", c.config.Server.MasterAddr)
	if err != nil {
		return ""
	}
	defer conn.Close()

	// 获取本地连接的IP地址
	localAddr := conn.LocalAddr().(*net.TCPAddr)
	return localAddr.IP.String()
}

// getPublicIP 获取公网IP（通过外部服务）
func (c *Client) getPublicIP() string {
	// 尝试多个服务获取公网IP
	services := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, service := range services {
		resp, err := client.Get(service)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}

			ip := strings.TrimSpace(string(body))
			if net.ParseIP(ip) != nil {
				return ip
			}
		}
	}

	return ""
}

// getPrivateIP 获取内网IP
func (c *Client) getPrivateIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip := ipnet.IP.String()
				// 优先返回内网IP段
				if strings.HasPrefix(ip, "192.168.") ||
					strings.HasPrefix(ip, "10.") ||
					strings.HasPrefix(ip, "172.") {
					return ip
				}
			}
		}
	}

	// 如果没有内网IP，返回第一个非回环IP
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return ""
}

// callFlashTradeAPI 调用 flash_trade.go 的 /trade 接口 - 带统一重试策略（兼容旧版本）
func (c *Client) callFlashTradeAPI(accountID, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, autoLoop bool, pricePrecision int, csrftoken, cookie string) bool {
	return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, 0)
}

// callFlashTradeAPIWithChain 调用 flash_trade.go 的 /trade 接口 - 支持链ID和价格模式
func (c *Client) callFlashTradeAPIWithChain(accountID, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, autoLoop bool, pricePrecision int, chainID, priceMode, csrftoken, cookie string) bool {
	return c.callFlashTradeAPIWithChainAndRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, chainID, priceMode, csrftoken, cookie, 0)
}

// callFlashTradeAPIWithChainAndRetry 带重试的 Flash Trade API 调用 - 支持链ID和价格模式
func (c *Client) callFlashTradeAPIWithChainAndRetry(accountID, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, autoLoop bool, pricePrecision int, chainID, priceMode, csrftoken, cookie string, retryCount int) bool {
	// 调用 flash_trade.go 的 /trade 接口

	// 构建请求体（完全按照 flash_trade.go 的 TradeRequest 结构）
	requestBody := map[string]interface{}{
		"token_address":   tokenAddress,
		"usdt_amount":     usdtAmount,
		"base_asset":      baseAsset,
		"csrftoken":       csrftoken,
		"cookie":          cookie,
		"target_volume":   targetVolume,
		"auto_loop":       autoLoop,
		"price_precision": pricePrecision,
		"chain_id":        chainID,    // 🔧 新增：传递链ID
		"price_mode":      priceMode,  // 🔧 新增：传递价格模式
		"min_delay":       1,          // 默认最小延迟
		"max_delay":       30,         // 默认最大延迟
	}

	// 转换为 JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("❌ [%s] JSON序列化失败: %v", accountID, err)
		// JSON序列化失败不重试
		return false
	}

	// 🔧 修复：支持配置Flash Trade服务地址
	flashTradeURL := os.Getenv("FLASH_TRADE_URL")
	if flashTradeURL == "" {
		flashTradeURL = "http://localhost:8080" // 默认本地服务
	}
	url := flashTradeURL + "/trade"

	// 调用Flash Trade服务

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ [%s] 创建HTTP请求失败: %v", accountID, err)
		// 请求创建失败不重试
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	// 设置超时
	client := &http.Client{
		Timeout: 30 * time.Second, // 30秒超时
	}

	log.Printf("🔧 [%s] 调用Flash Trade API: %s", accountID, url)
	log.Printf("   参数: 代币=%s, 金额=%.2f, 链=%s, 模式=%s", tokenAddress, usdtAmount, chainID, priceMode)

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ [%s] Flash Trade API调用失败: %v", accountID, err)

		// 网络错误重试逻辑
		maxRetries := 3
		if retryCount < maxRetries {
			log.Printf("🔄 [%s] 重试Flash Trade API调用 (%d/%d)", accountID, retryCount+1, maxRetries)
			time.Sleep(time.Duration(retryCount+1) * 2 * time.Second) // 递增延迟
			return c.callFlashTradeAPIWithChainAndRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, chainID, priceMode, csrftoken, cookie, retryCount+1)
		}
		return false
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ [%s] 读取Flash Trade响应失败: %v", accountID, err)
		return false
	}

	log.Printf("✅ [%s] Flash Trade API响应: %s", accountID, string(body))

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("❌ [%s] 解析Flash Trade响应失败: %v", accountID, err)
		return false
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		log.Printf("✅ [%s] Flash Trade执行成功", accountID)
		return true
	} else {
		message, _ := response["message"].(string)
		log.Printf("❌ [%s] Flash Trade执行失败: %s", accountID, message)
		return false
	}
}

// callFlashTradeAPIWithRetry 带重试的 Flash Trade API 调用（兼容旧版本）
func (c *Client) callFlashTradeAPIWithRetry(accountID, tokenAddress string, usdtAmount float64, baseAsset string, targetVolume float64, autoLoop bool, pricePrecision int, csrftoken, cookie string, retryCount int) bool {
	// 调用 flash_trade.go 的 /trade 接口

	// 构建请求体（完全按照 flash_trade.go 的 TradeRequest 结构）
	requestBody := map[string]interface{}{
		"token_address":   tokenAddress,
		"usdt_amount":     usdtAmount,
		"base_asset":      baseAsset,
		"csrftoken":       csrftoken,
		"cookie":          cookie,
		"target_volume":   targetVolume,
		"auto_loop":       autoLoop,
		"price_precision": pricePrecision,
		"min_delay":       1,  // 默认最小延迟
		"max_delay":       30, // 默认最大延迟
	}

	// 转换为 JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("❌ [%s] JSON序列化失败: %v", accountID, err)
		// JSON序列化失败不重试
		return false
	}

	// 🔧 修复：支持配置Flash Trade服务地址
	flashTradeURL := os.Getenv("FLASH_TRADE_URL")
	if flashTradeURL == "" {
		flashTradeURL = "http://localhost:8080" // 默认本地服务
	}
	url := flashTradeURL + "/trade"

	// 调用Flash Trade服务

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ [%s] 创建HTTP请求失败: %v", accountID, err)
		// 请求创建失败不重试
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second} // 增加超时时间到60秒
	resp, err := client.Do(req)
	if err != nil {
		// 🔧 统一异常处理：所有错误都使用统一重试策略
		// 计算等待时间：1分30秒 + retryCount * 30秒
		baseWaitTime := 90 * time.Second                               // 1分30秒
		additionalWait := time.Duration(retryCount) * 30 * time.Second // 累加30秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("❌ [%s] HTTP请求失败，%v后进行第%d次重试: %v", accountID, waitTime, retryCount+1, err)
		time.Sleep(waitTime)

		// 无限重试
		return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
	}
	defer resp.Body.Close()

	// 🔧 币安官方：监控权重使用情况
	c.logWeightUsage(resp, accountID)

	// 🔧 检查HTTP状态码
	if resp.StatusCode != 200 {
		var waitTime time.Duration

		// 🔧 币安官方：处理418错误 - IP被封禁
		if resp.StatusCode == 418 {
			retryAfter := resp.Header.Get("Retry-After")
			log.Printf("🚨 [%s] 检测到418错误 - IP已被封禁！", accountID)
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					banDuration := time.Duration(seconds) * time.Second
					log.Printf("🚨 [%s] IP封禁 %v，停止所有请求", accountID, banDuration)
					time.Sleep(banDuration)
				}
			} else {
				time.Sleep(5 * time.Minute) // 默认等待5分钟
			}
			// 重试
			return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
		}

		// 🔧 特殊处理429错误 - 支持Retry-After和指数退避
		if resp.StatusCode == 429 {
			retryAfter := resp.Header.Get("Retry-After")
			log.Printf("🚫 [%s] 检测到429错误", accountID)
			if retryAfter != "" {
				// Retry-After 响应头处理

				// 解析 Retry-After
				if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
					waitTime = time.Duration(seconds) * time.Second

				}
			}

			// 如果没有 Retry-After 或解析失败，使用指数退避
			if waitTime == 0 {
				// 简单的指数退避：2^retryCount 秒，最大64秒
				backoffSeconds := 1 << uint(retryCount) // 2^retryCount
				if backoffSeconds > 64 {
					backoffSeconds = 64
				}
				waitTime = time.Duration(backoffSeconds) * time.Second

			}
		} else {
			// 其他HTTP状态码错误使用统一重试策略
			baseWaitTime := 90 * time.Second                               // 1分30秒
			additionalWait := time.Duration(retryCount) * 30 * time.Second // 累加30秒
			waitTime = baseWaitTime + additionalWait
		}

		log.Printf("❌ [%s] HTTP状态码错误 %d，%v后进行第%d次重试", accountID, resp.StatusCode, waitTime, retryCount+1)
		time.Sleep(waitTime)

		// 无限重试
		return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
	}

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// 读取响应失败也重试
		baseWaitTime := 90 * time.Second                               // 1分30秒
		additionalWait := time.Duration(retryCount) * 30 * time.Second // 累加30秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("❌ [%s] 读取响应失败，%v后进行第%d次重试: %v", accountID, waitTime, retryCount+1, err)
		time.Sleep(waitTime)

		// 无限重试
		return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
	}

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		// JSON解析失败也重试
		baseWaitTime := 90 * time.Second                               // 1分30秒
		additionalWait := time.Duration(retryCount) * 30 * time.Second // 累加30秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("❌ [%s] 解析响应失败，%v后进行第%d次重试: %v", accountID, waitTime, retryCount+1, err)
		time.Sleep(waitTime)

		// 无限重试
		return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		// 🔧 重试成功时记录重试次数
		if retryCount > 0 {
			log.Printf("✅ [%s] flash_trade.go /trade 接口调用成功 (第%d次重试)", accountID, retryCount+1)
		} else {
			log.Printf("✅ [%s] flash_trade.go /trade 接口调用成功", accountID)
		}

		// 处理交易结果
		if message, ok := response["message"].(string); ok {
			log.Printf("✅ [%s] %s", accountID, message)
		}
		if _, ok := response["buy_price"].(float64); ok {
			// 买入价格处理
		}
		if _, ok := response["sell_price"].(float64); ok {
			// 卖出价格处理
		}
		if _, ok := response["profit"].(float64); ok {
			// 利润处理
		}
		if _, ok := response["execute_time"].(float64); ok {
			// 执行时间处理
		}

		return true
	} else {
		// 🔧 业务逻辑错误也使用统一重试策略
		var errorMessage string
		if message, ok := response["message"].(string); ok {
			errorMessage = message
		} else {
			errorMessage = "未知错误"
		}

		// 计算等待时间：1分30秒 + retryCount * 30秒
		baseWaitTime := 90 * time.Second                               // 1分30秒
		additionalWait := time.Duration(retryCount) * 30 * time.Second // 累加30秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("❌ [%s] flash_trade.go 交易失败，%v后进行第%d次重试: %s", accountID, waitTime, retryCount+1, errorMessage)

		time.Sleep(waitTime)

		// 无限重试
		return c.callFlashTradeAPIWithRetry(accountID, tokenAddress, usdtAmount, baseAsset, targetVolume, autoLoop, pricePrecision, csrftoken, cookie, retryCount+1)
	}
}

// 🔧 币安官方：记录权重使用情况
func (c *Client) logWeightUsage(resp *http.Response, accountID string) {
	// 检查所有权重相关的响应头
	for name, values := range resp.Header {
		if strings.HasPrefix(name, "X-Mbx-Used-Weight") {
			for _, value := range values {
				log.Printf("📊 [%s] 权重使用: %s = %s", accountID, name, value)

				// 解析权重值并警告
				if weight, err := strconv.Atoi(value); err == nil {
					if weight > 800 {
						log.Printf("⚠️ [%s] 权重使用过高: %d/1200，建议减缓请求", accountID, weight)
					} else if weight > 1000 {
						log.Printf("🚨 [%s] 权重使用危险: %d/1200，立即减缓请求！", accountID, weight)
					}
				}
			}
		}
	}
}

// startFlashTradeStatsReporting 启动 Flash Trade 统计上报
func (c *Client) startFlashTradeStatsReporting() {
	log.Printf("📊 启动 Flash Trade 统计上报")

	ticker := time.NewTicker(30 * time.Second) // 每30秒上报一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.reportFlashTradeStats()
		case <-c.stopChan:
			log.Printf("📊 Flash Trade 统计上报已停止")
			return
		}
	}
}

// reportFlashTradeStats 上报 Flash Trade 统计信息
func (c *Client) reportFlashTradeStats() {
	// 调用 flash_trade.go 的 /stats 接口获取统计信息
	stats := c.getFlashTradeStats()
	if stats == nil {
		return
	}

	// 将统计信息发送给主控端
	c.sendStatsToMaster(stats)
}

// getFlashTradeStats 从 flash_trade.go 获取统计信息
func (c *Client) getFlashTradeStats() map[string]interface{} {
	// 🔧 新增：先检查服务健康状态
	if !c.checkFlashTradeHealth() {
		log.Printf("⚠️ Flash Trade服务不可用，返回默认统计数据")
		return map[string]interface{}{
			"service_status": "unavailable",
			"全局统计": map[string]interface{}{
				"活跃账户数":  0,
				"总当前交易量": 0.0,
				"完成率":    0.0,
			},
			"账户统计": map[string]interface{}{},
		}
	}

	// 🔧 修改：使用快速统计接口，提高响应速度
	url := "http://localhost:8080/stats-fast"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("❌ 创建统计请求失败: %v", err)
		return nil
	}

	// 🔧 优化：使用快速接口，减少超时时间
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// 检查是否是连接被拒绝（服务已停止）
		if strings.Contains(err.Error(), "connection refused") {
			// flash_trade.go 服务已停止，静默处理
			return map[string]interface{}{
				"service_status": "stopped",
				"全局统计": map[string]interface{}{
					"活跃账户数":  0,
					"总当前交易量": 0.0,
					"完成率":    0.0,
				},
				"账户统计": map[string]interface{}{},
			}
		}

		// 检查是否是超时错误
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline exceeded") {
			log.Printf("⚠️ Flash Trade统计接口超时，返回默认数据: %v", err)
			return map[string]interface{}{
				"service_status": "timeout",
				"全局统计": map[string]interface{}{
					"活跃账户数":  0,
					"总当前交易量": 0.0,
					"完成率":    0.0,
				},
				"账户统计": map[string]interface{}{},
			}
		}

		log.Printf("❌ 获取统计信息失败: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("❌ 统计接口返回错误状态: %d", resp.StatusCode)
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ 读取统计响应失败: %v", err)
		return nil
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(body, &stats); err != nil {
		log.Printf("❌ 解析统计响应失败: %v", err)
		return nil
	}

	// 🔧 修复：添加详细的响应调试信息
	log.Printf("📊 Flash Trade统计接口响应内容: %s", string(body))

	// 🔧 修复：更详细的success字段检查
	log.Printf("📊 检查响应中的success字段:")
	log.Printf("   success字段值: %v (类型: %T)", stats["success"], stats["success"])
	log.Printf("   成功字段值: %v (类型: %T)", stats["成功"], stats["成功"])

	if success, ok := stats["success"].(bool); ok && success {
		log.Printf("📊 成功获取 Flash Trade 统计信息 (新格式)")

		// 打印关键统计信息 - 支持新格式
		if globalStats, ok := stats["global_stats"].(map[string]interface{}); ok {
			if activeAccounts, ok := globalStats["active_accounts"]; ok {
				log.Printf("   活跃账户数: %v", activeAccounts)
			}
			if totalVolume, ok := globalStats["total_current_volume"]; ok {
				log.Printf("   总当前交易量: %v", totalVolume)
			}
		}

		return stats
	} else if success, ok := stats["成功"].(bool); ok && success {
		// 兼容旧格式
		log.Printf("📊 成功获取 Flash Trade 统计信息 (旧格式)")

		// 打印关键统计信息 - 旧格式
		if globalStats, ok := stats["全局统计"].(map[string]interface{}); ok {
			if activeAccounts, ok := globalStats["活跃账户数"]; ok {
				log.Printf("   活跃账户数: %v", activeAccounts)
			}
			if totalVolume, ok := globalStats["总当前交易量"]; ok {
				log.Printf("   总当前交易量: %v", totalVolume)
			}
		}

		return stats
	} else {
		log.Printf("❌ 统计接口返回失败响应")
		log.Printf("   响应中的成功字段: %v (类型: %T)", stats["成功"], stats["成功"])
		log.Printf("   完整响应结构: %+v", stats)

		// 🔧 修复：即使success为false，也可能有有用的统计数据
		// 检查是否有全局统计数据
		if globalStats, ok := stats["全局统计"].(map[string]interface{}); ok {
			log.Printf("💡 虽然success为false，但发现全局统计数据，尝试使用")
			if activeAccounts, ok := globalStats["活跃账户数"]; ok {
				log.Printf("   活跃账户数: %v", activeAccounts)
			}
			// 如果有统计数据，仍然返回，让上层决定如何处理
			return stats
		}

		return nil
	}
}

// sendStatsToMaster 将统计信息发送给主控端
func (c *Client) sendStatsToMaster(stats map[string]interface{}) {
	if c.redisClient == nil {
		log.Printf("❌ Redis客户端未初始化，无法发送统计")
		return
	}

	// 🔧 修复：即使stats为nil，也发送基础节点信息
	if stats == nil {
		log.Printf("⚠️ Flash Trade统计信息为空，发送基础节点状态")
		// 创建基础统计信息
		stats = map[string]interface{}{
			"成功": false,
			"全局统计": map[string]interface{}{
				"活跃账户数":  0,
				"总当前交易量": 0,
				"完成率":    0,
				"总亏损":    0,
			},
			"状态": "重置或无活跃任务",
		}
	}

	// 构建统计报告
	statsReport := map[string]interface{}{
		"node_id":     c.nodeID,
		"timestamp":   float64(time.Now().Unix()), // 转换为float64以匹配主控端期望
		"flash_stats": stats,
		"source":      "flash_trade_service", // 标识来源是 flash_trade.go 服务
	}

	// 🔧 修复：提取关键统计信息到报告中 - 支持新旧格式
	if globalStats, ok := stats["global_stats"].(map[string]interface{}); ok {
		// 新格式（英文键名）
		statsReport["active_accounts"] = globalStats["active_accounts"]
		statsReport["total_volume"] = globalStats["total_current_volume"]
		statsReport["completion_rate"] = globalStats["completion_rate"]
		statsReport["total_loss"] = globalStats["total_loss"]
	} else if globalStats, ok := stats["全局统计"].(map[string]interface{}); ok {
		// 兼容旧格式（中文键名）
		statsReport["active_accounts"] = globalStats["活跃账户数"]
		statsReport["total_volume"] = globalStats["总当前交易量"]
		statsReport["completion_rate"] = globalStats["完成率"]
		statsReport["total_loss"] = globalStats["总亏损"]
	}

	// 通过 Redis 发送给主控端
	if err := c.redisClient.PublishCommand("flash_trade_stats", statsReport); err != nil {
		log.Printf("❌ Flash Trade 统计发送失败: %v", err)
	} else {
		log.Printf("📤 Flash Trade 统计已发送到主控端: 节点 %s", c.nodeID)
		log.Printf("   包含统计: 活跃账户=%v, 总交易量=%v",
			statsReport["active_accounts"], statsReport["total_volume"])
	}
}

// GetStats 获取节点统计信息（兼容性方法）
func (c *Client) GetStats() map[string]interface{} {
	// 获取 Flash Trade 统计
	flashStats := c.getFlashTradeStats()

	// 构建节点统计
	nodeStats := map[string]interface{}{
		"node_id":     c.nodeID,
		"status":      c.status,
		"start_time":  c.startTime.Unix(),
		"uptime":      time.Since(c.startTime).Seconds(),
		"flash_stats": flashStats,
	}

	// 如果有 Flash Trade 统计，提取关键信息
	if flashStats != nil {
		if globalStats, ok := flashStats["全局统计"].(map[string]interface{}); ok {
			nodeStats["active_accounts"] = globalStats["活跃账户数"]
			nodeStats["total_volume"] = globalStats["总当前交易量"]
			nodeStats["completion_rate"] = globalStats["完成率"]
		}
	}

	return nodeStats
}

// 🔧 新增：检查Flash Trade服务健康状态
func (c *Client) checkFlashTradeHealth() bool {
	url := "http://localhost:8080/health"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	// 使用较短的超时时间进行健康检查
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// 检查HTTP状态码
	return resp.StatusCode == 200
}

// getAccountsFromRedis 从 Redis 获取所有账号
func (c *Client) getAccountsFromRedis() (map[string]*AccountInfo, error) {
	if c.redisClient == nil {
		return nil, fmt.Errorf("Redis 客户端未初始化")
	}

	// 使用 account 包的方法从 Redis 获取账号
	nativeRedisClient := c.redisClient.GetNativeClient()
	redisAccounts, err := account.GetAllAccountsFromRedis(nativeRedisClient)
	if err != nil {
		log.Printf("❌ 从 Redis 获取账号失败: %v", err)
		return nil, fmt.Errorf("从 Redis 获取账号失败: %v", err)
	}

	// 转换为被控端使用的 AccountInfo 格式
	accounts := make(map[string]*AccountInfo)
	for id, acc := range redisAccounts {
		accounts[id] = &AccountInfo{
			AccountID:    acc.ID,
			Name:         acc.Name,
			Csrftoken:    acc.Csrftoken,
			Cookie:       acc.Cookie,
			Status:       acc.Status,
			Description:  acc.Description,
			AssignedNode: acc.AssignedNode, // 🔧 修复：包含节点分配信息
		}

	}

	// 从 Redis 获取到账号
	return accounts, nil
}

// getAccountFromRedis 从 Redis 获取单个账号
func (c *Client) getAccountFromRedis(accountID string) (*AccountInfo, error) {
	if c.redisClient == nil {
		return nil, fmt.Errorf("Redis 客户端未初始化")
	}

	// 使用 account 包的方法从 Redis 获取账号
	nativeRedisClient := c.redisClient.GetNativeClient()
	acc, err := account.GetAccountFromRedis(nativeRedisClient, accountID)
	if err != nil {
		log.Printf("❌ 从 Redis 获取账号 %s 失败: %v", accountID, err)
		return nil, fmt.Errorf("从 Redis 获取账号失败: %v", err)
	}

	// 显示账号的节点分配信息

	// 转换为被控端使用的 AccountInfo 格式
	return &AccountInfo{
		AccountID:    acc.ID,
		Name:         acc.Name,
		Csrftoken:    acc.Csrftoken,
		Cookie:       acc.Cookie,
		Status:       acc.Status,
		Description:  acc.Description,
		AssignedNode: acc.AssignedNode, // 🔧 修复：包含节点分配信息
	}, nil
}

// sendCommandAck 发送指令确认
func (c *Client) sendCommandAck(commandID, status, message string) {
	if c.redisClient == nil {
		log.Printf("⚠️ Redis客户端未初始化，无法发送指令确认")
		return
	}

	ack := map[string]interface{}{
		"command_id": commandID,
		"node_id":    c.nodeID,
		"status":     status,
		"message":    message,
		"timestamp":  time.Now().Unix(),
	}

	if err := c.redisClient.PublishCommand("flash_command_acks", ack); err != nil {
		log.Printf("❌ 发送指令确认失败: %v", err)
	} else {
		log.Printf("📨 指令确认已发送: 命令=%s, 状态=%s", commandID, status)
	}
}

// getAccountAuthFromRedis 从Redis获取账号认证信息
func (c *Client) getAccountAuthFromRedis(accountID string) (string, string, error) {
	// 如果使用MongoDB授权
	if c.useMongoAuth && c.mongoAuthManager != nil {
		// 从MongoDB获取账号信息
		account, err := c.mongoAuthManager.GetAccountAuth(accountID)
		if err != nil {
			return "", "", fmt.Errorf("从MongoDB获取账号认证信息失败: %v", err)
		}
		
		return account.Csrftoken, account.Cookie, nil
	}

	// 否则从Redis获取
	if c.redisClient == nil {
		return "", "", fmt.Errorf("Redis客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 尝试从flash_trade_auth键获取
	authKey := fmt.Sprintf("flash_trade_auth:%s", accountID)
	authData, err := c.redisClient.GetNativeClient().Get(ctx, authKey).Result()
	if err == nil {
		// 解析flash_trade认证数据
		var auth map[string]interface{}
		if err := json.Unmarshal([]byte(authData), &auth); err != nil {
			return "", "", fmt.Errorf("解析认证数据失败: %v", err)
		}

		csrftoken, _ := auth["csrftoken"].(string)
		cookie, _ := auth["cookie"].(string)

		if csrftoken != "" && cookie != "" {
			log.Printf("✅ 从flash_trade_auth获取账号 %s 认证信息成功", accountID)
			return csrftoken, cookie, nil
		}
	}

	// 尝试从account键获取
	accountKey := fmt.Sprintf("account:%s", accountID)
	accountData, err := c.redisClient.GetNativeClient().Get(ctx, accountKey).Result()
	if err != nil {
		return "", "", fmt.Errorf("账号不存在: %s", accountID)
	}

	// 解析账号数据
	var accountInfo map[string]interface{}
	if err := json.Unmarshal([]byte(accountData), &accountInfo); err != nil {
		return "", "", fmt.Errorf("解析账号数据失败: %v", err)
	}

	csrftoken, _ := accountInfo["csrftoken"].(string)
	cookie, _ := accountInfo["cookie"].(string)

	if csrftoken == "" || cookie == "" {
		return "", "", fmt.Errorf("账号认证信息不完整")
	}

	log.Printf("✅ 从account获取账号 %s 认证信息成功", accountID)
	return csrftoken, cookie, nil
}

// reportAutoSellResults 上报自动卖出结果给主控端（参考 Flash Trade 实现）
func (c *Client) reportAutoSellResults(data map[string]interface{}, results map[string]interface{}) {
	// 构建上报数据，格式与 Flash Trade 保持一致
	reportData := map[string]interface{}{
		"node_id":    c.nodeID,
		"timestamp":  time.Now().Unix(),
		"task_type":  "auto_sell",
		"account_id": data["account_id"],
		"task_data":  data,
		"results":    results,
	}

	// 发送到 Redis 频道（与 Flash Trade 使用相同的频道或新建专用频道）
	if err := c.redisClient.PublishCommand("autosell_results", reportData); err != nil {
		log.Printf("❌ 上报自动卖出结果失败: %v", err)
	} else {
		log.Printf("📊 自动卖出结果已上报给主控端: 账号=%s", data["account_id"])
	}
}

// isAccountAssignedToThisNode 检查账号是否分配给当前节点
func (c *Client) isAccountAssignedToThisNode(accountID string) bool {
	// 检查本地账号管理器中是否有该账号
	if c.accountManager != nil {
		accounts := c.accountManager.GetAllAccounts()
		for _, account := range accounts {
			if account.ID == accountID {
				log.Printf("🔧 [调试] 账号 %s 在本地账号管理器中找到", accountID)
				return true
			}
		}
	}

	// 如果本地没有，检查Redis中的账号分配信息
	if c.redisClient == nil {
		log.Printf("⚠️ Redis客户端未初始化，无法检查账号分配")
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 检查账号是否存在于Redis中
	accountKey := fmt.Sprintf("account:%s", accountID)
	exists, err := c.redisClient.GetNativeClient().Exists(ctx, accountKey).Result()
	if err != nil {
		log.Printf("⚠️ 检查账号 %s 是否存在时出错: %v", accountID, err)
		return false
	}

	if exists == 0 {
		log.Printf("⚠️ 账号 %s 在Redis中不存在", accountID)
		return false
	}

	// 获取账号信息
	accountData, err := c.redisClient.GetNativeClient().Get(ctx, accountKey).Result()
	if err != nil {
		log.Printf("⚠️ 获取账号 %s 信息时出错: %v", accountID, err)
		return false
	}

	// 解析账号数据
	var accountInfo map[string]interface{}
	if err := json.Unmarshal([]byte(accountData), &accountInfo); err != nil {
		log.Printf("⚠️ 解析账号 %s 数据时出错: %v", accountID, err)
		return false
	}

	// 检查账号是否分配给当前节点
	if assignedNode, ok := accountInfo["assigned_node"].(string); ok {
		if assignedNode == c.nodeID {
			log.Printf("✅ 账号 %s 已分配给当前节点 %s", accountID, c.nodeID)
			return true
		} else {
			log.Printf("⚠️ 账号 %s 已分配给其他节点 %s，当前节点: %s", accountID, assignedNode, c.nodeID)
			return false
		}
	}

	// 如果没有分配信息，默认允许处理（向后兼容）
	log.Printf("⚠️ 账号 %s 没有节点分配信息，默认允许处理", accountID)
	return true
}

// pauseFlashTradeAccount 暂停特定账户的Flash Trade
func (c *Client) pauseFlashTradeAccount(accountID string) bool {
	log.Printf("⏸️ 暂停Flash Trade账户: %s", accountID)

	// 调用 flash_trade.go 的暂停接口
	url := "http://localhost:8080/pause-status"

	payload := map[string]interface{}{
		"account_id": accountID,
		"action":     "pause",
		"duration":   300, // 暂停5分钟
		"reason":     "manual_pause_from_master",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("❌ 序列化暂停请求失败: %v", err)
		return false
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ 创建暂停请求失败: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 发送暂停请求失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Printf("✅ Flash Trade账户暂停成功: %s", accountID)
		return true
	} else {
		log.Printf("❌ Flash Trade账户暂停失败: %s, 状态码: %d", accountID, resp.StatusCode)
		return false
	}
}

// resumeFlashTradeAccount 恢复特定账户的Flash Trade
func (c *Client) resumeFlashTradeAccount(accountID string) bool {
	log.Printf("▶️ 恢复Flash Trade账户: %s", accountID)

	// 调用 flash_trade.go 的恢复接口
	url := fmt.Sprintf("http://localhost:8080/pause-status?account_id=%s", accountID)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("❌ 创建恢复请求失败: %v", err)
		return false
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 发送恢复请求失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Printf("✅ Flash Trade账户恢复成功: %s", accountID)
		return true
	} else {
		log.Printf("❌ Flash Trade账户恢复失败: %s, 状态码: %d", accountID, resp.StatusCode)
		return false
	}
}

// pauseAllFlashTrade 暂停所有Flash Trade账户
func (c *Client) pauseAllFlashTrade() int {
	log.Printf("⏸️ 暂停所有Flash Trade账户")

	// 首先获取所有账户列表
	stats := c.getFlashTradeStats()
	if stats == nil {
		log.Printf("❌ 获取Flash Trade统计失败")
		return 0
	}

	// 解析统计数据获取账户列表
	accountStats, ok := stats["账户统计"].(map[string]interface{})
	if !ok {
		log.Printf("❌ 无法解析账户统计数据")
		return 0
	}

	pausedCount := 0
	for _, accountData := range accountStats {
		if accountMap, ok := accountData.(map[string]interface{}); ok {
			if accountID, ok := accountMap["账户ID"].(string); ok {
				if c.pauseFlashTradeAccount(accountID) {
					pausedCount++
				}
			}
		}
	}

	log.Printf("✅ Flash Trade全部暂停完成，共暂停 %d 个账户", pausedCount)
	return pausedCount
}

// resumeAllFlashTrade 恢复所有Flash Trade账户
func (c *Client) resumeAllFlashTrade() int {
	log.Printf("▶️ 恢复所有Flash Trade账户")

	// 首先获取所有暂停的账户列表
	url := "http://localhost:8080/pause-status"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("❌ 创建暂停状态查询请求失败: %v", err)
		return 0
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 查询暂停状态失败: %v", err)
		return 0
	}
	defer resp.Body.Close()

	var pauseStatus map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&pauseStatus); err != nil {
		log.Printf("❌ 解析暂停状态响应失败: %v", err)
		return 0
	}

	// 解析暂停状态数据
	pauseStatusData, ok := pauseStatus["pause_status"].(map[string]interface{})
	if !ok {
		log.Printf("⚠️ 没有找到暂停的账户")
		return 0
	}

	resumedCount := 0
	for accountID := range pauseStatusData {
		if c.resumeFlashTradeAccount(accountID) {
			resumedCount++
		}
	}

	log.Printf("✅ Flash Trade全部恢复完成，共恢复 %d 个账户", resumedCount)
	return resumedCount
}

// getLocalIP 获取本地IP地址
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	
	return "unknown"
}
