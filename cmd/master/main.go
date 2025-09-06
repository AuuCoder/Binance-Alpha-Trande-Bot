package main

import (
	"alpha-autosell-bot/internal/account"
	"alpha-autosell-bot/internal/task"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"alpha-autosell-bot/internal/common"
	"alpha-autosell-bot/internal/master"
	"github.com/gin-gonic/gin"
)

// SellReportData 卖出上报数据（与被控端保持一致）
type SellReportData struct {
	NodeID           string  `json:"node_id"`
	AccountID        string  `json:"account_id"`
	TokenSymbol      string  `json:"token_symbol"`
	TokenAddress     string  `json:"token_address"`
	SoldAmount       float64 `json:"sold_amount"`        // 卖出的代币数量
	ActualUsdtAmount float64 `json:"actual_usdt_amount"` // 实际获得的USDT数量
	OrderID          string  `json:"order_id"`
	SellTime         string  `json:"sell_time"`
	TaskID           string  `json:"task_id"`
}

// NodeSellStats 节点卖出统计
type NodeSellStats struct {
	NodeID          string            `json:"node_id"`
	AccountCount    int               `json:"account_count"`
	TotalUsdtAmount float64           `json:"total_usdt_amount"`
	SellRecords     []*SellReportData `json:"sell_records"`
	LastUpdateTime  string            `json:"last_update_time"`
}

// MasterSellStats 主控端卖出统计
type MasterSellStats struct {
	TotalNodes      int                       `json:"total_nodes"`
	TotalAccounts   int                       `json:"total_accounts"`
	TotalUsdtAmount float64                   `json:"total_usdt_amount"`
	NodeStats       map[string]*NodeSellStats `json:"node_stats"`
	LastUpdateTime  string                    `json:"last_update_time"`
}

// 全局卖出统计数据
var (
	sellStatsManager = &SellStatsManager{
		nodeStats: make(map[string]*NodeSellStats),
	}
	sellStatsMutex sync.RWMutex
)

// SellStatsManager 卖出统计管理器
type SellStatsManager struct {
	nodeStats map[string]*NodeSellStats
}

// inferBaseAssetFromTokenAddress 根据代币地址推断 base_asset
func inferBaseAssetFromTokenAddress(tokenAddress string) string {
	// 根据已知的代币地址映射到对应的 base_asset
	tokenMappings := map[string]string{
		"0x06238c1b8e618abedf17669228dc95fb2d2e210b": "CAME",
		"0x6bf62ca91e397b5a7d1d6bce97d9092065d7a510": "CROSS",
		// 可以在这里添加更多的代币地址映射
		// "0x...": "ALPHA_261",
		// "0x...": "OTHER_TOKEN",
	}

	// 查找映射
	if baseAsset, exists := tokenMappings[strings.ToLower(tokenAddress)]; exists {
		log.Printf("🔍 [代币映射] 代币地址 %s 映射到 base_asset: %s", tokenAddress, baseAsset)
		return baseAsset
	}

	// 如果没有找到映射，使用默认值
	log.Printf("⚠️ [代币映射] 未找到代币地址 %s 的映射，使用默认 base_asset: ALPHA_251", tokenAddress)
	return "ALPHA_251"
}

// AddSellRecord 添加卖出记录
func (sm *SellStatsManager) AddSellRecord(data *SellReportData) {
	sellStatsMutex.Lock()
	defer sellStatsMutex.Unlock()

	// 获取或创建节点统计
	nodeStats, exists := sm.nodeStats[data.NodeID]
	if !exists {
		nodeStats = &NodeSellStats{
			NodeID:      data.NodeID,
			SellRecords: make([]*SellReportData, 0),
		}
		sm.nodeStats[data.NodeID] = nodeStats
	}

	// 添加卖出记录
	nodeStats.SellRecords = append(nodeStats.SellRecords, data)
	nodeStats.TotalUsdtAmount += data.ActualUsdtAmount
	nodeStats.LastUpdateTime = time.Now().Format(time.RFC3339)

	// 统计账号数量（去重）
	accountSet := make(map[string]bool)
	for _, record := range nodeStats.SellRecords {
		accountSet[record.AccountID] = true
	}
	nodeStats.AccountCount = len(accountSet)

	log.Printf("📊 [统计] 节点 %s 新增卖出记录: 账号=%s, USDT=%.6f, 总计USDT=%.6f",
		data.NodeID, data.AccountID, data.ActualUsdtAmount, nodeStats.TotalUsdtAmount)
}

// GetMasterStats 获取主控端统计数据
func (sm *SellStatsManager) GetMasterStats() *MasterSellStats {
	sellStatsMutex.RLock()
	defer sellStatsMutex.RUnlock()

	stats := &MasterSellStats{
		NodeStats:      make(map[string]*NodeSellStats),
		LastUpdateTime: time.Now().Format(time.RFC3339),
	}

	totalAccounts := make(map[string]bool)

	for nodeID, nodeStats := range sm.nodeStats {
		// 深拷贝节点统计
		statsCopy := &NodeSellStats{
			NodeID:          nodeStats.NodeID,
			AccountCount:    nodeStats.AccountCount,
			TotalUsdtAmount: nodeStats.TotalUsdtAmount,
			SellRecords:     make([]*SellReportData, len(nodeStats.SellRecords)),
			LastUpdateTime:  nodeStats.LastUpdateTime,
		}
		copy(statsCopy.SellRecords, nodeStats.SellRecords)

		stats.NodeStats[nodeID] = statsCopy
		stats.TotalUsdtAmount += nodeStats.TotalUsdtAmount

		// 统计总账号数（去重）
		for _, record := range nodeStats.SellRecords {
			totalAccounts[record.AccountID] = true
		}
	}

	stats.TotalNodes = len(sm.nodeStats)
	stats.TotalAccounts = len(totalAccounts)

	return stats
}

func main() {
	// 解析命令行参数
	configFile := flag.String("config", "config.json", "配置文件路径")
	flag.Parse()

	// 加载配置
	log.Printf("📋 使用配置文件: %s", *configFile)
	config, err := common.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 创建主控端服务器
	server, err := master.NewServer(config)
	if err != nil {
		log.Fatalf("❌ 创建主控端失败: %v", err)
	}

	// 启动主控端
	if err := server.Start(); err != nil {
		log.Fatalf("❌ 启动主控端失败: %v", err)
	}

	// 🔧 新增：启动自动重启监控器
	autoRestartManager := master.NewAutoRestartManager(server)
	go autoRestartManager.Start()
	defer autoRestartManager.Stop()

	// 启动HTTP服务器
	httpServer := setupHTTPServer(config, server, autoRestartManager)
	go func() {
		log.Printf("🌐 HTTP服务器监听: %s", config.GetServerAddr())
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ HTTP服务器错误: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 正在关闭服务器...")

	// 优雅关闭HTTP服务器
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("❌ HTTP服务器关闭失败: %v", err)
	}

	// 停止主控端
	server.Stop()
	log.Println("✅ 服务器已关闭")
}

// setupHTTPServer 设置HTTP服务器
func setupHTTPServer(config *common.Config, server *master.Server, autoRestartManager *master.AutoRestartManager) *http.Server {
	// 设置Gin模式
	if config.Server.Mode == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// CORS中间件
	router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		// 🔧 修复：不要强制设置Content-Type，让各个API自己设置

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// API路由
	api := router.Group("/api/v1")
	{
		// 健康检查
		api.GET("/health", handleHealth(config))

		// 节点管理（保留）
		api.GET("/nodes", handleGetNodes(server))
		api.POST("/nodes/register", handleRegisterNode(server))
		api.PUT("/nodes/:nodeId/name", handleUpdateNodeName(server))

		// 节点授权管理
		api.POST("/nodes/:nodeId/authorize", handleAuthorizeNode(server))
		api.DELETE("/nodes/:nodeId/authorize", handleRevokeNodeAuthorization(server))
		api.GET("/nodes/authorized", handleGetAuthorizedNodes(server))

		// Flash Trade 接口（保留 - Web 端需要）
		api.POST("/flash-trade/start", handleStartFlashTrade(server))
		api.POST("/flash-trade/stop", handleStopFlashTrade(server))
		api.POST("/flash-trade/pause-all", handlePauseAllFlashTrade(server))
		api.POST("/flash-trade/resume-all", handleResumeAllFlashTrade(server))
		api.GET("/flash-trade/stats", handleGetFlashTradeStats(server))
		api.GET("/flash-trade/realtime-stats", handleGetFlashTradeRealtimeStats(server))
		api.GET("/flash-trade/tasks", handleGetFlashTradeTasks(server))

		// 自动卖出接口
		api.POST("/auto-sell/start", handleStartAutoSell(server))
		api.POST("/auto-sell/stop", handleStopAutoSell(server))
		api.GET("/auto-sell/status", handleGetAutoSellStatus(server))
		api.GET("/auto-sell/tasks", handleGetAutoSellTasks(server))
		api.GET("/auto-sell/results", handleGetAutoSellResults(server))

		// 卖出统计接口
		api.POST("/sell-report", handleSellReport())
		api.GET("/sell-stats", handleGetSellStats())
		api.GET("/sell-stats/node/:nodeId", handleGetNodeSellStats())

		// 通用账号管理接口
		universal := api.Group("/universal")
		{
			// 账号管理
			accounts := universal.Group("/accounts")
			{
				accounts.GET("", handleGetUniversalAccounts(server))
				accounts.GET("/node/:nodeId", handleGetNodeAccounts(server))
				accounts.POST("", handleCreateUniversalAccount(server))
				accounts.PUT("/:accountId", handleUpdateUniversalAccount(server))
				accounts.DELETE("/:accountId", handleDeleteUniversalAccount(server))
				accounts.POST("/:accountId/assign/:nodeId", handleAssignAccountToNode(server))
				accounts.GET("/expiry", handleGetAccountExpiry(server))
				accounts.GET("/status", handleGetAccountStatus(server))
			}

			// 任务管理
			tasks := universal.Group("/tasks")
			{
				tasks.GET("", handleGetUniversalTasks(server))
				tasks.POST("", handleCreateUniversalTask(server))
				tasks.GET("/:taskId", handleGetUniversalTask(server))
				tasks.POST("/:taskId/start", handleStartUniversalTask(server))
				tasks.GET("/types", handleGetTaskTypes(server))
			}
		}
	}

	// 静态文件服务
	router.Static("/static", "./web/static")

	// 静态页面路由
	router.StaticFile("/", "./web/index.html")
	router.StaticFile("/nodes", "./web/nodes.html")
	router.StaticFile("/accounts", "./web/accounts.html")
	router.StaticFile("/tasks", "./web/tasks.html")
	router.StaticFile("/flash-trade", "./web/flash_trade.html")
	router.StaticFile("/account-status", "./web/account_status.html")
	router.StaticFile("/universal-management", "./web/universal_management.html")
	router.StaticFile("/auto-sell-results", "./web/auto_sell_results.html")

	// 隐藏的节点授权页面（需要特殊按键组合才能访问）
	router.StaticFile("/node-authorization", "./web/node_authorization.html")

	// 管理中心页面 - 重定向到通用管理中心
	router.GET("/dashboard", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/universal-management")
	})

	// 🔧 新增：自动重启管理API
	setupAutoRestartAPI(router, autoRestartManager)

	return &http.Server{
		Addr:         config.GetServerAddr(),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}

// handleHealth 健康检查
func handleHealth(config *common.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
			"version":   "1.0.0",
			"node_id":   config.Server.NodeID,
			"mode":      config.Server.Mode,
		})
	}
}

// handleGetNodes 获取所有节点
func handleGetNodes(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodes := server.GetNodes()
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"nodes":   nodes,
		})
	}
}

// handleRegisterNode 注册节点
func handleRegisterNode(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			NodeID       string            `json:"node_id" binding:"required"`
			Address      string            `json:"address"`
			FriendlyName string            `json:"friendly_name"`
			Metadata     map[string]string `json:"metadata"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		err := server.RegisterNodeSimple(req.NodeID, req.Address, req.FriendlyName, req.Metadata)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "节点注册成功",
			"node_id": req.NodeID,
		})
	}
}

// handleStartFlashTrade 启动Flash Trade
func handleStartFlashTrade(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			NodeID         string  `json:"node_id"`         // 目标节点ID，空表示所有节点
			AccountID      string  `json:"account_id"`      // 目标账号ID，空表示所有账号
			TokenAddress   string  `json:"token_address"`   // 代币地址
			USDTAmount     float64 `json:"usdt_amount"`     // USDT交易金额
			BaseAsset      string  `json:"base_asset"`      // 基础资产
			TargetVolume   float64 `json:"target_volume"`   // 目标交易额
			AutoLoop       bool    `json:"auto_loop"`       // 是否自动循环
			PricePrecision int32   `json:"price_precision"` // 价格精度位数
			ForceStart     bool    `json:"force_start"`     // 是否强制启动（忽略冲突检查）
			StopExisting   bool    `json:"stop_existing"`   // 是否停止现有任务
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 验证必要参数
		if req.TokenAddress == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "代币地址不能为空",
			})
			return
		}

		if req.USDTAmount <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "USDT金额必须大于0",
			})
			return
		}

		// 设置默认值
		if req.BaseAsset == "" {
			// 根据代币地址自动推断 base_asset
			req.BaseAsset = inferBaseAssetFromTokenAddress(req.TokenAddress)
		}
		if req.PricePrecision == 0 {
			req.PricePrecision = 8
		}

		// 检查是否有冲突的任务（除非强制启动）
		if !req.ForceStart {
			conflictCheck := checkFlashTradeConflicts(server, req.NodeID, req.AccountID, req.StopExisting)
			if conflictCheck.HasConflicts {
				if req.StopExisting {
					// 停止冲突的任务
					for _, taskID := range conflictCheck.ConflictingTasks {
						if err := server.GetTaskManager().StopTask(taskID); err != nil {
							log.Printf("⚠️ 停止冲突任务失败: %s, 错误: %v", taskID, err)
						} else {
							log.Printf("🛑 已停止冲突任务: %s", taskID)
						}
					}
				} else {
					// 返回冲突信息
					c.JSON(http.StatusConflict, gin.H{
						"success": false,
						"message": conflictCheck.Message,
						"conflicts": map[string]interface{}{
							"conflicting_tasks": conflictCheck.ConflictingTasks,
							"affected_accounts": conflictCheck.AffectedAccounts,
							"affected_nodes":    conflictCheck.AffectedNodes,
						},
						"suggestions": []string{
							"设置 force_start: true 强制启动新任务",
							"设置 stop_existing: true 停止现有任务后启动",
							"等待现有任务完成后再启动",
						},
					})
					return
				}
			}
		}

		// 构建目标账号列表
		var targetAccounts []string
		if req.AccountID != "" {
			// 支持逗号分隔的多个账号ID
			accountIDs := strings.Split(req.AccountID, ",")
			for _, accountID := range accountIDs {
				accountID = strings.TrimSpace(accountID) // 去除空格
				if accountID != "" {
					targetAccounts = append(targetAccounts, accountID)
				}
			}
		}

		// 构建目标节点列表
		var targetNodes []string
		if req.NodeID != "" {
			targetNodes = []string{req.NodeID}
		} else {
			// 如果没有指定节点，获取所有在线节点
			allNodes := server.GetNodes()
			for nodeID, nodeInfo := range allNodes {
				if nodeInfo.Status == "online" {
					targetNodes = append(targetNodes, nodeID)
				}
			}
			log.Printf("📡 未指定节点，将任务分发到所有在线节点: %v", targetNodes)
		}

		// 通过通用任务系统创建Flash Trade任务
		newTask := &task.UniversalTask{
			Name:           fmt.Sprintf("Flash Trade - %s", req.TokenAddress),
			Type:           "flash_trade",
			Description:    fmt.Sprintf("Web端启动的Flash Trade任务，代币: %s, 金额: %.2f USDT", req.TokenAddress, req.USDTAmount),
			TargetAccounts: targetAccounts,
			TargetNodes:    targetNodes,
			Parameters: map[string]interface{}{
				"token_address":   req.TokenAddress,
				"usdt_amount":     req.USDTAmount,
				"base_asset":      req.BaseAsset,
				"target_volume":   req.TargetVolume,
				"auto_loop":       req.AutoLoop,
				"price_precision": float64(req.PricePrecision),
			},
			Status:    task.TaskStatusPending,
			CreatedAt: time.Now(),
			Results:   make(map[string]interface{}),
		}

		log.Printf("🔧 [调试] 准备创建Flash Trade任务:")
		log.Printf("   任务名称: %s", newTask.Name)
		log.Printf("   目标节点: %v", newTask.TargetNodes)
		log.Printf("   目标账号: %v", newTask.TargetAccounts)
		log.Printf("   任务参数: %+v", newTask.Parameters)

		err := server.GetTaskManager().CreateTask(newTask)
		if err != nil {
			log.Printf("❌ [调试] 创建Flash Trade任务失败: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": "创建Flash Trade任务失败: " + err.Error(),
			})
			return
		}

		log.Printf("✅ [调试] Flash Trade任务创建成功，任务ID: %s", newTask.ID)

		// 立即启动任务
		log.Printf("🚀 [调试] 准备启动Flash Trade任务: %s", newTask.ID)
		if err := server.GetTaskManager().StartTask(newTask.ID); err != nil {
			log.Printf("❌ [调试] 启动Flash Trade任务失败: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": "启动Flash Trade任务失败: " + err.Error(),
			})
			return
		}

		log.Printf("✅ [调试] Flash Trade任务启动成功: %s", newTask.ID)

		c.JSON(http.StatusOK, gin.H{
			"success":           true,
			"message":           "Flash Trade任务创建并启动成功",
			"task_id":           newTask.ID,
			"affected_nodes":    targetNodes,
			"affected_accounts": targetAccounts,
		})
	}
}

// handleStopFlashTrade 停止Flash Trade（通过通用任务系统）
func handleStopFlashTrade(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			TaskID    string `json:"task_id"`
			NodeID    string `json:"node_id"`
			AccountID string `json:"account_id"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		stoppedCount := 0

		if req.TaskID != "" {
			// 停止指定任务
			if err := server.GetTaskManager().StopTask(req.TaskID); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"success": false,
					"message": "停止任务失败: " + err.Error(),
				})
				return
			}
			stoppedCount = 1
		} else {
			// 停止所有Flash Trade任务
			allTasks := server.GetTaskManager().GetAllTasks()

			for _, t := range allTasks {
				// 只处理Flash Trade任务
				if t.Type != "flash_trade" {
					continue
				}

				// 只处理运行中的任务
				if t.Status != task.TaskStatusRunning {
					continue
				}

				// 检查节点和账号过滤条件
				if req.NodeID != "" && !contains(t.TargetNodes, req.NodeID) {
					continue
				}
				if req.AccountID != "" && !contains(t.TargetAccounts, req.AccountID) {
					continue
				}

				if err := server.GetTaskManager().StopTask(t.ID); err != nil {
					log.Printf("❌ 停止任务失败: %s, 错误: %v", t.ID, err)
				} else {
					stoppedCount++
				}
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success":       true,
			"message":       fmt.Sprintf("成功停止 %d 个Flash Trade任务", stoppedCount),
			"stopped_count": stoppedCount,
		})
	}
}

// contains 检查字符串切片是否包含指定字符串
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// handleGetFlashTradeStats 获取Flash Trade统计（从通用任务系统）
func handleGetFlashTradeStats(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Query("node_id")
		accountID := c.Query("account_id")

		// 从通用任务系统获取Flash Trade任务统计
		allTasks := server.GetTaskManager().GetAllTasks()

		// 计算统计信息
		globalStats := map[string]interface{}{
			"active_accounts":      0,
			"total_current_volume": 0.0,
			"completion_rate":      0.0,
			"total_loss":           0.0,
			"total_loss_rate":      0.0,
			"projected_loss_100k":  0.0,
		}

		accountStats := make([]map[string]interface{}, 0)
		nodeStats := make([]map[string]interface{}, 0)

		activeAccounts := make(map[string]bool)
		totalVolume := 0.0
		totalProfit := 0.0
		completedTasks := 0

		for _, t := range allTasks {
			// 只处理Flash Trade任务
			if t.Type != "flash_trade" {
				continue
			}

			// 过滤条件
			if nodeID != "" && !contains(t.TargetNodes, nodeID) {
				continue
			}
			if accountID != "" && !contains(t.TargetAccounts, accountID) {
				continue
			}

			// 统计活跃账号
			for _, acc := range t.TargetAccounts {
				activeAccounts[acc] = true
			}

			// 统计交易量和利润
			if usdtAmount, ok := t.Parameters["usdt_amount"].(float64); ok {
				totalVolume += usdtAmount
			}

			if t.Status == task.TaskStatusCompleted {
				completedTasks++
				if totalProfitVal, ok := t.Results["total_profit"].(float64); ok {
					totalProfit += totalProfitVal
				}
			}
		}

		// 计算Flash Trade任务总数
		flashTradeTaskCount := 0
		for _, t := range allTasks {
			if t.Type == "flash_trade" {
				flashTradeTaskCount++
			}
		}

		// 更新全局统计
		globalStats["active_accounts"] = len(activeAccounts)
		globalStats["total_current_volume"] = totalVolume
		if flashTradeTaskCount > 0 {
			globalStats["completion_rate"] = float64(completedTasks) / float64(flashTradeTaskCount) * 100
		}
		globalStats["total_loss"] = -totalProfit // 负利润表示亏损
		if totalVolume > 0 {
			globalStats["total_loss_rate"] = (-totalProfit / totalVolume) * 10000      // 万分比
			globalStats["projected_loss_100k"] = (-totalProfit / totalVolume) * 100000 // 10万预计亏损
		}

		c.JSON(http.StatusOK, gin.H{
			"success":       true,
			"global_stats":  globalStats,
			"account_stats": accountStats,
			"node_stats":    nodeStats,
		})
	}
}

// handleGetFlashTradeTasks 获取Flash Trade任务列表（从通用任务系统）
func handleGetFlashTradeTasks(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从通用任务系统获取Flash Trade任务
		allTasks := server.GetTaskManager().GetAllTasks()

		// 转换为Web端期望的格式
		var webTasks []map[string]interface{}
		for _, t := range allTasks {
			// 只处理Flash Trade任务
			if t.Type != "flash_trade" {
				continue
			}
			// 从任务参数中提取信息
			tokenAddress, _ := t.Parameters["token_address"].(string)
			usdtAmount, _ := t.Parameters["usdt_amount"].(float64)
			baseAsset, _ := t.Parameters["base_asset"].(string)

			webTask := map[string]interface{}{
				"task_id":         t.ID,
				"token_address":   tokenAddress,
				"usdt_amount":     usdtAmount,
				"base_asset":      baseAsset,
				"status":          t.Status,
				"start_time":      t.CreatedAt,
				"end_time":        t.CompletedAt,
				"target_nodes":    t.TargetNodes,
				"target_accounts": t.TargetAccounts,
				"results":         t.Results,
			}

			webTasks = append(webTasks, webTask)
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"count":   len(webTasks),
			"tasks":   webTasks,
		})
	}
}

// ===== 通用账号管理 API =====

// handleGetUniversalAccounts 获取所有通用账号
func handleGetUniversalAccounts(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountManager := server.GetAccountManager()
		accounts := accountManager.GetAllAccounts()

		c.JSON(http.StatusOK, gin.H{
			"success":  true,
			"accounts": accounts,
			"count":    len(accounts),
		})
	}
}

// handleGetNodeAccounts 获取特定节点的账号
func handleGetNodeAccounts(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Param("nodeId")
		if nodeID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "节点ID不能为空",
			})
			return
		}

		accountManager := server.GetAccountManager()
		accounts := accountManager.GetNodeAccounts(nodeID)

		c.JSON(http.StatusOK, gin.H{
			"success":  true,
			"node_id":  nodeID,
			"accounts": accounts,
			"count":    len(accounts),
		})
	}
}

// handleCreateUniversalAccount 创建通用账号
func handleCreateUniversalAccount(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			ID           string `json:"id" binding:"required"`
			Name         string `json:"name"`
			Csrftoken    string `json:"csrftoken" binding:"required"`
			Cookie       string `json:"cookie" binding:"required"`
			AssignedNode string `json:"assigned_node"`
			Description  string `json:"description"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 检查分配的节点是否已授权
		if req.AssignedNode != "" {
			if !server.IsNodeAuthorized(req.AssignedNode) {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": fmt.Sprintf("节点 %s 未授权，无法分配账号", req.AssignedNode),
				})
				return
			}
		}

		accountManager := server.GetAccountManager()
		account := &account.UniversalAccount{
			ID:           req.ID,
			Name:         req.Name,
			Csrftoken:    req.Csrftoken,
			Cookie:       req.Cookie,
			AssignedNode: req.AssignedNode,
			Description:  req.Description,
		}

		err := accountManager.AddAccount(account)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success":    true,
			"message":    "账号创建成功",
			"account_id": req.ID,
		})
	}
}

// handleUpdateUniversalAccount 更新通用账号
func handleUpdateUniversalAccount(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.Param("accountId")

		var req struct {
			Name         string `json:"name"`
			Csrftoken    string `json:"csrftoken"`
			Cookie       string `json:"cookie"`
			AssignedNode string `json:"assigned_node"`
			Description  string `json:"description"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 检查分配的节点是否已授权
		if req.AssignedNode != "" {
			if !server.IsNodeAuthorized(req.AssignedNode) {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": fmt.Sprintf("节点 %s 未授权，无法分配账号", req.AssignedNode),
				})
				return
			}
		}

		accountManager := server.GetAccountManager()

		// 获取更新前的账号信息，用于检查节点分配是否变更
		oldAccount, err := accountManager.GetAccount(accountID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "账号不存在: " + err.Error(),
			})
			return
		}

		account := &account.UniversalAccount{
			ID:           accountID,
			Name:         req.Name,
			Csrftoken:    req.Csrftoken,
			Cookie:       req.Cookie,
			AssignedNode: req.AssignedNode,
			Description:  req.Description,
		}

		err = accountManager.UpdateAccount(accountID, account)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		// 更新成功后，处理节点同步
		go func() {
			// 如果节点分配发生变更
			if oldAccount.AssignedNode != account.AssignedNode {
				// 从旧节点删除账号
				if oldAccount.AssignedNode != "" {
					server.RemoveAccountFromNode(accountID, oldAccount.AssignedNode)
				}

				// 同步到新节点
				if account.AssignedNode != "" {
					server.SyncAccountToNode(accountID, account.AssignedNode)
				}
			} else if account.AssignedNode != "" {
				// 节点未变更，但需要更新账号信息
				server.SyncAccountToNode(accountID, account.AssignedNode)
			}
		}()

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "账号更新成功",
		})
	}
}

// handleDeleteUniversalAccount 删除通用账号
func handleDeleteUniversalAccount(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.Param("accountId")

		accountManager := server.GetAccountManager()
		err := accountManager.DeleteAccount(accountID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "账号删除成功",
		})
	}
}

// handleAssignAccountToNode 分配账号到节点
func handleAssignAccountToNode(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.Param("accountId")
		nodeID := c.Param("nodeId")

		// 检查节点是否已授权
		if !server.IsNodeAuthorized(nodeID) {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": fmt.Sprintf("节点 %s 未授权，无法分配账号", nodeID),
			})
			return
		}

		accountManager := server.GetAccountManager()
		err := accountManager.AssignAccountToNode(accountID, nodeID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		// 分配成功后立即同步到节点
		go server.SyncAccountToNode(accountID, nodeID)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "账号分配成功",
		})
	}
}

// handleGetAccountExpiry 获取账号过期信息
func handleGetAccountExpiry(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountManager := server.GetAccountManager()
		accounts := accountManager.GetAllAccounts()

		now := time.Now()

		// 分类账号
		var expired []map[string]interface{}
		var nearExpiry []map[string]interface{}
		var active []map[string]interface{}

		for id, acc := range accounts {
			accountInfo := map[string]interface{}{
				"account_id":    id,
				"name":          acc.Name,
				"status":        acc.Status,
				"assigned_node": acc.AssignedNode,
				"expires_at":    acc.ExpiresAt.Format("2006-01-02 15:04:05"),
				"created_at":    acc.CreatedAt.Format("2006-01-02 15:04:05"),
				"updated_at":    acc.UpdatedAt.Format("2006-01-02 15:04:05"),
			}

			// 计算剩余天数
			if !acc.ExpiresAt.IsZero() {
				remainingHours := acc.ExpiresAt.Sub(now).Hours()
				remainingDays := remainingHours / 24
				accountInfo["remaining_days"] = remainingDays
				accountInfo["remaining_hours"] = remainingHours

				if remainingDays < 0 {
					// 已过期
					accountInfo["status_desc"] = "已过期"
					expired = append(expired, accountInfo)
				} else if remainingDays <= 1 {
					// 即将过期（1天内）
					accountInfo["status_desc"] = "即将过期"
					nearExpiry = append(nearExpiry, accountInfo)
				} else {
					// 未过期
					accountInfo["status_desc"] = "正常"
					active = append(active, accountInfo)
				}
			} else {
				// 没有设置过期时间
				accountInfo["status_desc"] = "无过期时间"
				accountInfo["remaining_days"] = -1
				active = append(active, accountInfo)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"summary": map[string]interface{}{
				"total_accounts":    len(accounts),
				"active_count":      len(active),
				"near_expiry_count": len(nearExpiry),
				"expired_count":     len(expired),
			},
			"accounts": map[string]interface{}{
				"active":      active,
				"near_expiry": nearExpiry,
				"expired":     expired,
			},
		})
	}
}

// ===== 通用任务管理 API =====

// handleGetUniversalTasks 获取所有通用任务
func handleGetUniversalTasks(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskManager := server.GetTaskManager()
		tasks := taskManager.GetAllTasks()

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"tasks":   tasks,
			"count":   len(tasks),
		})
	}
}

// handleCreateUniversalTask 创建通用任务
func handleCreateUniversalTask(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Name           string                 `json:"name" binding:"required"`
			Type           string                 `json:"type" binding:"required"`
			Description    string                 `json:"description"`
			TargetAccounts []string               `json:"target_accounts"`
			Priority       int                    `json:"priority"`
			Parameters     map[string]interface{} `json:"parameters"`
			CreatedBy      string                 `json:"created_by"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		taskManager := server.GetTaskManager()
		newTask := &task.UniversalTask{
			Name:           req.Name,
			Type:           task.TaskType(req.Type),
			Description:    req.Description,
			TargetAccounts: req.TargetAccounts,
			Priority:       req.Priority,
			Parameters:     req.Parameters,
			CreatedBy:      req.CreatedBy,
		}

		err := taskManager.CreateTask(newTask)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "任务创建成功",
			"task":    newTask,
		})
	}
}

// handleGetUniversalTask 获取单个通用任务
func handleGetUniversalTask(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskID := c.Param("taskId")

		taskManager := server.GetTaskManager()
		task, err := taskManager.GetTask(taskID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"task":    task,
		})
	}
}

// handleStartUniversalTask 启动通用任务
func handleStartUniversalTask(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskID := c.Param("taskId")

		taskManager := server.GetTaskManager()
		err := taskManager.StartTask(taskID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "任务启动成功",
		})
	}
}

// handleGetTaskTypes 获取任务类型列表
func handleGetTaskTypes(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 返回支持的任务类型
		types := []string{"flash_trade", "auto_sell", "batch_trade"}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"types":   types,
		})
	}
}

// handleAuthorizeNode 授权节点
func handleAuthorizeNode(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Param("nodeId")
		if nodeID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "节点ID不能为空",
			})
			return
		}

		err := server.AuthorizeNode(nodeID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": fmt.Sprintf("节点 %s 授权成功", nodeID),
		})
	}
}

// handleRevokeNodeAuthorization 撤销节点授权
func handleRevokeNodeAuthorization(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Param("nodeId")
		if nodeID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "节点ID不能为空",
			})
			return
		}

		err := server.RevokeNodeAuthorization(nodeID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": fmt.Sprintf("节点 %s 授权已撤销", nodeID),
		})
	}
}

// handleUpdateNodeName 更新节点名称
func handleUpdateNodeName(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Param("nodeId")

		var req struct {
			FriendlyName string `json:"friendly_name" binding:"required"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 验证友好名称长度
		if len(req.FriendlyName) > 50 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "节点名称不能超过50个字符",
			})
			return
		}

		err := server.UpdateNodeName(nodeID, req.FriendlyName)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "节点名称更新成功",
		})
	}
}

// handleGetAuthorizedNodes 获取已授权的节点列表
func handleGetAuthorizedNodes(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		authorizedNodes, err := server.GetAuthorizedNodes()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		// 获取所有节点信息
		allNodes := server.GetNodes()

		// 构建授权节点的详细信息
		var nodeDetails []gin.H
		for _, nodeID := range authorizedNodes {
			if nodeInfo, exists := allNodes[nodeID]; exists {
				nodeDetails = append(nodeDetails, gin.H{
					"node_id":        nodeID,
					"friendly_name":  nodeInfo.FriendlyName,
					"status":         nodeInfo.Status,
					"last_heartbeat": nodeInfo.LastHeartbeat.Unix(),
					"authorized":     true,
				})
			} else {
				// 节点已授权但不在线
				nodeDetails = append(nodeDetails, gin.H{
					"node_id":        nodeID,
					"friendly_name":  nodeID,
					"status":         "offline",
					"last_heartbeat": 0,
					"authorized":     true,
				})
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"nodes":   nodeDetails,
		})
	}
}

// handleGetFlashTradeRealtimeStats 获取实时 Flash Trade 统计（从被控端上报）
func handleGetFlashTradeRealtimeStats(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Query("node_id")

		// 获取被控端上报的实时统计（只包含活跃节点的最新数据）
		nodeStats := server.GetNodesFlashTradeStats(nodeID)

		log.Printf("📊 实时统计API调用: 找到 %d 个活跃节点的数据", len(nodeStats))

		// 获取任务系统中的Flash Trade任务状态
		taskStats := getFlashTradeTaskStats(server)

		// 构建汇总统计
		globalSummary := map[string]interface{}{
			"总节点数":    len(nodeStats),
			"活跃账户数":   0,
			"总当前交易量":  0.0,
			"总目标交易量":  0.0,
			"总亏损":     0.0,
			"完成率":     0.0,
			"10万预计亏损": 0.0,
			"任务状态统计":  taskStats, // 添加任务状态信息
		}

		totalActiveAccounts := 0
		totalCurrentVolume := 0.0
		totalTargetVolume := 0.0
		totalLoss := 0.0

		// 汇总所有节点的统计 - 支持新旧两种格式
		for _, nodeData := range nodeStats {
			if nodeMap, ok := nodeData.(map[string]interface{}); ok {
				// 尝试新格式（英文键名）
				if globalStats, exists := nodeMap["global_stats"]; exists {
					if gs, ok := globalStats.(map[string]interface{}); ok {
						// 🔧 修复：支持int和float64两种类型的活跃账户数
						if activeAccounts, ok := gs["active_accounts"].(int); ok {
							totalActiveAccounts += activeAccounts
						} else if activeAccounts, ok := gs["active_accounts"].(float64); ok {
							totalActiveAccounts += int(activeAccounts)
						}
						if currentVolume, ok := gs["total_current_volume"].(float64); ok {
							totalCurrentVolume += currentVolume
						}
						if targetVolume, ok := gs["total_target_volume"].(float64); ok {
							totalTargetVolume += targetVolume
						}
						if loss, ok := gs["total_loss"].(float64); ok {
							totalLoss += loss
						}
					}
				} else if globalStats, exists := nodeMap["全局统计"]; exists {
					// 兼容旧格式（中文键名）
					if gs, ok := globalStats.(map[string]interface{}); ok {
						// 🔧 修复：支持int和float64两种类型的活跃账户数
						if activeAccounts, ok := gs["活跃账户数"].(int); ok {
							totalActiveAccounts += activeAccounts
						} else if activeAccounts, ok := gs["活跃账户数"].(float64); ok {
							totalActiveAccounts += int(activeAccounts)
						}
						if currentVolume, ok := gs["总当前交易量"].(float64); ok {
							totalCurrentVolume += currentVolume
						}
						if targetVolume, ok := gs["总目标交易量"].(float64); ok {
							totalTargetVolume += targetVolume
						}
						if loss, ok := gs["总亏损"].(float64); ok {
							totalLoss += loss
						}
					}
				}
			}
		}

		// 更新汇总统计
		globalSummary["活跃账户数"] = totalActiveAccounts
		globalSummary["总当前交易量"] = totalCurrentVolume
		globalSummary["总目标交易量"] = totalTargetVolume
		globalSummary["总亏损"] = totalLoss

		if totalTargetVolume > 0 {
			globalSummary["完成率"] = (totalCurrentVolume / totalTargetVolume) * 100
		}
		if totalCurrentVolume > 0 {
			globalSummary["10万预计亏损"] = (totalLoss / totalCurrentVolume) * 100000
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data": map[string]interface{}{
				"全局汇总":  globalSummary,
				"节点统计":  nodeStats,
				"数据来源":  "flash_trade_service", // 标识数据来源
				"更新时间":  time.Now().Unix(),
				"数据说明":  "只包含活跃节点的实时数据（心跳<2分钟，统计<30秒）",
				"活跃节点数": len(nodeStats),
			},
		})
	}
}

// getFlashTradeTaskStats 获取Flash Trade任务状态统计
func getFlashTradeTaskStats(server *master.Server) map[string]interface{} {
	allTasks := server.GetTaskManager().GetAllTasks()

	taskStats := map[string]interface{}{
		"总任务数":   0,
		"等待中":    0,
		"执行中":    0,
		"已完成":    0,
		"已失败":    0,
		"已取消":    0,
		"暂停中":    0,
		"最新任务":   nil,
		"活跃任务列表": []map[string]interface{}{},
	}

	var latestTask *task.UniversalTask
	var activeTasks []map[string]interface{}

	for _, t := range allTasks {
		// 只处理Flash Trade任务
		if t.Type != "flash_trade" {
			continue
		}

		taskStats["总任务数"] = taskStats["总任务数"].(int) + 1

		// 统计各状态任务数量
		switch t.Status {
		case task.TaskStatusPending:
			taskStats["等待中"] = taskStats["等待中"].(int) + 1
		case task.TaskStatusRunning:
			taskStats["执行中"] = taskStats["执行中"].(int) + 1
			// 添加到活跃任务列表
			activeTasks = append(activeTasks, map[string]interface{}{
				"task_id":         t.ID,
				"name":            t.Name,
				"created_at":      t.CreatedAt.Unix(),
				"started_at":      getTaskStartTime(t),
				"progress":        t.Progress.Percentage,
				"target_nodes":    t.TargetNodes,
				"target_accounts": t.TargetAccounts,
				"parameters":      t.Parameters,
			})
		case task.TaskStatusCompleted:
			taskStats["已完成"] = taskStats["已完成"].(int) + 1
		case task.TaskStatusFailed:
			taskStats["已失败"] = taskStats["已失败"].(int) + 1
		case task.TaskStatusCancelled:
			taskStats["已取消"] = taskStats["已取消"].(int) + 1
		case task.TaskStatusPaused:
			taskStats["暂停中"] = taskStats["暂停中"].(int) + 1
		}

		// 找到最新的任务
		if latestTask == nil || t.CreatedAt.After(latestTask.CreatedAt) {
			latestTask = t
		}
	}

	// 设置最新任务信息
	if latestTask != nil {
		taskStats["最新任务"] = map[string]interface{}{
			"task_id":      latestTask.ID,
			"name":         latestTask.Name,
			"status":       latestTask.Status,
			"created_at":   latestTask.CreatedAt.Unix(),
			"started_at":   getTaskStartTime(latestTask),
			"completed_at": getTaskCompletedTime(latestTask),
			"progress":     latestTask.Progress.Percentage,
			"parameters":   latestTask.Parameters,
		}
	}

	taskStats["活跃任务列表"] = activeTasks

	return taskStats
}

// getTaskStartTime 获取任务开始时间
func getTaskStartTime(t *task.UniversalTask) interface{} {
	if t.StartedAt != nil {
		return t.StartedAt.Unix()
	}
	return nil
}

// getTaskCompletedTime 获取任务完成时间
func getTaskCompletedTime(t *task.UniversalTask) interface{} {
	if t.CompletedAt != nil {
		return t.CompletedAt.Unix()
	}
	return nil
}

// ConflictCheckResult 冲突检查结果
type ConflictCheckResult struct {
	HasConflicts     bool     `json:"has_conflicts"`
	Message          string   `json:"message"`
	ConflictingTasks []string `json:"conflicting_tasks"`
	AffectedAccounts []string `json:"affected_accounts"`
	AffectedNodes    []string `json:"affected_nodes"`
}

// checkFlashTradeConflicts 检查Flash Trade任务冲突
func checkFlashTradeConflicts(server *master.Server, nodeID, accountID string, stopExisting bool) ConflictCheckResult {
	result := ConflictCheckResult{
		HasConflicts:     false,
		ConflictingTasks: []string{},
		AffectedAccounts: []string{},
		AffectedNodes:    []string{},
	}

	// 获取所有运行中的Flash Trade任务
	allTasks := server.GetTaskManager().GetAllTasks()

	for _, t := range allTasks {
		// 只检查Flash Trade任务
		if t.Type != "flash_trade" {
			continue
		}

		// 只检查运行中和等待中的任务
		if t.Status != task.TaskStatusRunning && t.Status != task.TaskStatusPending {
			continue
		}

		// 检查节点冲突
		if nodeID != "" {
			for _, targetNode := range t.TargetNodes {
				if targetNode == nodeID {
					result.HasConflicts = true
					result.ConflictingTasks = append(result.ConflictingTasks, t.ID)
					result.AffectedNodes = append(result.AffectedNodes, nodeID)
					break
				}
			}
		}

		// 检查账号冲突
		if accountID != "" {
			for _, targetAccount := range t.TargetAccounts {
				if targetAccount == accountID {
					result.HasConflicts = true
					result.ConflictingTasks = append(result.ConflictingTasks, t.ID)
					result.AffectedAccounts = append(result.AffectedAccounts, accountID)
					break
				}
			}
		}

		// 如果没有指定特定节点或账号，检查是否有全局冲突
		if nodeID == "" && accountID == "" {
			// 检查是否有任何运行中的Flash Trade任务
			result.HasConflicts = true
			result.ConflictingTasks = append(result.ConflictingTasks, t.ID)
			result.AffectedNodes = append(result.AffectedNodes, t.TargetNodes...)
			result.AffectedAccounts = append(result.AffectedAccounts, t.TargetAccounts...)
		}
	}

	// 构建冲突消息
	if result.HasConflicts {
		conflictCount := len(result.ConflictingTasks)
		if stopExisting {
			result.Message = fmt.Sprintf("检测到 %d 个冲突任务，将自动停止后启动新任务", conflictCount)
		} else {
			result.Message = fmt.Sprintf("检测到 %d 个冲突的Flash Trade任务正在运行", conflictCount)
			if len(result.AffectedAccounts) > 0 {
				result.Message += fmt.Sprintf("，涉及账号: %v", result.AffectedAccounts)
			}
			if len(result.AffectedNodes) > 0 {
				result.Message += fmt.Sprintf("，涉及节点: %v", result.AffectedNodes)
			}
		}
	}

	return result
}

// handleGetAccountStatus 获取账号状态信息
func handleGetAccountStatus(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取查询参数
		nodeID := c.Query("node_id")

		// 获取所有节点的账号状态
		accountStatus := server.GetNodesAccountStatus(nodeID)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    accountStatus,
		})
	}
}

// setupAutoRestartAPI 设置自动重启相关的API
func setupAutoRestartAPI(r *gin.Engine, autoRestartManager *master.AutoRestartManager) {
	// 启用自动重启监控
	r.POST("/api/auto-restart/enable", func(c *gin.Context) {
		var req struct {
			NodeID     string                 `json:"node_id"`
			AccountID  string                 `json:"account_id"`
			TaskParams map[string]interface{} `json:"task_params"`
			Global     bool                   `json:"global"` // 🔧 新增：全局启用标志
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if req.Global {
			// 全局启用：为所有活跃账号启用监控
			count := autoRestartManager.EnableGlobalMonitoring()
			c.JSON(http.StatusOK, gin.H{
				"success":          true,
				"message":          fmt.Sprintf("全局自动重启监控已启用，监控 %d 个账号", count),
				"monitoring_count": count,
			})
		} else {
			// 单个账号启用
			autoRestartManager.AddMonitoring(req.NodeID, req.AccountID, req.TaskParams)
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"message": "自动重启监控已启用",
			})
		}
	})

	// 禁用自动重启监控
	r.POST("/api/auto-restart/disable", func(c *gin.Context) {
		var req struct {
			NodeID    string `json:"node_id"`
			AccountID string `json:"account_id"`
			Global    bool   `json:"global"` // 🔧 新增：全局禁用标志
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if req.Global {
			// 全局禁用：禁用所有账号的监控
			count := autoRestartManager.DisableGlobalMonitoring()
			c.JSON(http.StatusOK, gin.H{
				"success":        true,
				"message":        fmt.Sprintf("全局自动重启监控已禁用，停止监控 %d 个账号", count),
				"disabled_count": count,
			})
		} else {
			// 单个账号禁用
			autoRestartManager.RemoveMonitoring(req.NodeID, req.AccountID)
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"message": "自动重启监控已禁用",
			})
		}
	})

	// 获取监控状态
	r.GET("/api/auto-restart/status", func(c *gin.Context) {
		status := autoRestartManager.GetMonitoringStatus()

		c.JSON(http.StatusOK, gin.H{
			"success":          true,
			"monitoring_tasks": status,
		})
	})
}

// ===== 自动卖出任务管理 API =====

// handleStartAutoSell 启动自动卖出监控
func handleStartAutoSell(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			NodeID        string  `json:"node_id"`        // 目标节点ID，空表示所有节点
			AccountID     string  `json:"account_id"`     // 目标账号ID，空表示所有账号
			TokenAddress  string  `json:"token_address"`  // 代币地址
			MonitorAmount float64 `json:"monitor_amount"` // 监控的代币数量
			BaseAsset     string  `json:"base_asset"`     // 基础资产
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 验证必要参数
		if req.TokenAddress == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "token_address 不能为空",
			})
			return
		}

		// 设置默认值
		if req.BaseAsset == "" {
			req.BaseAsset = "ALPHA_251"
		}
		if req.MonitorAmount <= 0 {
			req.MonitorAmount = 1.0 // 默认监控1个代币
		}

		// 确定目标账号和节点
		var targetAccounts []string
		var targetNodes []string

		if req.AccountID != "" {
			// 指定了账号
			account, err := server.GetAccountManager().GetAccount(req.AccountID)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "账号不存在: " + req.AccountID,
				})
				return
			}
			targetAccounts = []string{req.AccountID}

			if req.NodeID != "" {
				targetNodes = []string{req.NodeID}
			} else {
				targetNodes = []string{account.AssignedNode}
			}
		} else {
			// 没有指定账号，使用所有账号
			if req.NodeID != "" {
				// 指定了节点，使用该节点的所有账号
				accounts := server.GetAccountManager().GetNodeAccounts(req.NodeID)
				for _, account := range accounts {
					targetAccounts = append(targetAccounts, account.ID)
				}
				targetNodes = []string{req.NodeID}
			} else {
				// 没有指定节点，使用所有在线节点的所有账号
				allNodes := server.GetNodes()
				for nodeID, nodeInfo := range allNodes {
					if nodeInfo.Status == "online" {
						targetNodes = append(targetNodes, nodeID)
						accounts := server.GetAccountManager().GetNodeAccounts(nodeID)
						for _, account := range accounts {
							targetAccounts = append(targetAccounts, account.ID)
						}
					}
				}
			}
		}

		if len(targetAccounts) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "没有找到可用的账号",
			})
			return
		}

		// 生成任务ID
		taskID := fmt.Sprintf("autosell-%d", time.Now().UnixNano())

		// 发送命令到被控端
		successCount := 0
		errorCount := 0

		for _, nodeID := range targetNodes {
			// 获取该节点的账号
			nodeAccounts := server.GetAccountManager().GetNodeAccounts(nodeID)

			for _, account := range nodeAccounts {
				// 检查账号是否在目标账号列表中
				accountInTarget := false
				for _, targetAccountID := range targetAccounts {
					if account.ID == targetAccountID {
						accountInTarget = true
						break
					}
				}

				if !accountInTarget {
					continue
				}

				// 构建命令
				command := map[string]interface{}{
					"command_id":   fmt.Sprintf("%s_%s", taskID, account.ID),
					"command_type": "auto_sell",
					"target_node":  nodeID,
					"payload": map[string]interface{}{
						"action":         "start",
						"task_id":        taskID,
						"account_id":     account.ID,
						"token_address":  req.TokenAddress,
						"monitor_amount": req.MonitorAmount,
						"base_asset":     req.BaseAsset,
						"csrftoken":      account.Csrftoken,
						"cookie":         account.Cookie,
					},
				}

				// 通过Redis发送命令
				if err := server.GetRedisClient().PublishCommand("autosell_commands", command); err != nil {
					log.Printf("❌ 发送自动卖出命令失败: %v", err)
					errorCount++
				} else {
					log.Printf("📤 发送自动卖出命令到节点 %s, 账号: %s", nodeID, account.ID)
					successCount++
				}
			}
		}

		if successCount > 0 {
			c.JSON(http.StatusOK, gin.H{
				"success":         true,
				"message":         fmt.Sprintf("自动卖出监控启动成功，影响 %d 个账号", successCount),
				"task_id":         taskID,
				"success_count":   successCount,
				"error_count":     errorCount,
				"target_nodes":    targetNodes,
				"target_accounts": targetAccounts,
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": "自动卖出监控启动失败",
			})
		}
	}
}

// handleStopAutoSell 停止自动卖出监控
func handleStopAutoSell(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			NodeID       string `json:"node_id"`       // 目标节点ID，空表示所有节点
			AccountID    string `json:"account_id"`    // 目标账号ID，空表示所有账号
			TokenAddress string `json:"token_address"` // 代币地址，空表示所有代币
			TaskID       string `json:"task_id"`       // 任务ID，空表示根据其他条件停止
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "请求参数错误: " + err.Error(),
			})
			return
		}

		// 生成停止命令ID
		stopCommandID := fmt.Sprintf("autosell-stop-%d", time.Now().UnixNano())

		// 确定目标节点
		var targetNodes []string
		if req.NodeID != "" {
			targetNodes = []string{req.NodeID}
		} else {
			// 获取所有在线节点
			allNodes := server.GetNodes()
			for nodeID, nodeInfo := range allNodes {
				if nodeInfo.Status == "online" {
					targetNodes = append(targetNodes, nodeID)
				}
			}
		}

		// 发送停止命令到被控端
		successCount := 0
		errorCount := 0

		for _, nodeID := range targetNodes {
			command := map[string]interface{}{
				"command_id":   fmt.Sprintf("%s_%s", stopCommandID, nodeID),
				"command_type": "auto_sell",
				"target_node":  nodeID,
				"payload": map[string]interface{}{
					"action":        "stop",
					"task_id":       req.TaskID,
					"account_id":    req.AccountID,
					"token_address": req.TokenAddress,
				},
			}

			// 通过Redis发送命令
			if err := server.GetRedisClient().PublishCommand("autosell_commands", command); err != nil {
				log.Printf("❌ 发送自动卖出停止命令失败: %v", err)
				errorCount++
			} else {
				log.Printf("📤 发送自动卖出停止命令到节点 %s", nodeID)
				successCount++
			}
		}

		if successCount > 0 {
			c.JSON(http.StatusOK, gin.H{
				"success":       true,
				"message":       fmt.Sprintf("自动卖出停止命令已发送到 %d 个节点", successCount),
				"command_id":    stopCommandID,
				"success_count": successCount,
				"error_count":   errorCount,
				"target_nodes":  targetNodes,
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": "自动卖出停止命令发送失败",
			})
		}
	}
}

// handleGetAutoSellStatus 获取自动卖出状态
func handleGetAutoSellStatus(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Query("node_id")
		accountID := c.Query("account_id")

		// 生成状态查询命令ID
		queryCommandID := fmt.Sprintf("autosell-status-%d", time.Now().UnixNano())

		// 确定目标节点
		var targetNodes []string
		if nodeID != "" {
			targetNodes = []string{nodeID}
		} else {
			// 获取所有在线节点
			allNodes := server.GetNodes()
			for nodeID, nodeInfo := range allNodes {
				if nodeInfo.Status == "online" {
					targetNodes = append(targetNodes, nodeID)
				}
			}
		}

		// 发送状态查询命令到被控端
		for _, nodeID := range targetNodes {
			command := map[string]interface{}{
				"command_id":   fmt.Sprintf("%s_%s", queryCommandID, nodeID),
				"command_type": "auto_sell",
				"target_node":  nodeID,
				"payload": map[string]interface{}{
					"action":     "status",
					"account_id": accountID,
				},
			}

			// 通过Redis发送命令
			if err := server.GetRedisClient().PublishCommand("autosell_commands", command); err != nil {
				log.Printf("❌ 发送自动卖出状态查询命令失败: %v", err)
			} else {
				log.Printf("📤 发送自动卖出状态查询命令到节点 %s", nodeID)
			}
		}

		// 返回查询命令ID，实际状态需要通过其他接口获取
		c.JSON(http.StatusOK, gin.H{
			"success":      true,
			"message":      "状态查询命令已发送",
			"command_id":   queryCommandID,
			"target_nodes": targetNodes,
		})
	}
}

// handleGetAutoSellTasks 获取自动卖出任务列表
func handleGetAutoSellTasks(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 这里可以从Redis或数据库中获取任务列表
		// 目前返回一个简单的响应
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "自动卖出任务列表",
			"tasks":   []interface{}{}, // 空列表，实际任务状态由被控端维护
		})
	}
}

// handleSellReport 处理被控端上报的卖出数据
func handleSellReport() gin.HandlerFunc {
	return func(c *gin.Context) {
		var reportData SellReportData
		if err := c.ShouldBindJSON(&reportData); err != nil {
			log.Printf("❌ [卖出上报] 解析请求数据失败: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": fmt.Sprintf("请求数据格式错误: %v", err),
			})
			return
		}

		// 验证必要字段
		if reportData.NodeID == "" || reportData.AccountID == "" || reportData.ActualUsdtAmount <= 0 {
			log.Printf("❌ [卖出上报] 缺少必要字段: %+v", reportData)
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "缺少必要字段: node_id, account_id, actual_usdt_amount",
			})
			return
		}

		// 添加到统计管理器
		sellStatsManager.AddSellRecord(&reportData)

		log.Printf("✅ [卖出上报] 收到节点 %s 的卖出数据: 账号=%s, 代币=%s, USDT=%.6f",
			reportData.NodeID, reportData.AccountID, reportData.TokenSymbol, reportData.ActualUsdtAmount)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "卖出数据上报成功",
		})
	}
}

// handleGetSellStats 获取所有节点的卖出统计
func handleGetSellStats() gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := sellStatsManager.GetMasterStats()

		log.Printf("📊 [统计查询] 返回卖出统计: 总节点=%d, 总账号=%d, 总USDT=%.6f",
			stats.TotalNodes, stats.TotalAccounts, stats.TotalUsdtAmount)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "获取卖出统计成功",
			"data":    stats,
		})
	}
}

// handleGetNodeSellStats 获取指定节点的卖出统计
func handleGetNodeSellStats() gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.Param("nodeId")
		if nodeID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"message": "缺少节点ID参数",
			})
			return
		}

		sellStatsMutex.RLock()
		nodeStats, exists := sellStatsManager.nodeStats[nodeID]
		sellStatsMutex.RUnlock()

		if !exists {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"message": fmt.Sprintf("节点 %s 没有卖出记录", nodeID),
			})
			return
		}

		// 深拷贝节点统计
		statsCopy := &NodeSellStats{
			NodeID:          nodeStats.NodeID,
			AccountCount:    nodeStats.AccountCount,
			TotalUsdtAmount: nodeStats.TotalUsdtAmount,
			SellRecords:     make([]*SellReportData, len(nodeStats.SellRecords)),
			LastUpdateTime:  nodeStats.LastUpdateTime,
		}
		copy(statsCopy.SellRecords, nodeStats.SellRecords)

		log.Printf("📊 [节点统计] 返回节点 %s 的卖出统计: 账号数=%d, USDT=%.6f",
			nodeID, nodeStats.AccountCount, nodeStats.TotalUsdtAmount)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "获取节点卖出统计成功",
			"data":    statsCopy,
		})
	}
}

// handleGetAutoSellResults 获取自动卖出结果
func handleGetAutoSellResults(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取查询参数
		nodeID := c.Query("node_id")
		accountID := c.Query("account_id")
		limit := c.DefaultQuery("limit", "50") // 默认返回50条记录

		// 转换limit参数
		limitInt := 50
		if l, err := strconv.Atoi(limit); err == nil && l > 0 && l <= 1000 {
			limitInt = l
		}

		// 从主控端获取自动卖出结果
		results := server.GetAutoSellResults(nodeID, accountID, limitInt)

		// 统计信息
		stats := map[string]interface{}{
			"total_results": len(results),
			"success_count": 0,
			"failed_count":  0,
			"total_usdt":    0.0,
		}

		// 计算统计信息
		for _, result := range results {
			if resultMap, ok := result.(map[string]interface{}); ok {
				if resultsData, ok := resultMap["results"].(map[string]interface{}); ok {
					if status, ok := resultsData["status"].(string); ok {
						if status == "completed" {
							stats["success_count"] = stats["success_count"].(int) + 1
							if usdtAmount, ok := resultsData["actual_usdt_amount"].(float64); ok {
								stats["total_usdt"] = stats["total_usdt"].(float64) + usdtAmount
							}
						} else if status == "failed" {
							stats["failed_count"] = stats["failed_count"].(int) + 1
						}
					}
				}
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data": gin.H{
				"results": results,
				"stats":   stats,
				"filters": gin.H{
					"node_id":    nodeID,
					"account_id": accountID,
					"limit":      limitInt,
				},
			},
		})
	}
}

// handlePauseAllFlashTrade 一键暂停所有Flash Trade任务
func handlePauseAllFlashTrade(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Printf("🔄 处理一键暂停Flash Trade请求")

		// 获取所有正在运行的Flash Trade任务
		tasks := server.GetAllFlashTradeTasks()
		pausedCount := 0
		var errors []string

		for taskID, task := range tasks {
			// 只暂停正在运行的任务
			if task.Status == "running" {
				// 向所有目标节点发送暂停命令
				for _, nodeID := range task.TargetNodes {
					err := server.SendFlashTradeCommand(nodeID, "pause", map[string]interface{}{
						"task_id": taskID,
					})

					if err != nil {
						errors = append(errors, fmt.Sprintf("任务 %s 在节点 %s 暂停失败: %v", taskID, nodeID, err))
						log.Printf("❌ 暂停Flash Trade任务失败: %s (节点: %s), 错误: %v", taskID, nodeID, err)
					} else {
						pausedCount++
						log.Printf("✅ Flash Trade任务已暂停: %s (节点: %s)", taskID, nodeID)
					}
				}
			}
		}

		// 返回结果
		if len(errors) > 0 {
			c.JSON(http.StatusPartialContent, gin.H{
				"success": true,
				"message": fmt.Sprintf("部分任务暂停成功，%d个成功，%d个失败", pausedCount, len(errors)),
				"data": gin.H{
					"paused_count": pausedCount,
					"failed_count": len(errors),
					"errors":       errors,
				},
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"message": fmt.Sprintf("一键暂停成功，共暂停 %d 个任务", pausedCount),
				"data": gin.H{
					"paused_count": pausedCount,
					"failed_count": 0,
				},
			})
		}
	}
}

// handleResumeAllFlashTrade 一键恢复所有Flash Trade任务
func handleResumeAllFlashTrade(server *master.Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Printf("🔄 处理一键恢复Flash Trade请求")

		// 获取所有已暂停的Flash Trade任务
		tasks := server.GetAllFlashTradeTasks()
		resumedCount := 0
		var errors []string

		for taskID, task := range tasks {
			// 只恢复已暂停的任务
			if task.Status == "paused" {
				// 向所有目标节点发送恢复命令
				for _, nodeID := range task.TargetNodes {
					err := server.SendFlashTradeCommand(nodeID, "resume", map[string]interface{}{
						"task_id": taskID,
					})

					if err != nil {
						errors = append(errors, fmt.Sprintf("任务 %s 在节点 %s 恢复失败: %v", taskID, nodeID, err))
						log.Printf("❌ 恢复Flash Trade任务失败: %s (节点: %s), 错误: %v", taskID, nodeID, err)
					} else {
						resumedCount++
						log.Printf("✅ Flash Trade任务已恢复: %s (节点: %s)", taskID, nodeID)
					}
				}
			}
		}

		// 返回结果
		if len(errors) > 0 {
			c.JSON(http.StatusPartialContent, gin.H{
				"success": true,
				"message": fmt.Sprintf("部分任务恢复成功，%d个成功，%d个失败", resumedCount, len(errors)),
				"data": gin.H{
					"resumed_count": resumedCount,
					"failed_count":  len(errors),
					"errors":        errors,
				},
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"message": fmt.Sprintf("一键恢复成功，共恢复 %d 个任务", resumedCount),
				"data": gin.H{
					"resumed_count": resumedCount,
					"failed_count":  0,
				},
			})
		}
	}
}
