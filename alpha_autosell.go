package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// AutoSellRequest 自动卖出请求
type AutoSellRequest struct {
	AccountID     string  `json:"account_id"`
	TokenAddress  string  `json:"token_address"`
	MonitorAmount float64 `json:"monitor_amount"` // 监控的代币数量
	Csrftoken     string  `json:"csrftoken"`
	Cookie        string  `json:"cookie"`
	BaseAsset     string  `json:"base_asset"`
}

// AutoSellResponse 自动卖出响应
type AutoSellResponse struct {
	Success   bool                   `json:"success"`
	Message   string                 `json:"message"`
	TaskID    string                 `json:"task_id"`
	Results   map[string]interface{} `json:"results"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

// AutoSellTask 自动卖出任务
type AutoSellTask struct {
	TaskID        string                 `json:"task_id"`
	AccountID     string                 `json:"account_id"`
	TokenAddress  string                 `json:"token_address"`
	MonitorAmount float64                `json:"monitor_amount"`
	BaseAsset     string                 `json:"base_asset"`
	Status        string                 `json:"status"` // running, completed, failed
	StartTime     time.Time              `json:"start_time"`
	EndTime       *time.Time             `json:"end_time"`
	Results       map[string]interface{} `json:"results"`
	StopChan      chan bool              `json:"-"`
}

// AccountAuth 账号认证信息
type AccountAuth struct {
	AccountID string    `json:"account_id"`
	Csrftoken string    `json:"csrftoken"`
	Cookie    string    `json:"cookie"`
	LastUsed  time.Time `json:"last_used"`
}

// AlphaTokenResponse Alpha代币查询响应
type AlphaTokenResponse struct {
	Success   bool                   `json:"success"`
	Message   string                 `json:"message"`
	Data      map[string]interface{} `json:"data"`
	Timestamp int64                  `json:"timestamp"`
}

// SwapQuoteRequest 交换报价请求
type SwapQuoteRequest struct {
	FromToken            string `json:"fromToken"`
	FromContractAddress  string `json:"fromContractAddress"`
	FromBinanceChainId   string `json:"fromBinanceChainId"`
	FromCoinAmount       string `json:"fromCoinAmount"`
	ToToken              string `json:"toToken"`
	ToContractAddress    string `json:"toContractAddress"`
	ToBinanceChainId     string `json:"toBinanceChainId"`
	PriorityMode         string `json:"priorityMode"`
	CustomNetworkFeeMode string `json:"customNetworkFeeMode"`
	CustomSlippage       string `json:"customSlippage"`
}

// SwapQuoteResult 交换报价结果
type SwapQuoteResult struct {
	UniQuoteId   string `json:"uniQuoteId"`
	ToCoinAmount string `json:"toCoinAmount"`
	Success      bool   `json:"success"`
	Message      string `json:"message"`
}

// SwapOrderRequest 交换下单请求
type SwapOrderRequest struct {
	FromToken           string `json:"fromToken"`
	FromContractAddress string `json:"fromContractAddress"`
	FromBinanceChainId  string `json:"fromBinanceChainId"`
	FromCoinAmount      string `json:"fromCoinAmount"`
	ToToken             string `json:"toToken"`
	ToBinanceChainId    string `json:"toBinanceChainId"`
	ToCoinAmount        string `json:"toCoinAmount"`
	PriorityMode        string `json:"priorityMode"`
	Extra               string `json:"extra"`
}

// SwapOrderResult 交换下单结果
type SwapOrderResult struct {
	Success          bool    `json:"success"`
	Message          string  `json:"message"`
	OrderId          string  `json:"order_id"`
	ChainToAmount    float64 `json:"chain_to_amount"`      // 预计获得的USDT数量
	ChainGasFeeInUsd float64 `json:"chain_gas_fee_in_usd"` // 手续费
	ActualUsdtAmount float64 `json:"actual_usdt_amount"`   // 实际获得的USDT数量
}

// SellReportData 卖出上报数据
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

// RedisConfig Redis配置结构
type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// Config 配置文件结构
type Config struct {
	Redis RedisConfig `json:"redis"`
}

// 全局任务管理
var (
	tasks     = make(map[string]*AutoSellTask)
	taskMutex sync.RWMutex
)

// Redis 客户端
var (
	redisClient *redis.Client
	redisCtx    = context.Background()
)

func main() {
	log.Printf("Starting Alpha AutoSell Monitor Service")
	log.Printf("Listening on port: 8081")

	// 初始化Redis连接
	initRedis()

	// 设置HTTP路由
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/monitor", handleMonitor)
	http.HandleFunc("/stop", handleStop)
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/tasks", handleTasks)
	http.HandleFunc("/alpha", handleAlphaToken)

	// 启动HTTP服务器
	server := &http.Server{
		Addr:         ":8081",
		Handler:      nil,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Alpha AutoSell Monitor Service started successfully")
	log.Printf("API endpoints:")
	log.Printf("   GET  /         - Service info")
	log.Printf("   GET  /health   - Health check")
	log.Printf("   POST /monitor  - Start monitoring")
	log.Printf("   POST /stop     - Stop monitoring")
	log.Printf("   GET  /status   - View status")
	log.Printf("   GET  /tasks    - View task list")
	log.Printf("   GET  /alpha    - Query Alpha tokens")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed to start: %v", err)
	}
}

// handleIndex 首页
func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	taskMutex.RLock()
	taskCount := len(tasks)
	runningCount := 0
	for _, task := range tasks {
		if task.Status == "running" {
			runningCount++
		}
	}
	taskMutex.RUnlock()

	response := map[string]interface{}{
		"service":   "自动卖出监控服务",
		"version":   "1.0.0",
		"port":      8081,
		"status":    "running",
		"timestamp": time.Now().Format(time.RFC3339),
		"statistics": map[string]interface{}{
			"total_tasks":   taskCount,
			"running_tasks": runningCount,
		},
		"endpoints": map[string]string{
			"health":  "/health",
			"monitor": "/monitor",
			"stop":    "/stop",
			"status":  "/status",
			"tasks":   "/tasks",
			"alpha":   "/alpha",
		},
	}

	json.NewEncoder(w).Encode(response)
}

// handleHealth 健康检查
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "alpha_autosell",
		"port":      8081,
	}

	json.NewEncoder(w).Encode(response)
}

// handleMonitor 处理监控请求
func handleMonitor(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("🔧 [调试] 收到监控请求")

	var req AutoSellRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("❌ [调试] 请求参数解析失败: %v", err)
		response := AutoSellResponse{
			Success:   false,
			Message:   "请求参数解析失败: " + err.Error(),
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("🔧 [调试] 解析的请求参数: AccountID=%s, TokenAddress=%s, MonitorAmount=%.6f, Csrftoken长度=%d, Cookie长度=%d",
		req.AccountID, req.TokenAddress, req.MonitorAmount, len(req.Csrftoken), len(req.Cookie))

	// 验证必要参数
	if req.AccountID == "" || req.TokenAddress == "" || req.Csrftoken == "" || req.Cookie == "" {
		log.Printf("❌ [调试] 参数验证失败: AccountID=%s, TokenAddress=%s, Csrftoken=%s, Cookie=%s",
			req.AccountID, req.TokenAddress,
			func() string {
				if req.Csrftoken == "" {
					return "空"
				} else {
					return "有值"
				}
			}(),
			func() string {
				if req.Cookie == "" {
					return "空"
				} else {
					return "有值"
				}
			}())
		response := AutoSellResponse{
			Success:   false,
			Message:   "缺少必要参数: account_id, token_address, csrftoken, cookie",
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("✅ [调试] 参数验证通过，准备创建监控任务")

	// 设置默认值
	if req.BaseAsset == "" {
		req.BaseAsset = "ALPHA_251"
	}
	if req.MonitorAmount <= 0 {
		req.MonitorAmount = 1.0 // 默认监控1个代币
	}

	// 生成任务ID
	taskID := fmt.Sprintf("autosell_%s_%s_%d", req.AccountID, req.TokenAddress, time.Now().UnixNano())

	// 检查是否已有相同任务
	taskMutex.Lock()
	for _, task := range tasks {
		if task.AccountID == req.AccountID && task.TokenAddress == req.TokenAddress && task.Status == "running" {
			taskMutex.Unlock()
			response := AutoSellResponse{
				Success:   false,
				Message:   "该账户的代币监控任务已在运行",
				Timestamp: time.Now().Unix(),
			}
			json.NewEncoder(w).Encode(response)
			return
		}
	}

	// 创建新任务
	task := &AutoSellTask{
		TaskID:        taskID,
		AccountID:     req.AccountID,
		TokenAddress:  req.TokenAddress,
		MonitorAmount: req.MonitorAmount,
		BaseAsset:     req.BaseAsset,
		Status:        "running",
		StartTime:     time.Now(),
		Results:       make(map[string]interface{}),
		StopChan:      make(chan bool),
	}

	tasks[taskID] = task
	taskMutex.Unlock()

	// 启动监控协程
	go runMonitorTask(task, req)

	log.Printf("🤖 [%s] 启动自动卖出监控: %s, 监控数量: %.6f",
		req.AccountID, req.TokenAddress, req.MonitorAmount)

	response := AutoSellResponse{
		Success:   true,
		Message:   "自动卖出监控已启动",
		TaskID:    taskID,
		Results:   task.Results,
		Timestamp: time.Now().Unix(),
		// 添加与 Flash Trade 兼容的字段
		Data: map[string]interface{}{
			"task_id":        taskID,
			"account_id":     req.AccountID,
			"token_address":  req.TokenAddress,
			"monitor_amount": req.MonitorAmount,
			"status":         "monitoring",
		},
	}

	json.NewEncoder(w).Encode(response)
}

// runMonitorTask 运行监控任务
func runMonitorTask(task *AutoSellTask, req AutoSellRequest) {
	defer func() {
		taskMutex.Lock()
		task.Status = "completed"
		endTime := time.Now()
		task.EndTime = &endTime
		taskMutex.Unlock()
	}()

	log.Printf("🔍 [%s] 开始监控代币余额: %s (监控数量: %.6f)",
		task.AccountID, task.TokenAddress, task.MonitorAmount)

	// 立即执行第一次检查
	log.Printf("🚀 [%s] 立即执行第一次检查", task.AccountID)
	checkAndSell(task, req)

	// 检查第一次检查后是否已完成卖出
	if task.Status == "completed" {
		log.Printf("🎯 [%s] 第一次检查已完成卖出，停止监控", task.AccountID)
		return
	}

	// 设置定时器，每3秒检查一次
	ticker := time.NewTicker(3 * time.Second) // 每3秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-task.StopChan:
			log.Printf("⏹️ [%s] 监控任务已停止: %s", task.AccountID, task.TokenAddress)
			return
		case <-ticker.C:
			// 执行定期监控检查
			log.Printf("⏰ [%s] 执行定期检查（3秒间隔）", task.AccountID)
			checkAndSell(task, req)

			// 检查是否已完成卖出
			if task.Status == "completed" {
				log.Printf("🎯 [%s] 定期检查已完成卖出，停止监控", task.AccountID)
				return
			}
		}
	}
}

// checkAndSell 检查并执行卖出
func checkAndSell(task *AutoSellTask, req AutoSellRequest) {
	log.Printf("🔍 [%s] 检查代币余额: %s", task.AccountID, task.TokenAddress)

	// 1. 从Redis获取账号认证信息
	auth, err := getAccountAuthFromRedis(task.AccountID)
	if err != nil {
		log.Printf("❌ [%s] 获取账号认证信息失败: %v", task.AccountID, err)
		task.Results["last_error"] = fmt.Sprintf("获取账号认证信息失败: %v", err)
		return
	}

	// 2. 查询Alpha代币数据
	alphaData, err := queryAlphaToken(auth.Csrftoken, auth.Cookie)
	if err != nil {
		log.Printf("❌ [%s] 查询Alpha代币失败: %v", task.AccountID, err)
		task.Results["last_error"] = fmt.Sprintf("查询Alpha代币失败: %v", err)
		return
	}

	// 3. 检查是否达到监控数量
	shouldSell, tokenInfo := checkTokenAmount(alphaData, task.TokenAddress, task.MonitorAmount)

	// 4. 更新检查结果
	task.Results["last_check"] = time.Now().Format(time.RFC3339)
	task.Results["monitor_amount"] = task.MonitorAmount
	task.Results["should_sell"] = shouldSell

	if tokenInfo != nil {
		task.Results["current_amount"] = tokenInfo.Amount
		task.Results["symbol"] = tokenInfo.Symbol
		task.Results["name"] = tokenInfo.Name
		task.Results["token_id"] = tokenInfo.TokenID
	} else {
		task.Results["current_amount"] = 0.0
		task.Results["symbol"] = ""
		task.Results["name"] = ""
		task.Results["token_id"] = ""
	}

	if task.Results["check_count"] == nil {
		task.Results["check_count"] = 0
	}
	task.Results["check_count"] = task.Results["check_count"].(int) + 1

	if shouldSell && tokenInfo != nil {
		log.Printf("🎯 [%s] 代币 %s (%s) 达到监控数量 %.6f (当前: %.6f)，触发卖出条件",
			task.AccountID, tokenInfo.Symbol, tokenInfo.Name, task.MonitorAmount, tokenInfo.Amount)

		// 获取交换报价
		log.Printf("💰 [%s] 获取 %s -> USDT 交换报价...", task.AccountID, tokenInfo.Symbol)
		quoteResult, err := getSwapQuote(auth.Csrftoken, auth.Cookie, tokenInfo, task.TokenAddress)
		if err != nil {
			log.Printf("❌ [%s] 获取交换报价失败: %v", task.AccountID, err)
			task.Results["quote_error"] = fmt.Sprintf("获取交换报价失败: %v", err)
		} else if !quoteResult.Success {
			log.Printf("❌ [%s] 交换报价返回失败: %s", task.AccountID, quoteResult.Message)
			task.Results["quote_error"] = quoteResult.Message
		} else {
			log.Printf("✅ [%s] 交换报价获取成功 - UniQuoteId: %s, 预计获得USDT: %s",
				task.AccountID, quoteResult.UniQuoteId, quoteResult.ToCoinAmount)

			// 保存关键报价信息
			task.Results["quote_success"] = true
			task.Results["uni_quote_id"] = quoteResult.UniQuoteId
			task.Results["to_coin_amount"] = quoteResult.ToCoinAmount
			task.Results["quote_time"] = time.Now().Format(time.RFC3339)

			// 执行交换下单（最多尝试10次）
			log.Printf("🚀 [%s] 执行交换下单: %s -> USDT", task.AccountID, tokenInfo.Symbol)
			orderResult, err := executeSwapOrderWithRetry(auth.Csrftoken, auth.Cookie, tokenInfo, task.TokenAddress, quoteResult, 10)
			if err != nil {
				log.Printf("❌ [%s] 交换下单失败（已尝试10次）: %v", task.AccountID, err)
				task.Results["order_error"] = fmt.Sprintf("交换下单失败（已尝试10次）: %v", err)
				task.Results["retry_attempts"] = 10
				task.Results["status"] = "failed"

				// 下单失败，停止监控任务，等待下次任务分发
				log.Printf("❌ [%s] 自动卖出失败，停止监控任务，等待下次任务分发", task.AccountID)
				task.Status = "completed" // 标记为完成（虽然失败了）
				return                    // 立即返回，停止监控
			} else if !orderResult.Success {
				log.Printf("❌ [%s] 交换下单返回失败（已尝试10次）: %s", task.AccountID, orderResult.Message)
				task.Results["order_error"] = fmt.Sprintf("交换下单返回失败（已尝试10次）: %s", orderResult.Message)
				task.Results["retry_attempts"] = 10
				task.Results["status"] = "failed"

				// 下单失败，停止监控任务，等待下次任务分发
				log.Printf("❌ [%s] 自动卖出失败，停止监控任务，等待下次任务分发", task.AccountID)
				task.Status = "completed" // 标记为完成（虽然失败了）
				return                    // 立即返回，停止监控
			} else {
				log.Printf("🎉 [%s] 交换下单成功! 订单ID: %s, 实际获得USDT: %.6f (预计: %.6f, 手续费: %.6f)",
					task.AccountID, orderResult.OrderId, orderResult.ActualUsdtAmount,
					orderResult.ChainToAmount, orderResult.ChainGasFeeInUsd)

				task.Results["order_success"] = true
				task.Results["order_id"] = orderResult.OrderId
				task.Results["order_time"] = time.Now().Format(time.RFC3339)
				task.Results["order_message"] = orderResult.Message
				task.Results["chain_to_amount"] = orderResult.ChainToAmount
				task.Results["chain_gas_fee_in_usd"] = orderResult.ChainGasFeeInUsd
				task.Results["actual_usdt_amount"] = orderResult.ActualUsdtAmount

				// 下单成功后，可以停止监控任务
				task.Status = "completed"
				log.Printf("✅ [%s] 自动卖出完成，停止监控任务", task.AccountID)

				// 保存卖出结果到任务中（由被控端负责上报给主控端）
				task.Results["order_id"] = orderResult.OrderId
				task.Results["actual_usdt_amount"] = orderResult.ActualUsdtAmount
				task.Results["chain_gas_fee"] = orderResult.ChainGasFeeInUsd
				task.Results["chain_to_amount"] = orderResult.ChainToAmount
				task.Results["token_symbol"] = tokenInfo.Symbol
				task.Results["sold_amount"] = tokenInfo.Amount
				task.Results["sell_time"] = time.Now().Unix()
				task.Results["status"] = "completed"

				log.Printf("📊 [%s] 自动卖出完成: 订单ID=%s, 获得USDT=%.6f, 手续费=%.6f",
					task.AccountID, orderResult.OrderId, orderResult.ActualUsdtAmount, orderResult.ChainGasFeeInUsd)
				log.Printf("💾 [%s] 卖出数据已保存到任务结果，等待被控端获取", task.AccountID)

				// 卖出成功，立即停止监控任务，等待下次任务分发
				log.Printf("✅ [%s] 自动卖出完成，停止监控任务，等待下次任务分发", task.AccountID)
				return // 立即返回，停止监控
			}
		}

		task.Results["sell_triggered"] = true
		task.Results["sell_trigger_time"] = time.Now().Format(time.RFC3339)
	} else if tokenInfo != nil {
		log.Printf("📊 [%s] 代币 %s (%s) 当前数量 %.6f，未达到监控数量 %.6f",
			task.AccountID, tokenInfo.Symbol, tokenInfo.Name, tokenInfo.Amount, task.MonitorAmount)
	} else {
		log.Printf("📊 [%s] 未找到代币 %s", task.AccountID, task.TokenAddress)
	}

	log.Printf("✅ [%s] 监控检查完成: %s", task.AccountID, task.TokenAddress)
}

// handleStop 处理停止监控请求
func handleStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TaskID       string `json:"task_id"`
		AccountID    string `json:"account_id"`
		TokenAddress string `json:"token_address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response := AutoSellResponse{
			Success:   false,
			Message:   "请求参数解析失败: " + err.Error(),
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	taskMutex.Lock()
	defer taskMutex.Unlock()

	var stoppedTask *AutoSellTask
	if req.TaskID != "" {
		// 根据任务ID停止
		if task, exists := tasks[req.TaskID]; exists && task.Status == "running" {
			task.Status = "stopped"
			close(task.StopChan)
			stoppedTask = task
		}
	} else if req.AccountID != "" && req.TokenAddress != "" {
		// 根据账号和代币地址停止
		for _, task := range tasks {
			if task.AccountID == req.AccountID && task.TokenAddress == req.TokenAddress && task.Status == "running" {
				task.Status = "stopped"
				close(task.StopChan)
				stoppedTask = task
				break
			}
		}
	}

	if stoppedTask != nil {
		log.Printf("🛑 [%s] 停止自动卖出监控: %s", stoppedTask.AccountID, stoppedTask.TokenAddress)
		response := AutoSellResponse{
			Success:   true,
			Message:   "监控任务已停止",
			TaskID:    stoppedTask.TaskID,
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
	} else {
		response := AutoSellResponse{
			Success:   false,
			Message:   "未找到对应的运行中任务",
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
	}
}

// handleStatus 处理状态查询请求
func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("task_id")
	accountID := r.URL.Query().Get("account_id")

	taskMutex.RLock()
	defer taskMutex.RUnlock()

	if taskID != "" {
		// 查询特定任务状态
		if task, exists := tasks[taskID]; exists {
			response := map[string]interface{}{
				"success": true,
				"task":    task,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			response := map[string]interface{}{
				"success": false,
				"message": "任务不存在",
			}
			json.NewEncoder(w).Encode(response)
		}
	} else if accountID != "" {
		// 查询特定账号的所有任务
		accountTasks := make([]*AutoSellTask, 0)
		for _, task := range tasks {
			if task.AccountID == accountID {
				accountTasks = append(accountTasks, task)
			}
		}
		response := map[string]interface{}{
			"success": true,
			"tasks":   accountTasks,
			"count":   len(accountTasks),
		}
		json.NewEncoder(w).Encode(response)
	} else {
		// 查询所有任务状态汇总
		runningCount := 0
		completedCount := 0
		stoppedCount := 0
		for _, task := range tasks {
			switch task.Status {
			case "running":
				runningCount++
			case "completed":
				completedCount++
			case "stopped":
				stoppedCount++
			}
		}

		response := map[string]interface{}{
			"success": true,
			"summary": map[string]interface{}{
				"total_tasks":     len(tasks),
				"running_tasks":   runningCount,
				"completed_tasks": completedCount,
				"stopped_tasks":   stoppedCount,
			},
		}
		json.NewEncoder(w).Encode(response)
	}
}

// handleTasks 处理任务列表请求
func handleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskMutex.RLock()
	taskList := make([]*AutoSellTask, 0, len(tasks))
	for _, task := range tasks {
		taskList = append(taskList, task)
	}
	taskMutex.RUnlock()

	response := map[string]interface{}{
		"success": true,
		"tasks":   taskList,
		"count":   len(taskList),
	}

	json.NewEncoder(w).Encode(response)
}

// loadConfig 加载配置文件
func loadConfig() (*Config, error) {
	// 尝试多个可能的配置文件路径
	configPaths := []string{
		"config.json",
		"configs/master.json",
		"configs/config.json",
		"../config.json",
		"../configs/master.json",
	}

	var configData []byte
	var err error
	var usedPath string

	for _, path := range configPaths {
		configData, err = ioutil.ReadFile(path)
		if err == nil {
			usedPath = path
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("无法找到配置文件，尝试的路径: %v", configPaths)
	}

	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败 (%s): %v", usedPath, err)
	}

	log.Printf("📋 使用配置文件: %s", usedPath)
	return &config, nil
}

// initRedis 初始化Redis连接
func initRedis() {
	// 加载配置文件
	config, err := loadConfig()
	if err != nil {
		log.Printf("⚠️ 加载配置文件失败: %v", err)
		log.Printf("💡 使用默认Redis配置: localhost:6379")

		// 使用默认配置
		redisClient = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		})
	} else {
		// 使用配置文件中的Redis设置
		redisClient = redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.DB,
		})
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("❌ Redis连接失败 (%s): %v", redisClient.Options().Addr, err)
		log.Printf("💡 Alpha Token查询将无法使用Redis中的认证信息")
		redisClient = nil
	} else {
		log.Printf("✅ Redis连接成功: %s", redisClient.Options().Addr)
	}
}

// getAccountAuthFromRedis 从Redis获取账号认证信息
func getAccountAuthFromRedis(accountID string) (*AccountAuth, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("Redis客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 尝试从flash_trade_auth键获取
	key := fmt.Sprintf("flash_trade_auth:%s", accountID)
	authData, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// 尝试从account键获取
			key = fmt.Sprintf("account:%s", accountID)
			accountData, err := redisClient.Get(ctx, key).Result()
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

// queryAlphaToken 查询Alpha代币
func queryAlphaToken(csrftoken, cookie string) (map[string]interface{}, error) {
	// 构建请求URL - 直接调用币安API
	url := "https://www.binance.com/bapi/defi/v1/private/wallet-direct/cloud-wallet/alpha"

	// 创建HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("Cookie", cookie)
	req.Header.Set("content-type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 调试：打印响应内容的前200个字符
	bodyStr := string(body)
	if len(bodyStr) > 200 {
		log.Printf("🔧 [调试] API响应前200字符: %s...", bodyStr[:200])
	} else {
		log.Printf("🔧 [调试] API响应完整内容: %s", bodyStr)
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	return result, nil
}

// getSwapQuote 获取交换报价
func getSwapQuote(csrftoken, cookie string, tokenInfo *TokenInfo, contractAddress string) (*SwapQuoteResult, error) {
	// 构建请求URL
	url := "https://www.binance.com/bapi/defi/v1/private/wallet-direct/swap/cex/get-quote"

	// 构建请求参数（使用简化的格式，与正确参数一致）
	quoteRequest := map[string]interface{}{
		"fromToken":           tokenInfo.Symbol,
		"fromContractAddress": contractAddress,
		"fromBinanceChainId":  "56",
		"fromCoinAmount":      fmt.Sprintf("%.6f", tokenInfo.Amount),
		"toToken":             "USDT",
		"toContractAddress":   "",
		"toBinanceChainId":    "56",
	}

	// 调试：打印请求参数
	log.Printf("🔧 [调试] 报价API请求参数:")
	log.Printf("   fromToken: %s", quoteRequest["fromToken"])
	log.Printf("   fromContractAddress: %s", quoteRequest["fromContractAddress"])
	log.Printf("   fromBinanceChainId: %s", quoteRequest["fromBinanceChainId"])
	log.Printf("   fromCoinAmount: %s", quoteRequest["fromCoinAmount"])
	log.Printf("   toToken: %s", quoteRequest["toToken"])
	log.Printf("   toContractAddress: %s", quoteRequest["toContractAddress"])
	log.Printf("   toBinanceChainId: %s", quoteRequest["toBinanceChainId"])

	// 序列化请求体
	requestBody, err := json.Marshal(quoteRequest)
	if err != nil {
		return nil, fmt.Errorf("序列化请求参数失败: %v", err)
	}

	// 调试：打印完整的请求体JSON
	log.Printf("🔧 [调试] 报价API请求体JSON: %s", string(requestBody))

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("content-type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 调试：打印完整的报价API响应
	bodyStr := string(body)
	log.Printf("🔧 [调试] 报价API完整响应: %s", bodyStr)

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 构建结果
	result := &SwapQuoteResult{
		Success: false,
		Message: "解析报价数据失败",
	}

	// 检查响应是否成功 - 币安API使用 code="000000" 表示成功
	if code, ok := response["code"].(string); ok && code == "000000" {
		result.Success = true
		result.Message = "获取报价成功"

		// 提取 data 字段
		if data, ok := response["data"].(map[string]interface{}); ok {
			// 检查关键字段是否为null
			toCoinAmountRaw := data["toCoinAmount"]
			extraRaw := data["extra"]

			if toCoinAmountRaw == nil || extraRaw == nil {
				result.Success = false
				result.Message = "代币不支持兑换USDT或流动性不足"
				log.Printf("⚠️ [调试] 关键字段为null: toCoinAmount=%v, extra=%v", toCoinAmountRaw, extraRaw)
				return result, nil
			}

			// 提取 toCoinAmount
			if toCoinAmount, ok := toCoinAmountRaw.(string); ok && toCoinAmount != "" {
				result.ToCoinAmount = toCoinAmount
				log.Printf("🔧 [调试] 提取到 toCoinAmount: %s", toCoinAmount)
			} else {
				result.Success = false
				result.Message = "无法获取兑换数量"
				return result, nil
			}

			// 提取 uniQuoteId 从 extra 字段
			if extra, ok := extraRaw.(string); ok && extra != "" {
				log.Printf("🔧 [调试] 提取到 extra 字段: %s", extra)
				var extraData map[string]interface{}
				if err := json.Unmarshal([]byte(extra), &extraData); err == nil {
					if uniQuoteId, ok := extraData["uniQuoteId"].(string); ok && uniQuoteId != "" {
						result.UniQuoteId = uniQuoteId
						log.Printf("🔧 [调试] 提取到 uniQuoteId: %s", uniQuoteId)
					} else {
						result.Success = false
						result.Message = "无法获取报价ID"
						return result, nil
					}
				} else {
					result.Success = false
					result.Message = "解析报价ID失败"
					return result, nil
				}
			} else {
				result.Success = false
				result.Message = "无法获取报价详情"
				return result, nil
			}
		}
	} else {
		// 提取错误信息
		if message, ok := response["message"].(string); ok && message != "" {
			result.Message = message
		} else {
			result.Message = fmt.Sprintf("API返回错误代码: %s", code)
		}
	}

	return result, nil
}

// executeSwapOrder 执行交换下单
func executeSwapOrder(csrftoken, cookie string, tokenInfo *TokenInfo, contractAddress string, quoteResult *SwapQuoteResult) (*SwapOrderResult, error) {
	// 构建请求URL
	url := "https://www.binance.com/bapi/defi/v2/private/wallet-direct/swap/cex/sell/pre/payment"

	// 构建 extra 字段
	extraData := map[string]string{
		"uniQuoteId": quoteResult.UniQuoteId,
	}
	extraBytes, err := json.Marshal(extraData)
	if err != nil {
		return nil, fmt.Errorf("构建extra字段失败: %v", err)
	}

	// 构建请求参数
	orderRequest := SwapOrderRequest{
		FromToken:           tokenInfo.Symbol,                      // 之前的 symbol
		FromContractAddress: contractAddress,                       // 参数
		FromBinanceChainId:  "56",                                  // 固定56
		FromCoinAmount:      fmt.Sprintf("%.6f", tokenInfo.Amount), // 前面获取的 amount
		ToToken:             "USDT",                                // 固定USDT
		ToBinanceChainId:    "56",                                  // 固定56
		ToCoinAmount:        quoteResult.ToCoinAmount,              // 上个接口返回的 toCoinAmount
		PriorityMode:        "priorityOnPrice",                     // 固定的 priorityOnPrice
		Extra:               string(extraBytes),                    // 包含 uniQuoteId 的 JSON 字符串
	}

	// 调试：打印请求参数
	log.Printf("🔧 [调试] 下单API请求参数:")
	log.Printf("   FromToken: %s", orderRequest.FromToken)
	log.Printf("   FromContractAddress: %s", orderRequest.FromContractAddress)
	log.Printf("   FromBinanceChainId: %s", orderRequest.FromBinanceChainId)
	log.Printf("   FromCoinAmount: %s", orderRequest.FromCoinAmount)
	log.Printf("   ToToken: %s", orderRequest.ToToken)
	log.Printf("   ToBinanceChainId: %s", orderRequest.ToBinanceChainId)
	log.Printf("   ToCoinAmount: %s", orderRequest.ToCoinAmount)
	log.Printf("   PriorityMode: %s", orderRequest.PriorityMode)
	log.Printf("   Extra: %s", orderRequest.Extra)

	// 序列化请求体
	requestBody, err := json.Marshal(orderRequest)
	if err != nil {
		return nil, fmt.Errorf("序列化请求参数失败: %v", err)
	}

	// 调试：打印完整的请求体JSON
	log.Printf("🔧 [调试] 下单API请求体JSON: %s", string(requestBody))

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("content-type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 调试：打印完整的下单API响应
	bodyStr := string(body)
	log.Printf("🔧 [调试] 下单API完整响应: %s", bodyStr)

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 构建结果
	result := &SwapOrderResult{
		Success: false,
		Message: "下单失败",
	}

	// 检查响应是否成功 - 币安API使用 code="000000" 表示成功
	if code, ok := response["code"].(string); ok && code == "000000" {
		result.Success = true
		result.Message = "下单成功"

		// 提取订单详细信息
		if data, ok := response["data"].(map[string]interface{}); ok {
			// 提取订单ID
			if orderId, ok := data["orderId"].(string); ok {
				result.OrderId = orderId
				log.Printf("🔧 [调试] 提取到订单ID: %s", orderId)
			}

			// 提取 chainToAmount（预计获得的USDT数量）
			if chainToAmount, ok := data["chainToAmount"].(float64); ok {
				result.ChainToAmount = chainToAmount
				log.Printf("🔧 [调试] 提取到 chainToAmount: %.6f", chainToAmount)
			} else if chainToAmountStr, ok := data["chainToAmount"].(string); ok {
				if amount, err := strconv.ParseFloat(chainToAmountStr, 64); err == nil {
					result.ChainToAmount = amount
					log.Printf("🔧 [调试] 提取到 chainToAmount (字符串): %.6f", amount)
				}
			}

			// 提取 chainGasFeeInUsd（手续费）
			if chainGasFeeInUsd, ok := data["chainGasFeeInUsd"].(float64); ok {
				result.ChainGasFeeInUsd = chainGasFeeInUsd
				log.Printf("🔧 [调试] 提取到手续费: %.6f", chainGasFeeInUsd)
			}

			// 计算实际获得的USDT数量
			result.ActualUsdtAmount = result.ChainToAmount - result.ChainGasFeeInUsd
			log.Printf("🔧 [调试] 计算实际获得USDT: %.6f", result.ActualUsdtAmount)
		}
	} else {
		// 提取错误信息
		if message, ok := response["message"].(string); ok && message != "" {
			result.Message = message
		} else {
			result.Message = fmt.Sprintf("下单失败，错误代码: %s", code)
		}
		log.Printf("🔧 [调试] 下单失败，代码: %s, 消息: %s", code, result.Message)
	}

	return result, nil
}

// reportSellDataToMaster 向主控端上报卖出数据
func reportSellDataToMaster(reportData *SellReportData) error {
	// 这里应该根据你的主控端地址进行配置
	// 可以从配置文件或环境变量中读取主控端地址
	masterURL := "http://localhost:28080/api/v1/sell-report" // 主控端的卖出上报接口

	// 序列化上报数据
	requestBody, err := json.Marshal(reportData)
	if err != nil {
		return fmt.Errorf("序列化上报数据失败: %v", err)
	}

	// 创建HTTP请求
	req, err := http.NewRequest("POST", masterURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("创建上报请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("上报请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("上报失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
	}

	return nil
}

// getNodeID 获取节点ID（可以基于主机名、IP等生成）
func getNodeID() string {
	// 这里可以根据实际需求生成节点ID
	// 比如基于主机名、IP地址、配置文件等
	return "alpha_autosell_node_1" // 简单示例，实际应该动态生成
}

// TokenInfo 代币信息
type TokenInfo struct {
	Amount  float64 `json:"amount"`
	Symbol  string  `json:"symbol"`
	Name    string  `json:"name"`
	TokenID string  `json:"token_id"`
}

// checkTokenAmount 检查指定代币是否达到监控数量
func checkTokenAmount(alphaData map[string]interface{}, contractAddress string, monitorAmount float64) (bool, *TokenInfo) {
	// 检查响应格式
	if !alphaData["success"].(bool) {
		log.Printf("⚠️ Alpha API响应失败")
		return false, nil
	}

	data, ok := alphaData["data"].(map[string]interface{})
	if !ok {
		log.Printf("⚠️ Alpha API数据格式错误")
		return false, nil
	}

	list, ok := data["list"].([]interface{})
	if !ok {
		log.Printf("⚠️ Alpha API列表数据格式错误")
		return false, nil
	}

	// 遍历代币列表，查找匹配的合约地址
	for _, item := range list {
		token, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		// 检查合约地址是否匹配（不区分大小写）
		tokenContract, ok := token["contractAddress"].(string)
		if !ok {
			continue
		}

		if strings.EqualFold(tokenContract, contractAddress) {
			// 获取代币数量
			amountStr, ok := token["amount"].(string)
			if !ok {
				log.Printf("⚠️ 代币数量格式错误")
				return false, nil
			}

			// 转换为浮点数
			currentAmount, err := strconv.ParseFloat(amountStr, 64)
			if err != nil {
				log.Printf("⚠️ 代币数量转换失败: %v", err)
				return false, nil
			}

			// 获取代币信息
			symbol, _ := token["symbol"].(string)
			name, _ := token["name"].(string)
			tokenID, _ := token["tokenId"].(string)

			tokenInfo := &TokenInfo{
				Amount:  currentAmount,
				Symbol:  symbol,
				Name:    name,
				TokenID: tokenID,
			}

			// 检查是否达到监控数量
			shouldSell := currentAmount >= monitorAmount

			// 记录详细信息
			log.Printf("📊 找到代币: %s (%s) - %s, 当前数量: %.6f, 监控数量: %.6f, 触发卖出: %v",
				symbol, name, contractAddress, currentAmount, monitorAmount, shouldSell)

			return shouldSell, tokenInfo
		}
	}

	log.Printf("⚠️ 未找到合约地址为 %s 的代币", contractAddress)
	return false, nil
}

// handleAlphaToken 处理Alpha代币查询请求
func handleAlphaToken(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取账号ID参数
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		response := AlphaTokenResponse{
			Success:   false,
			Message:   "缺少必要参数: account_id",
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// 从Redis获取账号认证信息
	auth, err := getAccountAuthFromRedis(accountID)
	if err != nil {
		log.Printf("❌ [%s] 获取账号认证信息失败: %v", accountID, err)
		response := AlphaTokenResponse{
			Success:   false,
			Message:   fmt.Sprintf("获取账号认证信息失败: %v", err),
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// 查询Alpha代币
	data, err := queryAlphaToken(auth.Csrftoken, auth.Cookie)
	if err != nil {
		log.Printf("❌ [%s] 查询Alpha代币失败: %v", accountID, err)
		response := AlphaTokenResponse{
			Success:   false,
			Message:   fmt.Sprintf("查询Alpha代币失败: %v", err),
			Timestamp: time.Now().Unix(),
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("✅ [%s] Alpha代币查询成功", accountID)
	response := AlphaTokenResponse{
		Success:   true,
		Message:   "Alpha代币查询成功",
		Data:      data,
		Timestamp: time.Now().Unix(),
	}

	json.NewEncoder(w).Encode(response)
}

// shouldRetryOrder 判断是否应该重试下单
func shouldRetryOrder(err error, result *SwapOrderResult) (bool, string) {
	// 如果是网络错误，应该重试
	if err != nil {
		errStr := err.Error()
		// 网络相关错误 → 重试
		if strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "connection") ||
			strings.Contains(errStr, "network") ||
			strings.Contains(errStr, "dial tcp") {
			return true, "网络错误"
		}
		// 解析错误 → 重试
		if strings.Contains(errStr, "json") || strings.Contains(errStr, "parse") {
			return true, "解析错误"
		}
		// 其他未知错误 → 重试
		return true, "未知错误"
	}

	// 如果API调用成功但返回失败状态
	if result != nil && !result.Success {
		message := strings.ToLower(result.Message)

		// 临时性错误 → 重试
		if strings.Contains(message, "请稍后重试") ||
			strings.Contains(message, "系统繁忙") ||
			strings.Contains(message, "服务暂不可用") ||
			strings.Contains(message, "网络异常") ||
			strings.Contains(message, "timeout") {
			return true, "临时性错误"
		}

		// 流动性不足 → 重试（可能很快恢复）
		if strings.Contains(message, "流动性不足") ||
			strings.Contains(message, "insufficient liquidity") {
			return true, "流动性不足"
		}

		// 价格变动过大 → 重试（需要重新获取报价）
		if strings.Contains(message, "价格变动") ||
			strings.Contains(message, "price changed") ||
			strings.Contains(message, "滑点") {
			return true, "价格变动"
		}

		// 永久性错误 → 不重试
		if strings.Contains(message, "余额不足") ||
			strings.Contains(message, "insufficient balance") ||
			strings.Contains(message, "代币不支持") ||
			strings.Contains(message, "token not supported") ||
			strings.Contains(message, "账户被冻结") ||
			strings.Contains(message, "account frozen") {
			return false, "永久性错误"
		}

		// 其他未知失败 → 重试
		return true, "未知失败"
	}

	// 不应该到达这里
	return false, "未知状态"
}

// executeSwapOrderWithRetry 执行交换下单（带智能重试机制）
func executeSwapOrderWithRetry(csrftoken, cookie string, tokenInfo *TokenInfo, contractAddress string, quoteResult *SwapQuoteResult, maxRetries int) (*SwapOrderResult, error) {
	var lastErr error
	var lastResult *SwapOrderResult

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 [尝试 %d/%d] 执行交换下单", attempt, maxRetries)

		result, err := executeSwapOrder(csrftoken, cookie, tokenInfo, contractAddress, quoteResult)

		// 成功情况
		if err == nil && result != nil && result.Success {
			log.Printf("✅ [尝试 %d/%d] 交换下单成功!", attempt, maxRetries)
			return result, nil
		}

		// 判断是否应该重试
		shouldRetry, reason := shouldRetryOrder(err, result)

		if err != nil {
			lastErr = err
			log.Printf("❌ [尝试 %d/%d] 交换下单失败: %v (原因: %s)", attempt, maxRetries, err, reason)
		} else if result != nil && !result.Success {
			lastErr = fmt.Errorf("下单返回失败: %s", result.Message)
			lastResult = result
			log.Printf("❌ [尝试 %d/%d] 交换下单返回失败: %s (原因: %s)", attempt, maxRetries, result.Message, reason)
		}

		// 如果不应该重试，直接返回
		if !shouldRetry {
			log.Printf("🚫 [尝试 %d/%d] 检测到永久性错误，停止重试: %s", attempt, maxRetries, reason)
			if lastResult != nil {
				return lastResult, lastErr
			}
			return nil, lastErr
		}

		// 如果不是最后一次尝试，等待固定时间再重试
		if attempt < maxRetries {
			waitTime := 500 * time.Millisecond // 固定等待500毫秒
			log.Printf("⏳ 检测到可重试错误(%s)，等待 %v 后进行第 %d 次重试", reason, waitTime, attempt+1)
			time.Sleep(waitTime)
		}
	}

	// 所有尝试都失败了
	log.Printf("💥 所有 %d 次尝试都失败了", maxRetries)
	if lastResult != nil && !lastResult.Success {
		return lastResult, lastErr
	}
	return nil, fmt.Errorf("所有 %d 次尝试都失败了，最后错误: %v", maxRetries, lastErr)
}
