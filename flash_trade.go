package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"alpha-autosell-bot/price"

	"github.com/go-redis/redis/v8"
)

// 速度模式常量
const (
	SpeedModeFast   = "fast"   // 快速模式：更短的延迟，更高的API调用频率
	SpeedModeNormal = "normal" // 正常模式：平衡的延迟和API调用频率
	SpeedModeSlow   = "slow"   // 慢速模式：更长的延迟，更低的API调用频率
)

// inferBaseAssetFromTokenAddress 根据代币地址推断 base_asset
func inferBaseAssetFromTokenAddress(tokenAddress string) string {
	// 不再使用硬编码映射，默认返回ALPHA_251
	// 实际的BaseAsset应该由/trade接口传递并保存在accountTradeParams中
	return "ALPHA_251"
}

// getChainID 获取链ID，如果为空则返回默认值"56"(BSC)
func getChainID(req *TradeRequest) string {
	if req != nil && req.ChainID != "" {
		return req.ChainID
	}
	return "56" // 默认使用BSC链
}

// getTaskChainID 获取AutoSellTask的链ID，如果为空则返回默认值"56"(BSC)
func getTaskChainID(task *AutoSellTask) string {
	if task.ChainID != "" {
		return task.ChainID
	}
	return "56" // 默认使用BSC链
}

// getPriceMode 获取价格模式，如果为空则返回默认值"auto"
func getPriceMode(req *TradeRequest) price.PriceMode {
	if req != nil && req.PriceMode != "" {
		switch strings.ToLower(req.PriceMode) {
		case "limit":
			return price.PriceModeLimit
		case "market":
			return price.PriceModeMarket
		case "combined":
			return price.PriceModeCombined
		case "auto":
			return price.PriceModeAuto
		default:
			log.Printf("⚠️ 未知的价格模式: %s, 使用默认模式", req.PriceMode)
			return price.PriceModeAuto
		}
	}
	return price.PriceModeAuto // 默认使用自动模式
}

// getBuyPriceMode 获取买单专用价格模式，优化成功率
func getBuyPriceMode(req *TradeRequest) price.PriceMode {
	if req != nil && req.PriceMode != "" {
		userMode := strings.ToLower(req.PriceMode)
		switch userMode {
		case "limit":
			// 限价模式：买单使用market模式获取更真实的价格
			log.Printf("🎯 [%s] 买单优化：用户选择limit模式，切换到market模式确保成功率", req.AccountID)
			return price.PriceModeMarket
		case "market":
			// 链上模式：直接使用，获取真实成交价
			return price.PriceModeMarket
		case "combined":
			// 综合模式：买单使用market模式，避免限价订单影响
			log.Printf("🎯 [%s] 买单优化：用户选择combined模式，切换到market模式确保成功率", req.AccountID)
			return price.PriceModeMarket
		case "auto":
			// 自动模式：买单使用market模式
			return price.PriceModeMarket
		default:
			log.Printf("⚠️ 未知的价格模式: %s, 买单使用market模式", req.PriceMode)
			return price.PriceModeMarket
		}
	}
	// 默认买单使用market模式，确保获取真实成交价格
	return price.PriceModeMarket
}

// 🚀 应用速度模式配置
func applySpeedMode(speedMode string) {
	mode, exists := speedModes[speedMode]
	if !exists {
		log.Printf("⚠️ 未知的速度模式: %s, 使用默认普通模式", speedMode)
		mode = speedModes["normal"]
	}

	log.Printf("🚀 应用速度模式: %s - %s", mode.Name, mode.Description)

	// 更新全局配置
	tradeInterval = mode.TradeInterval
	maxOrdersPerSecond = mode.MaxOrdersPerSecond
	maxAPICallsPerSecond = mode.MaxAPICallsPerSecond
	apiCallInterval = mode.APICallInterval

	// 更新全局API限制器
	globalAPILimiter.mutex.Lock()
	globalAPILimiter.maxRequests = mode.GlobalAPILimit
	globalAPILimiter.mutex.Unlock()


}

// 🚀 获取速度模式信息
func getSpeedModeInfo(speedMode string) SpeedModeConfig {
	mode, exists := speedModes[speedMode]
	if !exists {
		return speedModes["normal"] // 默认返回普通模式
	}
	return mode
}

// 🚀 获取当前速度模式（根据当前配置推断）
func getCurrentSpeedMode() SpeedModeConfig {
	// 根据当前的tradeInterval判断速度模式
	for _, mode := range speedModes {
		if mode.TradeInterval == tradeInterval {
			return mode
		}
	}
	// 如果找不到匹配的，返回普通模式
	return speedModes["normal"]
}

// 账户速度模式映射
var accountSpeedModes = make(map[string]string)
var accountSpeedModesMutex sync.RWMutex

// getSpeedModeForAccount 获取指定账户的速度模式
func getSpeedModeForAccount(accountID string) string {
	accountSpeedModesMutex.RLock()
	defer accountSpeedModesMutex.RUnlock()
	
	if mode, exists := accountSpeedModes[accountID]; exists {
		return mode
	}
	return SpeedModeNormal // 默认使用正常模式
}

// setSpeedModeForAccount 设置指定账户的速度模式
func setSpeedModeForAccount(accountID, mode string) {
	accountSpeedModesMutex.Lock()
	defer accountSpeedModesMutex.Unlock()
	
	accountSpeedModes[accountID] = mode
	log.Printf("🚀 [%s] 设置速度模式为: %s", accountID, mode)
}

type TradeRequest struct {
	TokenAddress   string  `json:"token_address,omitempty" json_cn:"代币地址,omitempty"`
	USDTAmount     float64 `json:"usdt_amount,omitempty" json_cn:"USDT金额,omitempty"`
	BaseAsset      string  `json:"base_asset,omitempty" json_cn:"基础资产,omitempty"`
	Csrftoken      string  `json:"csrftoken,omitempty" json_cn:"CSRF令牌,omitempty"`
	Cookie         string  `json:"cookie,omitempty" json_cn:"Cookie,omitempty"`
	TargetVolume   float64 `json:"target_volume,omitempty" json_cn:"目标交易额,omitempty"`  // 目标交易额
	AutoLoop       bool    `json:"auto_loop,omitempty" json_cn:"自动循环,omitempty"`       // 是否自动循环
	PricePrecision int     `json:"price_precision,omitempty" json_cn:"价格精度,omitempty"` // 价格精度位数
	ChainID        string  `json:"chain_id,omitempty" json_cn:"链ID,omitempty"`         // 区块链ID，默认为"56"(BSC)
	PriceMode      string  `json:"price_mode,omitempty" json_cn:"价格模式,omitempty"`      // 价格模式: limit/market/combined/auto
	SpeedMode      string  `json:"speed_mode,omitempty" json_cn:"速度模式,omitempty"`      // 🚀 新增：速度模式 (fast/normal/slow)

	// 固定延迟值，不从JSON解析
	MinDelay int `json:"-"` // 固定为1秒
	MaxDelay int `json:"-"` // 固定为2秒

	// AccountID 将通过KYC接口获取firstName自动填充
	AccountID string `json:"-"` // 不从JSON解析，内部使用
}

type TradeResponse struct {
	Success     bool    `json:"success" json_cn:"成功"`
	Message     string  `json:"message" json_cn:"消息"`
	BuyPrice    float64 `json:"buy_price" json_cn:"买入价格"`
	SellPrice   float64 `json:"sell_price" json_cn:"卖出价格"`
	TokenAmount float64 `json:"token_amount" json_cn:"代币数量"`
	Profit      float64 `json:"profit" json_cn:"盈亏"`
	ExecuteTime int64   `json:"execute_time_ms" json_cn:"执行时间毫秒"`
}

type OrderRequest struct {
	BaseAsset      string          `json:"baseAsset"`
	QuoteAsset     string          `json:"quoteAsset"`
	Side           string          `json:"side"`
	Price          float64         `json:"price"`
	Quantity       float64         `json:"quantity"`
	PaymentDetails []PaymentDetail `json:"paymentDetails"`
	Csrftoken      string          `json:"csrftoken"`
	Cookie         string          `json:"cookie"`
}

type PaymentDetail struct {
	Amount            float64 `json:"-"`      // 不直接序列化
	AmountStr         string  `json:"amount"` // 使用字符串控制精度
	PaymentWalletType string  `json:"paymentWalletType"`
}

// AccountStats 账号统计
type AccountStats struct {
	AccountID     string    `json:"账户ID"`
	TotalVolume   float64   `json:"总交易量"`  // 总交易额
	TargetVolume  float64   `json:"目标交易量"` // 目标交易额
	TradeCount    int       `json:"交易次数"`  // 交易次数
	LastTradeTime time.Time `json:"-"`     // 内部使用，不直接序列化
	IsLooping     bool      `json:"是否循环中"` // 是否在循环交易
	TotalLoss     float64   `json:"总亏损"`   // 总磨损 (USDT)
	TotalLossRate float64   `json:"总亏损率"`  // 总磨损率 (万分比)
}

// 🔧 新增：为 AccountStats 添加自定义 JSON 序列化方法
func (a AccountStats) MarshalJSON() ([]byte, error) {
	type Alias AccountStats
	return json.Marshal(&struct {
		*Alias
		LastTradeTimeUnix int64  `json:"最后交易时间"`
		LastTradeTimeStr  string `json:"最后交易时间_格式化"`
	}{
		Alias:             (*Alias)(&a),
		LastTradeTimeUnix: a.LastTradeTime.Unix(),
		LastTradeTimeStr:  a.LastTradeTime.Format("2006-01-02 15:04:05"),
	})
}

// 🔧 新增：累积任务统计
type CumulativeTaskStats struct {
	TaskID          string              `json:"task_id" json_cn:"任务ID"`             // 当前任务ID
	OriginalTaskID  string              `json:"original_task_id" json_cn:"原始任务ID"`  // 原始任务ID
	OriginalVolume  float64             `json:"original_volume" json_cn:"原始目标交易额"`  // 原始目标交易额
	CurrentVolume   float64             `json:"current_volume" json_cn:"当前任务目标交易额"` // 当前任务目标交易额
	CompletedVolume float64             `json:"completed_volume" json_cn:"已完成交易额"`  // 已完成交易额
	RestartCount    int                 `json:"restart_count" json_cn:"重启次数"`       // 重启次数
	History         []TaskHistoryRecord `json:"history" json_cn:"历史记录"`             // 历史记录
	CreatedAt       time.Time           `json:"created_at" json_cn:"创建时间"`          // 创建时间
	LastRestartAt   time.Time           `json:"last_restart_at" json_cn:"最后重启时间"`   // 最后重启时间
}

// 任务历史记录
type TaskHistoryRecord struct {
	TaskID          string    `json:"task_id" json_cn:"任务ID"`           // 任务ID
	StartTime       time.Time `json:"start_time" json_cn:"开始时间"`        // 开始时间
	EndTime         time.Time `json:"end_time" json_cn:"结束时间"`          // 结束时间
	TargetVolume    float64   `json:"target_volume" json_cn:"目标交易额"`    // 目标交易额
	CompletedVolume float64   `json:"completed_volume" json_cn:"完成交易额"` // 完成交易额
	TradeCount      int       `json:"trade_count" json_cn:"交易次数"`       // 交易次数
	ProfitLoss      float64   `json:"profit_loss" json_cn:"盈亏"`         // 盈亏
	EndReason       string    `json:"end_reason" json_cn:"结束原因"`        // 结束原因（完成/超时/手动停止）
}

var accountStats = make(map[string]*AccountStats)
var statsMutex sync.RWMutex
var loopingAccounts = make(map[string]chan bool) // 控制循环停止
var loopMutex sync.RWMutex
var currentTradeTokens = make(map[string]string) // 存储账号当前交易的代币 (accountID -> base_asset)
var tokensMutex sync.RWMutex
var serviceStartTime = time.Now() // 🔧 新增：服务启动时间

// 默认链ID常量
const DEFAULT_CHAIN_ID = "56" // BSC链ID

// 🚀 速度模式配置
type SpeedModeConfig struct {
	Name                 string        // 模式名称
	TradeInterval        time.Duration // 交易间隔
	MaxOrdersPerSecond   int           // 每秒最大下单数
	MaxAPICallsPerSecond int           // 每秒最大API调用数
	APICallInterval      time.Duration // API调用间隔
	VolumeAPIInterval    time.Duration // 刷量模式API间隔
	GlobalAPILimit       int           // 全局API限制
	Description          string        // 模式描述
}

// 🚀 三种速度模式配置
var speedModes = map[string]SpeedModeConfig{
	"fast": {
		Name:                 "快速模式",
		TradeInterval:        5 * time.Second,        // 5秒交易间隔
		MaxOrdersPerSecond:   3,                      // 每秒3次下单
		MaxAPICallsPerSecond: 8,                      // 每秒8次API调用
		APICallInterval:      100 * time.Millisecond, // 100ms API间隔
		VolumeAPIInterval:    150 * time.Millisecond, // 150ms刷量间隔
		GlobalAPILimit:       6,                      // 全局6次/秒
		Description:          "高效率，中等风控风险",
	},
	"normal": {
		Name:                 "普通模式",
		TradeInterval:        8 * time.Second,        // 8秒交易间隔
		MaxOrdersPerSecond:   2,                      // 每秒2次下单
		MaxAPICallsPerSecond: 4,                      // 每秒4次API调用
		APICallInterval:      200 * time.Millisecond, // 200ms API间隔
		VolumeAPIInterval:    300 * time.Millisecond, // 300ms刷量间隔
		GlobalAPILimit:       3,                      // 全局3次/秒
		Description:          "平衡效率与安全，推荐使用",
	},
	"slow": {
		Name:                 "慢速模式",
		TradeInterval:        15 * time.Second,       // 15秒交易间隔
		MaxOrdersPerSecond:   1,                      // 每秒1次下单
		MaxAPICallsPerSecond: 2,                      // 每秒2次API调用
		APICallInterval:      500 * time.Millisecond, // 500ms API间隔
		VolumeAPIInterval:    800 * time.Millisecond, // 800ms刷量间隔
		GlobalAPILimit:       1,                      // 全局1次/秒
		Description:          "极度保守，最低风控风险",
	},
}

// 🛡️ 币安安全刷量模式配置参数
var (
	maxBuyRetryAttempts   = 2    // 🔥 刷量优化：减少买入重试次数
	maxOrderRetryAttempts = 5    // 🔥 刷量优化：减少下单重试次数
	maxHTTPRetryAttempts  = 3    // 🔥 刷量优化：减少HTTP重试次数
	isVolumeMode          = true // 🔥 刷量模式开关

	// 🛡️ 动态配置参数（根据速度模式调整）
	maxOrdersPerSecond   = 2 // 每秒最大下单数（默认普通模式）
	maxAPICallsPerSecond = 4 // 每秒最大API调用数（默认普通模式）

	// 🛡️ 智能限流计数器
	orderCounter   = make(map[int64]int) // 每秒下单计数
	apiCallCounter = make(map[int64]int) // 每秒API调用计数
	counterMutex   sync.RWMutex
)
var tradeResults = make(map[string][]TradeResponse) // 存储交易结果
var resultsMutex sync.RWMutex

// 🔧 新增：防止重复统计的交易ID跟踪
var processedTradeIDs = make(map[string]bool) // 已处理的交易ID
var tradeIDMutex sync.RWMutex

// 🔧 新增：累积任务统计
var cumulativeTaskStats = make(map[string]*CumulativeTaskStats) // accountID -> CumulativeTaskStats
var cumulativeStatsMutex sync.RWMutex

// 全局统计
type GlobalStats struct {
	TotalTargetVolume  float64 `json:"total_target_volume" json_cn:"总目标交易量"`  // 总目标交易额
	TotalCurrentVolume float64 `json:"total_current_volume" json_cn:"总当前交易量"` // 当前完成交易额
	TotalLoss          float64 `json:"total_loss" json_cn:"总亏损"`              // 总磨损
	TotalLossRate      float64 `json:"total_loss_rate" json_cn:"总亏损率"`        // 总磨损率
	ActiveAccounts     int     `json:"active_accounts" json_cn:"活跃账户数"`       // 活跃账号数
	CompletionRate     float64 `json:"completion_rate" json_cn:"完成率"`         // 完成率
}

var globalStats = &GlobalStats{}
var globalMutex sync.RWMutex

// 🔧 新增：生成唯一交易ID
func generateTradeID(accountID, tokenAddress string, buyPrice, tokenAmount float64) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s_%s_%.6f_%.6f_%d", accountID, tokenAddress, buyPrice, tokenAmount, timestamp)
}

// 🔧 新增：检查交易是否已被统计
func isTradeProcessed(tradeID string) bool {
	tradeIDMutex.RLock()
	defer tradeIDMutex.RUnlock()
	return processedTradeIDs[tradeID]
}

// 🔧 新增：标记交易已被统计
func markTradeProcessed(tradeID string) {
	tradeIDMutex.Lock()
	defer tradeIDMutex.Unlock()
	processedTradeIDs[tradeID] = true
}

// 🔧 新增：智能止损配置
type SmartStopLossConfig struct {
	MaxLossRate        float64 // 最大亏损率（千分比）
	EmergencyLossRate  float64 // 紧急止损率（千分比）
	QuickSellThreshold float64 // 快速卖出阈值（千分比）
}

var smartStopLoss = &SmartStopLossConfig{
	MaxLossRate:        8.0,  // 千8最大亏损
	EmergencyLossRate:  12.0, // 千12紧急止损
	QuickSellThreshold: 6.0,  // 千6快速卖出
}

// 🔧 新增：网络容错增强配置
type NetworkResilienceConfig struct {
	MaxRetryAttempts    int           // 最大重试次数
	BaseRetryInterval   time.Duration // 基础重试间隔
	MaxRetryInterval    time.Duration // 最大重试间隔
	ConnectivityTimeout time.Duration // 连通性检查超时
	FastFailThreshold   int           // 快速失败阈值
}

var networkConfig = &NetworkResilienceConfig{
	MaxRetryAttempts:    10,
	BaseRetryInterval:   30 * time.Second,
	MaxRetryInterval:    300 * time.Second, // 5分钟最大间隔
	ConnectivityTimeout: 5 * time.Second,   // 5秒连通性检查超时
	FastFailThreshold:   3,                 // 3次连续失败后快速失败
}

// 🔧 新增：增强的网络连通性检查
func checkNetworkConnectivityEnhanced() bool {
	// 并行检查多个端点
	endpoints := []string{
		"https://api.binance.com/api/v3/ping",
		"https://www.google.com",
		"https://www.cloudflare.com",
	}

	successChan := make(chan bool, len(endpoints))

	for _, endpoint := range endpoints {
		go func(url string) {
			client := &http.Client{
				Timeout: networkConfig.ConnectivityTimeout,
			}

			resp, err := client.Get(url)
			if err == nil && resp.StatusCode < 500 {
				resp.Body.Close()
				successChan <- true
				return
			}
			if resp != nil {
				resp.Body.Close()
			}
			successChan <- false
		}(endpoint)
	}

	// 只要有一个成功就认为网络正常
	for i := 0; i < len(endpoints); i++ {
		if <-successChan {
			return true
		}
	}

	return false
}

// 🔧 新增：智能止损判断
func shouldEmergencyStopLoss(buyPrice, currentPrice float64) (bool, string) {
	if currentPrice <= 0 || buyPrice <= 0 {
		return false, ""
	}

	lossRate := (buyPrice - currentPrice) / buyPrice * 1000 // 千分比

	if lossRate >= smartStopLoss.EmergencyLossRate {
		return true, fmt.Sprintf("紧急止损(%.1f‰)", lossRate)
	} else if lossRate >= smartStopLoss.QuickSellThreshold {
		return true, fmt.Sprintf("快速止损(%.1f‰)", lossRate)
	}

	return false, ""
}

// 🔧 新增：检查是否可以继续购买（即使有低价值代币）
func canContinueBuying(accountID string) bool {
	// 检查USDT余额是否足够购买
	// 这里可以添加更多的检查逻辑
	return true // 默认允许继续购买
}

// 🔧 新增：检查账户是否有低价值代币但仍可继续交易
func hasLowValueTokensButCanTrade(accountID string) bool {
	// 检查是否有低价值代币，但不影响继续购买
	autoSellMutex.Lock()
	defer autoSellMutex.Unlock()

	for taskID, task := range autoSellManager.tasks {
		if strings.Contains(taskID, accountID) && task.IsActive {
			// 检查是否是低价值代币（检查间隔被调整为120秒或180秒）
			if task.CheckInterval >= 120 {
				log.Printf("🔍 [%s] 发现低价值代币但继续允许交易: %s", accountID, task.TokenAddress)
				return true
			}
		}
	}
	return false
}

// 🔧 新增：中文字段转换函数
func convertToChineseFields(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	switch v := data.(type) {
	case *AccountStats:
		result["账户ID"] = v.AccountID
		result["总交易量"] = v.TotalVolume
		result["目标交易量"] = v.TargetVolume
		result["交易次数"] = v.TradeCount
		result["是否循环中"] = v.IsLooping
		result["总亏损"] = v.TotalLoss
		result["总亏损率"] = v.TotalLossRate
		// 保持英文字段兼容性
		result["account_id"] = v.AccountID
		result["total_volume"] = v.TotalVolume
		result["target_volume"] = v.TargetVolume
		result["trade_count"] = v.TradeCount
		result["is_looping"] = v.IsLooping
		result["total_loss"] = v.TotalLoss
		result["total_loss_rate"] = v.TotalLossRate

	case *GlobalStats:
		result["总目标交易量"] = v.TotalTargetVolume
		result["总当前交易量"] = v.TotalCurrentVolume
		result["总亏损"] = v.TotalLoss
		result["总亏损率"] = v.TotalLossRate
		result["活跃账户数"] = v.ActiveAccounts
		result["完成率"] = v.CompletionRate
		// 保持英文字段兼容性
		result["total_target_volume"] = v.TotalTargetVolume
		result["total_current_volume"] = v.TotalCurrentVolume
		result["total_loss"] = v.TotalLoss
		result["total_loss_rate"] = v.TotalLossRate
		result["active_accounts"] = v.ActiveAccounts
		result["completion_rate"] = v.CompletionRate

	case TradeResponse:
		result["成功"] = v.Success
		result["消息"] = v.Message
		result["买入价格"] = v.BuyPrice
		result["卖出价格"] = v.SellPrice
		result["代币数量"] = v.TokenAmount
		result["盈亏"] = v.Profit
		result["执行时间毫秒"] = v.ExecuteTime
		// 保持英文字段兼容性
		result["success"] = v.Success
		result["message"] = v.Message
		result["buy_price"] = v.BuyPrice
		result["sell_price"] = v.SellPrice
		result["token_amount"] = v.TokenAmount
		result["profit"] = v.Profit
		result["execute_time_ms"] = v.ExecuteTime

	default:
		// 对于其他类型，直接返回原始数据
		return map[string]interface{}{"data": data}
	}

	return result
}

// Redis客户端
var redisClient *redis.Client
var redisCtx = context.Background()

// 账号信息结构
type AccountInfo struct {
	ID           string    `json:"id" json_cn:"账号ID"`
	Name         string    `json:"name" json_cn:"账号名称"`
	Csrftoken    string    `json:"csrftoken" json_cn:"CSRF令牌"`
	Cookie       string    `json:"cookie" json_cn:"Cookie"`
	AssignedNode string    `json:"assigned_node" json_cn:"分配节点"`
	Status       string    `json:"status" json_cn:"状态"`
	CreatedAt    time.Time `json:"created_at" json_cn:"创建时间"`
	ExpiresAt    time.Time `json:"expires_at" json_cn:"过期时间"`
}

// 全局API频率控制器
type APIRateLimiter struct {
	requests    []time.Time
	mutex       sync.Mutex
	maxRequests int           // 最大请求数
	timeWindow  time.Duration // 时间窗口
}

// RateLimitHandler 🔧 增强：429错误处理器 - 支持Retry-After和指数退避
type RateLimitHandler struct {
	isPaused         bool
	pauseUntil       time.Time
	pauseCount       int
	lastBackoffDelay time.Duration // 上次指数退避延迟时间
	mutex            sync.RWMutex
	lastPauseTime    time.Time
	monitorStopChan  chan struct{} // 🔧 修复死循环：监控器停止通道
}

var globalRateLimitHandler = &RateLimitHandler{}

// 🔧 增强：网络连通性检查，增加更多测试点
func checkNetworkConnectivity() bool {
	testHosts := []string{
		"8.8.8.8:53",         // Google DNS
		"1.1.1.1:53",         // Cloudflare DNS
		"114.114.114.114:53", // 114 DNS
		"223.5.5.5:53",       // 阿里DNS
		"119.29.29.29:53",    // 腾讯DNS
	}

	successCount := 0
	for _, host := range testHosts {
		conn, err := net.DialTimeout("tcp", host, 5*time.Second) // 增加超时时间
		if err == nil {
			conn.Close()
			successCount++
			// 至少有一个成功就认为网络可用
			if successCount >= 1 {
				return true
			}
		}
	}

	// 如果所有DNS都失败，尝试连接币安API
	binanceHosts := []string{
		"api.binance.com:443",
		"www.binance.com:443",
	}

	for _, host := range binanceHosts {
		conn, err := net.DialTimeout("tcp", host, 8*time.Second)
		if err == nil {
			conn.Close()
			log.Printf("🌐 DNS不可用但币安API可达，网络连接正常")
			return true
		}
	}

	return false
}

// 🔧 币安官方：记录权重使用情况和订单计数
func logWeightUsage(resp *http.Response, accountID string) {
	// 检查所有权重相关的响应头
	for name, values := range resp.Header {
		if strings.HasPrefix(name, "X-Mbx-Used-Weight") {
			for _, value := range values {

				// 解析权重值并警告
				if weight, err := strconv.Atoi(value); err == nil {
					if weight > 800 { // 接近1200限制时警告
						log.Printf("⚠️ [%s] 权重使用过高: %d/1200，建议减缓请求", accountID, weight)
					} else if weight > 1000 {
						log.Printf("🚨 [%s] 权重使用危险: %d/1200，立即减缓请求！", accountID, weight)
					}
				}
			}
		}
	}

	// 🔧 币安官方：检查未成交订单计数限制
	if orderCount := resp.Header.Get("X-Mbx-Order-Count-10s"); orderCount != "" {
		// 10秒订单数检查
		if count, err := strconv.Atoi(orderCount); err == nil {
			if count > 80 { // 接近100限制时警告
				log.Printf("⚠️ [%s] 10秒订单数过高: %d/100，建议减缓下单", accountID, count)
			} else if count > 90 {
				log.Printf("🚨 [%s] 10秒订单数危险: %d/100，立即减缓下单！", accountID, count)
			}
		}
	}
	if orderCount := resp.Header.Get("X-Mbx-Order-Count-1d"); orderCount != "" {
		// 24小时订单数检查
		if count, err := strconv.Atoi(orderCount); err == nil {
			if count > 160000 { // 接近200000限制时警告
				log.Printf("⚠️ [%s] 24小时订单数过高: %d/200000，建议减缓下单", accountID, count)
			} else if count > 180000 {
				log.Printf("🚨 [%s] 24小时订单数危险: %d/200000，立即减缓下单！", accountID, count)
			}
		}
	}
}

// 🔧 新增：判断是否为网络错误
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// 检查常见的网络错误类型
	errStr := err.Error()
	networkErrors := []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"network is unreachable",
		"no route to host",
		"timeout",
		"dial tcp",
		"i/o timeout",
		"broken pipe",
		"connection aborted",
	}

	for _, netErr := range networkErrors {
		if strings.Contains(strings.ToLower(errStr), netErr) {
			return true
		}
	}

	return false
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
	// Redis地址配置

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
		log.Printf("💡 Flash Trade将以独立模式运行，不从Redis读取账号数据")
		redisClient = nil
	} else {
		log.Printf("✅ Redis连接成功: %s", redisClient.Options().Addr)
	}
}

// getAccountsFromRedis 从Redis获取账号数据
func getAccountsFromRedis() map[string]*AccountInfo {
	accounts := make(map[string]*AccountInfo)

	if redisClient == nil {
		// Redis客户端未初始化，直接返回空数据
		return accounts
	}

	// 减少超时时间，避免阻塞统计接口
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 获取所有账号键
	keys, err := redisClient.Keys(ctx, "account:*").Result()
	if err != nil {
		log.Printf("⚠️ 获取Redis账号键失败: %v", err)
		return accounts
	}

	for _, key := range keys {
		accountData, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var account AccountInfo
		if err := json.Unmarshal([]byte(accountData), &account); err != nil {
			continue
		}

		accounts[account.ID] = &account
	}

	// 从Redis加载账号
	return accounts
}

// getAccountsFromRedisWithFallback 从Redis获取账号数据（带快速失败机制）
func getAccountsFromRedisWithFallback() map[string]*AccountInfo {
	// 使用通道实现快速失败
	resultChan := make(chan map[string]*AccountInfo, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("⚠️ getAccountsFromRedis panic: %v", r)
				resultChan <- make(map[string]*AccountInfo)
			}
		}()

		accounts := getAccountsFromRedis()
		resultChan <- accounts
	}()

	// 2秒超时，快速失败
	select {
	case accounts := <-resultChan:
		return accounts
	case <-time.After(2 * time.Second):
		log.Printf("⚠️ Redis账号数据获取超时，返回空数据")
		return make(map[string]*AccountInfo)
	}
}

// sanitizeAccountID 清理和验证账号ID，防止Redis注入
func sanitizeAccountID(accountID string) (string, error) {
	// 1. 检查长度限制
	if len(accountID) == 0 {
		return "", fmt.Errorf("账号ID不能为空")
	}
	if len(accountID) > 100 {
		return "", fmt.Errorf("账号ID长度不能超过100字符")
	}

	// 2. 检查是否包含危险字符
	// Redis命令注入常见的危险字符和命令
	dangerousPatterns := []string{
		"\n", "\r", " ", "\t", // 换行和空白字符
		"SLAVEOF", "CONFIG", "EVAL", // 危险的Redis命令
		"FLUSHDB", "FLUSHALL", "SHUTDOWN", "DEBUG",
		"SCRIPT", "CLIENT", "MONITOR",
		"*", "?", "[", "]", // 通配符
		"$(", "`", ";", "|", "&", // Shell注入字符
	}

	accountIDUpper := strings.ToUpper(accountID)
	for _, pattern := range dangerousPatterns {
		if strings.Contains(accountIDUpper, strings.ToUpper(pattern)) {
			return "", fmt.Errorf("账号ID包含非法字符或命令: %s", pattern)
		}
	}

	// 3. 只允许安全字符：字母、数字、中文、下划线、连字符、点号
	for _, r := range accountID {
		if !((r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r >= 0x4e00 && r <= 0x9fff) || // 中文字符范围
			r == '_' || r == '-' || r == '.') {
			return "", fmt.Errorf("账号ID包含非法字符: %c", r)
		}
	}

	return accountID, nil
}

// loadAccountAuthsFromRedis 从Redis加载账号认证信息
func loadAccountAuthsFromRedis() {
	if redisClient == nil {
		log.Printf("⚠️ Redis客户端未初始化，跳过账号认证信息加载")
		return
	}

	authMutex.Lock()
	defer authMutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取所有flash_trade账号认证键
	keys, err := redisClient.Keys(ctx, "flash_trade_auth:*").Result()
	if err != nil {
		log.Printf("⚠️ 获取Redis账号认证键失败: %v", err)
		return
	}

	loadedCount := 0
	for _, key := range keys {
		authData, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var auth AccountAuth
		if err := json.Unmarshal([]byte(authData), &auth); err != nil {
			log.Printf("⚠️ 解析账号认证数据失败 %s: %v", key, err)
			continue
		}

		globalAccountAuths[auth.AccountID] = &auth
		loadedCount++
	}

	log.Printf("✅ 从Redis加载了 %d 个账号认证信息", loadedCount)
}

// saveAccountAuthToRedis 保存单个账号认证信息到Redis
func saveAccountAuthToRedis(auth *AccountAuth) error {
	if redisClient == nil {
		log.Printf("⚠️ Redis客户端未初始化，无法保存账号认证信息")
		return fmt.Errorf("Redis客户端未初始化")
	}

	// 🔒 安全验证：清理账号ID，防止Redis注入
	sanitizedAccountID, err := sanitizeAccountID(auth.AccountID)
	if err != nil {
		log.Printf("🚨 [%s] 账号ID安全验证失败: %v", auth.AccountID, err)
		return fmt.Errorf("账号ID安全验证失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 序列化账号认证信息
	authData, err := json.Marshal(auth)
	if err != nil {
		return fmt.Errorf("序列化账号认证信息失败: %v", err)
	}

	// 保存到Redis，使用清理后的账号ID构造键名
	key := fmt.Sprintf("flash_trade_auth:%s", sanitizedAccountID)
	err = redisClient.Set(ctx, key, authData, 7*24*time.Hour).Err() // 7天过期
	if err != nil {
		return fmt.Errorf("保存到Redis失败: %v", err)
	}

	// 账号认证信息已保存
	return nil
}

// deleteAccountAuthFromRedis 从Redis删除账号认证信息
func deleteAccountAuthFromRedis(accountID string) error {
	if redisClient == nil {
		return fmt.Errorf("Redis客户端未初始化")
	}

	// 🔒 安全验证：清理账号ID，防止Redis注入
	sanitizedAccountID, err := sanitizeAccountID(accountID)
	if err != nil {
		log.Printf("🚨 [%s] 账号ID安全验证失败: %v", accountID, err)
		return fmt.Errorf("账号ID安全验证失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := fmt.Sprintf("flash_trade_auth:%s", sanitizedAccountID)
	err = redisClient.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("从Redis删除失败: %v", err)
	}

	log.Printf("🗑️ [%s] 账号认证信息已从Redis删除", sanitizedAccountID)
	return nil
}

// 🔧 新增：检查是否因429错误而暂停
func (h *RateLimitHandler) IsPaused() bool {
	h.mutex.Lock() // 🔧 修复：使用写锁，允许修改状态
	defer h.mutex.Unlock()

	if !h.isPaused {
		return false
	}

	// 🔧 修复：检查暂停时间是否已过，如果已过则自动重置状态
	if time.Now().After(h.pauseUntil) {
		log.Printf("✅ 暂停时间已过，自动恢复正常状态")
		h.isPaused = false
		h.lastBackoffDelay = 0 // 重置指数退避
		return false
	}

	return true
}

// 🔧 新增：处理429错误，触发暂停
func (h *RateLimitHandler) Handle429Error(accountID string, retryAfter string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	now := time.Now()
	var pauseDuration time.Duration

	// 1. 优先使用 Retry-After 响应头
	if retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
			pauseDuration = time.Duration(seconds) * time.Second
			// 使用 Retry-After 响应头
		} else {
			// 尝试解析 HTTP 日期格式
			if retryTime, err := time.Parse(time.RFC1123, retryAfter); err == nil {
				pauseDuration = retryTime.Sub(now)
				if pauseDuration < 0 {
					pauseDuration = 2 * time.Second // 最小2秒
				}
				// 使用 Retry-After 日期
			}
		}
	}

	// 🔧 优化：如果没有 Retry-After 或解析失败，使用优化的指数退避策略
	if pauseDuration == 0 {
		if h.lastBackoffDelay == 0 {
			// 优化：首次退避1秒（从2秒缩短）
			h.lastBackoffDelay = 1 * time.Second
		} else {
			// 优化：指数退避：1s → 2s → 4s → 8s → 16s → 30s (最大30秒)
			h.lastBackoffDelay = h.lastBackoffDelay * 2
			if h.lastBackoffDelay > 30*time.Second {
				h.lastBackoffDelay = 30 * time.Second // 优化：从64秒缩短到30秒
			}
		}
		pauseDuration = h.lastBackoffDelay
		// 使用优化的指数退避策略
	}

	// 如果已经在暂停中，选择更长的等待时间
	if h.isPaused && now.Before(h.pauseUntil) {
		remainingTime := h.pauseUntil.Sub(now)
		if pauseDuration > remainingTime {
			h.pauseUntil = now.Add(pauseDuration)
			log.Printf("🚫 [%s] 暂停期间再次检测到429错误，延长暂停至: %s (新等待时间: %v)",
				accountID, h.pauseUntil.Format("2006-01-02 15:04:05"), pauseDuration)
		} else {
			log.Printf("🚫 [%s] 暂停期间再次检测到429错误，保持当前暂停时间: %s (剩余: %v)",
				accountID, h.pauseUntil.Format("2006-01-02 15:04:05"), remainingTime)
		}
		return
	}

	// 首次暂停或暂停已过期
	h.pauseCount++
	h.lastPauseTime = now
	h.pauseUntil = now.Add(pauseDuration)
	h.isPaused = true

	log.Printf("🚫 [%s] 检测到429错误，暂停API调用 %v (第%d次暂停)",
		accountID, pauseDuration, h.pauseCount)
	log.Printf("⏰ [%s] 预计恢复时间: %s",
		accountID, h.pauseUntil.Format("2006-01-02 15:04:05"))
}

// 🔧 修复：等待暂停结束
func (h *RateLimitHandler) WaitIfPaused(accountID string) {
	// 🔧 修复：简化逻辑，IsPaused()已经会自动重置状态
	for h.IsPaused() {
		h.mutex.RLock()
		remainingTime := time.Until(h.pauseUntil)
		h.mutex.RUnlock()

		if remainingTime <= 0 {
			// 时间已过，再次检查IsPaused()会自动重置状态
			break
		}

		log.Printf("⏳ [%s] API暂停中，剩余时间: %v", accountID, remainingTime.Round(time.Second))

		// 每10秒报告一次剩余时间
		sleepTime := 10 * time.Second
		if remainingTime < sleepTime {
			sleepTime = remainingTime
		}
		time.Sleep(sleepTime)
	}

}

// 🔧 新增：重置指数退避延迟（在成功请求后调用）
func (h *RateLimitHandler) ResetBackoff() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.lastBackoffDelay = 0
}

// 🔧 币安官方：获取交易所信息和订单限制
func getExchangeInfo() (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", "https://api.binance.com/api/v3/exchangeInfo", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

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

	var exchangeInfo map[string]interface{}
	if err := json.Unmarshal(body, &exchangeInfo); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 提取订单速率限制信息
	result := map[string]interface{}{
		"serverTime": exchangeInfo["serverTime"],
		"timezone":   exchangeInfo["timezone"],
	}

	// 提取速率限制信息
	if rateLimits, ok := exchangeInfo["rateLimits"].([]interface{}); ok {
		orderLimits := make([]map[string]interface{}, 0)
		for _, limit := range rateLimits {
			if limitMap, ok := limit.(map[string]interface{}); ok {
				rateLimitType, _ := limitMap["rateLimitType"].(string)
				if rateLimitType == "ORDERS" {
					orderLimits = append(orderLimits, limitMap)
					// 发现订单速率限制
				}
			}
		}
		result["orderLimits"] = orderLimits
	}

	return result, nil
}

// 🔧 新增：启动暂停状态监控器
func (h *RateLimitHandler) StartMonitor() {
	h.monitorStopChan = make(chan struct{})

	go func() {
		ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 主动检查暂停状态，触发自动恢复
				if h.IsPaused() {
					h.mutex.RLock()
					remaining := time.Until(h.pauseUntil)
					h.mutex.RUnlock()

					if remaining > 0 {
						log.Printf("⏳ 暂停监控：剩余时间 %v", remaining.Round(time.Second))
					}
				}
			case <-h.monitorStopChan:
				log.Printf("🛑 暂停状态监控器已停止")
				return
			}
		}
	}()
}

// 🔧 新增：强制恢复暂停状态
func (h *RateLimitHandler) ForceResume() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.isPaused {
		log.Printf("🔧 强制恢复：清除暂停状态")
		h.isPaused = false
		h.lastBackoffDelay = 0
		h.pauseUntil = time.Time{} // 重置暂停时间
		log.Printf("✅ 暂停状态已强制恢复")
	} else {
		log.Printf("ℹ️ 当前未处于暂停状态")
	}
}

// 🔧 新增：累积统计管理函数

// initCumulativeTask 初始化累积任务统计
func initCumulativeTask(accountID, taskID string, targetVolume float64) {
	cumulativeStatsMutex.Lock()
	defer cumulativeStatsMutex.Unlock()

	// 检查是否已存在累积统计
	if existing, exists := cumulativeTaskStats[accountID]; exists {
		// 保存当前任务的历史记录
		saveCurrentTaskHistory(existing, accountID, "restart")

		// 更新累积统计
		existing.TaskID = taskID
		existing.CurrentVolume = targetVolume
		existing.RestartCount++
		existing.LastRestartAt = time.Now()

		// 累积任务重启
	} else {
		// 创建新的累积统计
		cumulativeTaskStats[accountID] = &CumulativeTaskStats{
			TaskID:          taskID,
			OriginalTaskID:  taskID,
			OriginalVolume:  targetVolume,
			CurrentVolume:   targetVolume,
			CompletedVolume: 0,
			RestartCount:    0,
			History:         make([]TaskHistoryRecord, 0),
			CreatedAt:       time.Now(),
			LastRestartAt:   time.Now(),
		}

		// 创建累积任务统计
	}
}

// saveCurrentTaskHistory 保存当前任务的历史记录
func saveCurrentTaskHistory(cumulative *CumulativeTaskStats, accountID, endReason string) {
	// 获取当前任务的统计数据
	statsMutex.RLock()
	currentStats, exists := accountStats[accountID] // 🔧 修复：使用accountID而不是taskID
	statsMutex.RUnlock()

	if !exists {
		return
	}

	// 创建历史记录
	history := TaskHistoryRecord{
		TaskID:          cumulative.TaskID,
		StartTime:       cumulative.LastRestartAt,
		EndTime:         time.Now(),
		TargetVolume:    cumulative.CurrentVolume,
		CompletedVolume: currentStats.TotalVolume,
		TradeCount:      currentStats.TradeCount,
		ProfitLoss:      currentStats.TotalLoss,
		EndReason:       endReason,
	}

	// 添加到历史记录
	cumulative.History = append(cumulative.History, history)

	// 更新累积完成交易额
	cumulative.CompletedVolume += currentStats.TotalVolume

	// 保存任务历史
}

// getCumulativeStats 获取累积统计信息
func getCumulativeStats(accountID string) *CumulativeTaskStats {
	cumulativeStatsMutex.RLock()
	defer cumulativeStatsMutex.RUnlock()

	if cumulative, exists := cumulativeTaskStats[accountID]; exists {
		// 返回副本，包含最新的当前任务数据
		result := *cumulative

		// 获取当前任务的实时数据
		statsMutex.RLock()
		if currentStats, exists := accountStats[accountID]; exists {
			result.CompletedVolume = cumulative.CompletedVolume + currentStats.TotalVolume
		}
		statsMutex.RUnlock()

		return &result
	}

	return nil
}

// getTotalCompletedVolume 获取总完成交易额（历史+当前）
func getTotalCompletedVolume(accountID string) float64 {
	cumulativeStatsMutex.RLock()
	defer cumulativeStatsMutex.RUnlock()

	if cumulative, exists := cumulativeTaskStats[accountID]; exists {
		// 历史完成交易额
		totalHistorical := cumulative.CompletedVolume

		// 当前任务完成交易额
		statsMutex.RLock()
		currentVolume := float64(0)
		if currentStats, exists := accountStats[accountID]; exists {
			currentVolume = currentStats.TotalVolume
		}
		statsMutex.RUnlock()

		return totalHistorical + currentVolume
	}

	return 0
}

// 🔧 新增：获取暂停状态信息
func (h *RateLimitHandler) GetPauseStatus() map[string]interface{} {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	status := map[string]interface{}{
		"is_paused":             h.isPaused,
		"pause_count":           h.pauseCount,
		"last_pause_time":       h.lastPauseTime.Format("2006-01-02 15:04:05"),
		"backoff_delay_seconds": int(h.lastBackoffDelay.Seconds()),
	}

	if h.isPaused {
		status["pause_until"] = h.pauseUntil.Format("2006-01-02 15:04:05")
		status["remaining_seconds"] = int(time.Until(h.pauseUntil).Seconds())
	}

	return status
}

// 检查是否可以发送请求
func (r *APIRateLimiter) CanRequest() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	now := time.Now()

	// 清理超过时间窗口的请求记录
	cutoff := now.Add(-r.timeWindow)
	validRequests := make([]time.Time, 0)
	for _, reqTime := range r.requests {
		if reqTime.After(cutoff) {
			validRequests = append(validRequests, reqTime)
		}
	}
	r.requests = validRequests

	// 检查是否超过限制
	return len(r.requests) < r.maxRequests
}

// 记录一次请求
func (r *APIRateLimiter) RecordRequest() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.requests = append(r.requests, time.Now())
}

// 🔧 优化：等待直到可以发送请求（提升响应速度）
func (r *APIRateLimiter) WaitForSlot() {
	for !r.CanRequest() {
		time.Sleep(50 * time.Millisecond) // 适度等待时间，减少CPU占用，更平滑的请求分布
	}
	// 仅记录请求，不再添加随机延迟，确保价格获取后可以立即执行交易
	r.RecordRequest()
}

// 根据速度模式调整API调用间隔
func (r *APIRateLimiter) AdjustForSpeedMode(accountID string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	speedMode := getSpeedModeForAccount(accountID)
	switch speedMode {
	case SpeedModeFast:
		r.maxRequests = 1
		// 快速模式：1.5-2.5秒随机窗口
		r.timeWindow = time.Duration(1500+rand.Intn(1000)) * time.Millisecond
	case SpeedModeSlow:
		r.maxRequests = 1
		// 慢速模式：7-9秒随机窗口
		r.timeWindow = time.Duration(7000+rand.Intn(2000)) * time.Millisecond
	default: // SpeedModeNormal
		r.maxRequests = 1
		// 正常模式：4-6秒随机窗口
		r.timeWindow = time.Duration(4000+rand.Intn(2000)) * time.Millisecond
	}
}

// 🔧 优化：全局API频率限制器实例（提升并发能力）
var globalAPILimiter = &APIRateLimiter{
	requests:    make([]time.Time, 0),
	maxRequests: 1, // 🚨 风控优化：默认1次/5秒，可根据速度模式调整
	timeWindow:  5 * time.Second, // 默认时间窗口5秒
}

// API调用包装器
func safeAPICall(apiFunc func() error, funcName string, accountID string) error {
	// 🔧 新增：检查是否因429错误暂停
	globalRateLimitHandler.WaitIfPaused(accountID)
	
	// 根据账户速度模式调整API限速器
	globalAPILimiter.AdjustForSpeedMode(accountID)

	// 等待API调用槽位
	globalAPILimiter.WaitForSlot()
	
	// 🚨 优化：根据函数类型和速度模式添加延迟
	// 关键交易函数（下单、价格获取等）不添加延迟，非关键函数添加随机延迟
	if !isTimeCriticalFunction(funcName) {
		speedMode := getSpeedModeForAccount(accountID)
		var delayTime time.Duration
		
		switch speedMode {
		case SpeedModeFast:
			// 快速模式：0.5-0.8秒随机延迟
			delayTime = time.Duration(500+rand.Intn(300)) * time.Millisecond
			log.Printf("⚡ [%s] 快速模式：API调用前延迟%.1f秒 [%s]", accountID, delayTime.Seconds(), funcName)
		case SpeedModeSlow:
			// 慢速模式：3-5秒随机延迟
			delayTime = time.Duration(3000+rand.Intn(2000)) * time.Millisecond
			log.Printf("🐢 [%s] 慢速模式：API调用前延迟%.1f秒 [%s]", accountID, delayTime.Seconds(), funcName)
		default: // SpeedModeNormal
			// 正常模式：1.5-2.5秒随机延迟
			delayTime = time.Duration(1500+rand.Intn(1000)) * time.Millisecond
			log.Printf("⏱️ [%s] 正常模式：API调用前延迟%.1f秒 [%s]", accountID, delayTime.Seconds(), funcName)
		}
		
		time.Sleep(delayTime)
	}

	// 执行API调用
	err := apiFunc()

	if err != nil {
		log.Printf("⚠️ [%s] API调用失败 [%s]: %v", accountID, funcName, err)
	}

	return err
}

// isTimeCriticalFunction 判断是否为时间关键函数（不应添加延迟的函数）
func isTimeCriticalFunction(funcName string) bool {
	// 这些函数涉及价格获取和交易执行，不应添加延迟
	timeCriticalFunctions := map[string]bool{
		"PlaceOrder":        true, // 下单
		"GetPrice":          true, // 获取价格
		"ExecuteTrade":      true, // 执行交易
		"GetTokenPrice":     true, // 获取代币价格
		"GetMarketPrice":    true, // 获取市场价格
		"SubmitMarketOrder": true, // 提交市价单
	}
	
	return timeCriticalFunctions[funcName]
}

// 🔧 新增：带429错误检测的HTTP请求执行器
func executeHTTPRequestWithRateLimit(req *http.Request, client *http.Client, accountID string) (*http.Response, error) {
	return executeHTTPRequestWithRetry(req, client, accountID, 0)
}

// 🔧 优化重试策略：30秒起始，累加15秒，最大重试10次
const maxRetryAttempts = 10

func executeHTTPRequestWithRetry(req *http.Request, client *http.Client, accountID string, retryCount int) (*http.Response, error) {
	// 🔧 修复死循环：检查是否超过最大重试次数
	if retryCount >= maxRetryAttempts {
		return nil, fmt.Errorf("达到最大重试次数 %d，请求失败", maxRetryAttempts)
	}
	// 检查是否暂停
	globalRateLimitHandler.WaitIfPaused(accountID)

	// 🔧 优化重试策略：使用增强的网络连通性检查
	if !checkNetworkConnectivityEnhanced() {
		// 超过最大重试次数，返回错误
		if retryCount >= maxRetryAttempts {
			return nil, fmt.Errorf("网络连通性检查失败，已达到最大重试次数(%d)", maxRetryAttempts)
		}

		// 计算等待时间：30秒 + retryCount * 15秒
		baseWaitTime := 30 * time.Second                               // 30秒
		additionalWait := time.Duration(retryCount) * 15 * time.Second // 累加15秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("⚠️ [%s] 网络连通性检查失败，%v后进行第%d次重试(最大%d次)", accountID, waitTime, retryCount+1, maxRetryAttempts)
		time.Sleep(waitTime)

		// 有限重试
		return executeHTTPRequestWithRetry(req, client, accountID, retryCount+1)
	}

	// 执行请求
	resp, err := client.Do(req)
	if err != nil {
		// 超过最大重试次数，返回错误
		if retryCount >= maxRetryAttempts {
			return nil, fmt.Errorf("HTTP请求失败，已达到最大重试次数(%d): %v", maxRetryAttempts, err)
		}

		// 🔧 优化异常处理：使用优化重试策略
		// 计算等待时间：30秒 + retryCount * 15秒
		baseWaitTime := 30 * time.Second                               // 30秒
		additionalWait := time.Duration(retryCount) * 15 * time.Second // 累加15秒
		waitTime := baseWaitTime + additionalWait

		log.Printf("❌ [%s] HTTP请求失败，%v后进行第%d次重试(最大%d次): %v", accountID, waitTime, retryCount+1, maxRetryAttempts, err)
		time.Sleep(waitTime)

		// 有限重试
		return executeHTTPRequestWithRetry(req, client, accountID, retryCount+1)
	}

	// 🔧 币安官方：监控权重使用情况
	logWeightUsage(resp, accountID)

	// 🔧 币安官方：处理418错误 - IP被封禁
	if resp.StatusCode == 418 {
		retryAfter := resp.Header.Get("Retry-After")
		resp.Body.Close()

		log.Printf("🚨 [%s] 检测到418错误 - IP已被封禁！", accountID)
		if retryAfter != "" {
			log.Printf("📋 [%s] 封禁时间 Retry-After: %s 秒", accountID, retryAfter)

			// 解析封禁时间
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				banDuration := time.Duration(seconds) * time.Second
				log.Printf("🚨 [%s] IP封禁 %v，停止所有请求", accountID, banDuration)

				// 长时间等待，直到封禁结束
				time.Sleep(banDuration)
				log.Printf("✅ [%s] IP封禁结束，恢复请求", accountID)
			}
		} else {
			// 没有Retry-After，等待默认时间
			log.Printf("🚨 [%s] 没有Retry-After头，等待5分钟", accountID)
			time.Sleep(5 * time.Minute)
		}

		// 重试
		return executeHTTPRequestWithRetry(req, client, accountID, retryCount+1)
	}

	// 🔧 币安官方：处理429错误 - 频率限制
	if resp.StatusCode == 429 {
		retryAfter := resp.Header.Get("Retry-After")
		resp.Body.Close()

		log.Printf("🚫 [%s] 检测到429错误 - 频率限制 (第%d次重试)", accountID, retryCount+1)
		if retryAfter != "" {
			log.Printf("📋 [%s] Retry-After 响应头: %s", accountID, retryAfter)
		}

		// 使用新的429错误处理器
		globalRateLimitHandler.Handle429Error(accountID, retryAfter)

		// 等待暂停结束
		globalRateLimitHandler.WaitIfPaused(accountID)

		// 重试
		return executeHTTPRequestWithRetry(req, client, accountID, retryCount+1)
	}

	// 如果是重试成功，记录日志
	if retryCount > 0 {
		log.Printf("✅ [%s] 重试成功 (第%d次重试)", accountID, retryCount)
	}

	// 🔧 新增：成功请求后重置指数退避延迟
	globalRateLimitHandler.ResetBackoff()

	return resp, nil
}

// 极端行情配置
type ExtremeMarketConfig struct {
	Enabled             bool    `json:"enabled"`              // 是否启用极端行情检测
	DropThreshold       float64 `json:"drop_threshold"`       // 极端下跌阈值（百分比）
	VolatileThreshold   float64 `json:"volatile_threshold"`   // 极端波动阈值（百分比）
	AutoPause           bool    `json:"auto_pause"`           // 是否自动暂停
	PauseDuration       int     `json:"pause_duration"`       // 暂停持续时间（分钟）
	CleanupOrders       bool    `json:"cleanup_orders"`       // 是否清理挂单
	CleanupTokens       bool    `json:"cleanup_tokens"`       // 是否清理代币残留
	AutoResumeEnabled   bool    `json:"auto_resume_enabled"`  // 是否自动恢复
	StabilityThreshold  float64 `json:"stability_threshold"`  // 稳定阈值（百分比）
	StabilityDuration   int     `json:"stability_duration"`   // 稳定持续时间（分钟）
}

// 全局极端行情配置
var extremeMarketConfig = ExtremeMarketConfig{
	Enabled:             true,
	DropThreshold:       -15.0,  // 默认-15%为极端下跌
	VolatileThreshold:   10.0,   // 默认10%为极端波动
	AutoPause:           true,
	PauseDuration:       30,     // 默认暂停30分钟
	CleanupOrders:       true,
	CleanupTokens:       true,
	AutoResumeEnabled:   true,
	StabilityThreshold:  5.0,    // 默认波动小于5%视为稳定
	StabilityDuration:   15,     // 默认稳定15分钟后恢复
}

// 极端行情暂停记录
type ExtremeMarketPause struct {
	TokenAddress    string    // 代币地址
	ChainID         string    // 链ID
	PauseTime       time.Time // 暂停时间
	Reason          string    // 暂停原因
	AffectedAccounts []string  // 受影响的账户
}

// 全局极端行情暂停记录
var (
	extremeMarketPauses     = make(map[string]*ExtremeMarketPause) // 代币地址@链ID -> 暂停记录
	extremeMarketPauseMutex sync.RWMutex
	
	// 市场恢复监控器
	marketRecoveryMonitor     *time.Ticker
	marketRecoveryStopChan    chan struct{}
	marketRecoveryIsRunning   bool
	marketRecoveryMutex       sync.Mutex
)

// 检查是否应该暂停交易
func shouldPauseTrading(tokenAddress, chainID string) (bool, string) {
	// 如果未启用极端行情检测，直接返回false
	if !extremeMarketConfig.Enabled {
		return false, ""
	}
	
	// 检查是否已经暂停
	extremeMarketPauseMutex.RLock()
	key := tokenAddress + "@" + chainID
	if pause, exists := extremeMarketPauses[key]; exists {
		// 如果暂停时间未超过设定的暂停时长，继续暂停
		if time.Since(pause.PauseTime) < time.Duration(extremeMarketConfig.PauseDuration)*time.Minute {
			extremeMarketPauseMutex.RUnlock()
			return true, pause.Reason
		}
	}
	extremeMarketPauseMutex.RUnlock()
	
	// 检查当前市场状态
	isExtreme, reason := price.IsExtremeMarket(tokenAddress, chainID)
	if isExtreme && extremeMarketConfig.AutoPause {
		// 记录极端行情暂停
		pauseTrading(tokenAddress, chainID, reason)
		return true, reason
	}
	
	return false, ""
}

// 暂停交易
func pauseTrading(tokenAddress, chainID, reason string) {
	key := tokenAddress + "@" + chainID
	
	extremeMarketPauseMutex.Lock()
	defer extremeMarketPauseMutex.Unlock()
	
	// 如果已经存在暂停记录，更新时间和原因
	if pause, exists := extremeMarketPauses[key]; exists {
		pause.PauseTime = time.Now()
		pause.Reason = reason
		log.Printf("🚨 更新极端行情暂停: %s, 原因: %s", key, reason)
		return
	}
	
	// 创建新的暂停记录
	extremeMarketPauses[key] = &ExtremeMarketPause{
		TokenAddress:     tokenAddress,
		ChainID:          chainID,
		PauseTime:        time.Now(),
		Reason:           reason,
		AffectedAccounts: make([]string, 0),
	}
	
	log.Printf("🚨 检测到极端行情，暂停交易: %s, 原因: %s", key, reason)
	
	// 启动市场恢复监控（如果未启动）
	startMarketRecoveryMonitor()
}

// 添加账户到受影响列表
func addAffectedAccount(tokenAddress, chainID, accountID string) {
	key := tokenAddress + "@" + chainID
	
	extremeMarketPauseMutex.Lock()
	defer extremeMarketPauseMutex.Unlock()
	
	if pause, exists := extremeMarketPauses[key]; exists {
		// 检查账户是否已在列表中
		for _, acc := range pause.AffectedAccounts {
			if acc == accountID {
				return
			}
		}
		
		// 添加账户到列表
		pause.AffectedAccounts = append(pause.AffectedAccounts, accountID)
		
		// 设置账户暂停状态
		if extremeMarketConfig.AutoPause {
			setAccountPauseStatus(accountID, time.Duration(extremeMarketConfig.PauseDuration)*time.Minute, 
				fmt.Sprintf("极端行情暂停: %s", pause.Reason))
			
			// 清理账户挂单
			if extremeMarketConfig.CleanupOrders {
				auth, exists := getAccountAuth(accountID)
				if exists {
					go func(auth *AccountAuth) {
						cleanupAccountOrders(accountID, auth.Csrftoken, auth.Cookie)
					}(auth)
				}
			}
			
			// 清理代币残留
			if extremeMarketConfig.CleanupTokens {
				auth, exists := getAccountAuth(accountID)
				if exists {
					go func(auth *AccountAuth, tokenAddr, chainId string) {
						// 获取代币基础资产
						baseAsset := inferBaseAssetFromTokenAddress(tokenAddr)
						
						// 强制清理代币
						forceCleanToken(tokenAddr, baseAsset, auth.Csrftoken, auth.Cookie, 8)
					}(auth, tokenAddress, chainID)
				}
			}
		}
	}
}

// 启动市场恢复监控
func startMarketRecoveryMonitor() {
	marketRecoveryMutex.Lock()
	defer marketRecoveryMutex.Unlock()
	
	if marketRecoveryIsRunning {
		return
	}
	
	marketRecoveryIsRunning = true
	marketRecoveryStopChan = make(chan struct{})
	marketRecoveryMonitor = time.NewTicker(1 * time.Minute)
	
	go func() {
		for {
			select {
			case <-marketRecoveryMonitor.C:
				checkMarketRecovery()
			case <-marketRecoveryStopChan:
				marketRecoveryMonitor.Stop()
				return
			}
		}
	}()
	
	log.Printf("🔄 启动市场恢复监控")
}

// 停止市场恢复监控
func stopMarketRecoveryMonitor() {
	marketRecoveryMutex.Lock()
	defer marketRecoveryMutex.Unlock()
	
	if !marketRecoveryIsRunning {
		return
	}
	
	close(marketRecoveryStopChan)
	marketRecoveryIsRunning = false
	
	log.Printf("⏹️ 停止市场恢复监控")
}

// 检查市场是否恢复
func checkMarketRecovery() {
	extremeMarketPauseMutex.Lock()
	defer extremeMarketPauseMutex.Unlock()
	
	// 如果没有暂停记录或未启用自动恢复，直接返回
	if len(extremeMarketPauses) == 0 || !extremeMarketConfig.AutoResumeEnabled {
		return
	}
	
	now := time.Now()
	tokensToResume := make([]string, 0)
	
	// 检查每个暂停的代币
	for key, pause := range extremeMarketPauses {
		// 检查是否已经超过暂停时间
		if now.Sub(pause.PauseTime) > time.Duration(extremeMarketConfig.PauseDuration)*time.Minute {
			// 获取当前市场状态
			condition := price.GetMarketCondition(pause.TokenAddress, pause.ChainID)
			
			// 检查市场是否稳定
			if !condition.IsExtreme && math.Abs(condition.PriceChange) < extremeMarketConfig.StabilityThreshold {
				tokensToResume = append(tokensToResume, key)
				
				// 恢复受影响的账户
				for _, accountID := range pause.AffectedAccounts {
					clearAccountPauseStatus(accountID)
					log.Printf("✅ 市场恢复，重新启用账户: %s", accountID)
				}
				
				log.Printf("✅ 市场恢复，恢复交易: %s", key)
			}
		}
	}
	
	// 移除已恢复的代币
	for _, key := range tokensToResume {
		delete(extremeMarketPauses, key)
	}
	
	// 如果没有暂停记录，停止监控
	if len(extremeMarketPauses) == 0 {
		go stopMarketRecoveryMonitor()
	}
}

// 获取极端行情配置
func getExtremeMarketConfig() ExtremeMarketConfig {
	return extremeMarketConfig
}

// 设置极端行情配置
func setExtremeMarketConfig(config ExtremeMarketConfig) {
	extremeMarketConfig = config
	
	// 更新price包中的阈值
	price.SetExtremeThresholds(config.DropThreshold, config.VolatileThreshold)
}

// 获取所有极端行情暂停记录
func getExtremeMarketPauses() map[string]interface{} {
	extremeMarketPauseMutex.RLock()
	defer extremeMarketPauseMutex.RUnlock()
	
	result := make(map[string]interface{})
	
	for key, pause := range extremeMarketPauses {
		result[key] = map[string]interface{}{
			"token_address":     pause.TokenAddress,
			"chain_id":          pause.ChainID,
			"pause_time":        pause.PauseTime,
			"reason":            pause.Reason,
			"affected_accounts": pause.AffectedAccounts,
			"remaining_minutes": extremeMarketConfig.PauseDuration - int(time.Since(pause.PauseTime).Minutes()),
		}
	}
	
	return result
}

// 处理极端行情配置API
func handleExtremeMarketConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// 获取当前配置
		config := getExtremeMarketConfig()
		
		// 转换为JSON
		response := map[string]interface{}{
			"config": config,
			"pauses": getExtremeMarketPauses(),
		}
		
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			http.Error(w, "内部服务器错误", http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
		return
	}
	
	if r.Method == "POST" {
		// 解析请求体
		var config ExtremeMarketConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			http.Error(w, "无效的请求数据", http.StatusBadRequest)
			return
		}
		
		// 更新配置
		setExtremeMarketConfig(config)
		
		// 返回成功
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success": true, "message": "极端行情配置已更新"}`))
		return
	}
	
	http.Error(w, "不支持的请求方法", http.StatusMethodNotAllowed)
}

func main() {
	log.Printf("🚀 Multi-account trading server starting...")

	// 初始化Redis连接
	initRedis()

	// 从Redis加载已保存的账号认证信息
	loadAccountAuthsFromRedis()

	defer price.CloseGlobalClient()

	// 🔧 新增：启动暂停状态监控器
	globalRateLimitHandler.StartMonitor()
	log.Printf("✅ 暂停状态监控器已启动")

	// 🔧 低价值代币现在会自动停止监控，无需手动清理接口

	// 🔧 新增：添加累积统计查询接口
	http.HandleFunc("/cumulative-stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			// 返回所有账号的累积统计
			cumulativeStatsMutex.RLock()
			allStats := make(map[string]*CumulativeTaskStats)
			for id := range cumulativeTaskStats {
				allStats[id] = getCumulativeStats(id)
			}
			cumulativeStatsMutex.RUnlock()

			response := map[string]interface{}{
				"success": true,
				"data":    allStats,
				"成功":      true,
				"数据":      allStats,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			// 返回指定账号的累积统计
			stats := getCumulativeStats(accountID)
			if stats != nil {
				response := map[string]interface{}{
					"success": true,
					"data":    stats,
					"成功":      true,
					"数据":      stats,
				}
				json.NewEncoder(w).Encode(response)
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": "Account not found",
					"成功":      false,
					"消息":      "未找到账户",
				})
			}
		}
	})

	// 🔧 修复死循环：添加全局停止通道
	globalStopChan := make(chan struct{})

	// 启动定期清理过期异步状态的goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cleanupExpiredAsyncStates()
			case <-globalStopChan:
				log.Printf("🛑 异步状态清理器已停止")
				return
			}
		}
	}()

	// 启动全局代币检查器
	go func() {
		ticker := time.NewTicker(10 * time.Minute) // 每10分钟检查一次
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				performGlobalTokenCheck()
			case <-globalStopChan:
				log.Printf("🛑 全局代币检查器已停止")
				return
			}
		}
	}()

	http.HandleFunc("/trade", handleTrade)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/stats-fast", handleStatsFast) // 🔧 新增：快速统计接口
	http.HandleFunc("/global", handleGlobalStats)
	http.HandleFunc("/stop", handleStop)
	http.HandleFunc("/result", handleResult)
	// 自动卖出功能已迁移到 alpha_autosell.go (端口8081)
	// http.HandleFunc("/auto-sell", handleAutoSell)
	// http.HandleFunc("/stop-auto-sell", handleStopAutoSell)
	http.HandleFunc("/cleanup-orders", handleCleanupOrders)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/account-status", handleAccountStatus)
	http.HandleFunc("/pause-status", handlePauseStatus)
	http.HandleFunc("/trade-interval", handleTradeInterval)
	http.HandleFunc("/api-interval", handleAPIInterval)
	// 🔧 新增：429错误处理状态查询端点
	http.HandleFunc("/rate-limit-status", handleRateLimitStatus)
	// 🔧 新增：网络连接状态查询端点
	http.HandleFunc("/network-status", handleNetworkStatus)
	// 🚀 新增：速度模式管理端点
	http.HandleFunc("/speed-mode", handleSpeedMode)
	http.HandleFunc("/speed-modes", handleSpeedModes)
	// 🔧 新增：账号管理端点
	http.HandleFunc("/accounts", handleAccountsManagement)
	// 🧹 新增：完整清理接口（暂停+清理挂单+清理代币残留）
	http.HandleFunc("/complete-cleanup", handleCompleteCleanup)
	// 🎮 新增：账号控制接口（暂停/恢复）
	http.HandleFunc("/account-control", handleAccountControl)

	// 🔧 新增：手动恢复暂停状态的接口
	http.HandleFunc("/resume", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// 强制恢复暂停状态
		globalRateLimitHandler.ForceResume()

		response := map[string]interface{}{
			"success":   true,
			"message":   "暂停状态已手动恢复",
			"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			"成功":        true,
			"消息":        "暂停状态已手动恢复",
			"时间戳":       time.Now().Format("2006-01-02 15:04:05"),
		}

		log.Printf("🔧 手动恢复暂停状态")
		json.NewEncoder(w).Encode(response)
	})

	// 添加极端行情配置API
	http.HandleFunc("/api/extreme-market-config", handleExtremeMarketConfig)

	log.Printf("🚀 Multi-account trading server starting on :8080")
	// API endpoints已配置


	// 启动全局挂单清理器
	go startGlobalOrderCleaner()
	log.Printf("🧹 全局挂单清理器已启动 (每15秒检查一次，清理超过50秒的订单)")

	// 🔧 修复：使用更健壮的HTTP服务器启动方式，避免程序意外退出
	log.Printf("🚀 启动HTTP服务器，监听端口 :8080")

	// 创建HTTP服务器
	server := &http.Server{
		Addr:         ":8080",
		Handler:      nil,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 在goroutine中启动服务器，避免阻塞
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {

			// 等待一段时间后重试
			time.Sleep(5 * time.Second)

			// 尝试重新启动
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("❌ HTTP服务器重启失败: %v", err)
				log.Printf("💡 请检查端口是否被占用")
			}
		}
	}()

	// 保持主程序运行
	select {} // 永远阻塞，保持程序运行
}

func handleTrade(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	
	// 每次交易请求开始时，清理过期的异步状态和代币卖出状态
	cleanupExpiredAsyncStates()
	cleanupExpiredTokenSellStatuses()

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		return
	}

	var req TradeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response := TradeResponse{Success: false, Message: "Invalid request"}
		json.NewEncoder(w).Encode(convertToChineseFields(response))
		return
	}

	// 🚀 应用速度模式配置
	if req.SpeedMode != "" {
		applySpeedMode(req.SpeedMode)
	} else {
		// 默认使用普通模式
		applySpeedMode("normal")
	}

	// 通过KYC接口获取firstName作为AccountID
	firstName, err2 := getUserFirstName(req.Csrftoken, req.Cookie)
	if err2 != nil {
		response := TradeResponse{
			Success: false,
			Message: fmt.Sprintf("获取用户信息失败: %v", err2),
		}
		err := json.NewEncoder(w).Encode(convertToChineseFields(response))
		if err != nil {
			return
		}
		return
	}
	req.AccountID = firstName
	log.Printf("🔍 [%s] 通过KYC获取到用户标识", req.AccountID)

	// 🔧 新增：生成唯一的任务ID并初始化累积统计
	taskID := fmt.Sprintf("task_%s_%d", req.AccountID, time.Now().Unix())
	initCumulativeTask(req.AccountID, taskID, req.TargetVolume)
	// 任务ID已生成

	// 设置固定延迟值
	req.MinDelay = 1
	req.MaxDelay = 2

	// 设置默认值
	if req.BaseAsset == "" {
		// 根据代币地址自动推断 base_asset
		req.BaseAsset = inferBaseAssetFromTokenAddress(req.TokenAddress)
	}
	if req.MinDelay == 0 {
		req.MinDelay = 1
	}
	if req.MaxDelay == 0 {
		req.MaxDelay = 30
	}
	if req.PricePrecision == 0 {
		req.PricePrecision = 8 // 默认8位小数
	}

	// 验证金额不能为0
	if req.USDTAmount <= 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "交易金额必须大于0",
			"成功":      false,
			"消息":      "交易金额必须大于0",
		})
		return
	}

	// 验证最小交易金额（避免小额订单）
	minTradeAmount := 3.0
	if req.USDTAmount < minTradeAmount {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("交易金额过小，最小需要%.1f USDT，当前: %.2f USDT", minTradeAmount, req.USDTAmount),
			"成功":      false,
			"消息":      fmt.Sprintf("交易金额过小，最小需要%.1f USDT，当前: %.2f USDT", minTradeAmount, req.USDTAmount),
		})
		return
	}

	// 🚨 新增：检查全局停止状态
	globalStopMutex.RLock()
	if isGlobalStopping {
		timeSinceStop := time.Since(globalStopTime)
		globalStopMutex.RUnlock()

		// 如果停止时间超过10秒，认为可能卡住了，允许新请求
		if timeSinceStop < 10*time.Second {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": fmt.Sprintf("系统正在停止中，请等待%.0f秒后重试", (10*time.Second - timeSinceStop).Seconds()),
				"reason":  "global_stopping",
			})
			return
		} else {
			// 超过10秒，强制重置状态
			globalStopMutex.Lock()
			isGlobalStopping = false
			globalStopTime = time.Time{}
			globalStopMutex.Unlock()
			log.Printf("⚠️ 全局停止状态超时，强制重置")
		}
	} else {
		globalStopMutex.RUnlock()
	}

	// 🚨 新增：检查是否正在暂停中
	if isPaused, pauseInfo := checkAccountPauseStatus(req.AccountID); isPaused {
		remainingPause := time.Until(pauseInfo.PauseEnd)
		if remainingPause > 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":       false,
				"message":       fmt.Sprintf("账号正在暂停中，剩余%.0f秒 (原因: %s)", remainingPause.Seconds(), pauseInfo.Reason),
				"pause_seconds": int(remainingPause.Seconds()),
				"reason":        pauseInfo.Reason,
				"pause_start":   pauseInfo.PauseStart.Unix(),
				"pause_end":     pauseInfo.PauseEnd.Unix(),
			})
			return
		} else {
			// 暂停时间已过，清除暂停状态
			clearAccountPauseStatus(req.AccountID)
			log.Printf("✅ [%s] 暂停时间已结束，恢复正常交易", req.AccountID)
		}
	}

	// 检查交易间隔
	tradeTimeMutex.RLock()
	lastTime, exists := lastTradeTime[req.AccountID]
	tradeTimeMutex.RUnlock()

	if exists {
		timeSinceLastTrade := time.Since(lastTime)
		if timeSinceLastTrade < tradeInterval {
			remainingTime := tradeInterval - timeSinceLastTrade
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":           false,
				"message":           fmt.Sprintf("交易间隔太短，请等待%.1f秒后再试", remainingTime.Seconds()),
				"remaining_seconds": int(remainingTime.Seconds()),
			})
			return
		}

		// 🚨 修复：只在运行状态中才检查长时间未交易，新任务开始时不检查
		// 检查账号是否正在运行中（有活跃的交易统计）
		statsMutex.RLock()
		accountStat, isRunning := accountStats[req.AccountID]
		statsMutex.RUnlock()

		if isRunning && accountStat != nil && timeSinceLastTrade > 5*time.Minute {
			// 只有在运行状态中才检查长时间未交易
			log.Printf("⏸️ [%s] 运行中检测到最后下单时间超过5分钟 (%.1f分钟)，启动防频繁暂停机制",
				req.AccountID, timeSinceLastTrade.Minutes())

			// 设置暂停状态
			pauseDuration := 90 * time.Second // 1分30秒
			setAccountPauseStatus(req.AccountID, pauseDuration, "long_idle_pause")

			log.Printf("⏸️ [%s] 开始暂停 %.0f 秒，防止IP频繁请求", req.AccountID, pauseDuration.Seconds())

			// 返回暂停状态
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":            false,
				"message":            fmt.Sprintf("运行中检测到长时间未交易(%.1f分钟)，暂停%.0f秒防止IP频繁", timeSinceLastTrade.Minutes(), pauseDuration.Seconds()),
				"pause_seconds":      int(pauseDuration.Seconds()),
				"reason":             "long_idle_pause",
				"last_trade_minutes": timeSinceLastTrade.Minutes(),
				"pause_start":        time.Now().Unix(),
				"pause_end":          time.Now().Add(pauseDuration).Unix(),
			})

			return
		} else if timeSinceLastTrade > 5*time.Minute {
			// 新任务开始或挂机状态，不触发暂停，只记录日志
			log.Printf("💡 [%s] 新任务开始，上次交易时间 %.1f分钟前，跳过暂停检查",
				req.AccountID, timeSinceLastTrade.Minutes())
		}
	}

	// 🔧 检查是否因429错误暂停
	globalRateLimitHandler.WaitIfPaused(req.AccountID)

	// 先验证能否获取实时价格 - 使用买单专用价格模式
	buyPriceMode := getBuyPriceMode(&req)
	_, priceErr := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(&req), req.PricePrecision, buyPriceMode)
	if priceErr != nil {
		// 🔧 价格验证失败时的降级策略
		log.Printf("⚠️ [%s] 买单价格验证失败(模式:%s)，尝试降级: %v", req.AccountID, buyPriceMode, priceErr)

		// 降级到combined模式验证
		_, priceErr = price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(&req), req.PricePrecision, price.PriceModeCombined)
		if priceErr != nil {
			// 最后尝试limit模式
			_, priceErr = price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(&req), req.PricePrecision, price.PriceModeLimit)
			if priceErr != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": "所有价格模式验证失败",
					"error":   priceErr.Error(),
					"成功":      false,
					"消息":      "所有价格模式验证失败",
					"错误":      priceErr.Error(),
				})
				return
			} else {
				log.Printf("✅ [%s] 降级到limit模式验证成功", req.AccountID)
			}
		} else {
			log.Printf("✅ [%s] 降级到combined模式验证成功", req.AccountID)
		}
	} else {
		log.Printf("✅ [%s] 买单价格验证成功(模式:%s)", req.AccountID, buyPriceMode)
	}

	// 初始化账号统计
	initAccountStats(req.AccountID, req.TargetVolume)

	// 保存交易参数，确保恢复时能正确使用
	saveAccountTradeParams(&req)
	log.Printf("💾 [%s] 已保存交易参数: 代币=%s, 基础资产=%s, 金额=%.2f, 速度=%s",
		req.AccountID, req.TokenAddress, req.BaseAsset, req.USDTAmount, req.SpeedMode)
	
	// 自动启动定时清空功能
	startTokenCleanupIfNotRunning(&req)

	// 账号级别的请求管理
	accountMutex.Lock()

	// 如果该账号正在处理，取消之前的处理
	if cancel, exists := accountCancels[req.AccountID]; exists {
		log.Printf("🔄 [%s] 检测到新请求，取消之前的交易", req.AccountID)
		select {
		case cancel <- true:
		default:
		}
	}

	// 创建新的取消通道
	newCancel := make(chan bool, 1)
	accountCancels[req.AccountID] = newCancel
	accountProcessing[req.AccountID] = true

	accountMutex.Unlock()

	// 返回响应
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"message":    "交易请求已接收，之前的请求已取消",
		"account_id": req.AccountID,
		"status":     "processing",
		"成功":         true,
		"消息":         "交易请求已接收，之前的请求已取消",
		"账户ID":       req.AccountID,
		"状态":         "处理中",
	})

	// 异步执行交易
	go func() {
		defer func() {
			// 清理账号处理状态
			accountMutex.Lock()
			delete(accountProcessing, req.AccountID)
			delete(accountCancels, req.AccountID)
			accountMutex.Unlock()
		}()

		// 更新最后交易时间
		tradeTimeMutex.Lock()
		lastTradeTime[req.AccountID] = time.Now()
		tradeTimeMutex.Unlock()

		response := executeTradeWithCancel(&req, newCancel)

		// 存储交易结果
		storeTradeResult(req.AccountID, response)

		// 🔧 修复：主流程损益更新（买入交易额已在买入成功时统计，这里只更新损益）
		if response.Success && !strings.Contains(response.Message, "异步") {
			// 计算净损益：买入成本 - 卖出收入 (正数=亏损，负数=盈利)
			buyInCost := response.BuyPrice * response.TokenAmount       // 实际买入成本
			sellOutRevenue := response.SellPrice * response.TokenAmount // 卖出收入
			netLoss := buyInCost - sellOutRevenue                       // 净损益

			// 验证是否有真实交易量 (防止0交易量被统计)
			if buyInCost > 0 && response.TokenAmount > 0 {
				// 只有非自动循环的单次交易才在主流程更新损益
				if !req.AutoLoop {
					// 生成唯一交易ID
					tradeID := generateTradeID(req.AccountID, req.TokenAddress, response.BuyPrice, response.TokenAmount)
					// 🔧 新逻辑：只更新损益，不重复统计买入交易额（买入交易额已在买入成功时统计）
					updateAccountLossOnly(req.AccountID, netLoss, tradeID)
					if netLoss > 0 {
						log.Printf("✅ [%s] 单次交易完成，买单金额: %.6f USDT，亏损: %.6f USDT (损益更新)", req.AccountID, buyInCost, netLoss)
					} else {
						log.Printf("✅ [%s] 单次交易完成，买单金额: %.6f USDT，盈利: %.6f USDT (损益更新)", req.AccountID, buyInCost, -netLoss)
					}
				} else {
					// 自动循环交易在循环中统计，这里只记录日志
					if netLoss > 0 {
						log.Printf("✅ [%s] 自动循环交易完成，买单金额: %.6f USDT，亏损: %.6f USDT (循环中统计)", req.AccountID, buyInCost, netLoss)
					} else {
						log.Printf("✅ [%s] 自动循环交易完成，买单金额: %.6f USDT，盈利: %.6f USDT (循环中统计)", req.AccountID, buyInCost, -netLoss)
					}
				}
			}
		} else if response.Success && strings.Contains(response.Message, "异步") {
			log.Printf("⏳ [%s] 异步交易已启动: %s", req.AccountID, response.Message)
		}

		// 🔧 增强：检查是否需要启动自动循环
		// 即使单次交易失败，也应该启动自动循环继续尝试
		if req.AutoLoop {
			// 自动循环应该在以下情况启动：
			// 1. 交易成功
			// 2. 余额不足跳过（可能后续有余额）
			// 3. 买单失败（应该继续重试）
			// 4. 网络错误或临时性错误
			shouldStartLoop := response.Success ||
				strings.Contains(response.Message, "余额不足") ||
				strings.Contains(response.Message, "INSUFFICIENT_BALANCE") ||
				strings.Contains(response.Message, "buy order failed") ||
				strings.Contains(response.Message, "Retry buy order failed") ||
				strings.Contains(response.Message, "买单重试") ||
				strings.Contains(response.Message, "timeout") ||
				strings.Contains(response.Message, "超时") ||
				strings.Contains(response.Message, "网络") ||
				strings.Contains(response.Message, "Failed to get") ||
				strings.Contains(response.Message, "HTTP请求失败")

			if shouldStartLoop {
				log.Printf("🔄 [%s] 启动自动循环 - 原因: %s", req.AccountID, response.Message)
				startAutoLoop(&req)
								} else {
						// 只有在严重错误时才不启动循环（如认证失效）
						if strings.Contains(response.Message, "AUTH_FAILED") ||
							strings.Contains(response.Message, "认证失效") {
							log.Printf("🚨 [%s] 认证失效，不启动自动循环", req.AccountID)
						} else if strings.Contains(response.Message, "余额不足") || 
							strings.Contains(response.Message, "INSUFFICIENT_BALANCE") {
							// 检查代币是否已被卖出
							if isTokenSold(req.AccountID, req.TokenAddress) {
								status := getTokenSellStatus(req.AccountID, req.TokenAddress)
								log.Printf("✅ [%s] 余额不足错误，但代币已被其他流程卖出 - 方式: %s, 价格: %.8f, 数量: %.6f", 
									req.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
								// 不启动自动循环，因为代币已卖出
							} else {
								// 检查代币余额
								tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
								if balErr == nil {
									freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
									if freeAmount <= 0 {
										log.Printf("✅ [%s] 余额不足错误，代币余额为0，可能已被卖出，不启动自动循环", req.AccountID)
										// 设置代币已卖出状态，但卖出方式未知
										setTokenSold(req.AccountID, req.TokenAddress, "unknown", 0, 0)
									} else {
										log.Printf("⚠️ [%s] 余额不足错误，但代币余额不为0 (%.6f)，启动自动循环尝试卖出", 
											req.AccountID, freeAmount)
										startAutoLoop(&req)
									}
								} else {
									log.Printf("⚠️ [%s] 余额不足错误，无法检查代币余额: %v，启动自动循环尝试卖出", 
										req.AccountID, balErr)
									startAutoLoop(&req)
								}
							}
						} else {
							log.Printf("🔄 [%s] 其他错误也启动自动循环，确保持续尝试 - 响应: %s", req.AccountID, response.Message)
							startAutoLoop(&req)
						}
					}
		}
	}()
}

// executeSingleTrade 执行单次交易
func executeSingleTrade(req *TradeRequest) TradeResponse {
	startTime := time.Now()

	// 检查账户级别的冲突，而不是全局阻塞
	if isAccountBusy(req.AccountID) {
		return TradeResponse{
			Success: false,
			Message: "该账户有异步操作进行中，请稍后重试",
		}
	}
	
	// 设置账户的速度模式
	if req.SpeedMode != "" {
		speedMode := strings.ToLower(req.SpeedMode)
		switch speedMode {
		case SpeedModeFast, SpeedModeNormal, SpeedModeSlow:
			setSpeedModeForAccount(req.AccountID, speedMode)
			log.Printf("🚀 [%s] 设置速度模式为: %s", req.AccountID, speedMode)
		default:
			log.Printf("⚠️ [%s] 未知的速度模式: %s, 使用默认正常模式", req.AccountID, speedMode)
			setSpeedModeForAccount(req.AccountID, SpeedModeNormal)
		}
	} else {
		// 如果没有指定速度模式，使用默认的正常模式
		setSpeedModeForAccount(req.AccountID, SpeedModeNormal)
	}
	
	// 检查是否应该暂停交易
	shouldPause, reason := shouldPauseTrading(req.TokenAddress, req.ChainID)
	if shouldPause {
		// 添加账户到受影响列表
		addAffectedAccount(req.TokenAddress, req.ChainID, req.AccountID)
		
		return TradeResponse{
			Success: false,
			Message: fmt.Sprintf("极端行情暂停交易: %s", reason),
		}
	}

	// 🎯 买单使用专用价格模式，确保高成功率
	buyPriceMode := getBuyPriceMode(req)
	currentPrice, err := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, buyPriceMode)
	if err != nil {
		// 🔧 买单价格获取失败时，尝试降级策略
		log.Printf("⚠️ [%s] 买单价格获取失败(模式:%s)，尝试降级策略: %v", req.AccountID, buyPriceMode, err)

		// 降级策略1: 尝试combined模式
		if buyPriceMode != price.PriceModeCombined {
			currentPrice, err = price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, price.PriceModeCombined)
			if err == nil {
				log.Printf("✅ [%s] 降级到combined模式成功获取价格: %.8f", req.AccountID, currentPrice)
				goto priceObtained
			}
		}

		// 降级策略2: 尝试limit模式
		currentPrice, err = price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, price.PriceModeLimit)
		if err == nil {
			log.Printf("✅ [%s] 降级到limit模式成功获取价格: %.8f", req.AccountID, currentPrice)
			goto priceObtained
		}

		// 所有模式都失败
		return TradeResponse{Success: false, Message: "所有价格模式获取失败: " + err.Error()}
	}

priceObtained:
	log.Printf("📈 [%s] 买单价格获取成功(模式:%s): %.8f", req.AccountID, buyPriceMode, currentPrice)

	// 🎯 买单价格策略优化：根据价格模式调整加价幅度
	var buyPrice float64
	switch buyPriceMode {
	case price.PriceModeMarket:
		// 链上模式：加万二，确保超过真实成交价
		buyPrice = currentPrice * 1.0002 // 加万分之2
		log.Printf("💰 [%s] 链上模式买单：加万二提高成交率 %.8f → %.8f", req.AccountID, currentPrice, buyPrice)
	case price.PriceModeLimit:
		// 限价模式：加万一，避免过度加价
		buyPrice = currentPrice * 1.0001 // 加万分之1
		log.Printf("💰 [%s] 限价模式买单：加万一确保成交 %.8f → %.8f", req.AccountID, currentPrice, buyPrice)
	default:
		// 其他模式：加万一点五
		buyPrice = currentPrice * 1.0015 // 加万分之1.5
		log.Printf("💰 [%s] 综合模式买单：加万一点五平衡成交率 %.8f → %.8f", req.AccountID, currentPrice, buyPrice)
	}

	buyPrice = adjustPricePrecision(buyPrice, req.PricePrecision)

	// 2. 计算数量和金额，确保符合币安要求
	// 🔧 取消随机浮动，使用固定参数传递的数值
	adjustedUSDTAmount := req.USDTAmount

	log.Printf("📊 [%s] 买入量使用固定值: %.6f USDT (不再使用随机浮动)",
		req.AccountID, adjustedUSDTAmount)

	// 先计算理想数量（使用调整后的USDT金额和加价后的买入价格）
	idealTokenAmount := adjustedUSDTAmount / buyPrice

	// 数量必须取整（币安要求）
	tokenAmount := float64(int(idealTokenAmount))

	// 计算对应的金额（使用买入价格）
	calculatedAmount := buyPrice * tokenAmount

	// 将金额调整为8位小数（币安要求），确保精度一致
	exactAmount := math.Round(calculatedAmount*100000000) / 100000000

	// 🔧 记录实际使用的USDT金额用于统计
	actualUSDTUsed := exactAmount // 实际花费的USDT金额

	// 二次验证：确保价格×数量=金额完全匹配
	verification := math.Round((buyPrice*tokenAmount)*100000000) / 100000000
	if math.Abs(exactAmount-verification) > 0.00000001 {
		exactAmount = verification
	}

	// 最终验证
	finalCheck := exactAmount / tokenAmount
	if math.Abs(finalCheck-buyPrice) > 0.000000001 {
		exactAmount = math.Round((buyPrice*tokenAmount)*100000000) / 100000000
	}

	// 移除详细验证日志

	// 检查计算后的金额是否为0
	if exactAmount <= 0 || tokenAmount <= 0 {

		return TradeResponse{
			Success: false,
			Message: "计算后金额为0，无法下单",
		}
	}

	// 2.5. 检查资金账户USDT余额是否足够
	fundingBalance, err := getFundingAccountBalance(req.Csrftoken, req.Cookie)
	if err != nil {

		// 检查是否是认证失效
		if strings.Contains(err.Error(), "请检查是否已登录") || strings.Contains(err.Error(), "请求失败") {
			return TradeResponse{Success: false, Message: "认证失效，请重新登录"}
		}

	} else {

		// 全局资金管理规则
		if fundingBalance < 1.0 {
			// 规则1: 资金余额 < 1 USDT，不下单

			// 检查是否有代币余额需要处理
			tokenBalance, tokenErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if tokenErr == nil && tokenBalance != nil {
				freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
				if freeAmount > 0 {
					// 🔧 检查代币价值，只处理高价值代币
					priceMode := getPriceMode(req)
					marketPrice, priceErr := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), 8, priceMode)
					if priceErr == nil {
						tokenValue := freeAmount * marketPrice
						if tokenValue >= 0.1 {
							// 只有价值>=0.1 USDT的代币才启动强制卖出
							log.Printf("💰 [%s] 发现高价值代币余额 %.6f (价值%.4f USDT)，启动强制卖出清理", req.AccountID, freeAmount, tokenValue)
							go immediateForceSellingProcess(req.AccountID, req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, freeAmount)

							return TradeResponse{
								Success: false,
								Message: fmt.Sprintf("发现代币余额 %.6f (价值%.4f USDT)，正在强制卖出清理，请稍后重试", freeAmount, tokenValue),
							}
						} else {
							log.Printf("💸 [%s] 发现低价值代币余额 %.6f (价值%.4f USDT)，忽略并允许继续交易", req.AccountID, freeAmount, tokenValue)
						}
					} else {
						// 无法获取价格时，按原逻辑处理
						log.Printf("💰 [%s] 发现代币余额 %.6f (无法获取价格)，启动强制卖出清理", req.AccountID, freeAmount)
						go immediateForceSellingProcess(req.AccountID, req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, freeAmount)

						return TradeResponse{
							Success: false,
							Message: fmt.Sprintf("发现代币余额 %.6f，正在强制卖出清理，请稍后重试", freeAmount),
						}
					}
				}
			}

			return TradeResponse{
				Success: false,
				Message: fmt.Sprintf("资金余额过少(%.4f USDT)，最低需要1 USDT", fundingBalance),
			}
		} else if fundingBalance < exactAmount {
			// 规则2: 1 USDT < 资金 < 下单金额，用资金金额下单

			// 使用实际余额的97%，留一点余量
			adjustedAmount := fundingBalance * 0.97
			adjustedTokenAmount := math.Floor(adjustedAmount / buyPrice)
			adjustedExactAmount := buyPrice * adjustedTokenAmount
			adjustedExactAmount = math.Round(adjustedExactAmount*100000000) / 100000000

			if adjustedTokenAmount >= 1 && adjustedExactAmount > 0 {
				exactAmount = adjustedExactAmount
				tokenAmount = adjustedTokenAmount
			} else {

				return TradeResponse{
					Success: false,
					Message: "调整后金额仍不足，无法下单",
				}
			}
		}
	}

	// 3. 买入（使用加价后的买入价格）
	buyTime := time.Now().UnixMilli() // 在下单前记录时间

	buyOrderID, buySuccess := placeOrderWithID(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "BUY",
		Price:      buyPrice, // 使用加万二后的价格
		Quantity:   tokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            exactAmount,
			AmountStr:         fmt.Sprintf("%.8f", exactAmount),
			PaymentWalletType: "CARD",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	})

	if !buySuccess {
		if buyOrderID == "INSUFFICIENT_BALANCE" {

			// 先检查是否有未卖出的代币需要处理
			tokenBalance, tokenErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if tokenErr == nil && tokenBalance != nil {
				freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
				if freeAmount > 1.0 {
					// 🔧 新逻辑：检查代币价值，只处理高价值代币
					priceMode := getPriceMode(req)
					marketPrice, priceErr := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, priceMode)
					if priceErr == nil {
						tokenValue := freeAmount * marketPrice
						if tokenValue > 1.0 {
							// 只有价值>1 USDT的代币才启动强制卖出
							go immediateForceSellingProcess(req.AccountID, req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, freeAmount)
							return TradeResponse{
								Success: false,
								Message: fmt.Sprintf("发现%.0f个未卖出代币(价值%.2f USDT)，已启动自动卖出处理", freeAmount, tokenValue),
							}
						} else {
							log.Printf("💸 [%s] 发现低价值代币(%.4f USDT)，跳过处理，继续交易", req.AccountID, tokenValue)
							// 不返回错误，让交易继续
						}
					} else {
						// 无法获取价格时，按原逻辑处理
						go immediateForceSellingProcess(req.AccountID, req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, freeAmount)
						return TradeResponse{
							Success: false,
							Message: fmt.Sprintf("发现%.0f个未卖出代币，已启动自动卖出处理", freeAmount),
						}
					}
				}
			}

			// 检查是否有代币被锁定
			if hasLockedTokens(req.TokenAddress, req.Csrftoken, req.Cookie, req.AccountID) {

				// 循环等待锁定释放，最多等待5分钟
				for attempt := 1; attempt <= 30; attempt++ { // 30次 * 5秒 = 2分钟
					time.Sleep(5 * time.Second)

					if !hasLockedTokens(req.TokenAddress, req.Csrftoken, req.Cookie, req.AccountID) {

						// 重新尝试买入
						buyOrderID, buySuccess = placeOrderWithID(OrderRequest{
							BaseAsset:  req.BaseAsset,
							QuoteAsset: "USDT",
							Side:       "BUY",
							Price:      buyPrice,
							Quantity:   tokenAmount,
							PaymentDetails: []PaymentDetail{{
								Amount:            exactAmount,
								AmountStr:         fmt.Sprintf("%.8f", exactAmount),
								PaymentWalletType: "CARD",
							}},
							Csrftoken: req.Csrftoken,
							Cookie:    req.Cookie,
						})

						if buySuccess {

							break // 跳出等待循环
						}
					}

				}

				if !buySuccess {

					return TradeResponse{Success: false, Message: "代币锁定超时跳过"}
				}
			} else {

				return TradeResponse{Success: false, Message: "USDT余额不足，无未卖出代币"}
			}
		} else {
			return TradeResponse{Success: false, Message: "买入失败"}
		}
	}

	// 3.5. 等待买入确认，循环检查历史订单
	buyConfirmed := false

	for i := 0; i < 5; i++ { // 🔥 刷量优化：1秒极速确认，每200ms检查
		time.Sleep(200 * time.Millisecond)

		if checkBuyOrderConfirmed(buyOrderID, buyTime, req.Csrftoken, req.Cookie) {
			buyConfirmed = true
			log.Printf("✅ [%s] 买入确认成功", req.AccountID)

			// 🔧 新增：主买单成功后立即统计买入交易额（使用实际金额）
			buyInCost := actualUSDTUsed // 🎲 使用实际花费的USDT金额，确保统计准确
			if buyInCost > 0 && tokenAmount > 0 {
				// 生成唯一交易ID防止重复统计
				tradeID := generateTradeID(req.AccountID, req.TokenAddress, buyPrice, tokenAmount)
				// 只统计买入交易额，损益为0（因为还没卖出）
				updateAccountStatsWithID(req.AccountID, buyInCost, 0, tradeID)
				log.Printf("📊 [%s] 主买单成功立即统计 - 买单金额: %.6f USDT (ID:%s)",
					req.AccountID, buyInCost, tradeID[:8])
			}
			break
		}
	}

	// 如果超时还没成交，强制取消订单
	if !buyConfirmed {
		log.Printf("⚠️ [%s] 买入订单超时未成交，开始取消订单: %s", req.AccountID, buyOrderID)

		// 🔧 增强：取消订单，增加等待时间和验证
		cancelSuccess := false
		for attempt := 1; attempt <= 3; attempt++ {
			if cancelOrder(buyOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
				// 🔧 新增：等待取消生效，然后验证
				time.Sleep(500 * time.Millisecond) // 增加等待时间

				// 验证订单是否真的被取消（可选，避免过度验证）
				log.Printf("✅ [%s] 买入订单取消成功，准备重新买入", req.AccountID)
				cancelSuccess = true
				break
			} else if attempt < 3 {
				time.Sleep(300 * time.Millisecond) // 增加重试间隔
			}
		}

		if cancelSuccess {
			// 🔧 修复：买入失败后重新买入，不应该进行卖出
			log.Printf("🔄 [%s] 买入订单已取消，重新获取价格进行买入重试", req.AccountID)
			return retryBuyWithNewPrice(req, startTime)
		} else {
			// 单个订单取消失败，尝试取消所有订单
			log.Printf("❌ [%s] 买入订单取消失败，尝试取消所有订单", req.AccountID)
			if cancelAllOrders(req.Csrftoken, req.Cookie) {
				return TradeResponse{Success: false, Message: "买入超时，已取消所有订单"}
			} else {
				return TradeResponse{Success: false, Message: "买入超时且取消失败，需要人工处理"}
			}
		}
	}

	// 🔧 确保只有买入成功才会执行到这里
	log.Printf("✅ [%s] 买入订单确认成功，买入价格: %.8f, 代币数量: %.6f", req.AccountID, buyPrice, tokenAmount)

	// 4. 注册账户认证信息到全局管理器
	registerAccountAuth(req.AccountID, req.Csrftoken, req.Cookie)

	// 5. 启动该代币的自动监控卖出（必须卖出模式）- Flash Trade 内部使用
	go startForceSellMonitoring(req)

	// 6. 启动该代币的定时监控（检查未锁定余额）- Flash Trade 内部使用
	go startAutoTokenMonitoring(req)

	// 7. 智能卖出处理（使用实际买入价格）
	sellResult := smartSellWithRetry(req, buyPrice, tokenAmount, startTime)
	return sellResult
}

// placeOrderWithID 下单并返回订单ID
func placeOrderWithID(order OrderRequest) (string, bool) {
	// 使用新的API频率控制器
	accountID := "unknown"
	if order.Cookie != "" {
		// 尝试从cookie中提取账号信息，如果失败就用unknown
		accountID = "order-" + order.BaseAsset
	}

	var resp *http.Response
	var body []byte



	// 使用频率控制器包装API调用
	err := safeAPICall(func() error {
		// 在发送请求前，详细记录订单信息，特别是数量字段
		if order.Side == "SELL" {
			log.Printf("🔍 [%s] 下单详情 - 代币: %s, 价格: %.8f", 
				accountID, order.BaseAsset, order.Price)
			log.Printf("🔍 [%s] 数量字段 - Quantity: %v (类型: %T)", 
				accountID, order.Quantity, order.Quantity)
			
			if len(order.PaymentDetails) > 0 {
				log.Printf("🔍 [%s] PaymentDetails - Amount: %v (类型: %T), AmountStr: %s", 
					accountID, order.PaymentDetails[0].Amount, 
					order.PaymentDetails[0].Amount, 
					order.PaymentDetails[0].AmountStr)
			}
			
			// 确保数量字段一致性
					if len(order.PaymentDetails) > 0 {
			// 使用AmountStr重新设置所有数量字段，确保一致性
			amountStr := order.PaymentDetails[0].AmountStr
			amount, _ := strconv.ParseFloat(amountStr, 64)
			
			// 修复：将数量四舍五入到整数，解决"Order amount must be an integer multiple of the minimum amount movement"错误
			amount = math.Floor(amount)
			amountStr = fmt.Sprintf("%.0f", amount)
			
			// 更新所有数量字段
			order.Quantity = amount
			order.PaymentDetails[0].Amount = amount
			order.PaymentDetails[0].AmountStr = amountStr
			
			log.Printf("🔧 [%s] 统一后数量字段(取整) - Quantity: %v, Amount: %v, AmountStr: %s", 
				accountID, order.Quantity, order.PaymentDetails[0].Amount, amountStr)
		}
		
		// 确保价格精确到8位小数
		priceStr := fmt.Sprintf("%.8f", order.Price)
		order.Price, _ = strconv.ParseFloat(priceStr, 64)
		
		}
		
		jsonData, _ := json.Marshal(order)
		
		// 不打印完整的JSON请求体，避免敏感信息泄露
		log.Printf("📦 [%s] 发送下单请求", accountID)

		req, err := http.NewRequest("POST", "https://www.binance.com/bapi/asset/v1/private/alpha-trade/order/place", bytes.NewReader(jsonData))
		if err != nil {
			log.Printf("❌ [%s] 创建下单请求失败: %v", accountID, err)
			return err
		}

		req.Header.Set("accept", "*/*")
		req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
		req.Header.Set("clienttype", "web")
		req.Header.Set("content-type", "application/json")
		req.Header.Set("csrftoken", order.Csrftoken)
		req.Header.Set("cookie", order.Cookie)

		client := &http.Client{Timeout: 5 * time.Second}
		// 🔧 使用429错误检测的HTTP请求执行器
	
		resp, err = executeHTTPRequestWithRateLimit(req, client, accountID)
		if err != nil {
			log.Printf("❌ [%s] 下单请求发送失败: %v", accountID, err)
			return err
		}

		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("❌ [%s] 读取下单响应失败: %v", accountID, err)
			return err
		}
		

		
		return nil
	}, "PlaceOrder", accountID)

	if err != nil {
		log.Printf("❌ [%s] 下单API调用失败: %v", accountID, err)
		return "", false
	}
	
	// 注意：下单后不添加延迟，确保价格有效性和快速执行交易

	// 解析响应获取订单ID
	var orderResp map[string]interface{}
	if err := json.Unmarshal(body, &orderResp); err != nil {
		log.Printf("❌ [%s] 解析下单响应JSON失败: %v", accountID, err)
		return "", resp.StatusCode == 200
	}

	if resp.StatusCode == 200 {
		// 检查是否有错误码
		if code, exists := orderResp["code"]; exists {
			log.Printf("📋 [%s] 下单响应码: %v", accountID, code)
			
			if code != "000000" && code != 0 {
				// 记录错误信息
				message := "未知错误"
				if msg, ok := orderResp["message"].(string); ok {
					message = msg
				}
				log.Printf("❌ [%s] 下单失败，错误码: %v, 错误信息: %s", accountID, code, message)

				// 特殊错误处理
				if code == "481020" {
					// 余额不足直接返回特殊标识，避免无意义重试
					return "INSUFFICIENT_BALANCE", false
				}
				
				// 处理订单金额太小的情况 (481013)
				if code == "481013" && strings.Contains(message, "Total must be greater than") {
					log.Printf("⚠️ [%s] 订单金额太小，直接标记为卖出成功，继续下一轮", accountID)
					return "AMOUNT_TOO_SMALL", false
				}

				// 检查认证失效
				if strings.Contains(fmt.Sprintf("%v", orderResp["message"]), "请检查是否已登录") ||
					strings.Contains(fmt.Sprintf("%v", orderResp["message"]), "请求失败") {
					log.Printf("🚨 [%s] 认证失效，停止下单", accountID)
					return "AUTH_FAILED", false
				}
				
				// 检查是否有异步操作冲突
				if strings.Contains(fmt.Sprintf("%v", orderResp["message"]), "该账户有异步操作进行中") {
					log.Printf("⚠️ [%s] 检测到异步操作冲突: %s", accountID, orderResp["message"])
					return "ASYNC_CONFLICT", false
				}

				return "", false
			}
		}

		// 尝试多种方式获取订单ID
		orderID := extractOrderID(orderResp)
		if orderID != "" {
			log.Printf("✅ [%s] 下单成功，订单ID: %s", accountID, orderID)
			return orderID, true
		}
		log.Printf("⚠️ [%s] 下单成功但无法获取订单ID，使用unknown", accountID)
		return "unknown", true
	}

	log.Printf("❌ [%s] 下单失败，HTTP状态码: %d", accountID, resp.StatusCode)
	return "", false
}

// extractOrderID 从响应中提取订单ID
func extractOrderID(orderResp map[string]interface{}) string {
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

// cancelOrder 取消订单
func cancelOrder(orderID, symbol, csrftoken, cookie string) bool {
	accountID := "cancel-" + symbol

	var success bool

	// 使用频率控制器包装API调用
	err := safeAPICall(func() error {
		payload := fmt.Sprintf(`{"orderId":"%s","symbol":"%s"}`, orderID, symbol)

		req, err := http.NewRequest("POST", "https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/cancel", strings.NewReader(payload))
		if err != nil {
			return err
		}

		req.Header.Set("clienttype", "web")
		req.Header.Set("content-type", "application/json")
		req.Header.Set("csrftoken", csrftoken)
		req.Header.Set("cookie", cookie)

		client := &http.Client{Timeout: 5 * time.Second}
		// 🔧 使用429错误检测的HTTP请求执行器
		resp, err := executeHTTPRequestWithRateLimit(req, client, accountID)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		success = resp.StatusCode == 200
		return nil
	}, "CancelOrder", accountID)

	// 订单取消成功，无需额外延迟
	if err == nil && success {
		log.Printf("✅ [%s] 订单取消成功", accountID)
		return true
	}

	return false
}

// cancelAllOrders 取消所有订单
func cancelAllOrders(csrftoken, cookie string) bool {
	url := "https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/cancel-all"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte("{}")))
	if err != nil {
		return false
	}

	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Set("clienttype", "web")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("origin", "https://www.binance.com")
	req.Header.Set("referer", "https://www.binance.com/zh-CN/alpha/")
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
	req.Header.Set("Cookie", cookie)

	client := &http.Client{Timeout: 10 * time.Second}
	// 🔧 使用429错误检测的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "price_check")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		return false
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {

		return false
	}

	// 检查响应码
	if code, exists := response["code"]; exists {
		if code == "000000" || code == 0 {
			return true
		} else {
			return false
		}
	}

	return false
}

// cancelAllOrdersUntilSuccess 循环取消所有订单直到成功
func cancelAllOrdersUntilSuccess(tokenAddress, csrftoken, cookie string) bool {
	log.Printf("🗑️ 开始循环撤销所有订单直到成功")

	maxAttempts := 50 // 最多尝试50次，防止无限循环

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 1. 调用取消所有订单API
		if !cancelAllOrders(csrftoken, cookie) {
			time.Sleep(1 * time.Second)
			continue
		}

		// 2. 等待撤单生效
		time.Sleep(2 * time.Second)

		// 3. 查询余额确认是否还有锁定
		balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)

		// 4. 如果没有锁定，说明撤单成功
		if lockedAmount == 0 {
			return true
		}

		// 等待一段时间再重试
		if attempt < maxAttempts {
			waitTime := time.Duration(attempt) * time.Second // 递增等待时间
			if waitTime > 10*time.Second {
				waitTime = 10 * time.Second // 最多等待10秒
			}

			time.Sleep(waitTime)
		}
	}

	return false
}

// checkOrderCanceled 确认订单是否真的被取消了
func checkOrderCanceled(orderID, csrftoken, cookie string) bool {
	// 等待一小段时间让取消操作生效
	time.Sleep(100 * time.Millisecond)

	// 查询订单状态，如果订单不存在或状态为CANCELED则认为取消成功
	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=20&startTime=%d&endTime=%d",
		time.Now().UnixMilli()-300000, time.Now().UnixMilli()) // 查询最近5分钟的订单

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {

		return false
	}

	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("Cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	// 🔧 使用429错误检测的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "balance_check")
	if err != nil {

		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {

		return false
	}

	// 检查订单列表中是否还有这个订单ID且状态为PENDING
	if data, exists := response["data"]; exists {
		if orders, ok := data.([]interface{}); ok {
			for _, order := range orders {
				if orderMap, ok := order.(map[string]interface{}); ok {
					if id, exists := orderMap["orderId"]; exists && fmt.Sprintf("%v", id) == orderID {
						if status, exists := orderMap["orderStatus"]; exists {
							statusStr := fmt.Sprintf("%v", status)
							if statusStr == "PENDING" || statusStr == "PARTIALLY_FILLED" {

								return false // 订单仍然活跃
							}
						}
					}
				}
			}
		}
	}

	// 如果在活跃订单中没有找到这个订单ID，认为取消成功
	return true
}

// initAccountStats 初始化账号统计
func initAccountStats(accountID string, targetVolume float64) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	if _, exists := accountStats[accountID]; !exists {
		accountStats[accountID] = &AccountStats{
			AccountID:    accountID,
			TargetVolume: targetVolume,
			TotalVolume:  0,
			TradeCount:   0,
		}

	}
}

// updateAccountStats 更新账号统计
// volume: 真实的买单金额（实际花费的USDT）
// loss: 净损益（正数=亏损，负数=盈利）
func updateAccountStats(accountID string, volume float64, loss float64) {
	updateAccountStatsWithID(accountID, volume, loss, "")
}

// updateAccountStatsWithID 带交易ID的统计更新（防重复）
func updateAccountStatsWithID(accountID string, volume float64, loss float64, tradeID string) {
	// 🔧 防重复统计检查
	if tradeID != "" {
		if isTradeProcessed(tradeID) {
			log.Printf("⚠️ [%s] 交易已统计，跳过重复统计: %s", accountID, tradeID)
			return
		}
		markTradeProcessed(tradeID)
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	if stats, exists := accountStats[accountID]; exists {
		stats.TotalVolume += volume // 累计买单金额
		stats.TradeCount++          // 交易次数+1
		stats.LastTradeTime = time.Now()
		stats.TotalLoss += loss // 累计损益

		// 计算总磨损率（万分比）
		if stats.TotalVolume > 0 {
			stats.TotalLossRate = (stats.TotalLoss / stats.TotalVolume) * 10000
		}

		if tradeID != "" {
			log.Printf("📊 [%s] 统计更新(ID:%s): 买单金额=%.6f USDT, 累计=%.6f USDT, 交易次数=%d",
				accountID, tradeID[:8], volume, stats.TotalVolume, stats.TradeCount)
		} else {
			log.Printf("📊 [%s] 统计更新: 买单金额=%.6f USDT, 累计=%.6f USDT, 交易次数=%d",
				accountID, volume, stats.TotalVolume, stats.TradeCount)
		}
	}

	// 在锁外更新全局统计，避免死锁
	go updateGlobalStats()
}

// updateAccountLossOnly 只更新账号损益，不重复统计买入交易额和交易次数
func updateAccountLossOnly(accountID string, loss float64, tradeID string) {
	// 🔧 防重复统计检查
	if tradeID != "" {
		lossTradeID := tradeID + "_loss" // 损益更新使用不同的ID
		if isTradeProcessed(lossTradeID) {
			log.Printf("⚠️ [%s] 损益已更新，跳过重复更新: %s", accountID, lossTradeID)
			return
		}
		markTradeProcessed(lossTradeID)
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	if stats, exists := accountStats[accountID]; exists {
		stats.TotalLoss += loss // 只累计损益，不增加交易额和次数
		stats.LastTradeTime = time.Now()

		// 重新计算总磨损率（万分比）
		if stats.TotalVolume > 0 {
			stats.TotalLossRate = (stats.TotalLoss / stats.TotalVolume) * 10000
		}

		if tradeID != "" {
			log.Printf("📊 [%s] 损益更新(ID:%s): 净损益=%.6f USDT, 累计损益=%.6f USDT",
				accountID, tradeID[:8], loss, stats.TotalLoss)
		} else {
			log.Printf("📊 [%s] 损益更新: 净损益=%.6f USDT, 累计损益=%.6f USDT",
				accountID, loss, stats.TotalLoss)
		}
	}

	// 在锁外更新全局统计，避免死锁
	go updateGlobalStats()
}

// updateGlobalStats 更新全局统计 (修复锁顺序)
func updateGlobalStats() {
	// 🚨 新增：检查是否正在全局停止，如果是则跳过统计更新
	globalStopMutex.RLock()
	if isGlobalStopping {
		globalStopMutex.RUnlock()
		return // 跳过统计更新，避免重复触发停止
	}
	globalStopMutex.RUnlock()

	// 统一锁顺序：先 globalMutex，再 statsMutex，避免死锁
	globalMutex.Lock()
	defer globalMutex.Unlock()

	statsMutex.RLock()
	defer statsMutex.RUnlock()

	globalStats.TotalTargetVolume = 0
	globalStats.TotalCurrentVolume = 0
	globalStats.TotalLoss = 0
	globalStats.ActiveAccounts = 0

	// 遍历所有账号统计
	for _, stats := range accountStats {
		globalStats.TotalTargetVolume += stats.TargetVolume
		globalStats.TotalCurrentVolume += stats.TotalVolume
		globalStats.TotalLoss += stats.TotalLoss
		if stats.IsLooping {
			globalStats.ActiveAccounts++
		}
	}

	// 计算完成率
	if globalStats.TotalTargetVolume > 0 {
		globalStats.CompletionRate = (globalStats.TotalCurrentVolume / globalStats.TotalTargetVolume) * 100
	}

	// 计算总磨损率
	if globalStats.TotalCurrentVolume > 0 {
		globalStats.TotalLossRate = (globalStats.TotalLoss / globalStats.TotalCurrentVolume) * 10000
	}

	// 输出全局统计
	if globalStats.TotalLoss >= 0 {
		log.Printf("🌍 全局统计 - 目标: %.2f, 完成: %.2f (%.1f%%), 净亏损: %.6f (%.2f万分), 活跃: %d账号",
			globalStats.TotalTargetVolume, globalStats.TotalCurrentVolume, globalStats.CompletionRate,
			globalStats.TotalLoss, globalStats.TotalLossRate, globalStats.ActiveAccounts)
	} else {
		log.Printf("🌍 全局统计 - 目标: %.2f, 完成: %.2f (%.1f%%), 净盈利: %.6f (%.2f万分), 活跃: %d账号",
			globalStats.TotalTargetVolume, globalStats.TotalCurrentVolume, globalStats.CompletionRate,
			-globalStats.TotalLoss, -globalStats.TotalLossRate, globalStats.ActiveAccounts)
	}

	// 检查是否达到全局目标交易额
	if globalStats.TotalTargetVolume > 0 && globalStats.TotalCurrentVolume >= globalStats.TotalTargetVolume {
		// 检查是否已经在停止过程中
		globalStopMutex.RLock()
		alreadyStopping := isGlobalStopping
		globalStopMutex.RUnlock()

		if !alreadyStopping {
			globalStopMutex.Lock()
			if !isGlobalStopping { // 双重检查
				isGlobalStopping = true
				globalStopTime = time.Now()
				globalStopMutex.Unlock()

				log.Printf("🎉 全局交易额已达成目标！开始停止所有交易...")
				go stopAllTradingAndExit()
			} else {
				globalStopMutex.Unlock()
			}
		}
	}
}

// startAutoLoop 启动自动循环交易
func startAutoLoop(req *TradeRequest) {
	// 检查最小交易金额
	minTradeAmount := 3.0
	if req.USDTAmount < minTradeAmount {
		log.Printf("⚠️ [%s] 自动循环金额过小，跳过启动 - 金额: %.2f < %.1f", req.AccountID, req.USDTAmount, minTradeAmount)
		return
	}

	loopMutex.Lock()

	// 检查是否已经在循环
	if _, exists := loopingAccounts[req.AccountID]; exists {
		loopMutex.Unlock()
		return
	}

	// 创建停止通道
	stopChan := make(chan bool, 1)
	loopingAccounts[req.AccountID] = stopChan
	loopMutex.Unlock()

	// 存储当前交易的代币
	tokensMutex.Lock()
	currentTradeTokens[req.AccountID] = req.BaseAsset
	tokensMutex.Unlock()

	// 更新状态
	statsMutex.Lock()
	if stats, exists := accountStats[req.AccountID]; exists {
		stats.IsLooping = true
	}
	statsMutex.Unlock()

	log.Printf("🔄 [%s] 开始自动循环交易 - 目标交易额: %.2f USDT", req.AccountID, req.TargetVolume)
	log.Printf("💡 [%s] 新策略：代币价值<1 USDT时跳过卖出，继续购买新代币", req.AccountID)

	// 🔧 修复死循环：添加最大循环次数和超时保护
	maxLoopCount := 10000 // 最大循环次数
	loopStartTime := time.Now()
	maxLoopDuration := 24 * time.Hour // 最大循环时间24小时
	loopCount := 0

	for {
		loopCount++

		// 🔧 修复死循环：检查循环次数和时间限制
		if loopCount > maxLoopCount {
			log.Printf("⚠️ [%s] 达到最大循环次数 %d，停止自动循环", req.AccountID, maxLoopCount)
			break
		}

		if time.Since(loopStartTime) > maxLoopDuration {
			log.Printf("⚠️ [%s] 达到最大循环时间 %v，停止自动循环", req.AccountID, maxLoopDuration)
			break
		}

		// 检查是否达到目标交易额
		statsMutex.RLock()
		stats := accountStats[req.AccountID]
		reachedTarget := stats.TotalVolume >= stats.TargetVolume
		statsMutex.RUnlock()

		if reachedTarget {
			log.Printf("✅ [%s] 达到目标交易额，执行最终卖出确保", req.AccountID)

			// 🔧 确保最后一笔是卖单且没有残留
			if err := ensureFinalSellAndCleanup(req); err != nil {
				log.Printf("⚠️ [%s] 最终卖出确保失败: %v", req.AccountID, err)
			}
			break
		}

		// 检查停止信号
		select {
		case <-stopChan:
			log.Printf("🛑 [%s] 收到停止信号，停止自动循环", req.AccountID)
			goto cleanup
		default:
		}

		// 随机延迟
		delay := rand.Intn(req.MaxDelay-req.MinDelay+1) + req.MinDelay

		select {
		case <-time.After(time.Duration(delay) * time.Second):
			// 执行交易
			response := executeSingleTrade(req)

			// 存储交易结果
			storeTradeResult(req.AccountID, response)

			if response.Success && !strings.Contains(response.Message, "异步") {
				// 计算净损益：买入成本 - 卖出收入 (正数=亏损，负数=盈利)
				buyInCost := response.BuyPrice * response.TokenAmount       // 实际买入成本
				sellOutRevenue := response.SellPrice * response.TokenAmount // 卖出收入
				netLoss := buyInCost - sellOutRevenue                       // 净损益

				// 验证是否有真实交易量 (防止0交易量被统计)
				if buyInCost > 0 && response.TokenAmount > 0 {
					// 生成唯一交易ID
					tradeID := generateTradeID(req.AccountID, req.TokenAddress, response.BuyPrice, response.TokenAmount)
					// 🔧 新逻辑：自动循环只更新损益，买入交易额已在executeSingleTrade中统计
					updateAccountLossOnly(req.AccountID, netLoss, tradeID)

					if strings.Contains(response.Message, "挂单跳过") {
						if netLoss > 0 {
							log.Printf("📌 [%s] 自动循环-已挂单跳过，买单金额: %.6f USDT，亏损: %.6f USDT (损益更新)", req.AccountID, buyInCost, netLoss)
						} else {
							log.Printf("📌 [%s] 自动循环-已挂单跳过，买单金额: %.6f USDT，盈利: %.6f USDT (损益更新)", req.AccountID, buyInCost, -netLoss)
						}
					} else {
						if netLoss > 0 {
							log.Printf("✅ [%s] 自动循环-交易完成，买单金额: %.6f USDT，亏损: %.6f USDT (损益更新)", req.AccountID, buyInCost, netLoss)
						} else {
							log.Printf("✅ [%s] 自动循环-交易完成，买单金额: %.6f USDT，盈利: %.6f USDT (损益更新)", req.AccountID, buyInCost, -netLoss)
						}
					}
				}
			} else if response.Success && strings.Contains(response.Message, "异步") {
				log.Printf("⏳ [%s] 异步交易已启动: %s", req.AccountID, response.Message)
			} else {

				// 🔧 增强：特殊错误处理
				if strings.Contains(response.Message, "余额不足") || strings.Contains(response.Message, "481020") {
					time.Sleep(1 * time.Second) // 等待1秒，可能有新的余额
					continue                    // 继续循环，不退出
				}

				// 认证失效才真正退出循环
				if strings.Contains(response.Message, "AUTH_FAILED") || strings.Contains(response.Message, "认证失效") {
					log.Printf("🚨 [%s] 检测到认证失效，停止该账号自动交易", req.AccountID)
					goto cleanup // 退出循环
				}

				// 其他错误继续重试
				if !response.Success {
					if strings.Contains(response.Message, "该账户有异步操作进行中") {
						// 检测到异步操作冲突，增加等待时间并智能处理
						log.Printf("⏳ [%s] 检测到异步操作冲突，等待30秒后重试: %s", req.AccountID, response.Message)
						
						// 检查是否是强制卖出操作
						state := getAccountAsyncState(req.AccountID)
						if state.HasAsyncDecrement {
							log.Printf("🔄 [%s] 正在进行强制递减卖出，等待操作完成...", req.AccountID)
							// 等待更长时间，让强制卖出完成
							time.Sleep(30 * time.Second)
						} else {
							// 其他异步操作，等待标准时间
							time.Sleep(15 * time.Second)
						}
					} else {
						// 其他错误，使用标准等待时间
						log.Printf("⚠️ [%s] 交易失败，等待10秒后继续重试: %s", req.AccountID, response.Message)
						time.Sleep(10 * time.Second)
					}
				}
			}
		case <-stopChan:

			goto cleanup
		}
	}

cleanup:
	// 清理
	loopMutex.Lock()
	delete(loopingAccounts, req.AccountID)
	loopMutex.Unlock()

	// 清理当前交易代币记录
	tokensMutex.Lock()
	delete(currentTradeTokens, req.AccountID)
	tokensMutex.Unlock()

	statsMutex.Lock()
	if stats, exists := accountStats[req.AccountID]; exists {
		stats.IsLooping = false
	}
	statsMutex.Unlock()

	// 显示最终磨损统计
	showFinalLossStats(req.AccountID)

	// 🔧 新增：交易额达标后的强制清理确认
	log.Printf("🔍 [%s] 交易额达标，执行最终清理确认", req.AccountID)
	if err := ensureCompleteCleanupAfterTargetVolume(req); err != nil {
		log.Printf("⚠️ [%s] 最终清理确认失败: %v", req.AccountID, err)
	} else {
		log.Printf("✅ [%s] 最终清理确认完成", req.AccountID)
	}
}

// showFinalLossStats 显示最终磨损统计
func showFinalLossStats(accountID string) {
	statsMutex.RLock()
	defer statsMutex.RUnlock()

	if stats, exists := accountStats[accountID]; exists {
		log.Printf("📈 [%s] ========== 最终统计 ==========", accountID)
		log.Printf("📈 [%s] 总交易额: %.2f USDT", accountID, stats.TotalVolume)
		log.Printf("📈 [%s] 交易次数: %d 次", accountID, stats.TradeCount)
		log.Printf("📈 [%s] 总磨损: %.6f USDT", accountID, stats.TotalLoss)
		log.Printf("📈 [%s] 磨损率: %.2f万分", accountID, stats.TotalLossRate)

		// 计算磨损占比
		if stats.TotalVolume > 0 {
			lossPercentage := (stats.TotalLoss / stats.TotalVolume) * 100
			log.Printf("📈 [%s] 磨损占比: %.4f%%", accountID, lossPercentage)
		}

		// 如果是10万交易额，计算对应磨损
		if stats.TotalVolume > 0 {
			projectedLoss := (stats.TotalLoss / stats.TotalVolume) * 100000
			log.Printf("📈 [%s] 10万交易额预计磨损: %.2f USDT", accountID, projectedLoss)
		}

		log.Printf("📈 [%s] ================================", accountID)
	}
}

// handleStats 查看账号统计
func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 使用通道和超时机制防止阻塞
	responseChan := make(chan map[string]interface{}, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {

			}
		}()

		// 先更新全局统计（内部会处理锁）
		updateGlobalStats()

		// 然后读取数据
		statsMutex.RLock()
		globalMutex.RLock()

		// 计算10万预计磨损，避免除零错误
		var projectedLoss100k float64
		if globalStats.TotalCurrentVolume > 0 {
			projectedLoss100k = (globalStats.TotalLoss / globalStats.TotalCurrentVolume) * 100000
		}

		// 🔧 优化：跳过Redis查询以提高性能
		// redisAccounts := getAccountsFromRedisWithFallback()
		redisAccounts := make(map[string]*AccountInfo) // 使用空数据，避免Redis查询延迟

		// 构建账户统计，添加时间戳字段，使用英文键名
		accountStatsWithTimestamp := make(map[string]interface{})
		accountIndex := 0
		for accountID, stats := range accountStats {
			// 🔧 为了避免中文字符作为JSON键名的问题，使用安全的键名
			safeKey := fmt.Sprintf("account_%d", accountIndex)
			accountIndex++

			accountData := map[string]interface{}{
				"account_id":      stats.AccountID,
				"account_key":     accountID, // 保留原始账号ID
				"total_volume":    stats.TotalVolume,
				"target_volume":   stats.TargetVolume,
				"trade_count":     stats.TradeCount,
				"is_looping":      stats.IsLooping,
				"total_loss":      stats.TotalLoss,
				"total_loss_rate": stats.TotalLossRate,
			}

			// 🔧 优化：简化累积统计计算，避免性能问题
			// 只添加基本的累积统计信息，避免复杂计算
			accountData["total_cumulative_volume"] = 0.0               // 简化为0，避免复杂查询
			accountData["original_target_volume"] = stats.TargetVolume // 使用当前目标交易额
			accountData["cumulative_completion_rate"] = 0.0            // 简化为0，避免复杂计算

			// 添加Redis中的账号信息
			if redisAccount, exists := redisAccounts[accountID]; exists {
				accountData["account_name"] = redisAccount.Name
				accountData["assigned_node"] = redisAccount.AssignedNode
				accountData["status"] = redisAccount.Status
				accountData["expires_at"] = redisAccount.ExpiresAt.Unix()
				accountData["created_at"] = redisAccount.CreatedAt.Unix()
			}

			// 添加最后交易时间戳
			if !stats.LastTradeTime.IsZero() {
				accountData["last_trade_time"] = stats.LastTradeTime.Unix()
			} else {
				accountData["last_trade_time"] = 0
			}

			// 计算完成率
			if stats.TargetVolume > 0 {
				accountData["completion_rate"] = (stats.TotalVolume / stats.TargetVolume) * 100
			} else {
				accountData["completion_rate"] = 0
			}

			// 添加当前代币信息（如果有的话）
			accountData["current_token"] = getCurrentToken(accountID)

			// 添加暂停状态信息
			if isPaused, pauseInfo := checkAccountPauseStatus(accountID); isPaused {
				accountData["pause_status"] = map[string]interface{}{
					"is_paused":         true,
					"pause_reason":      pauseInfo.Reason,
					"pause_start_time":  pauseInfo.PauseStart.Unix(),
					"pause_end_time":    pauseInfo.PauseEnd.Unix(),
					"remaining_seconds": int(time.Until(pauseInfo.PauseEnd).Seconds()),
				}
			} else {
				accountData["pause_status"] = map[string]interface{}{
					"is_paused": false,
				}
			}

			accountStatsWithTimestamp[safeKey] = accountData
		}

		// 获取暂停状态信息
		pauseStatusMap := getAllPauseStatus()

		response := map[string]interface{}{
			// 英文字段（兼容性）
			"success": true,
			"global_stats": map[string]interface{}{
				"total_target_volume":  globalStats.TotalTargetVolume,
				"total_current_volume": globalStats.TotalCurrentVolume,
				"total_loss":           globalStats.TotalLoss,
				"total_loss_rate":      globalStats.TotalLossRate,
				"completion_rate":      globalStats.CompletionRate,
				"active_accounts":      globalStats.ActiveAccounts,
				"projected_loss_100k":  projectedLoss100k,
				"paused_accounts":      len(pauseStatusMap),
			},
			"account_stats":  accountStatsWithTimestamp,
			"pause_status":   pauseStatusMap,
			"redis_accounts": len(redisAccounts),
			// 中文字段
			"成功": true,
			"全局统计": map[string]interface{}{
				"总目标交易量":  globalStats.TotalTargetVolume,
				"总当前交易量":  globalStats.TotalCurrentVolume,
				"总亏损":     globalStats.TotalLoss,
				"总亏损率":    globalStats.TotalLossRate,
				"完成率":     globalStats.CompletionRate,
				"活跃账户数":   globalStats.ActiveAccounts,
				"10万预计亏损": projectedLoss100k,
				"暂停账户数":   len(pauseStatusMap),
				"服务开始时间":  serviceStartTime.Unix(), // 🔧 新增：服务开始时间
			},
			"账户统计":    accountStatsWithTimestamp,
			"暂停状态":    pauseStatusMap,
			"Redis账户": len(redisAccounts),
		}

		globalMutex.RUnlock()
		statsMutex.RUnlock()

		responseChan <- response
	}()

	// 🔧 优化：减少超时时间，提高响应速度
	select {
	case response := <-responseChan:
		json.NewEncoder(w).Encode(response)
	case <-time.After(5 * time.Second):
		log.Printf("⚠️ handleStats timeout after 5 seconds")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"成功": false,
			"消息": "请求超时",
		})
	}
}

// 🔧 新增：快速统计接口 - 只返回基本统计信息，避免复杂查询
func handleStatsFast(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 快速响应，不使用超时机制
	statsMutex.RLock()
	globalMutex.RLock()

	// 计算基本的全局统计
	var projectedLoss100k float64
	if globalStats.TotalCurrentVolume > 0 {
		projectedLoss100k = (globalStats.TotalLoss / globalStats.TotalCurrentVolume) * 100000
	}

	// 构建简化的账户统计
	accountStatsSimple := make(map[string]interface{})
	accountIndex := 0
	for accountID, stats := range accountStats {
		safeKey := fmt.Sprintf("account_%d", accountIndex)
		accountIndex++

		// 🔧 修复：添加最后交易时间字段
		accountData := map[string]interface{}{
			// 英文字段（兼容性）
			"account_id":      stats.AccountID,
			"account_key":     accountID,
			"total_volume":    stats.TotalVolume,
			"target_volume":   stats.TargetVolume,
			"trade_count":     stats.TradeCount,
			"is_looping":      stats.IsLooping,
			"total_loss":      stats.TotalLoss,
			"total_loss_rate": stats.TotalLossRate,
			// 中文字段
			"账户ID":  stats.AccountID,
			"账户键":   accountID,
			"总交易量":  stats.TotalVolume,
			"目标交易量": stats.TargetVolume,
			"交易次数":  stats.TradeCount,
			"是否循环中": stats.IsLooping,
			"总亏损":   stats.TotalLoss,
			"总亏损率":  stats.TotalLossRate,
		}

		// 🔧 修复：添加最后交易时间字段
		if !stats.LastTradeTime.IsZero() {
			accountData["last_trade_time"] = stats.LastTradeTime.Unix()
			accountData["最后交易时间"] = stats.LastTradeTime.Unix()
			accountData["最后交易时间_格式化"] = stats.LastTradeTime.Format("2006-01-02 15:04:05")
		} else {
			accountData["last_trade_time"] = 0
			accountData["最后交易时间"] = 0
			accountData["最后交易时间_格式化"] = ""
		}

		accountStatsSimple[safeKey] = accountData
	}

	response := map[string]interface{}{
		// 中文字段
		"成功": true,
		"全局统计": map[string]interface{}{
			"总目标交易量":  globalStats.TotalTargetVolume,
			"总当前交易量":  globalStats.TotalCurrentVolume,
			"总亏损":     globalStats.TotalLoss,
			"总亏损率":    globalStats.TotalLossRate,
			"完成率":     globalStats.CompletionRate,
			"活跃账户数":   globalStats.ActiveAccounts,
			"10万预计磨损": projectedLoss100k,
			"服务开始时间":  serviceStartTime.Unix(), // 🔧 新增：服务开始时间
		},
		"账户统计": accountStatsSimple,
		// 英文字段（兼容性）
		"success": true,
		"global_stats": map[string]interface{}{
			"total_target_volume":  globalStats.TotalTargetVolume,
			"total_current_volume": globalStats.TotalCurrentVolume,
			"total_loss":           globalStats.TotalLoss,
			"total_loss_rate":      globalStats.TotalLossRate,
			"completion_rate":      globalStats.CompletionRate,
			"active_accounts":      globalStats.ActiveAccounts,
			"projected_loss_100k":  projectedLoss100k,
			"service_start_time":   serviceStartTime.Unix(), // 🔧 新增：服务开始时间
		},
		"account_stats": accountStatsSimple,
	}

	statsMutex.RUnlock()
	globalMutex.RUnlock()

	json.NewEncoder(w).Encode(response)
}

// updateLastTradeTime 只更新最后交易时间，不计算交易额和交易次数
func updateLastTradeTime(accountID string) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	if stats, exists := accountStats[accountID]; exists {
		stats.LastTradeTime = time.Now()

	}
}

// getCurrentToken 获取账号当前交易的代币（直接从 base_asset 获取）
func getCurrentToken(accountID string) string {
	// 1. 优先从当前交易代币记录中获取
	tokensMutex.RLock()
	if baseAsset, exists := currentTradeTokens[accountID]; exists {
		tokensMutex.RUnlock()
		return baseAsset // 直接返回 base_asset，如 ALPHA_261
	}
	tokensMutex.RUnlock()

	// 2. 从Flash Trade内部自动卖单管理器中查找当前活跃的代币
	autoSellMutex.Lock()
	for taskID, task := range autoSellManager.tasks {
		if strings.Contains(taskID, accountID) && task.IsActive {
			if task.BaseAsset != "" {
				autoSellMutex.Unlock()
				return task.BaseAsset // 直接返回 base_asset，如 ALPHA_261
			}
		}
	}
	autoSellMutex.Unlock()

	// 3. 检查是否有循环中的账号
	loopMutex.RLock()
	defer loopMutex.RUnlock()

	if _, exists := loopingAccounts[accountID]; exists {
		// 如果在循环中但没有找到具体的 base_asset，返回通用标识
		return "交易中"
	}

	return "无"
}

// handleStop 停止账号循环
func handleStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Invalid request",
			"成功":      false,
			"消息":      "无效请求",
		})
		return
	}

	loopMutex.Lock()
	if stopChan, exists := loopingAccounts[req.AccountID]; exists {
		stopChan <- true
		loopMutex.Unlock()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Stop signal sent to " + req.AccountID,
			"成功":      true,
			"消息":      "停止信号已发送到 " + req.AccountID,
		})
	} else {
		loopMutex.Unlock()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Account not found or not looping",
			"成功":      false,
			"消息":      "未找到账户或账户未在循环中",
		})
	}
}

// storeTradeResult 存储交易结果
func storeTradeResult(accountID string, result TradeResponse) {
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	// 为每个账号最多保存最近10次交易结果
	if tradeResults[accountID] == nil {
		tradeResults[accountID] = make([]TradeResponse, 0)
	}

	tradeResults[accountID] = append(tradeResults[accountID], result)

	// 保持最近10次记录
	if len(tradeResults[accountID]) > 10 {
		tradeResults[accountID] = tradeResults[accountID][1:]
	}
}

// handleResult 查询交易结果
func handleResult(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "account_id parameter required",
			"成功":      false,
			"消息":      "需要account_id参数",
		})
		return
	}

	resultsMutex.RLock()
	results, exists := tradeResults[accountID]
	resultsMutex.RUnlock()

	if !exists || len(results) == 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "No trade results found",
			"results": []TradeResponse{},
			"成功":      true,
			"消息":      "未找到交易结果",
			"结果":      []TradeResponse{},
		})
		return
	}

	// 转换交易结果为中文字段
	resultsWithChinese := make([]map[string]interface{}, len(results))
	for i, result := range results {
		resultsWithChinese[i] = convertToChineseFields(result)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"account_id": accountID,
		"results":    results,
		"count":      len(results),
		"成功":         true,
		"账户ID":       accountID,
		"结果":         resultsWithChinese,
		"数量":         len(results),
	})
}

// smartSellWithRetry 智能卖出重试机制（新逻辑）
func smartSellWithRetry(req *TradeRequest, buyPrice, tokenAmount float64, startTime time.Time) TradeResponse {
	log.Printf("🚀 [%s] 开始执行智能卖出流程，买入价格: %.8f, 代币数量: %.6f", req.AccountID, buyPrice, tokenAmount)

	// 检查代币数量是否为0
	if tokenAmount <= 0 {
		log.Printf("⚠️ [%s] 卖出代币数量为0，跳过卖单 - 数量: %.0f", req.AccountID, tokenAmount)
		return TradeResponse{
			Success: false,
			Message: "卖出代币数量为0，无法下单",
		}
	}

	// 获取当前市场价格
	priceMode := getPriceMode(req)
	currentMarketPrice, err := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, priceMode)
	if err != nil {
		log.Printf("❌ [%s] 获取当前价格失败: %v，尝试使用买入价格作为卖出价格", req.AccountID, err)
		currentMarketPrice = buyPrice // 如果无法获取市场价，使用买入价格作为卖出价格
	}
	currentMarketPrice = adjustPricePrecision(currentMarketPrice, req.PricePrecision)
	log.Printf("📊 [%s] 获取到当前市场价格: %.8f", req.AccountID, currentMarketPrice)

	var initialSellPrice float64
	var strategy string
	var isHangingOrder bool = false // 是否为挂单等待

	// 判断当前价格与买入价格的关系
	if currentMarketPrice > buyPrice {
		// 情况1: 当前价格 > 买入价格，按当前价格降价万2卖出
		initialSellPrice = currentMarketPrice * 0.9998 // 降价万2
		initialSellPrice = adjustPricePrecision(initialSellPrice, req.PricePrecision)
		profit := (initialSellPrice - buyPrice) / buyPrice * 10000
		strategy = fmt.Sprintf("市场价-万2, 利润: %.2f万分", profit)
	} else if currentMarketPrice < buyPrice {
		// 情况2: 当前价格 < 买入价格，判断磨损
		loss := (buyPrice - currentMarketPrice) / buyPrice
		lossWanFen := loss * 10000

		// 🔧 新增：智能止损判断
		shouldStopLoss, stopLossReason := shouldEmergencyStopLoss(buyPrice, currentMarketPrice)

		if shouldStopLoss {
			// 智能止损：直接按市场价卖出
			initialSellPrice = currentMarketPrice
			strategy = fmt.Sprintf("智能止损: %s, 磨损: %.2f万分", stopLossReason, lossWanFen)
			log.Printf("🚨 [%s] 触发智能止损: %s", req.AccountID, stopLossReason)
		} else if loss > 0.1 { // 磨损 > 百10
			// 🔧 优化：挂百10磨损价格，2分钟后按市场价卖（从5分钟缩短到2分钟）
			initialSellPrice = buyPrice * 0.9 // 百10磨损
			initialSellPrice = adjustPricePrecision(initialSellPrice, req.PricePrecision)
			strategy = fmt.Sprintf("百10挂单(2分钟), 磨损: %.0f万分 (市场磨损: %.2f万分)", 1000.0, lossWanFen)
			isHangingOrder = true
		} else {
			// 磨损 ≤ 百10，直接按当前价格卖
			initialSellPrice = currentMarketPrice
			strategy = fmt.Sprintf("市场价卖出, 磨损: %.2f万分", lossWanFen)
		}
	} else {
		// 情况3: 当前价格 = 买入价格，按万一磨损卖出
		initialSellPrice = buyPrice * 0.9999 // 万一磨损
		initialSellPrice = adjustPricePrecision(initialSellPrice, req.PricePrecision)
		strategy = "万一磨损, 磨损: 1万分"
	}

	// 确保卖出价格不为0
	if initialSellPrice <= 0 {
		log.Printf("⚠️ [%s] 计算的卖出价格为0或负数，使用买入价格作为卖出价格", req.AccountID)
		initialSellPrice = buyPrice
		strategy = "使用买入价格卖出（价格计算错误）"
	}

	log.Printf("💰 [%s] 卖出策略: %s, 价格: %.12f", req.AccountID, strategy, initialSellPrice)
	log.Printf("💰 [%s] Initial sell attempt: price=%.8f", req.AccountID, initialSellPrice)

	// 🔧 保留价值0.3 USDT的代币，避免触发最低价格限制
	reserveTokens := 0.3 / initialSellPrice // 保留价值0.3 USDT的代币数量
	availableTokens := tokenAmount - reserveTokens

	if availableTokens <= 0 {
		log.Printf("💸 [%s] 代币数量不足，无法卖出 (总量: %.6f, 保留: %.6f)",
			req.AccountID, tokenAmount, reserveTokens)
		return TradeResponse{
			Success: false,
			Message: fmt.Sprintf("代币数量不足，无法卖出 (需保留价值0.3 USDT的代币)"),
		}
	}

	// 使用币安规定调整代币数量
	sellTokenAmount := adjustTokenAmountForBinance(initialSellPrice, availableTokens, req.PricePrecision)

	log.Printf("🔢 [%s] 代币数量调整: 买入 %.0f 个 → 可卖 %.0f 个 → 实际卖出 %.0f 个 (保留 %.0f 个，价值0.3 USDT)",
		req.AccountID, tokenAmount, availableTokens, sellTokenAmount, reserveTokens)

	// 移除详细验证日志

	// 确保代币数量为整数，并且使用整数格式的字符串
	intSellAmount := math.Floor(sellTokenAmount) // 确保是整数
	strAmount := fmt.Sprintf("%.0f", intSellAmount) // 使用整数格式的字符串，不带小数点
	
	log.Printf("🔢 [%s] 最终卖出数量: %s (整数格式)", req.AccountID, strAmount)

	// 立即下第一个卖单（带重试机制）
	sellOrderID, sellSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      initialSellPrice,
		Quantity:   intSellAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            intSellAmount,
			AmountStr:         strAmount, // 使用整数格式的字符串
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

	if !sellSuccess {
		// 首先检查代币是否已被其他流程卖出
		if isTokenSold(req.AccountID, req.TokenAddress) {
			status := getTokenSellStatus(req.AccountID, req.TokenAddress)
			log.Printf("✅ [%s] 代币已被其他流程卖出，无需重试 - 方式: %s, 价格: %.8f, 数量: %.6f", 
				req.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
			
			// 返回成功响应，避免继续尝试卖出
			return TradeResponse{
				Success:     true,
				Message:     fmt.Sprintf("代币已被%s方式卖出，无需重试", status.SoldBy),
				BuyPrice:    buyPrice,
				SellPrice:   status.SoldPrice,
				TokenAmount: status.SoldAmount,
				Profit:      (status.SoldPrice - buyPrice) * status.SoldAmount,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		}
		
		// 再次检查代币余额，确认是否还有可卖代币
		tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
		if balErr == nil {
			freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
			if freeAmount <= 0 {
				log.Printf("✅ [%s] 代币余额为0，可能已被卖出，无需重试", req.AccountID)
				// 设置代币已卖出状态，但卖出方式未知
				setTokenSold(req.AccountID, req.TokenAddress, "unknown", currentMarketPrice, sellTokenAmount)
				return TradeResponse{
					Success:     true,
					Message:     "代币余额为0，无需重试",
					BuyPrice:    buyPrice,
					SellPrice:   currentMarketPrice,
					TokenAmount: sellTokenAmount,
					Profit:      (currentMarketPrice - buyPrice) * sellTokenAmount,
					ExecuteTime: time.Since(startTime).Milliseconds(),
				}
			} else if freeAmount < sellTokenAmount {
				log.Printf("📊 [%s] 更新卖出数量: %.6f → %.6f (使用当前可用余额)", 
					req.AccountID, sellTokenAmount, freeAmount)
				sellTokenAmount = freeAmount
			}
		}

		// 处理余额不足错误
		if sellOrderID == "INSUFFICIENT_BALANCE" {
			log.Printf("⚠️ [%s] 余额不足错误，可能代币已被卖出或数量不足", req.AccountID)
		}

		// 🔧 修正：保持递减重试流程，但刷量模式优化时间
		if currentMarketPrice > buyPrice {
			// 有利润或等价：递减重试策略
			log.Printf("⏰ [%s] 主卖出失败，进入递减重试策略", req.AccountID)
			return executeDecrementRetry(req, buyPrice, sellTokenAmount, currentMarketPrice, startTime)
		} else {
			// 亏损情况：直接进入千8挂单
			loss := (buyPrice - currentMarketPrice) / buyPrice
			if loss > 0.1 { // 磨损 > 百10
				log.Printf("⏰ [%s] 主卖出失败且磨损>百10，进入千8挂单", req.AccountID)
				return hangOrderAtQian8Loss(req, buyPrice, sellTokenAmount, startTime)
			} else {
				log.Printf("⏰ [%s] 主卖出失败但磨损≤百10，递减重试", req.AccountID)
				return executeDecrementRetry(req, buyPrice, sellTokenAmount, currentMarketPrice, startTime)
			}
		}
	}

	sellTime := time.Now().UnixMilli()

	if isHangingOrder {
		// 百10挂单：启动异步监控，立即返回继续新交易
		log.Printf("📌 [%s] 百10挂单成功，启动异步监控，继续新交易", req.AccountID)

		// 🔧 增强：检查是否已有异步任务在处理该代币
		if !markTokenProcessing(req.AccountID, req.TokenAddress) {
			log.Printf("⚠️ [%s] 代币已被其他异步任务处理，取消挂单: %s", req.AccountID, req.TokenAddress)
			cancelOrder(sellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie)
			return TradeResponse{Success: false, Message: "代币处理冲突，已取消挂单"}
		}

		// 启动异步线程监控挂单
		go func() {
			defer unmarkTokenProcessing(req.AccountID, req.TokenAddress)
			monitorHangingOrder(req, sellOrderID, buyPrice, sellTokenAmount, initialSellPrice, sellTime, req.USDTAmount)
		}()

		// 立即返回，标记为成功，继续新的买卖循环
		profit := (initialSellPrice - buyPrice) * sellTokenAmount
		executeTime := time.Since(startTime).Milliseconds()

		return TradeResponse{
			Success:     true,
			Message:     "百10挂单异步监控",
			BuyPrice:    buyPrice,
			SellPrice:   initialSellPrice,
			TokenAmount: sellTokenAmount,
			Profit:      profit,
			ExecuteTime: executeTime,
		}

	} else {
		// 🔥 刷量优化：极速卖出确认
		for i := 0; i < 3; i++ { // 0.6秒，每200ms检查一次
			time.Sleep(200 * time.Millisecond)

			if checkSellOrderHistory(sellTime, req.Csrftoken, req.Cookie) {
				log.Printf("✅ [%s] 卖出成功", req.AccountID)
				profit := (initialSellPrice - buyPrice) * sellTokenAmount
				executeTime := time.Since(startTime).Milliseconds()

				return TradeResponse{
					Success:     true,
					Message:     "Initial sell completed",
					BuyPrice:    buyPrice,
					SellPrice:   initialSellPrice,
					TokenAmount: sellTokenAmount,
					Profit:      profit,
					ExecuteTime: executeTime,
				}
			}
		}
	}

	// 取消第一个订单（带重试）
	cancelSuccess := false
	for attempt := 1; attempt <= 3; attempt++ {
		if cancelOrder(sellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
			cancelSuccess = true
			break
		} else if attempt < 3 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if !cancelSuccess {
		log.Printf("🚨 [%s] 警告：可能存在未取消的卖单，请人工检查", req.AccountID)
	}
	
	// 🔧 修复：撤销订单后等待足够时间，确保币安释放锁定的代币余额
	log.Printf("⏳ [%s] 撤销订单后等待2秒，确保代币余额完全释放", req.AccountID)
	time.Sleep(2 * time.Second)
	
	// 再次检查代币余额，确认是否已释放
	tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
	if balErr == nil {
		freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
		lockedAmount, _ := strconv.ParseFloat(tokenBalance.Locked, 64)
		log.Printf("📊 [%s] 等待后代币余额: 可用=%.6f, 锁定=%.6f", 
			req.AccountID, freeAmount, lockedAmount)
		
		// 如果代币仍然被锁定，再等待1秒
		if freeAmount <= 0 && lockedAmount > 0 {
			log.Printf("⏳ [%s] 代币仍然被锁定，额外等待1秒", req.AccountID)
			time.Sleep(1 * time.Second)
			
			// 再次检查余额
			tokenBalance, balErr = getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if balErr == nil {
				freeAmount, _ = strconv.ParseFloat(tokenBalance.Free, 64)
				lockedAmount, _ = strconv.ParseFloat(tokenBalance.Locked, 64)
				log.Printf("📊 [%s] 额外等待后代币余额: 可用=%.6f, 锁定=%.6f", 
					req.AccountID, freeAmount, lockedAmount)
			}
		}
	}

	// 🔧 新增：在递减重试前检查智能止损
	shouldStopLoss, stopLossReason := shouldEmergencyStopLoss(buyPrice, currentMarketPrice)
	if shouldStopLoss {
		log.Printf("🚨 [%s] 递减重试前触发智能止损: %s", req.AccountID, stopLossReason)
		// 直接按市场价卖出，不进行递减
		return sellAtMarketPrice(req, buyPrice, sellTokenAmount, currentMarketPrice, startTime)
	}

	// 1秒后未成交，开始快速递减重试流程
	if currentMarketPrice >= buyPrice {
		// 有利润或等价：优化递减策略
		log.Printf("⏰ [%s] 初始价格未成交，开始优化递减重试(万1→百10)...", req.AccountID)

		// 优化递减策略：万1 → 万6 → 万11 → 万16 → ... → 百10 (适量步骤)
		decrementSteps := []float64{
			0.0001, // 万1
			0.0006, // 万6
			0.0011, // 万11
			0.0016, // 万16
			0.0021, // 万21
			0.0026, // 万26
			0.0031, // 万31
			0.0036, // 万36
			0.0041, // 万41
			0.0046, // 万46
			0.006,  // 千6
			0.008,  // 千8
			0.01,   // 千10
			0.02,   // 千20
			0.05,   // 千50
			0.1,    // 百10 (最终限制)
		}

		currentSellPrice := initialSellPrice
		for step, decrementRate := range decrementSteps {
			// 按递减率计算价格
			if currentMarketPrice > buyPrice {
				// 有利润情况：从市场价-万1开始递减
				currentSellPrice = currentMarketPrice * (0.9999 - decrementRate)
			} else {
				// 等价情况：从买入价-万1开始递减
				currentSellPrice = buyPrice * (0.9999 - decrementRate)
			}
			currentSellPrice = adjustPricePrecision(currentSellPrice, req.PricePrecision)

			// 检查是否达到百10限制
			var currentLoss float64
			if currentMarketPrice > buyPrice {
				// 有利润情况：检查是否还有利润
				if currentSellPrice <= buyPrice {
					// 价格降到买入价以下，按百10挂单
					currentSellPrice = buyPrice * 0.9 // 百10磨损
					currentSellPrice = adjustPricePrecision(currentSellPrice, req.PricePrecision)
					log.Printf("📌 [%s] 利润耗尽，挂百10价格: %.12f", req.AccountID, currentSellPrice)
					return hangOrderAsync(req, buyPrice, sellTokenAmount, currentSellPrice, startTime)
				}
				currentLoss = (buyPrice - currentSellPrice) / buyPrice
			} else {
				currentLoss = (buyPrice - currentSellPrice) / buyPrice
				if currentLoss >= 0.1 { // 达到百10
					currentSellPrice = buyPrice * 0.9 // 百10磨损
					currentSellPrice = adjustPricePrecision(currentSellPrice, req.PricePrecision)
					log.Printf("📌 [%s] 达到百10限制，挂百10价格: %.12f", req.AccountID, currentSellPrice)
					return hangOrderAsync(req, buyPrice, sellTokenAmount, currentSellPrice, startTime)
				}
			}

			// 计算递减幅度的万分比表示
			decrementWanFen := decrementRate * 10000
			lossWanFen := currentLoss * 10000

			if currentLoss < 0 {
				profitWanFen := -lossWanFen
				log.Printf("💰 [%s] 递减第%d步(%.0f万分): %.12f, 利润: %.2f万分",
					req.AccountID, step+1, decrementWanFen, currentSellPrice, profitWanFen)
			} else {
				log.Printf("💰 [%s] 递减第%d步(%.0f万分): %.12f, 磨损: %.2f万分",
					req.AccountID, step+1, decrementWanFen, currentSellPrice, lossWanFen)
			}

			// 验证价格和数量的匹配性
			expectedValue := currentSellPrice * sellTokenAmount
			expectedValue = math.Round(expectedValue*100000000) / 100000000
			log.Printf("🔍 [%s] 递减验证: 价格 %.12f × 数量 %.0f = %.8f USDT",
				req.AccountID, currentSellPrice, sellTokenAmount, expectedValue)

			// 下新的卖单
			newSellOrderID, newSellSuccess := placeOrderWithRetry(OrderRequest{
				BaseAsset:  req.BaseAsset,
				QuoteAsset: "USDT",
				Side:       "SELL",
				Price:      currentSellPrice,
				Quantity:   sellTokenAmount,
				PaymentDetails: []PaymentDetail{{
					Amount:            sellTokenAmount,
					AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
					PaymentWalletType: "ALPHA",
				}},
				Csrftoken: req.Csrftoken,
				Cookie:    req.Cookie,
			}, req.AccountID)

			if !newSellSuccess {
				if newSellOrderID == "INSUFFICIENT_BALANCE" {

					// 使用币安规定调整代币数量
					newSellTokenAmount := adjustTokenAmountForBinance(currentSellPrice, sellTokenAmount, req.PricePrecision)
					if newSellTokenAmount < 1 || newSellTokenAmount >= sellTokenAmount {
						return TradeResponse{Success: false, Message: "代币数量调整失败"}
					}

					sellTokenAmount = newSellTokenAmount
					continue // 用新的数量重试当前价格
				}
				continue
			}

			// 等待300ms快速检查成交
			newSellTime := time.Now().UnixMilli()
			for i := 0; i < 2; i++ { // 300ms，每200ms检查一次
				time.Sleep(200 * time.Millisecond)
				if checkSellOrderHistory(newSellTime, req.Csrftoken, req.Cookie) {
					profit := (currentSellPrice - buyPrice) * sellTokenAmount
					executeTime := time.Since(startTime).Milliseconds()

					return TradeResponse{
						Success:     true,
						Message:     fmt.Sprintf("递减重试成交 (第%d步)", step+1),
						BuyPrice:    buyPrice,
						SellPrice:   currentSellPrice,
						TokenAmount: sellTokenAmount,
						Profit:      profit,
						ExecuteTime: executeTime,
					}
				}
			}

			// 快速取消当前订单，继续下一次递减
			for attempt := 1; attempt <= 2; attempt++ {
				if cancelOrder(newSellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
					break
				} else if attempt < 2 {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	} else {
		// 非等价情况，按市场价卖出
		log.Printf("⏰ [%s] 初始卖单未成交，按市场价卖出", req.AccountID)

		// 获取最新市场价并卖出
		latestMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
		if err != nil {
			return TradeResponse{Success: false, Message: "获取最新价格失败"}
		}
		latestMarketPrice = adjustPricePrecision(latestMarketPrice, req.PricePrecision)

		return sellAtMarketPrice(req, buyPrice, sellTokenAmount, latestMarketPrice, startTime)
	}

	// 计算千8磨损的价格
	qian8Price := buyPrice * 0.992 // 千分之8磨损
	qian8Price = adjustPricePrecision(qian8Price, req.PricePrecision)

	// 挂千8磨损价格的卖单（使用减少后的代币数量）
	hangingSellOrderID, hangingSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      qian8Price,
		Quantity:   sellTokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellTokenAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

	if hangingSuccess {
		log.Printf("📌 [%s] 千8挂单成功: %s, 价格: %.12f, 跳过继续新交易", req.AccountID, hangingSellOrderID, qian8Price)
	} else {

		// 挂单失败，尝试市场价强制卖出避免代币积累（使用减少后的代币数量）
		forceSellOrderID, forceSellSuccess := placeOrderWithRetry(OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      currentMarketPrice,
			Quantity:   sellTokenAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellTokenAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: req.Csrftoken,
			Cookie:    req.Cookie,
		}, req.AccountID)

		if forceSellSuccess {
			log.Printf("✅ [%s] 市场价强制卖出成功: %s", req.AccountID, forceSellOrderID)
			// 按市场价计算磨损
			marketLoss := (buyPrice - currentMarketPrice) * sellTokenAmount
			return TradeResponse{
				Success:     true,
				Message:     "市场价强制卖出",
				BuyPrice:    buyPrice,
				SellPrice:   currentMarketPrice,
				TokenAmount: sellTokenAmount, // 使用实际卖出的代币数量
				Profit:      -marketLoss,
			}
		} else {
			log.Printf("🚨 [%s] 市场价强制卖出也失败，将由其他兜底机制处理", req.AccountID)
			
			// 更新最后交易时间，确保系统可以继续其他操作
			updateLastTradeTime(req.AccountID)

			// 返回特殊状态，表示已由其他兜底机制处理
			marketLoss := (buyPrice - currentMarketPrice) * sellTokenAmount
			return TradeResponse{
				Success:     true, // 标记为成功，因为已由其他兜底机制处理
				Message:     "市场价失败，由其他兜底机制处理",
				BuyPrice:    buyPrice,
				SellPrice:   currentMarketPrice,
				TokenAmount: sellTokenAmount,
				Profit:      -marketLoss,
			}
		}
	}

	// 返回特殊状态，表示已挂单跳过
	qian8Loss := 0.008 * req.USDTAmount // 千8磨损
	return TradeResponse{
		Success:     true, // 标记为成功，因为已经处理了
		Message:     "千8挂单跳过",
		BuyPrice:    buyPrice,
		SellPrice:   qian8Price,
		TokenAmount: sellTokenAmount, // 使用实际卖出的代币数量
		Profit:      -qian8Loss,      // 千8磨损
	}
}

// checkSellOrderHistory 检查卖单历史
func checkSellOrderHistory(sellTime int64, csrftoken, cookie string) bool {
	now := time.Now().UnixMilli()

	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=10&orderStatus=FILLED&startTime=%d&endTime=%d",
		sellTime-60000, now)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("Cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	// 🔧 使用429错误检测的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "token_check")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}

	data, ok := response["data"].([]interface{})
	if !ok || len(data) == 0 {
		return false
	}

	// 检查第一个订单是否是SELL订单
	firstOrder, ok := data[0].(map[string]interface{})
	if !ok {
		return false
	}

	side, ok := firstOrder["side"].(string)
	if !ok {
		return false
	}

	return side == "SELL"
}

// handleHealth 健康检查接口（无锁，快速响应）
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 简单的健康检查，不涉及任何锁
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"status":  "healthy",
		"time":    time.Now().Format("2006-01-02 15:04:05"),
		"成功":      true,
		"状态":      "健康",
		"时间":      time.Now().Format("2006-01-02 15:04:05"),
	})
}

// handleGlobalStats 查看全局统计
func handleGlobalStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 使用通道和超时机制防止阻塞
	responseChan := make(chan map[string]interface{}, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {

			}
		}()

		// 先更新全局统计（内部会处理锁）
		updateGlobalStats()

		// 然后读取数据
		globalMutex.RLock()

		// 计算10万交易额预计磨损
		var projectedLoss100k float64
		if globalStats.TotalCurrentVolume > 0 {
			projectedLoss100k = (globalStats.TotalLoss / globalStats.TotalCurrentVolume) * 100000
		}

		response := map[string]interface{}{
			// 英文字段（兼容性）
			"success": true,
			"summary": map[string]interface{}{
				"total_target_volume":  globalStats.TotalTargetVolume,
				"total_current_volume": globalStats.TotalCurrentVolume,
				"completion_rate":      globalStats.CompletionRate,
				"total_loss":           globalStats.TotalLoss,
				"total_loss_rate":      globalStats.TotalLossRate,
				"projected_loss_100k":  projectedLoss100k,
				"active_accounts":      globalStats.ActiveAccounts,
			},
			// 中文字段
			"成功": true,
			"摘要": map[string]interface{}{
				"总目标交易量":  globalStats.TotalTargetVolume,
				"总当前交易量":  globalStats.TotalCurrentVolume,
				"完成率":     globalStats.CompletionRate,
				"总亏损":     globalStats.TotalLoss,
				"总亏损率":    globalStats.TotalLossRate,
				"10万预计亏损": projectedLoss100k,
				"活跃账户数":   globalStats.ActiveAccounts,
			},
		}

		globalMutex.RUnlock()

		responseChan <- response
	}()

	// 5秒超时保护
	select {
	case response := <-responseChan:
		json.NewEncoder(w).Encode(response)
	case <-time.After(5 * time.Second):
		log.Printf("⚠️ handleGlobalStats timeout")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Request timeout",
			"成功":      false,
			"消息":      "请求超时",
		})
	}
}

// checkBuyOrderConfirmed 检查指定买单是否已确认成交
func checkBuyOrderConfirmed(buyOrderID string, buyTime int64, csrftoken, cookie string) bool {
	// API调用频率限制
	waitForAPIRateLimit()

	now := time.Now().UnixMilli()

	url := fmt.Sprintf("https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/get-order-history-web?page=1&rows=10&orderStatus=FILLED&startTime=%d&endTime=%d",
		buyTime-60000, now)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("❌ Failed to create request: %v", err)
		return false
	}

	req.Header.Set("clienttype", "web")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("lang", "zh-CN")
	req.Header.Set("Cookie", cookie)

	client := &http.Client{Timeout: 3 * time.Second}
	// 🔧 使用429错误检测和断网重连的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "token_exists_check")
	if err != nil {
		log.Printf("❌ HTTP request failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	//log.Printf("📄 Order history response: %s", string(body))

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {

		return false
	}

	data, ok := response["data"].([]interface{})
	if !ok {

		return false
	}

	if len(data) == 0 {
		return false
	}

	//log.Printf("📊 Found %d filled orders in history", len(data))

	// 检查所有订单，寻找匹配的买单
	for _, orderInterface := range data {
		order, ok := orderInterface.(map[string]interface{})
		if !ok {
			continue
		}

		// 检查订单ID是否匹配
		orderIDStr := ""
		if orderID, ok := order["orderId"].(string); ok {
			orderIDStr = orderID
		} else if orderID, ok := order["orderId"].(float64); ok {
			orderIDStr = fmt.Sprintf("%.0f", orderID)
		}

		// 检查side是否为BUY
		side, ok := order["side"].(string)
		if !ok {
			continue
		}

		// 检查订单时间是否在买入时间之后
		orderTime := int64(0)
		if updateTime, ok := order["updateTime"].(float64); ok {
			orderTime = int64(updateTime)
		} else if time, ok := order["time"].(float64); ok {
			orderTime = int64(time)
		}

		//log.Printf("🔍 Order[%d]: ID=%s, Side=%s, Time=%d, Target=%s",
		//	i, orderIDStr, side, orderTime, buyOrderID)

		// 匹配条件：订单ID相同 AND 是BUY订单 AND 时间在买入之后
		if orderIDStr == buyOrderID && side == "BUY" && orderTime >= buyTime {
			return true
		}
	}

	return false
}

// retryBuyWithNewPrice 重新获取价格并重试买入（增强版：支持多次重试）
func retryBuyWithNewPrice(req *TradeRequest, startTime time.Time) TradeResponse {
	return retryBuyWithNewPriceAttempt(req, startTime, 1)
}

// retryBuyWithNewPriceAttempt 重新获取价格并重试买入（带重试计数）
func retryBuyWithNewPriceAttempt(req *TradeRequest, startTime time.Time, retryAttempt int) TradeResponse {
	maxBuyRetries := maxBuyRetryAttempts // 🔧 使用全局配置参数

	log.Printf("🔄 [%s] 重新获取价格重试买入 (第%d/%d次)", req.AccountID, retryAttempt, maxBuyRetries)

	// 1. 重新获取实时价格 - 使用买单专用价格模式
	log.Printf("📡 [%s] 重试买单：获取最新价格 (第%d次)", req.AccountID, retryAttempt)
	buyPriceMode := getBuyPriceMode(req)
	newPrice, err := price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, buyPriceMode)
	if err != nil {
		// 🔧 重试时的降级策略
		log.Printf("⚠️ [%s] 重试价格获取失败(模式:%s)，尝试降级: %v", req.AccountID, buyPriceMode, err)

		// 降级到combined模式
		newPrice, err = price.GetTokenPriceWithPrecisionAndMode(req.TokenAddress, getChainID(req), req.PricePrecision, price.PriceModeCombined)
		if err != nil {
			// 🔥 刷量优化：价格获取失败快速重试
			if retryAttempt < maxBuyRetries {
				var priceRetryWait time.Duration
				if isVolumeMode {
					priceRetryWait = 2 * time.Second // 刷量模式：2秒快速重试
				} else {
					priceRetryWait = 5 * time.Second // 正常模式：5秒
				}
				log.Printf("⚠️ [%s] 所有价格模式失败，等待%v后重试 (%d/%d): %v", req.AccountID, priceRetryWait, retryAttempt, maxBuyRetries, err)
				time.Sleep(priceRetryWait)
				return retryBuyWithNewPriceAttempt(req, startTime, retryAttempt+1)
			}
			return TradeResponse{Success: false, Message: "Failed to get fresh price for retry: " + err.Error()}
		} else {
			log.Printf("✅ [%s] 重试降级到combined模式成功: %.8f", req.AccountID, newPrice)
		}
	} else {
		log.Printf("✅ [%s] 重试价格获取成功(模式:%s): %.8f", req.AccountID, buyPriceMode, newPrice)
	}

	// 2. 计算重试买入价格（更激进的加价策略）
	var retryBuyPrice float64
	if isVolumeMode {
		// 🔥 刷量模式：更激进的加价，确保成交
		retryBuyPrice = newPrice * (1.0003 + float64(retryAttempt)*0.0001) // 基础万三 + 每次重试增加万一
		log.Printf("🔥 [%s] 刷量模式重试买入价格: %.12f (第%d次重试，加价%.4f%%)", req.AccountID, retryBuyPrice, retryAttempt, (retryBuyPrice/newPrice-1)*100)
	} else {
		// 正常模式：根据价格模式调整重试策略
		switch buyPriceMode {
		case price.PriceModeMarket:
			// 链上模式：更激进加价，因为是真实成交价
			retryBuyPrice = newPrice * (1.0003 + float64(retryAttempt)*0.0001) // 基础万三 + 递增
			log.Printf("💰 [%s] 链上模式重试买入价格: %.12f (第%d次重试，加价%.4f%%)", req.AccountID, retryBuyPrice, retryAttempt, (retryBuyPrice/newPrice-1)*100)
		case price.PriceModeLimit:
			// 限价模式：适中加价
			retryBuyPrice = newPrice * (1.0002 + float64(retryAttempt)*0.00005) // 基础万二 + 递增
			log.Printf("💰 [%s] 限价模式重试买入价格: %.12f (第%d次重试，加价%.4f%%)", req.AccountID, retryBuyPrice, retryAttempt, (retryBuyPrice/newPrice-1)*100)
		default:
			// 其他模式：渐进式加价
			retryBuyPrice = newPrice * (1.0002 + float64(retryAttempt)*0.0001) // 基础万二 + 递增
			log.Printf("💰 [%s] 综合模式重试买入价格: %.12f (第%d次重试，加价%.4f%%)", req.AccountID, retryBuyPrice, retryAttempt, (retryBuyPrice/newPrice-1)*100)
		}
	}
	retryBuyPrice = adjustPricePrecision(retryBuyPrice, req.PricePrecision)

	// 2. 重新计算数量和金额（使用加价后的买入价格）
	// 🔧 重试时也使用固定参数传递的数值，不再使用随机浮动
	adjustedRetryUSDTAmount := req.USDTAmount

	log.Printf("📊 [%s] 重试买入量使用固定值: %.6f USDT (不再使用随机浮动)",
		req.AccountID, adjustedRetryUSDTAmount)

	idealTokenAmount := adjustedRetryUSDTAmount / retryBuyPrice
	tokenAmount := float64(int(idealTokenAmount))
	calculatedAmount := retryBuyPrice * tokenAmount
	exactAmount := math.Round(calculatedAmount*100000000) / 100000000

	// 🔧 记录重试时实际使用的USDT金额
	retryActualUSDTUsed := exactAmount

	// 验证精度匹配
	verification := math.Round((retryBuyPrice*tokenAmount)*100000000) / 100000000
	if math.Abs(exactAmount-verification) > 0.00000001 {
		log.Printf("⚠️ [%s] 重试精度不匹配，调整金额: %.8f -> %.8f", req.AccountID, exactAmount, verification)
		exactAmount = verification
	}

	log.Printf("[%s] Retry - Price: %.*f, Quantity: %.0f, Amount: %.8f", req.AccountID, req.PricePrecision, retryBuyPrice, tokenAmount, exactAmount)

	// 检查重试计算后的金额是否为0
	if exactAmount <= 0 || tokenAmount <= 0 {
		log.Printf("⚠️ [%s] 重试计算后金额为0，跳过下单 - 金额: %.8f, 数量: %.0f", req.AccountID, exactAmount, tokenAmount)
		return TradeResponse{
			Success: false,
			Message: "重试计算后金额为0，无法下单",
		}
	}

	// 2.5. 重试时也检查资金账户余额
	fundingBalance, err := getFundingAccountBalance(req.Csrftoken, req.Cookie)
	if err != nil {
		log.Printf("⚠️ [%s] 重试查询资金账户余额失败: %v，继续使用原金额", req.AccountID, err)
	} else {
		log.Printf("💰 [%s] 重试资金账户余额: %.8f, 需要金额: %.8f", req.AccountID, fundingBalance, exactAmount)

		if fundingBalance < exactAmount {

			// 检查余额是否太少（小于1 USDT）
			if fundingBalance < 1.0 {

				// 检查是否有代币余额或锁定订单
				tokenBalance, tokenErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
				if tokenErr == nil && tokenBalance != nil {
					freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
					if freeAmount > 0 {
						log.Printf("🔍 [%s] 重试时发现未卖出代币余额: %.6f，可能需要先卖出", req.AccountID, freeAmount)
					}
				}

				return TradeResponse{
					Success: false,
					Message: "重试时资金账户余额过少，可能有代币未卖出或卖单锁定",
				}
			}

			// 使用实际余额的95%，留一点余量
			adjustedAmount := fundingBalance * 0.95
			adjustedTokenAmount := math.Floor(adjustedAmount / retryBuyPrice)
			adjustedExactAmount := retryBuyPrice * adjustedTokenAmount
			adjustedExactAmount = math.Round(adjustedExactAmount*100000000) / 100000000

			if adjustedTokenAmount >= 1 && adjustedExactAmount > 0 {
				log.Printf("🔧 [%s] 重试余额调整: %.8f → %.8f, 数量: %.0f → %.0f",
					req.AccountID, exactAmount, adjustedExactAmount, tokenAmount, adjustedTokenAmount)
				exactAmount = adjustedExactAmount
				tokenAmount = adjustedTokenAmount
			} else {

				return TradeResponse{
					Success: false,
					Message: "重试调整后金额仍不足，无法下单",
				}
			}
		}
	}

	// 3. 重新下买单（使用加价后的买入价格）
	retryBuyTime := time.Now().UnixMilli() // 在下单前记录时间

	buyOrderID, buySuccess := placeOrderWithID(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "BUY",
		Price:      retryBuyPrice, // 使用加万二后的价格
		Quantity:   tokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            exactAmount,
			AmountStr:         fmt.Sprintf("%.8f", exactAmount),
			PaymentWalletType: "CARD",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	})

	if !buySuccess {
		return TradeResponse{Success: false, Message: "Retry buy order failed"}
	}

	log.Printf("📤 [%s] Retry buy order placed: %s", req.AccountID, buyOrderID)

	// 4. 等待买入确认
	buyConfirmed := false

	// 🔥 刷量优化：重试买单也使用极速确认
	for i := 0; i < 5; i++ { // 1秒，每200ms检查一次
		time.Sleep(200 * time.Millisecond)

		if checkBuyOrderConfirmed(buyOrderID, retryBuyTime, req.Csrftoken, req.Cookie) {
			buyConfirmed = true
			log.Printf("✅ [%s] Retry buy order confirmed: %s", req.AccountID, buyOrderID)
			break
		}
	}

	// 5. 如果重试买单也超时，取消订单并考虑进一步重试
	if !buyConfirmed {

		// 重试取消订单
		cancelSuccess := false
		for attempt := 1; attempt <= 3; attempt++ {
			if cancelOrder(buyOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
				log.Printf("🗑️ [%s] Retry buy order canceled", req.AccountID)
				cancelSuccess = true
				break
			} else if attempt < 3 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		if !cancelSuccess {
			// 单个订单取消失败，尝试取消所有订单
			log.Printf("⚠️ [%s] 重试买单取消失败，尝试取消所有订单", req.AccountID)
			if cancelAllOrders(req.Csrftoken, req.Cookie) {
				log.Printf("✅ [%s] 所有订单取消成功", req.AccountID)
			} else {
				log.Printf("❌ [%s] 取消所有订单也失败", req.AccountID)
			}
		}

		// 🔥 刷量优化：买单超时快速重试
		if retryAttempt < maxBuyRetries {
			var buyRetryWait time.Duration
			if isVolumeMode {
				buyRetryWait = 3 * time.Second // 刷量模式：3秒快速重试
			} else {
				buyRetryWait = 10 * time.Second // 正常模式：10秒
			}
			log.Printf("🔄 [%s] 买单超时，等待%v后进行下一次重试 (%d/%d)", req.AccountID, buyRetryWait, retryAttempt, maxBuyRetries)
			time.Sleep(buyRetryWait)
			return retryBuyWithNewPriceAttempt(req, startTime, retryAttempt+1)
		}

		// 所有重试都失败
		log.Printf("❌ [%s] 买单重试%d次均失败，停止交易", req.AccountID, maxBuyRetries)
		return TradeResponse{Success: false, Message: fmt.Sprintf("买单重试%d次均失败，已取消订单", maxBuyRetries)}
	}

	// 6. 买入成功，统计交易额并继续卖出流程
	log.Printf("✅ [%s] Retry buy successful, proceeding to sell", req.AccountID)

	// 🔧 修复：重试买单成功后需要统计交易额（使用防重复机制）
	buyInCost := retryActualUSDTUsed // 🎲 使用重试时实际花费的USDT金额

	// 🔧 新增：重试买单成功后立即统计买入交易额（防重复）
	if buyInCost > 0 && tokenAmount > 0 {
		tradeID := generateTradeID(req.AccountID, req.TokenAddress, newPrice, tokenAmount)
		updateAccountStatsWithID(req.AccountID, buyInCost, 0, tradeID)
		log.Printf("📊 [%s] 重试买单成功立即统计 - 买单金额: %.6f USDT (ID:%s)",
			req.AccountID, buyInCost, tradeID[:8])
	}

	// 先执行卖出流程
	sellResult := smartSellWithRetry(req, newPrice, tokenAmount, startTime)

	// 🔧 修复：卖出完成后只更新损益，不重复统计买入交易额
	if buyInCost > 0 && tokenAmount > 0 && sellResult.Success && sellResult.SellPrice > 0 {
		sellOutRevenue := sellResult.SellPrice * tokenAmount
		netLoss := buyInCost - sellOutRevenue

		// 生成相同的交易ID，只更新损益
		tradeID := generateTradeID(req.AccountID, req.TokenAddress, newPrice, tokenAmount)
		updateAccountLossOnly(req.AccountID, netLoss, tradeID)

		log.Printf("📊 [%s] 重试交易损益更新 - 净损益: %.6f USDT",
			req.AccountID, netLoss)
	}

	return sellResult
}

// 🛡️ 币安安全下单频率检查
func checkOrderRateLimit() bool {
	currentSecond := time.Now().Unix()

	counterMutex.Lock()
	defer counterMutex.Unlock()

	if orderCounter[currentSecond] >= maxOrdersPerSecond {
		// 当前秒已达到下单限制
		return false
	}

	// 增加当前秒的下单计数
	orderCounter[currentSecond]++

	// 清理旧的计数记录
	for timestamp := range orderCounter {
		if currentSecond-timestamp > 3 {
			delete(orderCounter, timestamp)
		}
	}

	return true
}

// placeOrderWithRetry 下单带重试机制（增强版+币安安全）
func placeOrderWithRetry(order OrderRequest, accountID string) (string, bool) {
	// 🛡️ 币安安全：检查下单频率限制
	if !checkOrderRateLimit() {
		log.Printf("🛡️ [%s] 下单频率达到限制(%d/秒)，等待1秒", accountID, maxOrdersPerSecond)
		time.Sleep(1 * time.Second)
		// 等待后再次检查
		if !checkOrderRateLimit() {
			log.Printf("⚠️ [%s] 下单频率仍然受限，跳过本次下单", accountID)
			return "", false
		}
	}

	maxRetries := maxOrderRetryAttempts // 🔧 使用全局配置参数

	for attempt := 1; attempt <= maxRetries; attempt++ {

		orderID, success := placeOrderWithID(order)

		if success {
			log.Printf("✅ [%s] Order successful on attempt %d: %s", accountID, attempt, orderID)
			return orderID, true
		}

		// 检查是否是余额不足，如果是则停止重试
		if orderID == "INSUFFICIENT_BALANCE" {
			return "", false
		}
		
		// 检查是否是金额太小，如果是则标记为卖出成功并继续
		if orderID == "AMOUNT_TOO_SMALL" {
			log.Printf("✅ [%s] 订单金额太小，视为卖出成功", accountID)
			return "AMOUNT_TOO_SMALL_SUCCESS", true
		}

		// 检查是否是认证失效，如果是则停止重试
		if orderID == "AUTH_FAILED" {
			log.Printf("🚨 [%s] 认证失效，停止重试", accountID)
			return "", false
		}

		// 如果不是最后一次尝试，等待后重试
		if attempt < maxRetries {

			time.Sleep(200 * time.Millisecond)
		}
	}

	return "", false
}

// placeOrderWithLimitedRetry 下单带限制重试机制（用于强制清空）
func placeOrderWithLimitedRetry(order OrderRequest, accountID string, maxRetries int) (string, bool) {
	retryDelay := 100 * time.Millisecond // 减少到100ms

	for attempt := 1; attempt <= maxRetries; attempt++ {

		orderID, success := placeOrderWithID(order)

		if success {
			log.Printf("✅ [%s] Order successful on attempt %d: %s", accountID, attempt, orderID)
			return orderID, true
		}

		// 检查是否是余额不足，如果是则停止重试
		if orderID == "INSUFFICIENT_BALANCE" {
			return "", false
		}
		
		// 检查是否是金额太小，如果是则标记为卖出成功并继续
		if orderID == "AMOUNT_TOO_SMALL" {
			log.Printf("✅ [%s] 订单金额太小，视为卖出成功", accountID)
			return "AMOUNT_TOO_SMALL_SUCCESS", true
		}

		// 如果不是最后一次尝试，等待后重试
		if attempt < maxRetries {

			time.Sleep(retryDelay)
		}
	}

	return "", false
}

// adjustPricePrecision 根据指定精度调整价格（智能精度）
func adjustPricePrecision(price float64, precision int) float64 {
	if precision <= 0 {
		precision = 8 // 默认8位
	}

	// 对于极小的价格，确保不会被截断为0
	if price > 0 && price < 1e-10 {
		// 对于极小价格，使用更高精度
		precision = max(precision, 15)
	}

	multiplier := float64(1)
	for i := 0; i < precision; i++ {
		multiplier *= 10
	}

	adjusted := float64(int(price*multiplier)) / multiplier

	// 确保调整后的价格不为0（除非原价格就是0）
	if price > 0 && adjusted == 0 {
		// 如果调整后变成0，使用最小可能值
		adjusted = 1.0 / multiplier
	}

	return adjusted
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// sellAtMarketPrice 按市场价卖出
func sellAtMarketPrice(req *TradeRequest, buyPrice, sellTokenAmount, marketPrice float64, startTime time.Time) TradeResponse {
	log.Printf("💰 [%s] 按市场价卖出: %.12f", req.AccountID, marketPrice)
	
	// 🚨 修改：提高最低价值阈值，避免币安拒绝下单
	tokenValue := marketPrice * sellTokenAmount
	if tokenValue < 1.0 { // 如果代币总价值低于1.0 USDT，则中断卖出
		log.Printf("💸 [%s] 代币价值过低(%.8f USDT < 1.0 USDT)，币安会拒绝下单，跳过卖出操作", 
			req.AccountID, tokenValue)
		log.Printf("📊 [%s] 代币详情: 数量=%.6f, 价格=%.8f", req.AccountID, sellTokenAmount, marketPrice)
		// 更新最后交易时间，确保系统可以继续其他操作
		updateLastTradeTime(req.AccountID)
		return TradeResponse{
			Success:     true, // 标记为成功，避免系统继续尝试卖出
			Message:     "代币价值过低(< 1.0 USDT)，币安会拒绝下单，跳过卖出操作",
			BuyPrice:    buyPrice,
			SellPrice:   marketPrice,
			TokenAmount: sellTokenAmount,
			Profit:      0,
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}
	}

	// 按币安规定调整代币数量
	adjustedTokenAmount := adjustTokenAmountForBinance(marketPrice, sellTokenAmount, req.PricePrecision)
	if adjustedTokenAmount != sellTokenAmount {
		log.Printf("🔢 [%s] 市场价卖出数量调整: %.0f → %.0f", req.AccountID, sellTokenAmount, adjustedTokenAmount)
		sellTokenAmount = adjustedTokenAmount
	}

	// 验证市场价卖出的精度
	expectedValue := marketPrice * sellTokenAmount
	expectedValue = math.Round(expectedValue*100000000) / 100000000
	log.Printf("🔍 [%s] 市场价验证: 价格 %.12f × 数量 %.0f = %.8f USDT",
		req.AccountID, marketPrice, sellTokenAmount, expectedValue)
		
	// 🔧 修复：在下单前检查代币余额是否被锁定
	tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
	if balErr == nil {
		freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
		lockedAmount, _ := strconv.ParseFloat(tokenBalance.Locked, 64)
		
		if freeAmount <= 0 && lockedAmount > 0 {
			log.Printf("⏳ [%s] 代币余额已锁定，等待1秒后再尝试", req.AccountID)
			// 等待1秒，让币安释放锁定的代币
			time.Sleep(1 * time.Second)
			
			// 重新检查余额
			tokenBalance, balErr = getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if balErr == nil {
				freeAmount, _ = strconv.ParseFloat(tokenBalance.Free, 64)
				lockedAmount, _ = strconv.ParseFloat(tokenBalance.Locked, 64)
				log.Printf("📊 [%s] 等待后代币余额: 可用=%.6f, 锁定=%.6f", 
					req.AccountID, freeAmount, lockedAmount)
				
				if freeAmount <= 0 {
					if lockedAmount > 0 {
						log.Printf("⚠️ [%s] 代币仍然被锁定，可能有其他订单正在处理", req.AccountID)
					} else {
						log.Printf("✅ [%s] 代币余额为0，可能已被卖出", req.AccountID)
						// 设置代币已卖出状态
						setTokenSold(req.AccountID, req.TokenAddress, "unknown", marketPrice, sellTokenAmount)
					}
					
					return TradeResponse{
						Success:     true,
						Message:     "代币余额不可用，跳过市场价卖出",
						BuyPrice:    buyPrice,
						SellPrice:   marketPrice,
						TokenAmount: sellTokenAmount,
						Profit:      (marketPrice - buyPrice) * sellTokenAmount,
						ExecuteTime: time.Since(startTime).Milliseconds(),
					}
				} else if freeAmount < sellTokenAmount {
					log.Printf("📊 [%s] 市场价卖出更新数量: %.6f → %.6f (使用当前可用余额)", 
						req.AccountID, sellTokenAmount, freeAmount)
					sellTokenAmount = freeAmount
				}
			}
		}
	}

	// 确保使用整数数量，避免精度问题
	intAmount := math.Floor(sellTokenAmount) // 确保是整数
	
	// 统一使用整数格式的字符串，避免精度问题
	strAmount := fmt.Sprintf("%.0f", intAmount)
	
	// 将字符串转回为整数，确保完全一致
	exactAmount, _ := strconv.ParseInt(strAmount, 10, 64)
	
	// 使用整数作为数量，避免浮点数精度问题
	log.Printf("🔢 [%s] 市场价卖出统一使用整数数量: %d", req.AccountID, exactAmount)
	
	// 构造请求体JSON字符串，确保数量字段完全一致
	orderJSON := fmt.Sprintf(`{
		"baseAsset": "%s",
		"quoteAsset": "USDT",
		"side": "SELL",
		"price": %.8f,
		"quantity": %s,
		"paymentDetails": [
			{
				"amount": "%s",
				"paymentWalletType": "ALPHA"
			}
		]
	}`, req.BaseAsset, marketPrice, strAmount, strAmount)
	
	// 解析为OrderRequest结构体
	var orderReq OrderRequest
	if err := json.Unmarshal([]byte(orderJSON), &orderReq); err != nil {
		log.Printf("❌ [%s] 解析订单JSON失败: %v", req.AccountID, err)
		// 使用备用方法构造请求
		orderReq = OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      marketPrice,
			Quantity:   float64(exactAmount),
			PaymentDetails: []PaymentDetail{{
				Amount:            float64(exactAmount),
				AmountStr:         strAmount,
				PaymentWalletType: "ALPHA",
			}},
		}
	}
	
	// 添加认证信息
	orderReq.Csrftoken = req.Csrftoken
	orderReq.Cookie = req.Cookie
	
	// 打印最终请求对象，确认字段一致性
	log.Printf("📝 [%s] 最终订单对象 - Quantity: %v, PaymentDetails.Amount: %v, PaymentDetails.AmountStr: %s", 
		req.AccountID, orderReq.Quantity, 
		orderReq.PaymentDetails[0].Amount, 
		orderReq.PaymentDetails[0].AmountStr)
	
	sellOrderID, sellSuccess := placeOrderWithRetry(orderReq, req.AccountID)

	if !sellSuccess {
		// 首先检查代币是否已被其他流程卖出
		if isTokenSold(req.AccountID, req.TokenAddress) {
			status := getTokenSellStatus(req.AccountID, req.TokenAddress)
			log.Printf("✅ [%s] 市场价卖出失败，但代币已被其他流程卖出 - 方式: %s, 价格: %.8f, 数量: %.6f", 
				req.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
			
			// 返回成功响应，避免继续尝试卖出
			return TradeResponse{
				Success:     true,
				Message:     fmt.Sprintf("代币已被%s方式卖出，无需市场价卖出", status.SoldBy),
				BuyPrice:    buyPrice,
				SellPrice:   status.SoldPrice,
				TokenAmount: status.SoldAmount,
				Profit:      (status.SoldPrice - buyPrice) * status.SoldAmount,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		}
		
		// 检查余额不足错误
		if sellOrderID == "INSUFFICIENT_BALANCE" {
			// 检查代币余额
			tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if balErr == nil {
				freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
				if freeAmount <= 0 {
					log.Printf("✅ [%s] 市场价卖出余额不足，代币余额为0，可能已被卖出", req.AccountID)
					// 设置代币已卖出状态，但卖出方式未知
					setTokenSold(req.AccountID, req.TokenAddress, "unknown", marketPrice, sellTokenAmount)
					return TradeResponse{
						Success:     true,
						Message:     "代币余额为0，无需市场价卖出",
						BuyPrice:    buyPrice,
						SellPrice:   marketPrice,
						TokenAmount: sellTokenAmount,
						Profit:      (marketPrice - buyPrice) * sellTokenAmount,
						ExecuteTime: time.Since(startTime).Milliseconds(),
					}
				} else if freeAmount < sellTokenAmount {
					log.Printf("📊 [%s] 市场价卖出更新数量: %.6f → %.6f (使用当前可用余额)", 
						req.AccountID, sellTokenAmount, freeAmount)
					// 使用可用余额重试
					return sellAtMarketPrice(req, buyPrice, freeAmount, marketPrice, startTime)
				} else {
					log.Printf("⚠️ [%s] 市场价卖出精度问题，减少代币数量重试", req.AccountID)
					// 减少代币数量重试
					reducedAmount := math.Floor(sellTokenAmount * 0.99)
					if reducedAmount >= 1 {
						return sellAtMarketPrice(req, buyPrice, reducedAmount, marketPrice, startTime)
					}
				}
			}
		}
		
		// 其他错误，返回失败
		log.Printf("❌ [%s] 市场价卖出下单失败: %s", req.AccountID, sellOrderID)
		return TradeResponse{Success: false, Message: "市场价卖出下单失败"}
	}

	// 等待成交确认
	sellTime := time.Now().UnixMilli()
	for i := 0; i < 5; i++ { // 1.5秒，每200ms检查一次
		time.Sleep(200 * time.Millisecond)
		if checkSellOrderHistory(sellTime, req.Csrftoken, req.Cookie) {
			log.Printf("✅ [%s] 市场价卖出成功", req.AccountID)
			profit := (marketPrice - buyPrice) * sellTokenAmount
			executeTime := time.Since(startTime).Milliseconds()

			return TradeResponse{
				Success:     true,
				Message:     "市场价卖出成功",
				BuyPrice:    buyPrice,
				SellPrice:   marketPrice,
				TokenAmount: sellTokenAmount,
				Profit:      profit,
				ExecuteTime: executeTime,
			}
		}
	}

	// 市场价卖出超时，记录日志
	log.Printf("🚨 [%s] 市场价卖出超时，将由其他兜底机制处理", req.AccountID)
	// 更新最后交易时间，确保系统可以继续其他操作
	updateLastTradeTime(req.AccountID)

	// 返回特殊状态，表示已由其他兜底机制处理
	marketLoss := (buyPrice - marketPrice) * sellTokenAmount
	return TradeResponse{
		Success:     true, // 标记为成功，因为已由其他兜底机制处理
		Message:     "市场价超时，由其他兜底机制处理",
		BuyPrice:    buyPrice,
		SellPrice:   marketPrice,
		TokenAmount: sellTokenAmount,
		Profit:      -marketLoss,
		ExecuteTime: time.Since(startTime).Milliseconds(),
	}
}

// hangOrderAsync 异步挂单处理
func hangOrderAsync(req *TradeRequest, buyPrice, sellTokenAmount, hangPrice float64, startTime time.Time) TradeResponse {
	log.Printf("📌 [%s] 挂百10价格: %.12f", req.AccountID, hangPrice)
	
	// 🚨 修改：提高最低价值阈值，避免币安拒绝下单
	tokenValue := hangPrice * sellTokenAmount
	if tokenValue < 1.0 { // 如果代币总价值低于1.0 USDT，则中断卖出
		log.Printf("💸 [%s] 代币价值过低(%.8f USDT < 1.0 USDT)，币安会拒绝下单，跳过挂单操作", 
			req.AccountID, tokenValue)
		log.Printf("📊 [%s] 代币详情: 数量=%.6f, 价格=%.8f", req.AccountID, sellTokenAmount, hangPrice)
		// 更新最后交易时间，确保系统可以继续其他操作
		updateLastTradeTime(req.AccountID)
		return TradeResponse{
			Success:     true, // 标记为成功，避免系统继续尝试卖出
			Message:     "代币价值过低(< 1.0 USDT)，币安会拒绝下单，跳过挂单操作",
			BuyPrice:    buyPrice,
			SellPrice:   hangPrice,
			TokenAmount: sellTokenAmount,
			Profit:      0,
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}
	}

	sellOrderID, sellSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      hangPrice,
		Quantity:   sellTokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellTokenAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

	if !sellSuccess {
		// 检查是否是"余额不足"错误
		if sellOrderID == "INSUFFICIENT_BALANCE" {
			// 检查代币是否已被其他流程卖出
			if isTokenSold(req.AccountID, req.TokenAddress) {
				status := getTokenSellStatus(req.AccountID, req.TokenAddress)
				log.Printf("✅ [%s] 挂单余额不足，但代币已被其他流程卖出 - 方式: %s, 价格: %.8f, 数量: %.6f", 
					req.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
				
				// 返回成功响应，避免继续尝试卖出
				return TradeResponse{
					Success:     true,
					Message:     fmt.Sprintf("代币已被%s方式卖出，无需挂单", status.SoldBy),
					BuyPrice:    buyPrice,
					SellPrice:   status.SoldPrice,
					TokenAmount: status.SoldAmount,
					Profit:      (status.SoldPrice - buyPrice) * status.SoldAmount,
					ExecuteTime: time.Since(startTime).Milliseconds(),
				}
			}
			
			// 检查代币余额
			tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
			if balErr == nil {
				freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
				lockedAmount, _ := strconv.ParseFloat(tokenBalance.Locked, 64)
				
				log.Printf("📊 [%s] 挂单余额不足，当前余额: 可用=%.6f, 锁定=%.6f", 
					req.AccountID, freeAmount, lockedAmount)
				
				if freeAmount <= 0 && lockedAmount > 0 {
					log.Printf("✅ [%s] 代币已全部锁定，可能已在挂单中", req.AccountID)
					// 设置代币已卖出状态
					setTokenSold(req.AccountID, req.TokenAddress, "pending", hangPrice, sellTokenAmount)
					return TradeResponse{
						Success:     true,
						Message:     "代币已全部锁定，可能已在挂单中",
						BuyPrice:    buyPrice,
						SellPrice:   hangPrice,
						TokenAmount: sellTokenAmount,
						Profit:      (hangPrice - buyPrice) * sellTokenAmount,
						ExecuteTime: time.Since(startTime).Milliseconds(),
					}
				}
				
				if freeAmount <= 0 && lockedAmount <= 0 {
					log.Printf("✅ [%s] 代币余额为0，可能已被卖出", req.AccountID)
					// 设置代币已卖出状态
					setTokenSold(req.AccountID, req.TokenAddress, "unknown", hangPrice, sellTokenAmount)
					return TradeResponse{
						Success:     true,
						Message:     "代币余额为0，可能已被卖出",
						BuyPrice:    buyPrice,
						SellPrice:   hangPrice,
						TokenAmount: sellTokenAmount,
						Profit:      (hangPrice - buyPrice) * sellTokenAmount,
						ExecuteTime: time.Since(startTime).Milliseconds(),
					}
				}
				
				if freeAmount < sellTokenAmount {
					log.Printf("📊 [%s] 挂单更新数量: %.6f → %.6f (使用当前可用余额)", 
						req.AccountID, sellTokenAmount, freeAmount)
					// 使用可用余额重试
					return hangOrderAsync(req, buyPrice, freeAmount, hangPrice, startTime)
				}
			}
		}
		
		// 检查是否是"incorrect price or amount"错误
		if strings.Contains(sellOrderID, "incorrect price or amount") {
			log.Printf("⚠️ [%s] 币安拒绝挂单(价格或数量不正确)，尝试使用forceCleanToken方法", req.AccountID)
			
			// 使用forceCleanToken方法，该方法更适合处理小额代币
			go func() {
				if err := forceCleanToken(req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, 8); err != nil {
					log.Printf("⚠️ [%s] forceCleanToken失败: %v", req.AccountID, err)
				} else {
					log.Printf("✅ [%s] forceCleanToken成功清理代币", req.AccountID)
					// 设置代币已卖出状态
					setTokenSold(req.AccountID, req.TokenAddress, "clean", hangPrice, sellTokenAmount)
				}
			}()
			
			return TradeResponse{
				Success:     true,
				Message:     "使用forceCleanToken方法处理代币",
				BuyPrice:    buyPrice,
				SellPrice:   hangPrice,
				TokenAmount: sellTokenAmount,
				Profit:      0,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		}
		
		return TradeResponse{Success: false, Message: "挂单失败: " + sellOrderID}
	}

	// 启动异步监控
	sellTime := time.Now().UnixMilli()
	go monitorHangingOrder(req, sellOrderID, buyPrice, sellTokenAmount, hangPrice, sellTime, req.USDTAmount)

	// 立即返回，继续新的买卖循环
	log.Printf("📌 [%s] 百10挂单已启动异步监控，继续新交易", req.AccountID)
	profit := (hangPrice - buyPrice) * sellTokenAmount
	executeTime := time.Since(startTime).Milliseconds()

	return TradeResponse{
		Success:     true,
		Message:     "百10挂单异步监控",
		BuyPrice:    buyPrice,
		SellPrice:   hangPrice,
		TokenAmount: sellTokenAmount,
		Profit:      profit,
		ExecuteTime: executeTime,
	}
}

// monitorHangingOrder 异步监控挂单（2分钟优化）
func monitorHangingOrder(req *TradeRequest, sellOrderID string, buyPrice, sellTokenAmount, hangPrice float64, sellTime int64, originalUSDTAmount float64) {
	log.Printf("🔍 [%s] 开始异步监控挂单: %s (1分钟超时)", req.AccountID, sellOrderID)

	// 设置挂单监控状态
	setAccountAsyncState(req.AccountID, "hanging", true)
	defer setAccountAsyncState(req.AccountID, "hanging", false)

	// 🔧 优化：添加总超时保护（从4分钟缩短到2.5分钟）
	timeout := time.After(60 * time.Second) // 1分钟绝对超时，与循环次数保持一致
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// 🔥 刷量优化：监控1分钟（快速处理异步挂单）
	for i := 0; i < 60; i++ { // 1分钟，每1秒检查一次
		select {
		case <-timeout:
			log.Printf("🚨 [%s] 异步监控绝对超时(1分钟)，强制处理: %s", req.AccountID, sellOrderID)
				// 强制取消订单，由其他兜底机制处理
	cancelOrder(sellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie)
	log.Printf("🔄 [%s] 挂单监控超时，取消订单后由其他兜底机制处理", req.AccountID)
	// 更新最后交易时间，确保系统可以继续其他操作
	updateLastTradeTime(req.AccountID)
			return
		case <-ticker.C:
			// 正常的1秒检查
			if checkSellOrderHistory(sellTime, req.Csrftoken, req.Cookie) {
				log.Printf("✅ [%s] 挂单异步成交: %s, 价格: %.12f", req.AccountID, sellOrderID, hangPrice)

				// 异步卖单完成，需要统计买入交易额
				buyInCost := buyPrice * sellTokenAmount
				sellOutRevenue := hangPrice * sellTokenAmount
				netLoss := buyInCost - sellOutRevenue // 净损益
				updateLastTradeTime(req.AccountID)

				// 🔧 修正：异步挂单成交需要统计买入交易额
				actualBuyVolume := buyInCost // 真实的买单金额

				tradeID := generateTradeID(req.AccountID, req.TokenAddress, buyPrice, sellTokenAmount)
				updateAccountStatsWithID(req.AccountID, actualBuyVolume, netLoss, tradeID)

				if netLoss > 0 {
					log.Printf("📊 [%s] 异步挂单成交统计: 买单金额=%.6f USDT, 亏损=%.6f USDT",
						req.AccountID, actualBuyVolume, netLoss)
				} else {
					log.Printf("📊 [%s] 异步挂单成交统计: 买单金额=%.6f USDT, 盈利=%.6f USDT",
						req.AccountID, actualBuyVolume, -netLoss)
				}
				return
			}
		}
	}

	// 🔥 刷量优化：1分钟未成交，取消订单并按市场价卖出
	log.Printf("⏰ [%s] 挂单1分钟未成交，取消并按市场价卖出: %s", req.AccountID, sellOrderID)

	// 取消挂单
	cancelSuccess := false
	for attempt := 1; attempt <= 3; attempt++ {
		if cancelOrder(sellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
			log.Printf("🗑️ [%s] 挂单取消成功: %s", req.AccountID, sellOrderID)
			cancelSuccess = true
			break
		} else if attempt < 3 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if !cancelSuccess {
		log.Printf("⚠️ [%s] 挂单取消失败，尝试取消所有订单: %s", req.AccountID, sellOrderID)
		// 尝试取消所有订单作为兜底
		cancelAllOrders(req.Csrftoken, req.Cookie)
		time.Sleep(1 * time.Second) // 增加到1秒，确保取消完成
	}

	// 🔧 修复：增加取消订单后的缓冲时间，避免竞态条件
	time.Sleep(800 * time.Millisecond) // 等待订单状态同步

	// 获取最新市场价并卖出
	latestMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {

		return
	}
	latestMarketPrice = adjustPricePrecision(latestMarketPrice, req.PricePrecision)

	log.Printf("💰 [%s] 异步按市场价卖出: %.12f", req.AccountID, latestMarketPrice)

	// 按市场价下单
	marketSellOrderID, marketSellSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      latestMarketPrice,
		Quantity:   sellTokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellTokenAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

	if marketSellSuccess {
		log.Printf("✅ [%s] 异步市场价卖出下单成功: %s，等待成交确认", req.AccountID, marketSellOrderID)

		// 等待1.5秒检查成交
		marketSellTime := time.Now().UnixMilli()
		marketSellConfirmed := false
		for i := 0; i < 5; i++ { // 1秒，每200ms检查一次
			time.Sleep(200 * time.Millisecond)
			if checkSellOrderHistory(marketSellTime, req.Csrftoken, req.Cookie) {
				log.Printf("✅ [%s] 异步市场价卖出成交确认", req.AccountID)
				marketSellConfirmed = true

				// 异步市场价卖单完成，只更新最后交易时间，不计算交易额和交易次数
				buyInCost := buyPrice * sellTokenAmount
				sellOutRevenue := latestMarketPrice * sellTokenAmount
				netLoss := buyInCost - sellOutRevenue
				updateLastTradeTime(req.AccountID)

				if netLoss > 0 {
					log.Printf("📊 [%s] 异步市场价卖出: 买入成本=%.6f, 卖出收入=%.6f, 亏损=%.6f",
						req.AccountID, buyInCost, sellOutRevenue, netLoss)
				} else {
					log.Printf("📊 [%s] 异步市场价卖出: 买入成本=%.6f, 卖出收入=%.6f, 盈利=%.6f",
						req.AccountID, buyInCost, sellOutRevenue, -netLoss)
				}

				break
			}
		}

		if !marketSellConfirmed {
			log.Printf("⏰ [%s] 异步市场价卖出1.5秒未成交，启动异步递减万1", req.AccountID)
			// 取消市场价订单
			for attempt := 1; attempt <= 3; attempt++ {
				if cancelOrder(marketSellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
					log.Printf("✅ [%s] 异步市场价订单取消成功 (尝试%d次)", req.AccountID, attempt)
					break
				} else {
					if attempt < 3 {
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
			// 启动异步递减万1策略
			go asyncFastDecrement(req, buyPrice, sellTokenAmount, latestMarketPrice)
		}
	} else {
		log.Printf("❌ [%s] 异步市场价卖出下单失败，启动异步递减万1强制卖出", req.AccountID)
		// 异步递减万1策略，确保代币能够卖出
		go asyncFastDecrement(req, buyPrice, sellTokenAmount, latestMarketPrice)
	}
}

// asyncFastDecrement 异步快速递减万1策略，确保代币能够卖出
func asyncFastDecrement(req *TradeRequest, buyPrice, sellTokenAmount, startPrice float64) {
	log.Printf("🔄 [%s] 异步开始快速递减策略，确保代币卖出", req.AccountID)
	
	// 先尝试重置账户所有异步状态，确保没有残留的状态
	resetAccountAsyncState(req.AccountID)

	// 🚨 修改：提高最低价值阈值，避免币安拒绝下单
	tokenValue := startPrice * sellTokenAmount
	if tokenValue < 1.0 { // 如果代币总价值低于1.0 USDT，则中断卖出
		log.Printf("💸 [%s] 代币价值过低(%.8f USDT < 1.0 USDT)，币安会拒绝下单，跳过异步卖出", 
			req.AccountID, tokenValue)
		log.Printf("📊 [%s] 代币详情: 数量=%.6f, 价格=%.8f", req.AccountID, sellTokenAmount, startPrice)
		// 更新最后交易时间，确保系统可以继续其他操作
		updateLastTradeTime(req.AccountID)
		return // 直接返回，不执行后续卖出操作
	}

	// 设置异步递减状态
	setAccountAsyncState(req.AccountID, "decrement", true)
	defer setAccountAsyncState(req.AccountID, "decrement", false)

	// 异步递减策略：4步递减
	decrementSteps := []float64{
		0.0002, // 万2（第1步）
		0.001,  // 千1（第2步）
		0.005,  // 千5（第3步）
		0.1,    // 百10（第4步，最终限制）
	}

	for step, decrementRate := range decrementSteps {
		// 按递减率计算价格
		currentSellPrice := startPrice * (1.0 - decrementRate)
		currentSellPrice = adjustPricePrecision(currentSellPrice, req.PricePrecision)

		// 计算当前磨损
		currentLoss := (buyPrice - currentSellPrice) / buyPrice

		// 计算递减幅度的万分比表示
		decrementWanFen := decrementRate * 10000
		lossWanFen := currentLoss * 10000

		if currentLoss < 0 {
			profitWanFen := -lossWanFen
			log.Printf("💰 [%s] 异步递减第%d步(%.0f万分): %.12f, 利润: %.2f万分",
				req.AccountID, step+1, decrementWanFen, currentSellPrice, profitWanFen)
		} else {
			log.Printf("💰 [%s] 异步递减第%d步(%.0f万分): %.12f, 磨损: %.2f万分",
				req.AccountID, step+1, decrementWanFen, currentSellPrice, lossWanFen)
		}

		// 下单
			// 确保使用整数数量，避免精度问题
	intAmount := math.Floor(sellTokenAmount) // 确保是整数
	
	// 统一使用整数格式的字符串，避免精度问题
	strAmount := fmt.Sprintf("%.0f", intAmount)
	
	// 将字符串转回为整数，确保完全一致
	exactAmount, _ := strconv.ParseInt(strAmount, 10, 64)
	
	// 使用整数作为数量，避免浮点数精度问题
	log.Printf("🔢 [%s] 递减重试统一使用整数数量: %d", req.AccountID, exactAmount)
	
	newSellOrderID, newSellSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      currentSellPrice,
		Quantity:   float64(exactAmount), // 使用从整数转换的浮点数
		PaymentDetails: []PaymentDetail{{
			Amount:            float64(exactAmount), // 使用完全相同的值
			AmountStr:         strAmount,            // 使用整数格式字符串
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

		if !newSellSuccess {
			// 如果是余额不足，视为卖出成功并退出循环
			if newSellOrderID == "INSUFFICIENT_BALANCE" {
				log.Printf("✅ [%s] 检测到余额不足错误，代币可能已被其他操作卖出，视为成功", req.AccountID)
				
				// 设置代币已卖出状态
				setTokenSold(req.AccountID, req.TokenAddress, "auto_sold", currentSellPrice, sellTokenAmount)
				
				// 更新最后交易时间
				updateLastTradeTime(req.AccountID)
				
				// 退出整个递减循环
				return
			} else {
				continue
			}
		}

		// 🔥 刷量优化：异步递减也使用快速检查
		newSellTime := time.Now().UnixMilli()
		sellConfirmed := false
		var asyncWaitTime int
		if isVolumeMode {
			asyncWaitTime = 3 // 刷量模式：600ms快速检查
		} else {
			asyncWaitTime = 5 // 正常模式：1秒
		}

		for i := 0; i < asyncWaitTime; i++ { // 动态等待时间
			time.Sleep(200 * time.Millisecond)

							if checkSellOrderHistory(newSellTime, req.Csrftoken, req.Cookie) {
					log.Printf("✅ [%s] 异步递减重试成交 (第%d步)", req.AccountID, step+1)

					// 设置代币已卖出状态，供其他流程检查
					setTokenSold(req.AccountID, req.TokenAddress, "async", currentSellPrice, sellTokenAmount)

					// 🔧 修正：异步递减成交需要统计买入交易额
					buyInCost := buyPrice * sellTokenAmount              // 买入成本
					sellOutRevenue := currentSellPrice * sellTokenAmount // 卖出收入
					netLoss := buyInCost - sellOutRevenue                // 净损益

					actualBuyVolume := buyInCost // 真实的买单金额

					// 生成唯一交易ID防止重复统计
					tradeID := generateTradeID(req.AccountID, req.TokenAddress, buyPrice, sellTokenAmount)
					updateAccountStatsWithID(req.AccountID, actualBuyVolume, netLoss, tradeID)

					if netLoss > 0 {
						log.Printf("📊 [%s] 异步递减成交统计: 买单金额=%.6f USDT, 亏损=%.6f USDT",
							req.AccountID, actualBuyVolume, netLoss)
					} else {
						log.Printf("📊 [%s] 异步递减成交统计: 买单金额=%.6f USDT, 盈利=%.6f USDT",
							req.AccountID, actualBuyVolume, -netLoss)
					}

					sellConfirmed = true
					break
				}
		}

		if sellConfirmed {
			return // 成交成功，结束
		}

			// 必须取消当前订单，否则代币被锁定
	log.Printf("⏰ [%s] 异步递减第%d步未成交，取消订单: %s", req.AccountID, step+1, newSellOrderID)

	cancelSuccess := false
	for attempt := 1; attempt <= 3; attempt++ {
		if cancelOrder(newSellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
			log.Printf("✅ [%s] 异步递减订单取消成功 (尝试%d次)", req.AccountID, attempt)
			cancelSuccess = true
			break
		} else {
			if attempt < 3 {
				time.Sleep(200 * time.Millisecond) // 增加重试间隔
			}
		}
	}
	
	// 如果订单取消成功，等待1秒确保系统状态完全更新
	if cancelSuccess {
		log.Printf("⏳ [%s] 异步递减订单已取消，等待1秒确保系统状态更新", req.AccountID)
		time.Sleep(1 * time.Second)
	} else {
		// 取消失败，仍然等待一段时间
		time.Sleep(300 * time.Millisecond)
	}
	}

	// 最终兜底：强制按千1磨损卖出，确保代币不残留
	finalEmergencyPrice := buyPrice * 0.999 // 千1磨损
	finalEmergencyPrice = adjustPricePrecision(finalEmergencyPrice, req.PricePrecision)

	log.Printf("🚨 [%s] 最终兜底：强制千1磨损卖出 %.12f", req.AccountID, finalEmergencyPrice)

	// 调整代币数量以确保能够下单
	finalTokenAmount := adjustTokenAmountForBinance(finalEmergencyPrice, sellTokenAmount, req.PricePrecision)
	if finalTokenAmount < 1 {
		finalTokenAmount = math.Floor(sellTokenAmount * 0.9) // 减少10%
		if finalTokenAmount < 1 {
			finalTokenAmount = 1 // 最少1个代币
		}
	}

	// 最多尝试5次强制卖出
	for attempt := 1; attempt <= 5; attempt++ {

		emergencyOrderID, emergencySuccess := placeOrderWithRetry(OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      finalEmergencyPrice,
			Quantity:   finalTokenAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            finalTokenAmount,
				AmountStr:         fmt.Sprintf("%.6f", finalTokenAmount), // 🔧 修复：保留6位小数
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: req.Csrftoken,
			Cookie:    req.Cookie,
		}, req.AccountID)

		if emergencySuccess {

			// 等待3秒检查成交
			emergencyTime := time.Now().UnixMilli()
			for i := 0; i < 15; i++ { // 3秒，每200ms检查一次
				time.Sleep(200 * time.Millisecond)
				if checkSellOrderHistory(emergencyTime, req.Csrftoken, req.Cookie) {
					log.Printf("✅ [%s] 最终兜底成交成功，代币清理完成", req.AccountID)

					// 记录最终兜底成交信息 (不重复统计)
					usdtAmount := finalEmergencyPrice * finalTokenAmount
					profit := (finalEmergencyPrice - buyPrice) * finalTokenAmount
					log.Printf("📊 [%s] 最终兜底成交: %.0f个代币, %.6f USDT, 利润: %.6f USDT",
						req.AccountID, finalTokenAmount, usdtAmount, profit)

					return // 成功清理代币
				}
			}

			// 如果3秒内未成交，取消订单继续下一次尝试
			for cancelAttempt := 1; cancelAttempt <= 3; cancelAttempt++ {
				if cancelOrder(emergencyOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
					break
				} else {
					if cancelAttempt < 3 {
						time.Sleep(100 * time.Millisecond)
					}
				}
			}

			// 降低价格重试
			finalEmergencyPrice = finalEmergencyPrice * 0.995 // 再降低0.5%
			finalEmergencyPrice = adjustPricePrecision(finalEmergencyPrice, req.PricePrecision)
			log.Printf("📉 [%s] 最终兜底降低价格重试: %.12f", req.AccountID, finalEmergencyPrice)

		} else {

			// 调整数量和价格重试
			finalTokenAmount = math.Floor(finalTokenAmount * 0.95) // 减少5%
			if finalTokenAmount < 1 {
				finalTokenAmount = 1
			}
			finalEmergencyPrice = finalEmergencyPrice * 0.99 // 降低1%
			finalEmergencyPrice = adjustPricePrecision(finalEmergencyPrice, req.PricePrecision)

			log.Printf("🔧 [%s] 最终兜底调整参数: 数量=%.0f, 价格=%.12f", req.AccountID, finalTokenAmount, finalEmergencyPrice)
		}

		time.Sleep(1 * time.Second) // 每次尝试间隔1秒
	}

	log.Printf("🚨 [%s] 最终兜底5次尝试全部失败，代币可能残留，需要人工检查", req.AccountID)
}

// TokenBalance 代币余额结构
type TokenBalance struct {
	ChainID         string `json:"chainId"`
	ContractAddress string `json:"contractAddress"`
	Name            string `json:"name"`
	Symbol          string `json:"symbol"`
	TokenID         string `json:"tokenId"`
	Free            string `json:"free"`
	Freeze          string `json:"freeze"`
	Locked          string `json:"locked"`
	Withdrawing     string `json:"withdrawing"`
	Amount          string `json:"amount"`
	Valuation       string `json:"valuation"`
}

// WalletResponse 钱包查询响应
type WalletResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		TotalValuation string         `json:"totalValuation"`
		List           []TokenBalance `json:"list"`
	} `json:"data"`
	Success bool `json:"success"`
}

// 全局变量控制清空状态
var (
	isCleaningInProgress = false
	cleaningMutex        sync.Mutex
)

// AccountAsyncState 账户异步操作状态
type AccountAsyncState struct {
	HasHangingOrder   bool      // 是否有挂单监控
	HasAsyncDecrement bool      // 是否有异步递减
	HasCleanup        bool      // 是否在清空中
	LastUpdateTime    time.Time // 最后更新时间
}

// 账户异步状态管理
// TokenSellStatus 代币卖出状态
type TokenSellStatus struct {
	IsSold         bool      // 是否已卖出
	SoldTime       time.Time // 卖出时间
	SoldBy         string    // 卖出方式 (force/async/market)
	SoldPrice      float64   // 卖出价格
	SoldAmount     float64   // 卖出数量
	LastUpdateTime time.Time // 最后更新时间
}

var (
	accountAsyncStates = make(map[string]*AccountAsyncState)
	asyncStateMutex    sync.RWMutex
	
	// 代币卖出状态管理
	tokenSellStatuses = make(map[string]*TokenSellStatus) // key: accountID_tokenAddress
	sellStatusMutex   sync.RWMutex
)

// AutoSellTask 自动卖单任务（Flash Trade 内部使用）
type AutoSellTask struct {
	AccountID     string    `json:"account_id"`
	TokenAddress  string    `json:"token_address"`
	BaseAsset     string    `json:"base_asset"`
	Csrftoken     string    `json:"csrftoken"`
	Cookie        string    `json:"cookie"`
	CheckInterval int       `json:"check_interval"` // 检查间隔(秒)
	SellPrice     float64   `json:"sell_price"`     // 卖出价格
	ChainID       string    `json:"chain_id"`       // 区块链ID，默认为"56"(BSC)
	IsActive      bool      `json:"is_active"`      // 是否活跃
	StartTime     time.Time `json:"start_time"`
	LastCheck     time.Time `json:"last_check"`
	StopChan      chan bool `json:"-"`
}

// AutoSellManager 自动卖单管理器（Flash Trade 内部使用）
type AutoSellManager struct {
	tasks    map[string]*AutoSellTask
	stopChan chan struct{}
	mutex    sync.RWMutex
}

// Flash Trade 内部自动卖单管理（与 alpha_autosell.go 独立）
var (
	autoSellManager = &AutoSellManager{
		tasks:    make(map[string]*AutoSellTask),
		stopChan: make(chan struct{}),
	}
	autoSellMutex sync.Mutex
)

// 全局账户认证信息管理
type AccountAuth struct {
	AccountID string
	Csrftoken string
	Cookie    string
	LastUsed  time.Time
}

var (
	globalAccountAuths = make(map[string]*AccountAuth)
	authMutex          sync.RWMutex
)

// 全局代币处理状态管理（避免冲突）
var (
	processingTokens = make(map[string]bool) // 正在处理的代币
	processingMutex  sync.Mutex
)

// 账号级别的请求管理（避免同账号并发）
var (
	accountProcessing = make(map[string]bool)      // 正在处理的账号
	accountCancels    = make(map[string]chan bool) // 账号取消通道
	accountMutex      sync.RWMutex
)

// 交易间隔控制 - 币安安全刷量模式
var (
	lastTradeTime  = make(map[string]time.Time) // 每个账号的最后交易时间
	tradeInterval  = 15 * time.Second           // 🛡️🚨 极度风控：15秒间隔（每分钟4单，极度保守）
	tradeTimeMutex sync.RWMutex
)

// 暂停状态管理
var (
	accountPauseStatus = make(map[string]*PauseStatus) // 记录每个账号的暂停状态
	pauseMutex         sync.RWMutex
)

// 全局停止状态管理
var (
	isGlobalStopping bool      // 是否正在全局停止
	globalStopTime   time.Time // 全局停止时间
	globalStopMutex  sync.RWMutex
)

// PauseStatus 暂停状态结构
type PauseStatus struct {
	IsPaused   bool      `json:"is_paused"`   // 是否正在暂停
	PauseStart time.Time `json:"pause_start"` // 暂停开始时间
	PauseEnd   time.Time `json:"pause_end"`   // 暂停结束时间
	Reason     string    `json:"reason"`      // 暂停原因
	LastCheck  time.Time `json:"last_check"`  // 最后检查时间
}

// setAccountPauseStatus 设置账号暂停状态
func setAccountPauseStatus(accountID string, duration time.Duration, reason string) {
	pauseMutex.Lock()
	defer pauseMutex.Unlock()

	now := time.Now()
	accountPauseStatus[accountID] = &PauseStatus{
		IsPaused:   true,
		PauseStart: now,
		PauseEnd:   now.Add(duration),
		Reason:     reason,
		LastCheck:  now,
	}

	log.Printf("⏸️ [%s] 设置暂停状态: 持续%.0f秒, 原因: %s", accountID, duration.Seconds(), reason)
}

// checkAccountPauseStatus 检查账号暂停状态
func checkAccountPauseStatus(accountID string) (bool, *PauseStatus) {
	pauseMutex.RLock()
	defer pauseMutex.RUnlock()

	if status, exists := accountPauseStatus[accountID]; exists {
		status.LastCheck = time.Now()

		// 检查暂停是否已过期
		if time.Now().After(status.PauseEnd) {
			return false, status
		}

		return status.IsPaused, status
	}

	return false, nil
}

// clearAccountPauseStatus 清除账号暂停状态
func clearAccountPauseStatus(accountID string) {
	pauseMutex.Lock()
	defer pauseMutex.Unlock()

	if _, exists := accountPauseStatus[accountID]; exists {
		delete(accountPauseStatus, accountID)
		log.Printf("✅ [%s] 暂停状态已清除", accountID)
	}
}

// getAllPauseStatus 获取所有账号的暂停状态（用于统计接口）
func getAllPauseStatus() map[string]interface{} {
	pauseMutex.RLock()
	defer pauseMutex.RUnlock()

	result := make(map[string]interface{})
	now := time.Now()
	pauseIndex := 0

	for accountID, status := range accountPauseStatus {
		// 只返回仍在暂停中的账号
		if status.IsPaused && now.Before(status.PauseEnd) {
			// 🔧 使用安全的键名避免中文字符问题
			safeKey := fmt.Sprintf("paused_account_%d", pauseIndex)
			pauseIndex++

			result[safeKey] = map[string]interface{}{
				"account_id":        accountID,
				"is_paused":         status.IsPaused,
				"pause_start_time":  status.PauseStart.Unix(),
				"pause_end_time":    status.PauseEnd.Unix(),
				"remaining_seconds": int(time.Until(status.PauseEnd).Seconds()),
				"reason":            status.Reason,
				"last_check":        status.LastCheck.Unix(),
			}
		}
	}

	return result
}

// handlePauseStatus 处理暂停状态查询
func handlePauseStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method == "GET" {
		// 查询暂停状态
		accountID := r.URL.Query().Get("account_id")

		if accountID != "" {
			// 查询特定账号的暂停状态
			if isPaused, pauseInfo := checkAccountPauseStatus(accountID); isPaused {
				remainingSeconds := int(time.Until(pauseInfo.PauseEnd).Seconds())
				if remainingSeconds < 0 {
					remainingSeconds = 0
				}

				json.NewEncoder(w).Encode(map[string]interface{}{
					"success":    true,
					"account_id": accountID,
					"is_paused":  true,
					"pause_info": map[string]interface{}{
						"reason":            pauseInfo.Reason,
						"pause_start":       pauseInfo.PauseStart.Unix(),
						"pause_end":         pauseInfo.PauseEnd.Unix(),
						"remaining_seconds": remainingSeconds,
					},
				})
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success":    true,
					"account_id": accountID,
					"is_paused":  false,
				})
			}
		} else {
			// 查询所有账号的暂停状态
			allPauseStatus := getAllPauseStatus()
			// getAllPauseStatus 现在返回的已经是处理好的数据，包含剩余时间

			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":      true,
				"pause_count":  len(allPauseStatus),
				"pause_status": allPauseStatus,
			})
		}
		return
	}

	if r.Method == "DELETE" {
		// 清除暂停状态（手动恢复）
		accountID := r.URL.Query().Get("account_id")

		if accountID == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "缺少 account_id 参数",
			})
			return
		}

		if _, pauseInfo := checkAccountPauseStatus(accountID); pauseInfo != nil {
			clearAccountPauseStatus(accountID)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": fmt.Sprintf("账号 %s 的暂停状态已手动清除", accountID),
			})
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": fmt.Sprintf("账号 %s 当前没有暂停状态", accountID),
			})
		}
		return
	}

	if r.Method == "POST" {
		// 设置暂停状态
		var req struct {
			AccountID string `json:"account_id"`
			Duration  int    `json:"duration"` // 暂停时长（秒）
			Reason    string `json:"reason"`   // 暂停原因
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "请求参数解析失败",
			})
			return
		}

		if req.AccountID == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "缺少 account_id 参数",
			})
			return
		}

		// 设置默认值
		if req.Duration <= 0 {
			req.Duration = 300 // 默认5分钟
		}
		if req.Reason == "" {
			req.Reason = "manual_pause"
		}

		// 设置暂停状态
		duration := time.Duration(req.Duration) * time.Second
		setAccountPauseStatus(req.AccountID, duration, req.Reason)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"message":    fmt.Sprintf("账号 %s 已设置暂停 %d 秒", req.AccountID, req.Duration),
			"account_id": req.AccountID,
			"duration":   req.Duration,
			"reason":     req.Reason,
			"pause_end":  time.Now().Add(duration).Unix(),
		})
		return
	}

	// 不支持的方法
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": "只支持 GET、POST 和 DELETE 方法",
	})
}

// 🔧 优化：币安API调用频率控制（提升响应速度）
var (
	lastAPICall     time.Time
	apiCallInterval = 200 * time.Millisecond // 🚨 风控优化：大幅增加到200ms，避免风控
	apiCallMutex    sync.Mutex
)

// 🛡️ 币安安全智能限流API调用
func waitForAPIRateLimit() {
	apiCallMutex.Lock()
	defer apiCallMutex.Unlock()

	// 🛡️ 智能限流：检查当前秒的API调用次数
	currentSecond := time.Now().Unix()

	counterMutex.Lock()
	if apiCallCounter[currentSecond] >= maxAPICallsPerSecond {
		// 当前秒已达到限制，等待到下一秒
		counterMutex.Unlock()
		waitTime := time.Until(time.Unix(currentSecond+1, 0))
		log.Printf("🛡️ API调用达到限制(%d/秒)，等待%v", maxAPICallsPerSecond, waitTime)
		time.Sleep(waitTime)

		// 重新获取锁并更新计数
		counterMutex.Lock()
		currentSecond = time.Now().Unix()
	}

	// 增加当前秒的API调用计数
	apiCallCounter[currentSecond]++

	// 清理旧的计数记录（保留最近3秒）
	for timestamp := range apiCallCounter {
		if currentSecond-timestamp > 3 {
			delete(apiCallCounter, timestamp)
		}
	}
	counterMutex.Unlock()

	// 基础间隔控制
	timeSinceLastCall := time.Since(lastAPICall)
	var interval time.Duration
	if isVolumeMode {
		// 🚀 动态刷量模式间隔（根据当前速度模式）
		currentMode := getCurrentSpeedMode()
		interval = currentMode.VolumeAPIInterval
	} else {
		interval = apiCallInterval // 正常模式：使用当前API间隔
	}

	if timeSinceLastCall < interval {
		waitTime := interval - timeSinceLastCall
		time.Sleep(waitTime)
	}
	lastAPICall = time.Now()
}

// KYCResponse KYC接口响应结构
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

// getUserFirstName 通过KYC接口获取用户firstName
func getUserFirstName(csrftoken, cookie string) (string, error) {
	// API调用频率限制
	waitForAPIRateLimit()

	

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
	// 🔧 使用429错误检测和断网重连的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "get_account_info")
	if err != nil {
		return "", fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
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

// getTokenBalance 查询指定token的余额
func getTokenBalance(tokenAddress, csrftoken, cookie string) (*TokenBalance, error) {
	// API调用频率限制
	waitForAPIRateLimit()

	log.Printf("🔍 查询token余额: %s", tokenAddress)

	req, err := http.NewRequest("GET", "https://www.binance.com/bapi/defi/v1/private/wallet-direct/cloud-wallet/alpha", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("accept", "*/*")
	req.Header.Set("clienttype", "web")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("csrftoken", csrftoken)
	req.Header.Set("cookie", cookie)
	req.Header.Set("lang", "zh-CN")

	client := &http.Client{Timeout: 10 * time.Second}
	// 🔧 使用429错误检测和断网重连的HTTP请求执行器
	resp, err := executeHTTPRequestWithRateLimit(req, client, "get_account_balance")
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	var walletResp WalletResponse
	if err := json.Unmarshal(body, &walletResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	if walletResp.Code != "000000" {
		return nil, fmt.Errorf("API返回错误: %s", walletResp.Message)
	}

	// 查找指定token
	for _, token := range walletResp.Data.List {
		if strings.EqualFold(token.ContractAddress, tokenAddress) {
			log.Printf("✅ 找到token: %s, 总量: %s, 锁定: %s, 可用: %s",
				token.Symbol, token.Amount, token.Locked, token.Free)
			return &token, nil
		}
	}

	return nil, fmt.Errorf("未找到指定token: %s", tokenAddress)
}

// 自动卖出功能已迁移到 alpha_autosell.go (端口8081)
// 以下函数已删除：handleAutoSell, handleStopAutoSell

// handleCleanupOrders 处理手动清理挂单请求
func handleCleanupOrders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
		Csrftoken string `json:"csrftoken"`
		Cookie    string `json:"cookie"`
		Force     bool   `json:"force"` // 是否强制清理所有挂单
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Invalid request format",
		})
		return
	}

	if req.AccountID == "" || req.Csrftoken == "" || req.Cookie == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "缺少必要参数: account_id, csrftoken, cookie",
		})
		return
	}

	log.Printf("🧹 [%s] 手动触发挂单清理，强制模式: %v", req.AccountID, req.Force)

	var cleanedCount int
	if req.Force {
		// 强制清理所有挂单
		cleanedCount = forceCleanupAllOrders(req.AccountID, req.Csrftoken, req.Cookie)
	} else {
		// 只清理长时间挂单
		cleanedCount = cleanupAccountOrders(req.AccountID, req.Csrftoken, req.Cookie)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"message":       "挂单清理完成",
		"cleaned_count": cleanedCount,
	})
}

// forceCleanupAllOrders 强制清理所有挂单
func forceCleanupAllOrders(accountID, csrftoken, cookie string) int {
	orders, err := getAccountOpenOrders(csrftoken, cookie)
	if err != nil {
		log.Printf("⚠️ [%s] 强制清理查询挂单失败: %v", accountID, err)
		return 0
	}

	if len(orders) == 0 {
		log.Printf("✅ [%s] 无挂单需要清理", accountID)
		return 0
	}

	log.Printf("🚨 [%s] 强制清理所有挂单，共 %d 个", accountID, len(orders))

	cleanedCount := 0
	for _, order := range orders {
		success := cancelOrderByID(order.OrderID, order.Symbol, csrftoken, cookie)
		if success {
			log.Printf("✅ [%s] 强制清理挂单成功: %s", accountID, order.OrderID)
			cleanedCount++
		}
		time.Sleep(200 * time.Millisecond)
	}

	log.Printf("🧹 [%s] 强制清理完成，成功清理 %d/%d 个挂单", accountID, cleanedCount, len(orders))
	return cleanedCount
}

// forceCleanupAllAccountOrders 强制清理所有账户的剩余挂单
func forceCleanupAllAccountOrders() {
	log.Printf("🧹 开始强制清理所有账户的剩余挂单...")

	// 获取所有已注册的账户认证信息
	authMutex.RLock()
	accounts := make(map[string]*AccountAuth)
	for accountID, auth := range globalAccountAuths {
		accounts[accountID] = auth
	}
	authMutex.RUnlock()

	if len(accounts) == 0 {
		log.Printf("🧹 没有已注册的账户，跳过挂单清理")
		return
	}

	log.Printf("🧹 发现 %d 个已注册账户，开始逐一清理挂单", len(accounts))

	totalCleanedCount := 0
	successCount := 0

	for accountID, auth := range accounts {
		log.Printf("🧹 [%s] 开始清理账户挂单...", accountID)

		// 使用现有的强制清理函数
		cleanedCount := forceCleanupAllOrders(accountID, auth.Csrftoken, auth.Cookie)

		if cleanedCount > 0 {
			log.Printf("✅ [%s] 成功清理 %d 个挂单", accountID, cleanedCount)
			totalCleanedCount += cleanedCount
			successCount++
		} else {
			log.Printf("ℹ️ [%s] 无挂单需要清理或清理失败", accountID)
		}

		// 避免请求过快，每个账户间隔1秒
		time.Sleep(1 * time.Second)
	}

	log.Printf("🧹 ========== 挂单清理完成 ==========")
	log.Printf("🧹 处理账户数: %d", len(accounts))
	log.Printf("🧹 成功清理账户数: %d", successCount)
	log.Printf("🧹 总清理挂单数: %d", totalCleanedCount)

	if totalCleanedCount > 0 {
		log.Printf("⚠️ 发现并清理了 %d 个剩余挂单，建议检查账户状态", totalCleanedCount)
	} else {
		log.Printf("✅ 所有账户都没有剩余挂单，状态良好")
	}
}

// runAutoSellTask 运行自动卖单任务（Flash Trade 内部使用）
func runAutoSellTask(task *AutoSellTask) {
	ticker := time.NewTicker(time.Duration(task.CheckInterval) * time.Second)
	defer ticker.Stop()

	log.Printf("🔍 [%s] 开始监控指定代币未锁定余额: %s", task.AccountID, task.TokenAddress)

	for {
		select {
		case <-task.StopChan:
			log.Printf("⏹️ [%s] 自动卖单监控已停止: %s", task.AccountID, task.TokenAddress)
			return
		case <-ticker.C:
			// 🔧 修复死循环：检查任务是否仍然活跃
			if !task.IsActive {
				log.Printf("⏹️ [%s] 自动卖单任务已非活跃，停止监控: %s", task.AccountID, task.TokenAddress)
				return
			}
			task.LastCheck = time.Now()
			checkAndSellToken(task)
		}
	}
}

// checkAndSellToken 检查并卖出长时间未交易的代币（Flash Trade 内部使用）
func checkAndSellToken(task *AutoSellTask) {
	// 1. 检查是否有其他处理在进行（最重要的冲突检查）
	if isTokenProcessing(task.AccountID, task.TokenAddress) {
		log.Printf("🔍 [%s] 跳过处理，代币正在被其他流程处理: %s",
			task.AccountID, task.TokenAddress)
		return
	}

	// 2. 检查是否有活跃的挂单（避免和正常交易冲突）
	orders, err := getAccountOpenOrders(task.Csrftoken, task.Cookie)
	if err == nil {
		for _, order := range orders {
			// 如果有该代币的活跃订单且时间<30秒，跳过处理
			if strings.Contains(order.Symbol, task.TokenAddress) {
				orderAge := time.Duration(time.Now().UnixMilli()-order.CreateTime) * time.Millisecond
				if orderAge < 30*time.Second {
					log.Printf("🔍 [%s] 跳过处理，有活跃订单: %s (%.0f秒前)",
						task.AccountID, task.TokenAddress, orderAge.Seconds())
					return
				}
			}
		}
	}

	// 3. 使用现有的getTokenBalance接口查询代币余额
	balance, err := getTokenBalance(task.TokenAddress, task.Csrftoken, task.Cookie)
	if err != nil {
		log.Printf("⚠️ [%s] 查询代币余额失败: %v", task.AccountID, err)
		return
	}

	// 4. 解析free字段（未锁定的代币数量）
	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)
	lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)

	// 5. 如果没有未锁定的代币，跳过
	if freeAmount <= 1.0 {
		// 只有在有代币时才记录日志，避免刷屏
		if freeAmount > 0 || lockedAmount > 0 {
			log.Printf("🔍 [%s] 代币状态: %s, 未锁定: %.6f, 锁定: %.6f (无需处理)",
				task.AccountID, task.TokenAddress, freeAmount, lockedAmount)
		}
		return
	}

	// 6. 计算代币价值，只有价值>1 USDT才处理
	marketPrice, err := price.GetTokenPriceWithPrecision(task.TokenAddress, getTaskChainID(task), 8)
	if err != nil {
		log.Printf("⚠️ [%s] 无法获取代币价格，跳过价值检查: %v", task.AccountID, err)
		return
	}

	tokenValue := freeAmount * marketPrice
	if tokenValue <= 1.0 {
		log.Printf("💸 [%s] 代币价值过低(%.4f USDT)，停止监控: %s",
			task.AccountID, tokenValue, task.TokenAddress)

		// 🔧 修复：低价值代币直接停止监控，避免无意义的循环检查
		task.IsActive = false

		// 发送停止信号
		select {
		case task.StopChan <- true:
		default:
		}

		log.Printf("⏹️ [%s] 低价值代币监控已停止: %s (价值: %.4f USDT)",
			task.AccountID, task.TokenAddress, tokenValue)
		return
	}

	log.Printf("🎯 [%s] 发现长时间未交易的高价值代币: %s, 未锁定: %.6f, 锁定: %.6f, 价值: %.2f USDT",
		task.AccountID, task.TokenAddress, freeAmount, lockedAmount, tokenValue)

	// 7. 确定卖出价格（长时间未交易，使用更激进的价格）
	var sellPrice float64
	if task.SellPrice > 0 {
		// 使用用户指定的价格
		sellPrice = task.SellPrice
		log.Printf("💰 [%s] 使用指定价格: %.8f", task.AccountID, sellPrice)
	} else {
		// 使用市场价的92%，确保快速成交（比正常交易更激进）
		sellPrice = marketPrice * 0.92
		log.Printf("💰 [%s] 长时间未交易，使用市场价92%%: %.8f (市场价: %.8f)", task.AccountID, sellPrice, marketPrice)
	}

	// 8. 计算卖出数量（保留1个代币，避免精度问题）
	sellAmount := freeAmount - 1.0
	if sellAmount <= 0 {
		log.Printf("⚠️ [%s] 计算后卖出数量≤0，跳过", task.AccountID)
		return
	}

	// 9. 标记开始处理，避免冲突
	if !markTokenProcessing(task.AccountID, task.TokenAddress) {
		log.Printf("🔍 [%s] 跳过处理，代币已被其他流程标记: %s",
			task.AccountID, task.TokenAddress)
		return
	}
	defer unmarkTokenProcessing(task.AccountID, task.TokenAddress)

	// 10. 下卖单（长时间未交易的代币处理）
	log.Printf("📤 [%s] 准备卖出长时间未交易代币: %s, 数量: %.6f, 价格: %.8f",
		task.AccountID, task.TokenAddress, sellAmount, sellPrice)

	orderID, success := placeOrderWithLimitedRetry(OrderRequest{
		BaseAsset:  task.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      sellPrice,
		Quantity:   sellAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: task.Csrftoken,
		Cookie:    task.Cookie,
	}, task.AccountID, 3)

	if success {
		log.Printf("✅ [%s] 自动卖单成功: %s, 数量: %.6f, 价格: %.8f, 订单ID: %s",
			task.AccountID, task.TokenAddress, sellAmount, sellPrice, orderID)
	}
}

// startForceSellMonitoring 启动强制卖出监控（Flash Trade 内部使用）
func startForceSellMonitoring(req *TradeRequest) {
	taskID := fmt.Sprintf("FORCE_%s_%s", req.AccountID, req.TokenAddress)

	autoSellMutex.Lock()
	// 检查是否已有相同任务
	if task, exists := autoSellManager.tasks[taskID]; exists && task.IsActive {
		autoSellMutex.Unlock()
		return // 已有任务在运行
	}

	// 创建强制卖出任务
	task := &AutoSellTask{
		AccountID:     req.AccountID,
		TokenAddress:  req.TokenAddress,
		BaseAsset:     req.BaseAsset,
		Csrftoken:     req.Csrftoken,
		Cookie:        req.Cookie,
		CheckInterval: 30,              // 30秒检查一次，更频繁
		SellPrice:     0,               // 使用市场价
		ChainID:       getChainID(req), // 从请求获取链ID
		IsActive:      true,
		StartTime:     time.Now(),
		StopChan:      make(chan bool),
	}

	autoSellManager.tasks[taskID] = task
	autoSellMutex.Unlock()

	log.Printf("🚨 [%s] 启动强制卖出监控: %s (必须卖出模式)", req.AccountID, req.TokenAddress)

	// 启动强制监控协程
	go runForceSellTask(task)
}

// runForceSellTask 运行强制卖出任务（Flash Trade 内部使用）
func runForceSellTask(task *AutoSellTask) {
	ticker := time.NewTicker(time.Duration(task.CheckInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-task.StopChan:
			log.Printf("⏹️ [%s] 强制卖出监控已停止: %s", task.AccountID, task.TokenAddress)
			return
		case <-ticker.C:
			// 🔧 修复死循环：检查任务是否仍然活跃
			if !task.IsActive {
				log.Printf("⏹️ [%s] 强制卖出任务已非活跃，停止监控: %s", task.AccountID, task.TokenAddress)
				return
			}
			task.LastCheck = time.Now()
			forceSellToken(task)
		}
	}
}

// forceSellToken 强制卖出代币（Flash Trade 内部使用）
func forceSellToken(task *AutoSellTask) {
	// 1. 查询代币余额
	balance, err := getTokenBalance(task.TokenAddress, task.Csrftoken, task.Cookie)
	if err != nil {
		// 检查是否是认证失效
		if strings.Contains(err.Error(), "请检查是否已登录") || strings.Contains(err.Error(), "请求失败") {
			log.Printf("🚨 [%s] 强制卖出检测到认证失效，停止监控", task.AccountID)
			task.IsActive = false
			return
		}
		return
	}

	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)

	// 2. 如果没有未锁定代币，跳过
	if freeAmount <= 1.0 {
		return
	}

	// 3. 获取当前市场价格，检查代币价值
	marketPrice, err := price.GetTokenPriceWithPrecision(task.TokenAddress, getTaskChainID(task), 8)
	if err != nil {
		log.Printf("⚠️ [%s] 强制卖出获取价格失败: %v", task.AccountID, err)
		return
	}

	tokenValue := freeAmount * marketPrice
	log.Printf("💰 [%s] 强制卖出检测到代币: %s, 数量: %.6f, 价值: %.4f USDT",
		task.AccountID, task.TokenAddress, freeAmount, tokenValue)

	// 🔧 针对币安0.1 USDT最低价格限制的处理
	if tokenValue < 0.1 {
		log.Printf("💸 [%s] 代币价值低于0.1 USDT(%.4f)，忽略此代币，允许继续交易", task.AccountID, tokenValue)

		// 停止强制卖出监控，让系统继续正常交易
		task.IsActive = false
		select {
		case task.StopChan <- true:
		default:
		}

		log.Printf("⏹️ [%s] 低价值代币已忽略，系统可继续正常交易: %s", task.AccountID, task.TokenAddress)
		return
	}

	// 3. 计算卖出数量 - 🔧 保留价值0.3 USDT的代币，避免触发最低价格限制
	reserveTokens := 0.3 / marketPrice // 保留价值0.3 USDT的代币数量
	sellAmount := freeAmount - reserveTokens

	if sellAmount <= 0 {
		log.Printf("💸 [%s] 代币数量不足，无需卖出: %s (总量: %.6f, 保留: %.6f)",
			task.AccountID, task.TokenAddress, freeAmount, reserveTokens)

		// 停止强制卖出监控，让系统继续正常交易
		task.IsActive = false
		select {
		case task.StopChan <- true:
		default:
		}
		return
	}

	log.Printf("🚨 [%s] 准备强制卖出代币: %s, 卖出数量: %.6f, 保留数量: %.6f (价值0.3 USDT)",
		task.AccountID, task.TokenAddress, sellAmount, reserveTokens)

	// 4. 强制卖出策略：从市场价开始，逐步降价直到成功
	success := executeForceDecrementSell(task, sellAmount, marketPrice)

	if success {
		log.Printf("✅ [%s] 强制卖出成功: %s", task.AccountID, task.TokenAddress)
	} else {
		log.Printf("⚠️ [%s] 强制卖出暂时失败，下次继续尝试: %s", task.AccountID, task.TokenAddress)
	}
}

// executeForceDecrementSell 执行强制递减卖出
func executeForceDecrementSell(task *AutoSellTask, sellAmount float64, marketPrice float64) bool {
	// 🚨 完全同步模式：强制重置所有异步状态，确保不会有冲突
	log.Printf("🔄 [%s] 强制卖出开始 - 使用同步模式", task.AccountID)
	
	// 强制重置所有异步状态
	resetAccountAsyncState(task.AccountID)
	
	// 不再设置异步状态标志，完全同步执行
	log.Printf("🔒 [%s] 强制卖出使用同步模式，不设置异步状态", task.AccountID)
	
	// 从WebSocket获取最新价格（优先使用长连接返回的实时价格）
	wsPrice, err := price.GetTokenPriceFromWebSocket(task.TokenAddress, getTaskChainID(task))
	if err == nil && wsPrice > 0 {
		// 如果成功从WebSocket获取到价格，使用该价格替换传入的marketPrice
		log.Printf("📊 [%s] 使用WebSocket实时价格: %.8f (原价格: %.8f)", task.AccountID, wsPrice, marketPrice)
		marketPrice = wsPrice
	} else {
		log.Printf("⚠️ [%s] WebSocket价格获取失败，使用传入价格: %.8f", task.AccountID, marketPrice)
	}
	
	// 🚨 修改：设置最低价值阈值，避免币安拒绝下单
	tokenValue := marketPrice * sellAmount
	if tokenValue < 1.0 { // 如果代币总价值低于1.0 USDT，则中断卖出
		log.Printf("💸 [%s] 代币价值过低(%.8f USDT < 1.0 USDT)，币安会拒绝下单，跳过卖出操作", 
			task.AccountID, tokenValue)
		log.Printf("📊 [%s] 代币详情: 数量=%.6f, 价格=%.8f", task.AccountID, sellAmount, marketPrice)
		log.Printf("⚠️ [%s] 按要求：只有余额大于1u才启动卖出", task.AccountID)
		// 更新最后交易时间，确保系统可以继续其他操作
		updateLastTradeTime(task.AccountID)
		return true // 返回true表示处理完成，避免系统继续尝试卖出
	}
	
	// 获取买入价格（从任务中获取或从历史记录中获取）
	buyPrice := getBuyPriceForToken(task.AccountID, task.TokenAddress)
	
	// 确定起始价格
	var startPrice float64
	if marketPrice > buyPrice && marketPrice > 0 {
		// 如果当前市场价高于买入价，优先使用市场价
		startPrice = marketPrice
		log.Printf("💰 [%s] 市场价(%.8f)高于买入价(%.8f)，优先使用市场价", task.AccountID, marketPrice, buyPrice)
	} else if buyPrice > 0 {
		// 如果买入价有效，使用买入价
		startPrice = buyPrice
		log.Printf("💰 [%s] 使用买入价作为起始价格: %.8f", task.AccountID, buyPrice)
	} else {
		// 如果无法获取买入价，使用市场价的95%
		startPrice = marketPrice * 0.95
		log.Printf("💰 [%s] 无法获取买入价，使用市场价95%: %.8f", task.AccountID, startPrice)
	}

	// 新的递减策略：
	// 1. 先尝试市场价或买入价（取较高者）
	// 2. 如果失败，尝试买入价（如果第一步用的是市场价且市场价>买入价）
	// 3. 然后开始递减：买入价的97%，93%，88%，80%，70%，60%，50%
	var decrementRates []float64
	
	if marketPrice > buyPrice && buyPrice > 0 {
		// 如果市场价高于买入价，第二步尝试买入价
		decrementRates = []float64{1.0, buyPrice/startPrice, 0.97, 0.93, 0.88, 0.80, 0.70, 0.60, 0.50}
	} else {
		// 否则直接开始递减
		decrementRates = []float64{1.0, 0.97, 0.93, 0.88, 0.80, 0.70, 0.60, 0.50}
	}

	// 先检查并取消所有该代币的挂单，确保不会有多个订单同时存在
	orders, err := getAccountOpenOrders(task.Csrftoken, task.Cookie)
	if err == nil {
		hasOrders := false
		for _, order := range orders {
			if strings.Contains(order.Symbol, task.BaseAsset) {
				hasOrders = true
				log.Printf("🗑️ [%s] 强制卖出前取消已有订单: %s, ID: %s", task.AccountID, order.Symbol, order.OrderID)
				cancelOrder(order.OrderID, order.Symbol, task.Csrftoken, task.Cookie)
				// 等待订单取消完成
				time.Sleep(500 * time.Millisecond)
			}
		}
		
		// 如果有订单被取消，额外等待1秒确保系统状态完全更新
		if hasOrders {
			log.Printf("⏳ [%s] 订单已取消，等待1秒确保系统状态更新", task.AccountID)
			time.Sleep(1 * time.Second)
		}
	}

	for i, rate := range decrementRates {
		// 首先检查代币是否已经被其他流程卖出
		if isTokenSold(task.AccountID, task.TokenAddress) {
			status := getTokenSellStatus(task.AccountID, task.TokenAddress)
			log.Printf("✅ [%s] 代币已被其他流程卖出，停止强制卖出 - 方式: %s, 价格: %.8f, 数量: %.6f", 
				task.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
			updateLastTradeTime(task.AccountID)
			return true // 代币已卖出，视为成功
		}
		
		// 如果没有卖出记录，再检查代币余额，避免无效尝试
		tokenBalance, balErr := getTokenBalance(task.TokenAddress, task.Csrftoken, task.Cookie)
		if balErr != nil {
			log.Printf("⚠️ [%s] 检查代币余额失败: %v，尝试继续卖出", task.AccountID, balErr)
		} else {
			freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
			if freeAmount <= 0 {
				log.Printf("✅ [%s] 代币余额为0，可能已被卖出，停止强制卖出流程", task.AccountID)
				// 设置代币已卖出状态，但卖出方式未知
				setTokenSold(task.AccountID, task.TokenAddress, "unknown", marketPrice, sellAmount)
				updateLastTradeTime(task.AccountID)
				return true // 代币已卖出，视为成功
			}
			
			// 再次检查代币价值
			tokenValue := marketPrice * freeAmount
			if tokenValue < 1.0 {
				log.Printf("💸 [%s] 代币余额价值过低(%.8f USDT < 1.0 USDT)，停止强制卖出流程", 
					task.AccountID, tokenValue)
				updateLastTradeTime(task.AccountID)
				return true // 代币价值过低，视为处理完成
			}
			
			// 更新卖出数量为当前可用余额
			if freeAmount < sellAmount {
				log.Printf("📊 [%s] 更新卖出数量: %.6f → %.6f (使用当前可用余额)", 
					task.AccountID, sellAmount, freeAmount)
				sellAmount = freeAmount
			}
		}
		
		sellPrice := startPrice * rate
		if sellPrice < 0.00000001 {
			sellPrice = 0.00000001 // 最低价格
		}

		log.Printf("🔄 [%s] 强制卖出第%d步: %s, 价格: %.8f (市场价%.0f%%)",
			task.AccountID, i+1, task.TokenAddress, sellPrice, rate*100)

			// 先检查代币是否已被其他流程卖出
	if isTokenSold(task.AccountID, task.TokenAddress) {
		status := getTokenSellStatus(task.AccountID, task.TokenAddress)
		log.Printf("✅ [%s] 代币已被其他流程卖出，停止强制卖出 - 方式: %s, 价格: %.8f, 数量: %.6f", 
			task.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
		updateLastTradeTime(task.AccountID)
		return true // 代币已卖出，视为成功
	}
	
	// 下单 - 确保数量一致性
	// 使用整数数量，避免精度问题
	intSellAmount := math.Floor(sellAmount)
	
	// 只检查最小金额要求，不限制最小数量
	adjustedTokenValue := sellPrice * intSellAmount
	if adjustedTokenValue < 3.0 { // 🚨 设置最小价值要求为3 USDT
		log.Printf("💸 [%s] 代币交易价值过低(%.2f USDT < 3.0 USDT)，币安可能拒绝下单", 
			task.AccountID, adjustedTokenValue)
		
		// 如果价格太低，尝试增加数量来达到最小价值要求
		if sellPrice > 0 {
			neededAmount := math.Ceil(3.0 / sellPrice)
			if neededAmount <= sellAmount {
				intSellAmount = neededAmount
				log.Printf("📊 [%s] 调整卖出数量: %.0f → %.0f (确保达到最小价值要求3 USDT)", 
					task.AccountID, sellAmount, intSellAmount)
			} else {
				log.Printf("⚠️ [%s] 即使卖出全部余额(%.6f)也无法达到最小价值要求3 USDT", 
					task.AccountID, sellAmount)
				// 继续尝试，让币安决定是否接受订单
			}
		}
	}
	
	// 严格确保数量是整数，并且字符串表示与数值完全一致
	intSellAmount = math.Floor(intSellAmount) // 确保是整数
	
	// 统一使用整数格式的字符串，避免精度问题
	strAmount := fmt.Sprintf("%.0f", intSellAmount) // 使用整数格式的字符串，不带小数点
	
	// 记录详细的订单信息，便于调试
	log.Printf("📝 [%s] 订单详情 - 代币: %s, 价格: %.8f, 数量: %s, 总价值: %.2f USDT", 
		task.AccountID, task.BaseAsset, sellPrice, strAmount, sellPrice * intSellAmount)
	
	// 将字符串转回为整数，确保完全一致
	exactAmount, _ := strconv.ParseInt(strAmount, 10, 64)
	
	// 使用整数作为数量，避免浮点数精度问题
	log.Printf("🔢 [%s] 统一使用整数数量: %d", task.AccountID, exactAmount)
	
	// 构造请求体JSON字符串，确保数量字段完全一致
	orderJSON := fmt.Sprintf(`{
		"baseAsset": "%s",
		"quoteAsset": "USDT",
		"side": "SELL",
		"price": %.8f,
		"quantity": %s,
		"paymentDetails": [
			{
				"amount": "%s",
				"paymentWalletType": "ALPHA"
			}
		]
	}`, task.BaseAsset, sellPrice, strAmount, strAmount)
	
	// 不打印完整的JSON请求体，避免敏感信息泄露
	log.Printf("📦 [%s] 发送强制卖出请求", task.AccountID)
	
	// 解析为OrderRequest结构体
	var orderReq OrderRequest
	if err := json.Unmarshal([]byte(orderJSON), &orderReq); err != nil {
		log.Printf("❌ [%s] 解析订单JSON失败: %v", task.AccountID, err)
		// 使用备用方法构造请求
		orderReq = OrderRequest{
			BaseAsset:  task.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      sellPrice,
			Quantity:   float64(exactAmount),
			PaymentDetails: []PaymentDetail{{
				Amount:            float64(exactAmount),
				AmountStr:         strAmount,
				PaymentWalletType: "ALPHA",
			}},
		}
	}
	
	// 添加认证信息
	orderReq.Csrftoken = task.Csrftoken
	orderReq.Cookie = task.Cookie
	
	// 打印最终请求对象，确认字段一致性
	log.Printf("📝 [%s] 最终订单对象 - Quantity: %v, PaymentDetails.Amount: %v, PaymentDetails.AmountStr: %s", 
		task.AccountID, orderReq.Quantity, 
		orderReq.PaymentDetails[0].Amount, 
		orderReq.PaymentDetails[0].AmountStr)
		
		// 🚨 同步模式：直接尝试下单，简化流程
		log.Printf("🔄 [%s] 强制卖出同步下单: %.8f USDT", task.AccountID, sellPrice)
		
		// 直接使用placeOrderWithID下单
		orderID, success := placeOrderWithID(orderReq)
		
		// 如果下单失败，尝试重试几次
		if !success {
			// 检查是否是"incorrect price or amount"错误
			if strings.Contains(orderID, "incorrect price or amount") {
				log.Printf("⚠️ [%s] 币安拒绝下单(价格或数量不正确)，尝试使用forceCleanToken方法", task.AccountID)
				
				// 使用forceCleanToken方法，该方法更适合处理小额代币
				go func() {
					if err := forceCleanToken(task.TokenAddress, task.BaseAsset, task.Csrftoken, task.Cookie, 8); err != nil {
						log.Printf("⚠️ [%s] forceCleanToken失败: %v", task.AccountID, err)
					} else {
						log.Printf("✅ [%s] forceCleanToken成功清理代币", task.AccountID)
						// 设置代币已卖出状态
						setTokenSold(task.AccountID, task.TokenAddress, "clean", sellPrice, sellAmount)
					}
				}()
				
				// 更新最后交易时间，确保系统可以继续其他操作
				updateLastTradeTime(task.AccountID)
				return true // 返回true表示处理完成
			}
			
			// 检查是否是"余额不足"错误
			if strings.Contains(orderID, "余额不足") || strings.Contains(orderID, "INSUFFICIENT_BALANCE") {
				// 再次检查代币余额
				tokenBalance, balErr := getTokenBalance(task.TokenAddress, task.Csrftoken, task.Cookie)
				if balErr == nil {
					freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
					lockedAmount, _ := strconv.ParseFloat(tokenBalance.Locked, 64)
					
					log.Printf("📊 [%s] 余额不足错误，当前余额: 可用=%.6f, 锁定=%.6f", 
						task.AccountID, freeAmount, lockedAmount)
					
					if freeAmount <= 0 && lockedAmount > 0 {
						log.Printf("✅ [%s] 代币已全部锁定，可能已在挂单中，等待成交", task.AccountID)
						// 设置代币已卖出状态
						setTokenSold(task.AccountID, task.TokenAddress, "pending", sellPrice, sellAmount)
						updateLastTradeTime(task.AccountID)
						return true // 返回true表示处理完成
					}
					
					if freeAmount <= 0 && lockedAmount <= 0 {
						log.Printf("✅ [%s] 代币余额为0，可能已被卖出", task.AccountID)
						// 设置代币已卖出状态
						setTokenSold(task.AccountID, task.TokenAddress, "unknown", sellPrice, sellAmount)
						updateLastTradeTime(task.AccountID)
						return true // 返回true表示处理完成
					}
				}
			}
			
			// 其他错误，尝试常规重试
			for retry := 1; retry <= 3; retry++ {
				log.Printf("⚠️ [%s] 强制卖出下单失败，进行第%d次重试", task.AccountID, retry)
				
				// 每次重试前等待一小段时间
				time.Sleep(time.Duration(500*retry) * time.Millisecond)
				
				// 重新尝试下单
				orderID, success = placeOrderWithID(orderReq)
				
				// 如果成功则跳出循环
				if success {
					log.Printf("✅ [%s] 强制卖出重试成功，订单ID: %s", task.AccountID, orderID)
					break
				}
				
				// 如果遇到特殊错误，中断重试
				if strings.Contains(orderID, "incorrect price or amount") || 
				   strings.Contains(orderID, "余额不足") || 
				   strings.Contains(orderID, "INSUFFICIENT_BALANCE") {
					log.Printf("⚠️ [%s] 检测到特殊错误，中断重试: %s", task.AccountID, orderID)
					break
				}
			}
		}

		if success {
			log.Printf("✅ [%s] 强制卖出下单成功: %s, 订单ID: %s", task.AccountID, task.TokenAddress, orderID)

			// 等待500ms检查成交
			time.Sleep(500 * time.Millisecond)
			sellTime := time.Now().UnixMilli()
			if checkSellOrderHistory(sellTime, task.Csrftoken, task.Cookie) {
				log.Printf("✅ [%s] 强制卖出成交成功", task.AccountID)

				// 设置代币已卖出状态，供其他流程检查
				setTokenSold(task.AccountID, task.TokenAddress, "force", sellPrice, sellAmount)

				// 强制卖出完成，只更新最后交易时间，不计算交易额和交易次数
				updateLastTradeTime(task.AccountID)

				return true
			}

			// 未成交，取消订单继续下一步
			log.Printf("⏰ [%s] 强制卖出未成交，取消订单继续降价", task.AccountID)
			cancelOrder(orderID, task.BaseAsset+"USDT", task.Csrftoken, task.Cookie)
		}

		// 等待一下再尝试下一个价格
		time.Sleep(100 * time.Millisecond)
	}

	// 所有价格都尝试失败，记录日志
	log.Printf("🔄 [%s] 强制卖出所有价格都失败: %s，将由其他兜底机制处理", task.AccountID, task.TokenAddress)
	
	// 更新最后交易时间，确保系统可以继续其他操作
	updateLastTradeTime(task.AccountID)
	
	// 同步模式结束，确保清理所有状态
	resetAccountAsyncState(task.AccountID)
	log.Printf("🔓 [%s] 强制卖出同步模式结束，已清理所有状态", task.AccountID)

	return false // 所有价格都尝试过了，仍未成功
}

// registerAccountAuth 注册账户认证信息
func registerAccountAuth(accountID, csrftoken, cookie string) {
	// 🔒 安全验证：在注册时就验证账号ID
	sanitizedAccountID, err := sanitizeAccountID(accountID)
	if err != nil {
		log.Printf("🚨 [%s] 拒绝注册，账号ID安全验证失败: %v", accountID, err)
		return
	}

	authMutex.Lock()
	defer authMutex.Unlock()

	// 检查是否是新账号或信息有更新
	existingAuth, exists := globalAccountAuths[sanitizedAccountID]
	isNewOrUpdated := !exists ||
		existingAuth.Csrftoken != csrftoken ||
		existingAuth.Cookie != cookie

	auth := &AccountAuth{
		AccountID: sanitizedAccountID, // 使用清理后的账号ID
		Csrftoken: csrftoken,
		Cookie:    cookie,
		LastUsed:  time.Now(),
	}

	globalAccountAuths[sanitizedAccountID] = auth

	if isNewOrUpdated {
		log.Printf("📝 [%s] 注册账户认证信息到全局管理器 (新增或更新)", sanitizedAccountID)
		// 异步保存到Redis，避免阻塞
		go func() {
			if err := saveAccountAuthToRedis(auth); err != nil {
				log.Printf("⚠️ [%s] 保存账号认证信息到Redis失败: %v", sanitizedAccountID, err)
			}
		}()
	} else {
		log.Printf("📝 [%s] 更新账户最后使用时间", sanitizedAccountID)
		// 即使只是更新使用时间，也要保存到Redis
		go func() {
			if err := saveAccountAuthToRedis(auth); err != nil {
				log.Printf("⚠️ [%s] 更新账号认证信息到Redis失败: %v", sanitizedAccountID, err)
			}
		}()
	}
}

// getAccountAuth 获取账户认证信息
func getAccountAuth(accountID string) (*AccountAuth, bool) {
	authMutex.RLock()
	defer authMutex.RUnlock()

	auth, exists := globalAccountAuths[accountID]
	if exists {
		// 更新最后使用时间
		auth.LastUsed = time.Now()
	}
	return auth, exists
}

// getAllActiveAccountAuths 获取所有活跃的账户认证信息
func getAllActiveAccountAuths() []*AccountAuth {
	authMutex.RLock()
	defer authMutex.RUnlock()

	var activeAuths []*AccountAuth
	cutoffTime := time.Now().Add(-24 * time.Hour) // 24小时内使用过的认为是活跃的

	for _, auth := range globalAccountAuths {
		if auth.LastUsed.After(cutoffTime) {
			activeAuths = append(activeAuths, auth)
		}
	}

	return activeAuths
}

// executeTradeWithCancel 带取消机制的交易执行
func executeTradeWithCancel(req *TradeRequest, cancelChan chan bool) TradeResponse {
	// 检查是否被取消
	select {
	case <-cancelChan:
		log.Printf("🚫 [%s] 交易被取消", req.AccountID)
		return TradeResponse{
			Success: false,
			Message: "交易被新请求取消",
		}
	default:
	}

	// 执行正常的交易逻辑
	return executeSingleTrade(req)
}

// handleAccountStatus 处理账号状态查询
func handleAccountStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		// 返回所有账号状态
		allStatus := make(map[string]interface{})

		// 获取所有已注册的账户认证信息
		authMutex.RLock()
		for id, auth := range globalAccountAuths {
			// 获取处理状态
			accountMutex.RLock()
			isProcessing := accountProcessing[id]
			hasCancel := accountCancels[id] != nil
			accountMutex.RUnlock()

			// 获取Flash Trade内部自动卖单任务状态
			autoSellMutex.Lock()
			activeTasks := make([]map[string]interface{}, 0)
			for taskID, task := range autoSellManager.tasks {
				if strings.Contains(taskID, id) && task.IsActive {
					activeTasks = append(activeTasks, map[string]interface{}{
						"task_id":        taskID,
						"token_address":  task.TokenAddress,
						"check_interval": task.CheckInterval,
						"start_time":     task.StartTime.Format("2006-01-02 15:04:05"),
						"last_check":     task.LastCheck.Format("2006-01-02 15:04:05"),
					})
				}
			}
			autoSellMutex.Unlock()

			allStatus[id] = map[string]interface{}{
				"account_id":         id,
				"processing":         isProcessing,
				"has_cancel_channel": hasCancel,
				"last_used":          auth.LastUsed.Format("2006-01-02 15:04:05"),
				"active_tasks":       activeTasks,
				"task_count":         len(activeTasks),
				"status":             getAccountStatusText(isProcessing, len(activeTasks)),
			}
		}
		authMutex.RUnlock()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"accounts":    allStatus,
			"total_count": len(allStatus),
			"timestamp":   time.Now().Format("2006-01-02 15:04:05"),
			"成功":          true,
			"账户":          allStatus,
			"总数量":         len(allStatus),
			"时间戳":         time.Now().Format("2006-01-02 15:04:05"),
		})
		return
	}

	// 返回特定账号状态
	authMutex.RLock()
	auth, authExists := globalAccountAuths[accountID]
	authMutex.RUnlock()

	accountMutex.RLock()
	isProcessing := accountProcessing[accountID]
	hasCancel := accountCancels[accountID] != nil
	accountMutex.RUnlock()

	// 获取该账号的Flash Trade内部自动卖单任务
	autoSellMutex.Lock()
	activeTasks := make([]map[string]interface{}, 0)
	for taskID, task := range autoSellManager.tasks {
		if strings.Contains(taskID, accountID) && task.IsActive {
			activeTasks = append(activeTasks, map[string]interface{}{
				"task_id":        taskID,
				"token_address":  task.TokenAddress,
				"check_interval": task.CheckInterval,
				"start_time":     task.StartTime.Format("2006-01-02 15:04:05"),
				"last_check":     task.LastCheck.Format("2006-01-02 15:04:05"),
			})
		}
	}
	autoSellMutex.Unlock()

	response := map[string]interface{}{
		"success":            true,
		"account_id":         accountID,
		"processing":         isProcessing,
		"has_cancel_channel": hasCancel,
		"active_tasks":       activeTasks,
		"task_count":         len(activeTasks),
		"status":             getAccountStatusText(isProcessing, len(activeTasks)),
		"registered":         authExists,
		"成功":                 true,
		"账户ID":               accountID,
		"处理中":                isProcessing,
		"有取消通道":              hasCancel,
		"活跃任务":               activeTasks,
		"任务数量":               len(activeTasks),
		"状态":                 getAccountStatusText(isProcessing, len(activeTasks)),
		"已注册":                authExists,
	}

	if authExists {
		response["last_used"] = auth.LastUsed.Format("2006-01-02 15:04:05")
	}

	json.NewEncoder(w).Encode(response)
}

// getAccountStatusText 获取账号状态文本描述
func getAccountStatusText(isProcessing bool, taskCount int) string {
	if isProcessing {
		return "交易中"
	}
	if taskCount > 0 {
		return fmt.Sprintf("监控中(%d个任务)", taskCount)
	}
	return "空闲"
}

// handleTradeInterval 处理交易间隔配置
func handleTradeInterval(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		// 查询当前交易间隔
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":                true,
			"trade_interval_seconds": int(tradeInterval.Seconds()),
			"成功":                     true,
			"交易间隔秒数":                 int(tradeInterval.Seconds()),
		})
		return
	}

	if r.Method == "POST" {
		// 设置交易间隔
		var req struct {
			IntervalSeconds int `json:"interval_seconds"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "请求格式错误",
				"成功":      false,
				"消息":      "请求格式错误",
			})
			return
		}

		if req.IntervalSeconds < 1 || req.IntervalSeconds > 300 {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "交易间隔必须在1-300秒之间",
				"成功":      false,
				"消息":      "交易间隔必须在1-300秒之间",
			})
			return
		}

		tradeInterval = time.Duration(req.IntervalSeconds) * time.Second
		log.Printf("⏱️ 交易间隔已设置为: %d秒", req.IntervalSeconds)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":                true,
			"message":                fmt.Sprintf("交易间隔已设置为%d秒", req.IntervalSeconds),
			"trade_interval_seconds": req.IntervalSeconds,
			"成功":                     true,
			"消息":                     fmt.Sprintf("交易间隔已设置为%d秒", req.IntervalSeconds),
			"交易间隔秒数":                 req.IntervalSeconds,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": "只支持GET和POST方法",
		"成功":      false,
		"消息":      "只支持GET和POST方法",
	})
}

// 🔧 新增：handleRateLimitStatus 处理429错误状态查询
func handleRateLimitStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取429错误处理状态
	status := globalRateLimitHandler.GetPauseStatus()

	// 添加API频率限制器状态
	globalAPILimiter.mutex.Lock()
	recentRequests := len(globalAPILimiter.requests)
	globalAPILimiter.mutex.Unlock()

	response := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"rate_limit_handler": status,
			"api_limiter": map[string]interface{}{
				"recent_requests": recentRequests,
				"max_requests":    globalAPILimiter.maxRequests,
				"time_window":     globalAPILimiter.timeWindow.String(),
			},
			"timestamp": time.Now().Format("2006-01-02 15:04:05"),
		},
		"成功": true,
		"数据": map[string]interface{}{
			"频率限制处理器": status,
			"API限制器": map[string]interface{}{
				"最近请求数": recentRequests,
				"最大请求数": globalAPILimiter.maxRequests,
				"时间窗口":  globalAPILimiter.timeWindow.String(),
			},
			"时间戳": time.Now().Format("2006-01-02 15:04:05"),
		},
	}

	json.NewEncoder(w).Encode(response)
}

// 🔧 新增：handleNetworkStatus 处理网络连接状态查询
func handleNetworkStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 检查网络连通性
	isConnected := checkNetworkConnectivity()

	// 测试各个主机的连通性
	testHosts := []string{
		"8.8.8.8:53",          // Google DNS
		"1.1.1.1:53",          // Cloudflare DNS
		"114.114.114.114:53",  // 114 DNS
		"www.binance.com:443", // Binance
	}

	hostStatus := make(map[string]bool)
	for _, host := range testHosts {
		conn, err := net.DialTimeout("tcp", host, 3*time.Second)
		if err == nil {
			conn.Close()
			hostStatus[host] = true
		} else {
			hostStatus[host] = false
		}
	}

	// 获取价格客户端状态（如果存在）
	var priceClientStatus map[string]interface{}
	if globalPriceClient := price.GetGlobalClient(); globalPriceClient != nil {
		// 这里可以添加价格客户端的状态信息
		priceClientStatus = map[string]interface{}{
			"connected": true, // 简化处理
			"url":       "wss://nbstream.binance.com/w3w/wsa/stream",
		}
	} else {
		priceClientStatus = map[string]interface{}{
			"connected": false,
		}
	}

	response := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"network_connected":   isConnected,
			"host_connectivity":   hostStatus,
			"price_client_status": priceClientStatus,
			"timestamp":           time.Now().Format("2006-01-02 15:04:05"),
			"check_time":          time.Now().Unix(),
		},
		"成功": true,
		"数据": map[string]interface{}{
			"网络已连接":   isConnected,
			"主机连通性":   hostStatus,
			"价格客户端状态": priceClientStatus,
			"时间戳":     time.Now().Format("2006-01-02 15:04:05"),
			"检查时间":    time.Now().Unix(),
		},
	}

	json.NewEncoder(w).Encode(response)
}

// handleAPIInterval 处理API调用间隔配置
func handleAPIInterval(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		// 查询当前API调用间隔
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":         true,
			"api_interval_ms": int(apiCallInterval.Milliseconds()),
			"成功":              true,
			"API间隔毫秒":         int(apiCallInterval.Milliseconds()),
		})
		return
	}

	if r.Method == "POST" {
		// 设置API调用间隔
		var req struct {
			IntervalMs int `json:"interval_ms"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "请求格式错误",
				"成功":      false,
				"消息":      "请求格式错误",
			})
			return
		}

		if req.IntervalMs < 100 || req.IntervalMs > 2000 {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "API调用间隔必须在100-2000毫秒之间",
				"成功":      false,
				"消息":      "API调用间隔必须在100-2000毫秒之间",
			})
			return
		}

		apiCallInterval = time.Duration(req.IntervalMs) * time.Millisecond
		log.Printf("⏱️ API调用间隔已设置为: %dms", req.IntervalMs)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":         true,
			"message":         fmt.Sprintf("API调用间隔已设置为%dms", req.IntervalMs),
			"api_interval_ms": req.IntervalMs,
			"成功":              true,
			"消息":              fmt.Sprintf("API调用间隔已设置为%dms", req.IntervalMs),
			"API间隔毫秒":         req.IntervalMs,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": "只支持GET和POST方法",
		"成功":      false,
		"消息":      "只支持GET和POST方法",
	})
}

// handleAccountsManagement 处理账号管理请求
func handleAccountsManagement(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		return
	}

	switch r.Method {
	case "GET":
		// 获取所有账号信息
		authMutex.RLock()
		accounts := make(map[string]interface{})
		accountIndex := 0

		for accountID, auth := range globalAccountAuths {
			safeKey := fmt.Sprintf("account_%d", accountIndex)
			accountIndex++

			accounts[safeKey] = map[string]interface{}{
				"account_id":        auth.AccountID,
				"account_key":       accountID,
				"last_used":         auth.LastUsed.Unix(),
				"last_used_str":     auth.LastUsed.Format("2006-01-02 15:04:05"),
				"has_csrftoken":     len(auth.Csrftoken) > 0,
				"has_cookie":        len(auth.Cookie) > 0,
				"csrftoken_preview": auth.Csrftoken[:min(8, len(auth.Csrftoken))] + "...",
			}
		}
		authMutex.RUnlock()

		response := map[string]interface{}{
			"success":        true,
			"total_accounts": len(globalAccountAuths),
			"accounts":       accounts,
			"storage":        "Redis",
			"redis_keys":     "flash_trade_auth:*",
			"成功":             true,
			"总账户数":           len(globalAccountAuths),
			"账户":             accounts,
			"存储":             "Redis",
			"Redis键":         "flash_trade_auth:*",
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

	case "DELETE":
		// 删除指定账号
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			http.Error(w, "缺少account_id参数", http.StatusBadRequest)
			return
		}

		// 🔒 安全验证：验证要删除的账号ID
		sanitizedAccountID, err := sanitizeAccountID(accountID)
		if err != nil {
			log.Printf("🚨 [%s] 拒绝删除，账号ID安全验证失败: %v", accountID, err)
			http.Error(w, fmt.Sprintf("账号ID安全验证失败: %v", err), http.StatusBadRequest)
			return
		}

		authMutex.Lock()
		_, exists := globalAccountAuths[sanitizedAccountID]
		if exists {
			delete(globalAccountAuths, sanitizedAccountID)
		}
		authMutex.Unlock()

		if exists {
			// 从Redis删除
			go func() {
				if err := deleteAccountAuthFromRedis(sanitizedAccountID); err != nil {
					log.Printf("⚠️ [%s] 从Redis删除账号认证信息失败: %v", sanitizedAccountID, err)
				}
			}()

			response := map[string]interface{}{
				"success": true,
				"message": fmt.Sprintf("账号 %s 已删除", sanitizedAccountID),
				"成功":      true,
				"消息":      fmt.Sprintf("账号 %s 已删除", sanitizedAccountID),
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		} else {
			http.Error(w, "账号不存在", http.StatusNotFound)
		}

	default:
		http.Error(w, "不支持的请求方法", http.StatusMethodNotAllowed)
	}
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// getBuyPriceForToken 获取指定账户和代币的买入价格
func getBuyPriceForToken(accountID, tokenAddress string) float64 {
	// 尝试从累积任务统计中获取
	stats := getCumulativeStats(accountID)
	if stats != nil && len(stats.History) > 0 {
		// 查找最近的历史记录
		for _, record := range stats.History {
			// 这里需要从任务ID中提取代币地址进行比较
			// 通常任务ID格式为: accountID_tokenAddress_timestamp
			parts := strings.Split(record.TaskID, "_")
			if len(parts) >= 2 && strings.Contains(parts[1], tokenAddress) {
				// 如果找到匹配的记录，估算买入价格
				if record.CompletedVolume > 0 && record.ProfitLoss < 0 {
					// 根据完成交易额和亏损估算买入价格
					return record.CompletedVolume / (record.CompletedVolume + record.ProfitLoss)
				}
			}
		}
	}
	
	// 如果没找到，返回0表示无法确定买入价格
	return 0
}

// markTokenProcessing 标记代币开始处理（避免冲突）
func markTokenProcessing(accountID, tokenAddress string) bool {
	processingMutex.Lock()
	defer processingMutex.Unlock()

	key := fmt.Sprintf("%s_%s", accountID, tokenAddress)
	if processingTokens[key] {
		return false // 已在处理中
	}
	processingTokens[key] = true
	log.Printf("🔒 [%s] 标记代币处理中: %s", accountID, tokenAddress)
	return true // 可以处理
}

// unmarkTokenProcessing 取消代币处理标记
func unmarkTokenProcessing(accountID, tokenAddress string) {
	processingMutex.Lock()
	defer processingMutex.Unlock()

	key := fmt.Sprintf("%s_%s", accountID, tokenAddress)
	delete(processingTokens, key)
	log.Printf("🔓 [%s] 取消代币处理标记: %s", accountID, tokenAddress)
}

// isTokenProcessing 检查代币是否正在被处理
func isTokenProcessing(accountID, tokenAddress string) bool {
	processingMutex.Lock()
	defer processingMutex.Unlock()

	key := fmt.Sprintf("%s_%s", accountID, tokenAddress)
	return processingTokens[key]
}

// 全局挂单清理器相关结构和变量
type GlobalOrderCleaner struct {
	cleanupInterval time.Duration
	maxOrderAge     time.Duration
	isRunning       bool
	stopChan        chan struct{}
}

var (
	globalCleaner = &GlobalOrderCleaner{
		cleanupInterval: 1 * time.Minute, // 每1分钟检查一次
		maxOrderAge:     1 * time.Minute, // 超过1分钟的订单被清理
		stopChan:        make(chan struct{}),
	}
)

// startGlobalOrderCleaner 启动全局挂单清理器
func startGlobalOrderCleaner() {
	globalCleaner.isRunning = true
	ticker := time.NewTicker(globalCleaner.cleanupInterval)
	defer ticker.Stop()

	log.Printf("🧹 全局挂单清理器开始运行，检查间隔: %v, 最大订单存活时间: %v",
		globalCleaner.cleanupInterval, globalCleaner.maxOrderAge)

	for {
		select {
		case <-globalCleaner.stopChan:
			log.Printf("🛑 全局挂单清理器已停止")
			globalCleaner.isRunning = false
			return
		case <-ticker.C:
			performGlobalOrderCleanup()
		}
	}
}

// performGlobalOrderCleanup 执行全局挂单清理
func performGlobalOrderCleanup() {
	log.Printf("🔍 开始全局挂单清理检查")

	// 获取所有活跃账户的认证信息（优先从全局管理器获取）
	cleanupCount := 0
	processedAccounts := make(map[string]bool)

	// 1. 从全局认证管理器获取账户信息
	activeAuths := getAllActiveAccountAuths()
	for _, auth := range activeAuths {
		if !processedAccounts[auth.AccountID] {
			cleaned := cleanupAccountOrders(auth.AccountID, auth.Csrftoken, auth.Cookie)
			cleanupCount += cleaned
			processedAccounts[auth.AccountID] = true
		}
	}

	// 2. 从自动卖单任务中获取额外的认证信息（兜底）
	autoSellMutex.Lock()
	activeTasks := make([]*AutoSellTask, 0)
	for _, task := range autoSellManager.tasks {
		if task.IsActive && !processedAccounts[task.AccountID] {
			activeTasks = append(activeTasks, task)
		}
	}
	autoSellMutex.Unlock()

	// 为未处理的账户执行清理
	for _, task := range activeTasks {
		cleaned := cleanupAccountOrders(task.AccountID, task.Csrftoken, task.Cookie)
		cleanupCount += cleaned
		processedAccounts[task.AccountID] = true
	}

	if cleanupCount > 0 {
		log.Printf("🧹 全局挂单清理完成，共清理 %d 个超时挂单", cleanupCount)
	} else {
		log.Printf("✅ 全局挂单检查完成，无需清理")
	}
}

// cleanupAccountOrders 清理指定账户的长时间挂单并重新挂单
func cleanupAccountOrders(accountID, csrftoken, cookie string) int {
	// 1. 查询账户的所有挂单
	orders, err := getAccountOpenOrders(csrftoken, cookie)
	if err != nil {

		return 0
	}

	if len(orders) == 0 {
		return 0
	}

	log.Printf("🔍 [%s] 发现 %d 个挂单，检查是否需要清理", accountID, len(orders))

	cleanedCount := 0
	currentTime := time.Now().UnixMilli()

	for _, order := range orders {
		// 2. 检查订单时间
		orderTime := order.CreateTime
		orderAge := time.Duration(currentTime-orderTime) * time.Millisecond

		if orderAge > globalCleaner.maxOrderAge {
			// 3. 提取代币地址进行冲突检查
			tokenAddress := extractBaseAssetFromSymbol(order.Symbol)

			// 4. 检查是否有其他处理在进行（避免冲突）
			if isTokenProcessing(accountID, tokenAddress) {
				log.Printf("🔍 [%s] 跳过清理，代币正在被其他流程处理: %s (订单ID: %s)",
					accountID, tokenAddress, order.OrderID)
				continue
			}

			log.Printf("🚨 [%s] 发现超时挂单: ID=%s, 时间=%.0f秒前, 价格=%s, 方向=%s",
				accountID, order.OrderID, orderAge.Seconds(), order.Price, order.Side)

			// 5. 取消长时间挂单
			success := cancelOrderByID(order.OrderID, order.Symbol, csrftoken, cookie)
			if success {
				log.Printf("✅ [%s] 成功清理超时挂单: %s", accountID, order.OrderID)
				cleanedCount++

				// 6. 如果是卖单，需要重新挂买单避免代币残留
				if order.Side == "SELL" {
					go reCreateSellOrderAtMarketPrice(accountID, order, csrftoken, cookie)
				}
			}

			// 避免API调用过于频繁
			time.Sleep(200 * time.Millisecond)
		}
	}

	return cleanedCount
}

// reCreateSellOrderAtMarketPrice 重新按市场价创建卖单
func reCreateSellOrderAtMarketPrice(accountID string, canceledOrder OpenOrder, csrftoken, cookie string) {
	log.Printf("🔄 [%s] 开始重新挂卖单: %s", accountID, canceledOrder.Symbol)

	// 1. 解析代币地址和基础资产
	baseAsset := extractBaseAssetFromSymbol(canceledOrder.Symbol)
	tokenAddress := baseAsset // 假设代币地址就是基础资产

	// 2. 检查是否有其他处理在进行（避免冲突）
	if isTokenProcessing(accountID, tokenAddress) {
		log.Printf("🔍 [%s] 跳过重新挂单，代币正在被其他流程处理: %s",
			accountID, tokenAddress)
		return
	}

	// 3. 查询代币余额
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {

		return
	}

	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)
	if freeAmount <= 1.0 {
		log.Printf("✅ [%s] 无需重新挂单，代币余额≤1: %.6f", accountID, freeAmount)
		return
	}

	// 4. 获取当前市场价
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {

		return
	}

	// 4. 计算新的卖出价格（市场价的95%，确保快速成交）
	newSellPrice := marketPrice * 0.95
	newSellPrice = adjustPricePrecision(newSellPrice, 8)

	// 5. 计算卖出数量（保留1个代币）
	sellAmount := freeAmount - 1.0
	if sellAmount <= 0 {
		log.Printf("✅ [%s] 计算后无需卖出: %.6f", accountID, freeAmount)
		return
	}

	log.Printf("🔄 [%s] 重新挂卖单: %s, 数量: %.6f, 市场价: %.8f, 卖价: %.8f (95%%)",
		accountID, tokenAddress, sellAmount, marketPrice, newSellPrice)

	// 6. 下新的卖单
	orderID, success := placeOrderWithLimitedRetry(OrderRequest{
		BaseAsset:  baseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      newSellPrice,
		Quantity:   sellAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: csrftoken,
		Cookie:    cookie,
	}, accountID, 2) // 只重试2次

	if success {
		log.Printf("✅ [%s] 重新挂卖单成功: %s, 订单ID: %s", accountID, tokenAddress, orderID)
	} else {

		// 重新挂单失败，启动强制卖出监控确保代币能被卖出
		go startEmergencySellMonitoring(accountID, tokenAddress, baseAsset, csrftoken, cookie)
	}
}

// extractBaseAssetFromSymbol 从交易对符号中提取基础资产
func extractBaseAssetFromSymbol(symbol string) string {
	// 移除USDT后缀，例如: ALPHA_251USDT -> ALPHA_251
	if strings.HasSuffix(symbol, "USDT") {
		return strings.TrimSuffix(symbol, "USDT")
	}
	return symbol
}

// OpenOrder 挂单信息结构
type OpenOrder struct {
	OrderID    string `json:"orderId"`
	Symbol     string `json:"symbol"`
	Price      string `json:"price"`
	Quantity   string `json:"origQty"`
	Side       string `json:"side"`
	CreateTime int64  `json:"time"`
}

// OrderQueryResponse 订单查询响应
type OrderQueryResponse struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Data    []OpenOrder `json:"data"`
	Success bool        `json:"success"`
}

// getAccountOpenOrders 查询账户的所有挂单
func getAccountOpenOrders(csrftoken, cookie string) ([]OpenOrder, error) {
	req, err := http.NewRequest("GET", "https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/open-orders", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
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
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	var orderResp OrderQueryResponse
	if err := json.Unmarshal(body, &orderResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	if orderResp.Code != "000000" {
		return nil, fmt.Errorf("API返回错误: %s", orderResp.Message)
	}

	return orderResp.Data, nil
}

// cancelOrderByID 根据订单ID取消订单
func cancelOrderByID(orderID, symbol, csrftoken, cookie string) bool {
	cancelData := map[string]interface{}{
		"orderId": orderID,
		"symbol":  symbol,
	}

	jsonData, _ := json.Marshal(cancelData)

	req, err := http.NewRequest("POST", "https://www.binance.com/bapi/defi/v1/private/alpha-trade/order/cancel", bytes.NewBuffer(jsonData))
	if err != nil {

		return false
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

		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {

		return false
	}

	var cancelResp map[string]interface{}
	if err := json.Unmarshal(body, &cancelResp); err != nil {

		return false
	}

	// 检查取消是否成功
	if code, exists := cancelResp["code"]; exists {
		if code == "000000" || code == 0 {
			// 订单取消成功，无需额外延迟
			log.Printf("✅ [%s] 订单取消成功", orderID)
			return true
		}

	}

	return false
}

// startEmergencySellMonitoring 启动紧急卖出监控（重新挂单失败时的兜底）
func startEmergencySellMonitoring(accountID, tokenAddress, baseAsset, csrftoken, cookie string) {
	log.Printf("🚨 [%s] 启动紧急卖出监控: %s", accountID, tokenAddress)

	// 最多尝试4次，每次间隔20秒
	for attempt := 1; attempt <= 4; attempt++ {
		time.Sleep(20 * time.Second)

		// 检查代币余额
		balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
		if err != nil {
			log.Printf("⚠️ [%s] 紧急监控查询余额失败(第%d次): %v", accountID, attempt, err)
			continue
		}

		freeAmount, _ := strconv.ParseFloat(balance.Free, 64)
		if freeAmount <= 1.0 {
			log.Printf("✅ [%s] 紧急监控完成，代币已清理: %.6f", accountID, freeAmount)
			return
		}

		log.Printf("🔄 [%s] 紧急监控第%d次尝试卖出: %s, 数量: %.6f", accountID, attempt, tokenAddress, freeAmount)

		// 获取市场价格
		marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
		if err != nil {
			log.Printf("⚠️ [%s] 紧急监控获取价格失败: %v", accountID, err)
			marketPrice = 0.00000001 // 使用极低价格
		}

		// 使用强制递减策略卖出
		sellAmount := freeAmount - 1.0
		if sellAmount > 0 {
			success := executeForceDecrementSell(&AutoSellTask{
				AccountID:    accountID,
				TokenAddress: tokenAddress,
				BaseAsset:    baseAsset,
				Csrftoken:    csrftoken,
				Cookie:       cookie,
				ChainID:      DEFAULT_CHAIN_ID, // 使用默认链ID
			}, sellAmount, marketPrice)

			if success {
				log.Printf("✅ [%s] 紧急卖出成功: %s", accountID, tokenAddress)
				return
			}
		}
	}

	log.Printf("⚠️ [%s] 紧急卖出监控结束，仍有代币残留: %s", accountID, tokenAddress)
}

// immediateForceSellingProcess 立即强制卖出处理（发现代币余额时立即执行）
func immediateForceSellingProcess(accountID, tokenAddress, baseAsset, csrftoken, cookie string, freeAmount float64) {
	log.Printf("🚨 [%s] 开始立即强制卖出处理: %s, 数量: %.6f", accountID, tokenAddress, freeAmount)

	// 1. 先取消所有可能的挂单
	log.Printf("🗑️ [%s] 先取消所有挂单", accountID)
	cancelAllOrders(csrftoken, cookie)
	time.Sleep(2 * time.Second) // 等待取消生效

	// 2. 重新查询余额
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {

		return
	}

	currentFreeAmount, _ := strconv.ParseFloat(balance.Free, 64)
	if currentFreeAmount <= 1.0 {
		log.Printf("✅ [%s] 代币已清理完成: %.6f", accountID, currentFreeAmount)
		return
	}

	log.Printf("💰 [%s] 当前可卖代币: %.6f", accountID, currentFreeAmount)

	// 3. 获取当前市场价
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {

		marketPrice = 0.00000001 // 使用极低价格
	}

	// 4. 执行卖出策略 - 🔧 保留价值0.3 USDT的代币
	reserveTokens := 0.3 / marketPrice // 保留价值0.3 USDT的代币数量
	sellAmount := currentFreeAmount - reserveTokens

	if sellAmount <= 0 {
		log.Printf("💸 [%s] 代币数量不足，无需卖出 (总量: %.6f, 保留: %.6f)",
			accountID, currentFreeAmount, reserveTokens)
		updateLastTradeTime(accountID) // 更新时间，避免重复检查
		return
	}

	tokenValue := sellAmount * marketPrice
	log.Printf("💰 [%s] 准备卖出代币: %.6f (价值约 %.4f USDT), 保留: %.6f (价值0.3 USDT)",
		accountID, sellAmount, tokenValue, reserveTokens)

	// 递减策略：市场价98% → 95% → 90% → 80% → 70% → 50% → 30% → 10% → 1%
	decrementRates := []float64{0.98, 0.95, 0.90, 0.80, 0.70, 0.50, 0.30, 0.10, 0.01}

	for i, rate := range decrementRates {
		sellPrice := marketPrice * rate
		if sellPrice < 0.00000001 {
			sellPrice = 0.00000001
		}

		log.Printf("🔄 [%s] 立即卖出第%d步: %s, 价格: %.8f (市场价%.0f%%)",
			accountID, i+1, tokenAddress, sellPrice, rate*100)

		// 下单
		orderID, success := placeOrderWithLimitedRetry(OrderRequest{
			BaseAsset:  baseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      sellPrice,
			Quantity:   sellAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellAmount,
				AmountStr:         fmt.Sprintf("%.6f", sellAmount), // 🔧 修复：保留6位小数，支持小额代币
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: csrftoken,
			Cookie:    cookie,
		}, accountID, 2) // 只重试2次

		if success {
			log.Printf("✅ [%s] 立即卖出下单成功: %s, 订单ID: %s", accountID, tokenAddress, orderID)

			// 等待1秒检查成交
			time.Sleep(1 * time.Second)
			sellTime := time.Now().UnixMilli()
			if checkSellOrderHistory(sellTime, csrftoken, cookie) {
				log.Printf("✅ [%s] 立即卖出成交成功", accountID)

				// 立即卖出完成，只更新最后交易时间，不计算交易额和交易次数
				updateLastTradeTime(accountID)

				return
			}

			// 未成交，取消订单继续下一步
			log.Printf("⏰ [%s] 立即卖出未成交，取消订单继续降价", accountID)
			cancelOrder(orderID, baseAsset+"USDT", csrftoken, cookie)
		}

		// 等待一下再尝试下一个价格
		time.Sleep(200 * time.Millisecond)
	}

	log.Printf("⚠️ [%s] 立即卖出所有价格尝试完毕，启动持续监控", accountID)

	// 如果所有价格都不成交，启动持续的强制监控 - Flash Trade 内部使用
	go startForceSellMonitoring(&TradeRequest{
		AccountID:    accountID,
		TokenAddress: tokenAddress,
		BaseAsset:    baseAsset,
		Csrftoken:    csrftoken,
		Cookie:       cookie,
	})
}

// startAutoTokenMonitoring 自动启动代币监控（交易时自动启动）
func startAutoTokenMonitoring(req *TradeRequest) {
	taskID := fmt.Sprintf("AUTO_%s_%s", req.AccountID, req.TokenAddress)

	autoSellMutex.Lock()
	// 检查是否已有相同任务
	if task, exists := autoSellManager.tasks[taskID]; exists && task.IsActive {
		autoSellMutex.Unlock()
		log.Printf("🔍 [%s] 代币监控已存在，跳过启动: %s", req.AccountID, req.TokenAddress)
		return
	}

	// 创建自动监控任务（专门监控长时间未交易的代币）
	task := &AutoSellTask{
		AccountID:     req.AccountID,
		TokenAddress:  req.TokenAddress,
		BaseAsset:     req.BaseAsset,
		Csrftoken:     req.Csrftoken,
		Cookie:        req.Cookie,
		CheckInterval: 45,              // 45秒检查一次（避免和30秒清理冲突）
		SellPrice:     0,               // 使用市场价95%
		ChainID:       getChainID(req), // 从请求获取链ID
		IsActive:      true,
		StartTime:     time.Now(),
		StopChan:      make(chan bool),
	}

	autoSellManager.tasks[taskID] = task
	autoSellMutex.Unlock()

	log.Printf("🤖 [%s] 自动启动代币监控: %s (45秒间隔，监控长时间未交易代币，>1U价值限制)", req.AccountID, req.TokenAddress)

	// 启动监控协程
	go runAutoSellTask(task)
}

// stopAllTradingAndExit 停止所有交易但保持服务运行
func stopAllTradingAndExit() {
	log.Printf("🛑 ========== 开始停止所有交易 ==========")

	// 1. 停止所有自动循环
	log.Printf("🛑 停止所有自动循环交易...")
	loopMutex.Lock()
	for accountID, stopChan := range loopingAccounts {
		log.Printf("🛑 停止账户循环: %s", accountID)
		select {
		case stopChan <- true:
		default:
		}
	}
	loopMutex.Unlock()

	// 2. 停止所有自动卖单监控
	log.Printf("🛑 停止所有自动卖单监控...")
	autoSellMutex.Lock()
	for taskID, task := range autoSellManager.tasks {
		if task.IsActive {
			log.Printf("🛑 停止监控任务: %s", taskID)
			task.IsActive = false
			select {
			case task.StopChan <- true:
			default:
			}
		}
	}
	autoSellMutex.Unlock()

	// 3. 停止全局挂单清理器
	log.Printf("🛑 停止全局挂单清理器...")
	stopGlobalOrderCleaner()

	// 4. 等待所有处理完成
	log.Printf("⏳ 等待所有处理完成...")
	time.Sleep(5 * time.Second)

	// 5. 🚨 新增：强制清理所有账户的剩余挂单
	log.Printf("🧹 ========== 强制清理所有账户的剩余挂单 ==========")
	forceCleanupAllAccountOrders()

	// 6. 输出最终统计结果
	log.Printf("📊 ========== 最终统计结果 ==========")
	outputFinalGlobalStats()

	// 7. 输出每个账户的详细统计
	statsMutex.RLock()
	for accountID := range accountStats {
		outputAccountStats(accountID)
	}
	statsMutex.RUnlock()

	log.Printf("🎉 ========== 交易任务完成，服务继续运行 ==========")
	log.Printf("💡 服务保持运行状态，可以继续接收新的交易请求")

	// 重置全局状态，为下次交易做准备
	resetGlobalStateForNextTrade()

	// 不退出程序，保持服务运行
}

// outputFinalGlobalStats 输出最终全局统计
func outputFinalGlobalStats() {
	// 🚨 修复：在最终统计时不调用updateGlobalStats，避免重复触发停止
	// updateGlobalStats() // 注释掉，使用当前的统计数据

	log.Printf("🌍 ========== 最终全局统计 ==========")
	log.Printf("🌍 总目标交易额: %.2f USDT", globalStats.TotalTargetVolume)
	log.Printf("🌍 总完成交易额: %.2f USDT", globalStats.TotalCurrentVolume)
	log.Printf("🌍 完成率: %.2f%%", globalStats.CompletionRate)

	if globalStats.TotalLoss >= 0 {
		log.Printf("🌍 总净亏损: %.6f USDT", globalStats.TotalLoss)
		log.Printf("🌍 总磨损率: %.2f万分", globalStats.TotalLossRate)
	} else {
		log.Printf("🌍 总净盈利: %.6f USDT", -globalStats.TotalLoss)
		log.Printf("🌍 总盈利率: %.2f万分", -globalStats.TotalLossRate)
	}

	// 计算10万交易额预计磨损
	if globalStats.TotalCurrentVolume > 0 {
		projectedLoss := (globalStats.TotalLoss / globalStats.TotalCurrentVolume) * 100000
		if projectedLoss >= 0 {
			log.Printf("🌍 10万交易额预计磨损: %.2f USDT", projectedLoss)
		} else {
			log.Printf("🌍 10万交易额预计盈利: %.2f USDT", -projectedLoss)
		}
	}

	log.Printf("🌍 总账户数: %d", len(accountStats))
	log.Printf("🌍 =====================================")
}

// outputAccountStats 输出单个账户的详细统计
func outputAccountStats(accountID string) {
	statsMutex.RLock()
	stats, exists := accountStats[accountID]
	statsMutex.RUnlock()

	if !exists {
		return
	}

	log.Printf("👤 ========== 账户统计: %s ==========", accountID)
	log.Printf("👤 目标交易额: %.2f USDT", stats.TargetVolume)
	log.Printf("👤 完成交易额: %.2f USDT", stats.TotalVolume)

	if stats.TargetVolume > 0 {
		completionRate := (stats.TotalVolume / stats.TargetVolume) * 100
		log.Printf("👤 完成率: %.2f%%", completionRate)
	}

	if stats.TotalLoss >= 0 {
		log.Printf("👤 净亏损: %.6f USDT", stats.TotalLoss)
	} else {
		log.Printf("👤 净盈利: %.6f USDT", -stats.TotalLoss)
	}

	if stats.TotalVolume > 0 {
		lossRate := (stats.TotalLoss / stats.TotalVolume) * 10000
		if lossRate >= 0 {
			log.Printf("👤 磨损率: %.2f万分", lossRate)
		} else {
			log.Printf("👤 盈利率: %.2f万分", -lossRate)
		}

		// 计算10万交易额预计磨损
		projectedLoss := (stats.TotalLoss / stats.TotalVolume) * 100000
		if projectedLoss >= 0 {
			log.Printf("👤 10万交易额预计磨损: %.2f USDT", projectedLoss)
		} else {
			log.Printf("👤 10万交易额预计盈利: %.2f USDT", -projectedLoss)
		}
	}

	log.Printf("👤 =====================================")
}

// stopGlobalOrderCleaner 停止全局挂单清理器
func stopGlobalOrderCleaner() {
	if globalCleaner.isRunning {
		close(globalCleaner.stopChan)
		log.Printf("🛑 正在停止全局挂单清理器...")
	}
}

// WalletGroupResponse 钱包组响应结构
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

// getFundingAccountBalance 查询资金账户USDT余额
func getFundingAccountBalance(csrftoken, cookie string) (float64, error) {
	// API调用频率限制
	waitForAPIRateLimit()
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

// forceCleanToken 强制清空指定token (改进版)
func forceCleanToken(tokenAddress, baseAsset, csrftoken, cookie string, pricePrecision int) error {
	// 使用账户级别的清空状态，而不是全局状态
	accountID := "CLEANUP_" + tokenAddress // 为清空操作创建虚拟账户ID

	if isAccountBusy(accountID) {
		log.Printf("⚠️ [%s] 清空操作正在进行中，跳过本次执行", accountID)
		return nil
	}

	// 设置清空状态
	setAccountAsyncState(accountID, "cleanup", true)
	defer setAccountAsyncState(accountID, "cleanup", false)

	// 1. 查询token余额
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {

		return err
	}

	// 解析数量
	totalAmount, _ := strconv.ParseFloat(balance.Amount, 64)

	// 🔧 修复：改为按价值判断是否需要清空
	if totalAmount <= 0 {
		return nil
	}

	// 获取代币价格计算价值
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {
		log.Printf("⚠️ 无法获取代币价格，按数量判断: %v", err)
		// 如果无法获取价格，按原逻辑：总量≤1认为不需要清空
		if totalAmount <= 1.0 {
			return nil
		}
	} else {
		// 计算代币价值
		tokenValue := totalAmount * marketPrice
		log.Printf("📊 定时清空价值检查: %.6f × %.8f = %.2f USDT", totalAmount, marketPrice, tokenValue)

		// 🔧 防死循环：使用动态阈值和尝试次数限制
		forceCleanKey := fmt.Sprintf("force_clean_%s", tokenAddress)
		attempts := getCleanupAttempts(forceCleanKey)

		// 动态阈值：第1次1 USDT，第2次0.5 USDT，第3次及以上0.2 USDT
		var threshold float64
		switch {
		case attempts == 0:
			threshold = 1.0
		case attempts == 1:
			threshold = 1.0 // 修改：提高阈值，避免币安拒绝下单
		case attempts == 2:
			threshold = 0.5 // 第三次尝试时可以降低阈值
		default:
			// 超过3次尝试，强制完成
			log.Printf("⚠️ 强制清空已尝试%d次，防止死循环，强制完成", attempts)
			resetCleanupAttempts(forceCleanKey)
			return nil
		}

		if tokenValue < threshold {
			log.Printf("✅ 代币价值 %.2f USDT < %.1f USDT，无需清空", tokenValue, threshold)
			return nil
		}

		log.Printf("⚠️ 代币价值 %.2f USDT ≥ %.1f USDT，需要清空（第%d次尝试）", tokenValue, threshold, attempts+1)
		incrementCleanupAttempts(forceCleanKey)
	}

	// 2. 循环撤单直到成功
	if !cancelAllOrdersUntilSuccess(tokenAddress, csrftoken, cookie) {
		return fmt.Errorf("循环撤销所有订单失败")
	}

	// 3. 最终查询余额确认
	balance, err = getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {

		return err
	}

	_, _ = strconv.ParseFloat(balance.Locked, 64)
	totalAmount, _ = strconv.ParseFloat(balance.Amount, 64)

	// 4. 计算要卖出的数量（保留1个）
	sellAmount := totalAmount - 1.0
	if sellAmount <= 0 {
		log.Printf("✅ 计算后无需卖出，保留数量: %.6f", totalAmount)
		return nil
	}

	log.Printf("💰 准备卖出数量: %.6f (保留1个)", sellAmount)

	// 5. 强制卖出
	return executeForceCleanSell(tokenAddress, baseAsset, sellAmount, csrftoken, cookie, pricePrecision)
}

// executeForceCleanSell 执行强制清空卖出
func executeForceCleanSell(tokenAddress, baseAsset string, sellAmount float64, csrftoken, cookie string, pricePrecision int) error {
	log.Printf("💰 开始强制清空卖出: %.6f %s", sellAmount, baseAsset)

	// 1. 获取当前市场价
	currentPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, pricePrecision)
	if err != nil {

		return err
	}
	currentPrice = adjustPricePrecision(currentPrice, pricePrecision)
	log.Printf("📈 当前市场价: %.*f", pricePrecision, currentPrice)

	// 2. 计算加万一的价格
	sellPrice := currentPrice * 1.0001 // 加万一
	sellPrice = adjustPricePrecision(sellPrice, pricePrecision)
	log.Printf("💰 加万一卖出价: %.*f", pricePrecision, sellPrice)

	// 调整代币数量
	adjustedSellAmount := adjustTokenAmountForBinance(sellPrice, sellAmount, pricePrecision)
	if adjustedSellAmount != sellAmount {
		log.Printf("🔢 数量调整: %.6f → %.6f", sellAmount, adjustedSellAmount)
		sellAmount = adjustedSellAmount
	}

	// 如果调整后数量为0，跳过下单
	if sellAmount <= 0 {
		return fmt.Errorf("强制清空调整后数量为0")
	}

	// 3. 尝试加万二卖出
	orderID, success := placeOrderWithRetry(OrderRequest{
		BaseAsset:  baseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      sellPrice,
		Quantity:   sellAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: csrftoken,
		Cookie:    cookie,
	}, "FORCE_CLEAN")

	if success {

		// 等待1秒检查成交
		sellTime := time.Now().UnixMilli()
		for i := 0; i < 5; i++ { // 1秒，每200ms检查一次
			time.Sleep(200 * time.Millisecond)
			if checkSellOrderHistory(sellTime, csrftoken, cookie) {

				return nil
			}
		}

		// 1秒未成交，取消订单进入递减

		cancelOrder(orderID, baseAsset+"USDT", csrftoken, cookie)
	}

	// 4. 递减卖出策略
	return executeForceCleanDecrement(tokenAddress, baseAsset, sellAmount, currentPrice, csrftoken, cookie, pricePrecision)
}

// executeForceCleanDecrement 执行强制清空递减卖出
func executeForceCleanDecrement(tokenAddress, baseAsset string, sellAmount, marketPrice float64, csrftoken, cookie string, pricePrecision int) error {
	log.Printf("🔄 开始强制清空递减卖出")

	// 递减策略：万1 → 万5 → 百10 (3步快速递减)
	decrementSteps := []float64{
		0.0003, // 万3
		0.0006, // 万6
		0.1,    // 百10 (最终限制)
	}

	for step, decrementRate := range decrementSteps {
		// 计算递减价格
		sellPrice := marketPrice * (1.0002 - decrementRate) // 从加万二开始递减
		sellPrice = adjustPricePrecision(sellPrice, pricePrecision)

		decrementWanFen := decrementRate * 10000
		log.Printf("💰 强制清空递减第%d步(%.0f万分): %.*f", step+1, decrementWanFen, pricePrecision, sellPrice)

		// 调整代币数量
		adjustedSellAmount := adjustTokenAmountForBinance(sellPrice, sellAmount, pricePrecision)
		if adjustedSellAmount != sellAmount {

			sellAmount = adjustedSellAmount
		}

		// 如果调整后数量为0，跳过当前步骤
		if sellAmount <= 0 {
			log.Printf("⚠️ 强制清空递减第%d步调整后数量为0，跳过当前步骤", step+1)
			continue
		}

		// 下单（强制清空只重试1次）
		orderID, success := placeOrderWithLimitedRetry(OrderRequest{
			BaseAsset:  baseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      sellPrice,
			Quantity:   sellAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: csrftoken,
			Cookie:    cookie,
		}, "FORCE_CLEAN", 1)

		if !success {

			continue
		}

		log.Printf("✅ 强制清空递减第%d步下单成功: %s", step+1, orderID)

		// 等待100ms检查成交
		sellTime := time.Now().UnixMilli()
		for i := 0; i < 2; i++ { // 400ms，每200ms检查一次
			time.Sleep(200 * time.Millisecond)
			if checkSellOrderHistory(sellTime, csrftoken, cookie) {
				log.Printf("✅ 强制清空递减第%d步成交成功", step+1)

				// 强制清空递减完成，只更新最后交易时间，不计算交易额和交易次数
				updateLastTradeTime("FORCE_CLEAN")
				log.Printf("📊 强制清空递减第%d步完成", step+1)
				return nil
			}
		}

		// 100ms未成交，取消订单继续下一步
		log.Printf("⏰ 强制清空递减第%d步100ms未成交，取消订单", step+1)

		// 并行处理：取消订单的同时准备下一步
		cancelDone := make(chan bool, 1)
		go func() {
			cancelOrder(orderID, baseAsset+"USDT", csrftoken, cookie)
			cancelDone <- true
		}()

		// 等待取消完成，最多等待200ms
		select {
		case <-cancelDone:
			// 取消成功，继续下一步
		case <-time.After(200 * time.Millisecond):
			// 取消超时，但继续下一步（避免卡死）
			log.Printf("⚠️ 取消订单超时，继续下一步")
		}
	}

	// 递减完成仍未成交，最后尝试市场价
	log.Printf("🚨 强制清空递减完成仍未成交，尝试市场价卖出")
	return executeForceCleanMarketSell(tokenAddress, baseAsset, sellAmount, marketPrice, csrftoken, cookie, pricePrecision)
}

// executeForceCleanMarketSell 执行强制清空市场价卖出
func executeForceCleanMarketSell(tokenAddress, baseAsset string, sellAmount, marketPrice float64, csrftoken, cookie string, pricePrecision int) error {
	log.Printf("💰 强制清空市场价卖出: %.6f %s", sellAmount, baseAsset)

	// 使用市场价卖出
	sellPrice := adjustPricePrecision(marketPrice, pricePrecision)

	// 调整代币数量
	adjustedSellAmount := adjustTokenAmountForBinance(sellPrice, sellAmount, pricePrecision)
	if adjustedSellAmount != sellAmount {
		log.Printf("🔢 市场价数量调整: %.6f → %.6f", sellAmount, adjustedSellAmount)
		sellAmount = adjustedSellAmount
	}

	// 如果调整后数量为0，直接返回失败
	if sellAmount <= 0 {
		log.Printf("⚠️ 强制清空市场价调整后数量为0，无法下单")
		return fmt.Errorf("强制清空市场价调整后数量为0")
	}

	// 最多尝试3次市场价卖出
	for attempt := 1; attempt <= 3; attempt++ {
		log.Printf("🔄 强制清空市场价第%d次尝试", attempt)

		orderID, success := placeOrderWithLimitedRetry(OrderRequest{
			BaseAsset:  baseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      sellPrice,
			Quantity:   sellAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: csrftoken,
			Cookie:    cookie,
		}, "FORCE_CLEAN", 2)

		if success {
			log.Printf("✅ 强制清空市场价第%d次下单成功: %s", attempt, orderID)

			// 等待1秒检查成交
			sellTime := time.Now().UnixMilli()
			for i := 0; i < 5; i++ { // 1秒，每200ms检查一次
				time.Sleep(200 * time.Millisecond)
				if checkSellOrderHistory(sellTime, csrftoken, cookie) {
					log.Printf("✅ 强制清空市场价第%d次成交成功", attempt)

					// 强制清空市场价完成，只更新最后交易时间，不计算交易额和交易次数
					updateLastTradeTime("FORCE_CLEAN")
					log.Printf("📊 强制清空市场价第%d次完成", attempt)
					return nil
				}
			}

			// 1秒未成交，取消订单
			log.Printf("⏰ 强制清空市场价第%d次1秒未成交，取消订单", attempt)
			cancelOrder(orderID, baseAsset+"USDT", csrftoken, cookie)
		}

		// 降低价格重试
		if attempt < 3 { // 修复：只在前2次失败后降价
			sellPrice = sellPrice * 0.995 // 降低0.5%
			sellPrice = adjustPricePrecision(sellPrice, pricePrecision)
			log.Printf("📉 强制清空降低价格重试: %.*f", pricePrecision, sellPrice)
			time.Sleep(200 * time.Millisecond)
		}
	}

	log.Printf("🚨 强制清空市场价3次尝试全部失败，进行最终确认")

	// 最终确认：检查是否还有代币余额或挂单
	return finalCleanupConfirmation(tokenAddress, baseAsset, csrftoken, cookie)
}

// finalCleanupConfirmation 最终清空确认（增强版，防死循环）
func finalCleanupConfirmation(tokenAddress, baseAsset, csrftoken, cookie string) error {
	log.Printf("🔍 开始最终清空确认（增强版，防死循环）")

	// 🔧 防死循环：检查清理尝试次数
	cleanupKey := fmt.Sprintf("cleanup_attempts_%s", tokenAddress)
	if attempts := getCleanupAttempts(cleanupKey); attempts >= 3 {
		log.Printf("⚠️ 已尝试清理3次，防止死循环，强制标记为完成")
		resetCleanupAttempts(cleanupKey)
		return nil
	}
	incrementCleanupAttempts(cleanupKey)

	// 1. 强制取消所有订单，最多尝试5次
	log.Printf("🗑️ 强制取消所有订单（最多5次尝试）")
	for attempt := 1; attempt <= 5; attempt++ {
		if cancelAllOrdersUntilSuccess(tokenAddress, csrftoken, cookie) {
			log.Printf("✅ 第%d次尝试成功取消所有订单", attempt)
			break
		}
		if attempt < 5 {
			log.Printf("⚠️ 第%d次取消订单失败，等待后重试", attempt)
			time.Sleep(time.Duration(attempt) * time.Second)
		} else {
			log.Printf("❌ 5次尝试后仍无法取消所有订单，继续检查余额")
		}
	}

	// 2. 等待3秒让取消生效
	time.Sleep(3 * time.Second)

	// 3. 检查代币余额
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {
		return fmt.Errorf("最终余额查询失败: %v", err)
	}

	totalAmount, _ := strconv.ParseFloat(balance.Amount, 64)
	lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)
	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)

	log.Printf("📊 最终余额检查: 总量=%.6f, 锁定=%.6f, 可用=%.6f", totalAmount, lockedAmount, freeAmount)

	// 4. 如果仍有锁定代币，说明还有挂单，继续强制取消
	if lockedAmount > 0 {
		log.Printf("🚨 检测到仍有锁定代币%.6f，执行紧急订单清理", lockedAmount)
		return executeEmergencyOrderCleanup(tokenAddress, baseAsset, csrftoken, cookie)
	}

	// 5. 计算剩余代币价值，使用更严格的标准（0.1 USDT）
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {
		log.Printf("⚠️ 无法获取代币价格，按数量判断: %v", err)
		// 如果无法获取价格，按更严格标准：总量≤0.1认为成功
		if totalAmount <= 0.1 {
			log.Printf("✅ 强制清空最终确认成功，剩余代币≤0.1")
			return nil
		}
		// 如果代币数量>0.1，尝试最后的强制卖出
		return executeUltimateForceCleanup(tokenAddress, baseAsset, totalAmount, csrftoken, cookie)
	}

	tokenValue := totalAmount * marketPrice
	log.Printf("📊 最终余额价值计算: %.6f × %.8f = %.4f USDT", totalAmount, marketPrice, tokenValue)

	// 6. 🔧 防死循环：使用动态阈值判断
	dynamicThreshold := calculateDynamicThreshold(tokenValue, totalAmount)
	log.Printf("📊 动态清理阈值: %.4f USDT (代币价值: %.4f USDT)", dynamicThreshold, tokenValue)

	if tokenValue < dynamicThreshold {
		log.Printf("✅ 强制清空最终确认成功，剩余代币价值: %.4f USDT (<%0.4fU)", tokenValue, dynamicThreshold)
		stopTokenCleanup(tokenAddress)
		resetCleanupAttempts(cleanupKey)
		return nil
	}

	// 7. 检查是否可交易（防止无法交易的小额代币循环）
	if !isTokenTradeable(totalAmount, tokenValue, marketPrice) {
		log.Printf("⚠️ 代币无法交易（数量太少或价值太低），强制标记为完成: %.6f个, 价值: %.4f USDT", totalAmount, tokenValue)
		stopTokenCleanup(tokenAddress)
		resetCleanupAttempts(cleanupKey)
		return nil
	}

	// 8. 如果价值≥动态阈值且可交易，执行最终强制清理
	log.Printf("⚠️ 强制清空后仍有代币: %.6f个, 价值: %.4f USDT (≥%.4fU)，执行最终强制清理", totalAmount, tokenValue, dynamicThreshold)
	return executeUltimateForceCleanup(tokenAddress, baseAsset, totalAmount, csrftoken, cookie)
}

// executeEmergencyOrderCleanup 执行紧急订单清理（当检测到锁定代币时）
func executeEmergencyOrderCleanup(tokenAddress, baseAsset, csrftoken, cookie string) error {
	log.Printf("🚨 开始紧急订单清理流程")

	// 1. 查询所有挂单
	orders, err := getAccountOpenOrders(csrftoken, cookie)
	if err != nil {
		log.Printf("❌ 查询挂单失败: %v", err)
		return fmt.Errorf("查询挂单失败: %v", err)
	}

	if len(orders) == 0 {
		log.Printf("✅ 没有发现挂单，但仍有锁定代币，可能是系统延迟")
		time.Sleep(5 * time.Second) // 等待系统同步
		return nil
	}

	log.Printf("🔍 发现%d个挂单，开始逐个取消", len(orders))

	// 2. 逐个取消挂单
	canceledCount := 0
	for _, order := range orders {
		// 检查是否是目标代币的订单
		orderTokenAddress := extractBaseAssetFromSymbol(order.Symbol)
		if strings.EqualFold(orderTokenAddress, tokenAddress) {
			success := cancelOrderByID(order.OrderID, order.Symbol, csrftoken, cookie)
			if success {
				log.Printf("✅ 成功取消订单: %s", order.OrderID)
				canceledCount++
			} else {
				log.Printf("❌ 取消订单失败: %s", order.OrderID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	log.Printf("📊 紧急清理完成，成功取消%d个订单", canceledCount)

	// 3. 等待取消生效后重新检查
	time.Sleep(3 * time.Second)
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {
		return fmt.Errorf("紧急清理后余额查询失败: %v", err)
	}

	lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)
	if lockedAmount > 0 {
		log.Printf("⚠️ 紧急清理后仍有锁定代币%.6f，可能需要人工处理", lockedAmount)
		return fmt.Errorf("紧急清理后仍有锁定代币: %.6f", lockedAmount)
	}

	log.Printf("✅ 紧急订单清理成功，无锁定代币")
	return nil
}

// executeUltimateForceCleanup 执行最终强制清理（极限清理模式，防死循环）
func executeUltimateForceCleanup(tokenAddress, baseAsset string, tokenAmount float64, csrftoken, cookie string) error {
	log.Printf("🔥 开始最终强制清理模式: %.6f %s", tokenAmount, baseAsset)

	// 🔧 防死循环：检查极限清理尝试次数
	ultimateKey := fmt.Sprintf("ultimate_cleanup_%s", tokenAddress)
	if attempts := getCleanupAttempts(ultimateKey); attempts >= 2 {
		log.Printf("⚠️ 极限清理已尝试2次，防止死循环，强制完成")
		resetCleanupAttempts(ultimateKey)
		return nil
	}
	incrementCleanupAttempts(ultimateKey)

	// 1. 获取当前市场价
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {
		log.Printf("⚠️ 无法获取市场价，使用极低价格")
		marketPrice = 0.00000001 // 使用极低价格
	}

	// 🔧 防死循环：检查是否可交易
	tokenValue := tokenAmount * marketPrice
	if !isTokenTradeable(tokenAmount, tokenValue, marketPrice) {
		log.Printf("⚠️ 代币无法交易，跳过极限清理: %.6f个, 价值: %.4f USDT", tokenAmount, tokenValue)
		resetCleanupAttempts(ultimateKey)
		return nil
	}

	// 2. 使用极激进的价格策略：市场价的80%
	ultimatePrice := marketPrice * 0.8
	ultimatePrice = adjustPricePrecision(ultimatePrice, 8)

	log.Printf("💰 最终强制清理价格: %.12f (市场价80%%)", ultimatePrice)

	// 3. 计算卖出数量（不保留任何代币）
	sellAmount := tokenAmount
	if sellAmount <= 0 {
		log.Printf("✅ 无代币需要清理")
		return nil
	}

	// 4. 调整数量以符合币安要求
	adjustedSellAmount := adjustTokenAmountForBinance(ultimatePrice, sellAmount, 8)
	if adjustedSellAmount != sellAmount {
		log.Printf("🔢 最终清理数量调整: %.6f → %.6f", sellAmount, adjustedSellAmount)
		sellAmount = adjustedSellAmount
	}

	if sellAmount <= 0 {
		log.Printf("⚠️ 调整后数量为0，无法执行最终清理")
		return fmt.Errorf("调整后数量为0，无法执行最终清理")
	}

	// 5. 执行最终强制卖出（最多尝试5次，每次降价10%）
	for attempt := 1; attempt <= 5; attempt++ {
		log.Printf("🔄 最终强制清理第%d次尝试，价格: %.12f", attempt, ultimatePrice)

		orderID, success := placeOrderWithLimitedRetry(OrderRequest{
			BaseAsset:  baseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      ultimatePrice,
			Quantity:   sellAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: csrftoken,
			Cookie:    cookie,
		}, "ULTIMATE_CLEANUP", 2)

		if success && orderID != "" {
			log.Printf("✅ 最终强制清理订单成功: %s", orderID)

			// 等待成交确认
			sellTime := time.Now().UnixMilli()
			for i := 0; i < 10; i++ { // 等待2秒
				time.Sleep(200 * time.Millisecond)
				if checkSellOrderHistory(sellTime, csrftoken, cookie) {
					log.Printf("✅ 最终强制清理成交确认")
					resetCleanupAttempts(ultimateKey)
					return nil
				}
			}

			// 如果2秒内未成交，取消订单并降价重试
			log.Printf("⏰ 最终强制清理未及时成交，取消订单并降价重试")
			cancelOrder(orderID, baseAsset+"USDT", csrftoken, cookie)
			time.Sleep(500 * time.Millisecond)
		}

		// 降价重试（每次降价10%）
		if attempt < 5 {
			ultimatePrice = ultimatePrice * 0.9
			ultimatePrice = adjustPricePrecision(ultimatePrice, 8)
			log.Printf("📉 最终强制清理降价重试: %.12f", ultimatePrice)
		}
	}

	log.Printf("❌ 最终强制清理5次尝试全部失败")
	return fmt.Errorf("最终强制清理失败")
}

// 防死循环相关变量和函数
var (
	cleanupAttemptsMutex sync.RWMutex
	cleanupAttemptsMap   = make(map[string]int)
	cleanupAttemptsTime  = make(map[string]time.Time)
)

// getCleanupAttempts 获取清理尝试次数
func getCleanupAttempts(key string) int {
	cleanupAttemptsMutex.RLock()
	defer cleanupAttemptsMutex.RUnlock()

	// 检查是否超过1小时，如果是则重置
	if lastTime, exists := cleanupAttemptsTime[key]; exists {
		if time.Since(lastTime) > time.Hour {
			cleanupAttemptsMutex.RUnlock()
			cleanupAttemptsMutex.Lock()
			delete(cleanupAttemptsMap, key)
			delete(cleanupAttemptsTime, key)
			cleanupAttemptsMutex.Unlock()
			cleanupAttemptsMutex.RLock()
			return 0
		}
	}

	return cleanupAttemptsMap[key]
}

// incrementCleanupAttempts 增加清理尝试次数
func incrementCleanupAttempts(key string) {
	cleanupAttemptsMutex.Lock()
	defer cleanupAttemptsMutex.Unlock()
	cleanupAttemptsMap[key]++
	cleanupAttemptsTime[key] = time.Now()
}

// resetCleanupAttempts 重置清理尝试次数
func resetCleanupAttempts(key string) {
	cleanupAttemptsMutex.Lock()
	defer cleanupAttemptsMutex.Unlock()
	delete(cleanupAttemptsMap, key)
	delete(cleanupAttemptsTime, key)
}

// calculateDynamicThreshold 计算动态清理阈值（防死循环）
func calculateDynamicThreshold(tokenValue, tokenAmount float64) float64 {
	// 基础阈值0.1 USDT
	baseThreshold := 0.1

	// 如果代币数量很少（<100个），提高阈值到0.2 USDT
	if tokenAmount < 100 {
		return 0.2
	}

	// 如果代币价值在0.08-0.12之间（接近阈值），使用0.05作为阈值避免循环
	if tokenValue >= 0.08 && tokenValue <= 0.12 {
		return 0.05
	}

	return baseThreshold
}

// isTokenTradeable 检查代币是否可交易（防止无法交易的循环）
func isTokenTradeable(tokenAmount, tokenValue, marketPrice float64) bool {
	// 1. 检查代币数量是否足够（至少1个）
	if tokenAmount < 1 {
		return false
	}

	// 2. 检查代币价值是否足够（至少0.01 USDT）
	if tokenValue < 0.01 {
		return false
	}

	// 3. 检查价格是否合理（不能太低）
	if marketPrice < 0.000001 {
		return false
	}

	// 4. 检查计算的交易金额是否符合币安最小要求
	minTradeValue := marketPrice * tokenAmount
	if minTradeValue < 0.00000001 { // 币安最小交易金额
		return false
	}

	return true
}

// ensureCompleteCleanupAfterTargetVolume 确保交易额达标后的完全清理
func ensureCompleteCleanupAfterTargetVolume(req *TradeRequest) error {
	log.Printf("🔍 [%s] 开始交易额达标后的完全清理检查", req.AccountID)

	// 1. 强制取消所有挂单
	log.Printf("🗑️ [%s] 强制取消所有挂单", req.AccountID)
	if !cancelAllOrdersUntilSuccess(req.TokenAddress, req.Csrftoken, req.Cookie) {
		log.Printf("⚠️ [%s] 取消挂单失败，但继续检查余额", req.AccountID)
	}

	// 2. 等待取消生效
	time.Sleep(3 * time.Second)

	// 3. 检查代币余额
	balance, err := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
	if err != nil {
		log.Printf("❌ [%s] 余额查询失败: %v", req.AccountID, err)
		return fmt.Errorf("余额查询失败: %v", err)
	}

	totalAmount, _ := strconv.ParseFloat(balance.Amount, 64)
	lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)
	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)

	log.Printf("📊 [%s] 最终余额状态: 总量=%.6f, 锁定=%.6f, 可用=%.6f",
		req.AccountID, totalAmount, lockedAmount, freeAmount)

	// 4. 如果仍有锁定代币，执行紧急清理
	if lockedAmount > 0 {
		log.Printf("🚨 [%s] 检测到锁定代币%.6f，执行紧急清理", req.AccountID, lockedAmount)
		if err := executeEmergencyOrderCleanup(req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie); err != nil {
			log.Printf("❌ [%s] 紧急清理失败: %v", req.AccountID, err)
		}

		// 重新查询余额
		time.Sleep(2 * time.Second)
		balance, err = getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
		if err != nil {
			return fmt.Errorf("紧急清理后余额查询失败: %v", err)
		}
		totalAmount, _ = strconv.ParseFloat(balance.Amount, 64)
		freeAmount, _ = strconv.ParseFloat(balance.Free, 64)
	}

	// 5. 检查剩余代币价值
	if totalAmount <= 0 {
		log.Printf("✅ [%s] 完全清理成功，无剩余代币", req.AccountID)
		return nil
	}

	// 6. 计算代币价值
	marketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {
		log.Printf("⚠️ [%s] 无法获取代币价格: %v", req.AccountID, err)
		// 如果无法获取价格且代币数量>0.1，执行强制清理
		if totalAmount > 0.1 {
			return executeUltimateForceCleanup(req.TokenAddress, req.BaseAsset, totalAmount, req.Csrftoken, req.Cookie)
		}
		return nil
	}

	tokenValue := totalAmount * marketPrice
	log.Printf("📊 [%s] 剩余代币价值: %.6f × %.8f = %.4f USDT",
		req.AccountID, totalAmount, marketPrice, tokenValue)

	// 7. 如果代币价值≥0.1 USDT，执行最终强制清理
	if tokenValue >= 0.1 {
		log.Printf("⚠️ [%s] 剩余代币价值%.4f USDT ≥ 0.1 USDT，执行最终强制清理",
			req.AccountID, tokenValue)
		return executeUltimateForceCleanup(req.TokenAddress, req.BaseAsset, totalAmount, req.Csrftoken, req.Cookie)
	}

	log.Printf("✅ [%s] 完全清理检查通过，剩余代币价值%.4f USDT < 0.1 USDT",
		req.AccountID, tokenValue)
	return nil
}

// ensureFinalSellAndCleanup 确保最后一笔是卖单且没有残留
func ensureFinalSellAndCleanup(req *TradeRequest) error {
	log.Printf("🎯 [%s] 开始最终卖出确保流程", req.AccountID)

	// 1. 先取消所有挂单
	log.Printf("🗑️ [%s] 取消所有挂单", req.AccountID)
	if !cancelAllOrdersUntilSuccess(req.TokenAddress, req.Csrftoken, req.Cookie) {
		log.Printf("⚠️ [%s] 取消挂单失败，但继续执行", req.AccountID)
	}

	// 2. 等待取消生效
	time.Sleep(2 * time.Second)

	// 3. 检查代币余额
	balance, err := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
	if err != nil {
		log.Printf("❌ [%s] 余额查询失败: %v", req.AccountID, err)
		return fmt.Errorf("余额查询失败: %v", err)
	}

	totalAmount, _ := strconv.ParseFloat(balance.Amount, 64)
	freeAmount, _ := strconv.ParseFloat(balance.Free, 64)

	log.Printf("📊 [%s] 当前代币状态: 总量=%.6f, 可用=%.6f", req.AccountID, totalAmount, freeAmount)

	// 4. 如果没有代币，直接返回成功
	if totalAmount <= 0 {
		log.Printf("✅ [%s] 无代币残留，最终确保完成", req.AccountID)
		return nil
	}

	// 5. 如果有代币，检查价值并执行最终卖出
	marketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {
		log.Printf("⚠️ [%s] 无法获取代币价格: %v", req.AccountID, err)
		// 无法获取价格时，如果代币数量>0.1，强制卖出
		if totalAmount > 0.1 {
			return executeFinalForceSell(req, totalAmount)
		}
		return nil
	}

	tokenValue := totalAmount * marketPrice
	log.Printf("📊 [%s] 代币价值: %.6f × %.8f = %.4f USDT", req.AccountID, totalAmount, marketPrice, tokenValue)

	// 6. 如果代币价值≥0.1 USDT，执行最终卖出
	if tokenValue >= 0.1 {
		log.Printf("💰 [%s] 代币价值%.4f USDT ≥ 0.1 USDT，执行最终卖出", req.AccountID, tokenValue)
		return executeFinalForceSell(req, totalAmount)
	}

	log.Printf("✅ [%s] 代币价值%.4f USDT < 0.1 USDT，最终确保完成", req.AccountID, tokenValue)
	return nil
}

// executeFinalForceSell 执行最终强制卖出（确保最后一笔是卖单）
func executeFinalForceSell(req *TradeRequest, tokenAmount float64) error {
	log.Printf("💰 [%s] 开始最终强制卖出: %.6f %s", req.AccountID, tokenAmount, req.BaseAsset)

	// 1. 获取当前市场价
	marketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {
		log.Printf("⚠️ [%s] 无法获取市场价，使用极低价格", req.AccountID)
		marketPrice = 0.00000001
	}

	// 2. 使用市场价的95%确保快速成交
	finalSellPrice := marketPrice * 0.95
	finalSellPrice = adjustPricePrecision(finalSellPrice, req.PricePrecision)

	log.Printf("💰 [%s] 最终卖出价格: %.12f (市场价95%%)", req.AccountID, finalSellPrice)

	// 3. 计算卖出数量（不保留代币）
	sellAmount := tokenAmount
	adjustedSellAmount := adjustTokenAmountForBinance(finalSellPrice, sellAmount, req.PricePrecision)
	if adjustedSellAmount != sellAmount {
		log.Printf("🔢 [%s] 最终卖出数量调整: %.6f → %.6f", req.AccountID, sellAmount, adjustedSellAmount)
		sellAmount = adjustedSellAmount
	}

	if sellAmount <= 0 {
		log.Printf("⚠️ [%s] 调整后数量为0，无法执行最终卖出", req.AccountID)
		return fmt.Errorf("调整后数量为0")
	}

	// 4. 执行最终卖出（最多尝试3次）
	for attempt := 1; attempt <= 3; attempt++ {
		log.Printf("🔄 [%s] 最终卖出第%d次尝试", req.AccountID, attempt)

		orderID, success := placeOrderWithLimitedRetry(OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      finalSellPrice,
			Quantity:   sellAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: req.Csrftoken,
			Cookie:    req.Cookie,
		}, "FINAL_SELL", 2)

		if success && orderID != "" {
			log.Printf("✅ [%s] 最终卖出订单成功: %s", req.AccountID, orderID)

			// 等待成交确认
			sellTime := time.Now().UnixMilli()
			for i := 0; i < 15; i++ { // 等待3秒
				time.Sleep(200 * time.Millisecond)
				if checkSellOrderHistory(sellTime, req.Csrftoken, req.Cookie) {
					log.Printf("✅ [%s] 最终卖出成交确认，确保流程完成", req.AccountID)
					return nil
				}
			}

			// 如果3秒内未成交，取消订单并降价重试
			log.Printf("⏰ [%s] 最终卖出未及时成交，取消订单并降价重试", req.AccountID)
			cancelOrder(orderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie)
			time.Sleep(500 * time.Millisecond)
		}

		// 降价重试（每次降价5%）
		if attempt < 3 {
			finalSellPrice = finalSellPrice * 0.95
			finalSellPrice = adjustPricePrecision(finalSellPrice, req.PricePrecision)
			log.Printf("📉 [%s] 最终卖出降价重试: %.12f", req.AccountID, finalSellPrice)
		}
	}

	log.Printf("❌ [%s] 最终卖出3次尝试全部失败", req.AccountID)
	return fmt.Errorf("最终卖出失败")
}

// isCleaningActive 检查是否正在清空
func isCleaningActive() bool {
	cleaningMutex.Lock()
	defer cleaningMutex.Unlock()
	return isCleaningInProgress
}

// setAccountAsyncState 设置账户异步状态
func setAccountAsyncState(accountID string, stateType string, active bool) {
	asyncStateMutex.Lock()
	defer asyncStateMutex.Unlock()

	if accountAsyncStates[accountID] == nil {
		accountAsyncStates[accountID] = &AccountAsyncState{}
	}

	state := accountAsyncStates[accountID]
	state.LastUpdateTime = time.Now()

	switch stateType {
	case "hanging":
		state.HasHangingOrder = active
	case "decrement":
		state.HasAsyncDecrement = active
	case "cleanup":
		state.HasCleanup = active
	}

	log.Printf("🔄 [%s] 异步状态更新: %s=%v", accountID, stateType, active)
}

// getAccountAsyncState 获取账户异步状态
func getAccountAsyncState(accountID string) *AccountAsyncState {
	asyncStateMutex.RLock()
	defer asyncStateMutex.RUnlock()

	if state, exists := accountAsyncStates[accountID]; exists {
		// 返回副本，避免并发修改
		return &AccountAsyncState{
			HasHangingOrder:   state.HasHangingOrder,
			HasAsyncDecrement: state.HasAsyncDecrement,
			HasCleanup:        state.HasCleanup,
			LastUpdateTime:    state.LastUpdateTime,
		}
	}

	return &AccountAsyncState{}
}

// isAccountBusy 检查账户是否有异步操作在进行
func isAccountBusy(accountID string) bool {
	state := getAccountAsyncState(accountID)
	return state.HasHangingOrder || state.HasAsyncDecrement || state.HasCleanup
}

// resetAccountAsyncState 重置账户的所有异步状态
func resetAccountAsyncState(accountID string) {
	asyncStateMutex.Lock()
	defer asyncStateMutex.Unlock()
	
	if accountAsyncStates[accountID] != nil {
		log.Printf("🧹 [%s] 重置所有异步状态", accountID)
		delete(accountAsyncStates, accountID)
	}
}

// cleanupExpiredAsyncStates 清理过期的异步状态
func cleanupExpiredAsyncStates() {
	asyncStateMutex.Lock()
	defer asyncStateMutex.Unlock()

	now := time.Now()
	for accountID, state := range accountAsyncStates {
		// 如果状态超过10分钟没更新，认为已过期
		if now.Sub(state.LastUpdateTime) > 10*time.Minute {
			delete(accountAsyncStates, accountID)
			log.Printf("🧹 清理过期异步状态: %s", accountID)
		}
	}
}

// 获取代币卖出状态的键
func getTokenSellStatusKey(accountID, tokenAddress string) string {
	return accountID + "_" + tokenAddress
}

// 设置代币已卖出状态
func setTokenSold(accountID, tokenAddress string, soldBy string, soldPrice, soldAmount float64) {
	sellStatusMutex.Lock()
	defer sellStatusMutex.Unlock()
	
	key := getTokenSellStatusKey(accountID, tokenAddress)
	tokenSellStatuses[key] = &TokenSellStatus{
		IsSold:         true,
		SoldTime:       time.Now(),
		SoldBy:         soldBy,
		SoldPrice:      soldPrice,
		SoldAmount:     soldAmount,
		LastUpdateTime: time.Now(),
	}
	
	log.Printf("🔔 [%s] 设置代币 %s 已卖出状态 - 方式: %s, 价格: %.8f, 数量: %.6f", 
		accountID, tokenAddress, soldBy, soldPrice, soldAmount)
}

// 检查代币是否已卖出
func isTokenSold(accountID, tokenAddress string) bool {
	sellStatusMutex.RLock()
	defer sellStatusMutex.RUnlock()
	
	key := getTokenSellStatusKey(accountID, tokenAddress)
	status, exists := tokenSellStatuses[key]
	return exists && status.IsSold
}

// 获取代币卖出状态
func getTokenSellStatus(accountID, tokenAddress string) *TokenSellStatus {
	sellStatusMutex.RLock()
	defer sellStatusMutex.RUnlock()
	
	key := getTokenSellStatusKey(accountID, tokenAddress)
	if status, exists := tokenSellStatuses[key]; exists {
		return status
	}
	return nil
}

// 清理过期的代币卖出状态
func cleanupExpiredTokenSellStatuses() {
	sellStatusMutex.Lock()
	defer sellStatusMutex.Unlock()
	
	now := time.Now()
	for key, status := range tokenSellStatuses {
		// 如果状态超过30分钟没更新，认为已过期
		if now.Sub(status.LastUpdateTime) > 30*time.Minute {
			delete(tokenSellStatuses, key)
			log.Printf("🧹 清理过期代币卖出状态: %s", key)
		}
	}
}

// CleanupManager 定时清空管理器
type CleanupManager struct {
	timer        *time.Ticker
	stopChan     chan bool
	running      bool
	tokenAddress string
	mutex        sync.Mutex
}

// 全局定时清空管理器
var globalCleanupManager = &CleanupManager{
	stopChan: make(chan bool, 1),
}

// startTokenCleanupIfNotRunning 如果定时清空未运行则启动 (改进版)
func startTokenCleanupIfNotRunning(req *TradeRequest) {
	globalCleanupManager.mutex.Lock()
	defer globalCleanupManager.mutex.Unlock()

	// 如果已经在运行且是同一个token，不需要重新启动
	if globalCleanupManager.running && globalCleanupManager.tokenAddress == req.TokenAddress {
		return
	}

	// 如果在运行但是不同token，先停止旧的
	if globalCleanupManager.running && globalCleanupManager.tokenAddress != req.TokenAddress {
		log.Printf("⏹️ 停止旧token的定时清空: %s", globalCleanupManager.tokenAddress)
		globalCleanupManager.stopChan <- true
		if globalCleanupManager.timer != nil {
			globalCleanupManager.timer.Stop()
		}
		globalCleanupManager.running = false
	}

	// 启动新的定时清空
	log.Printf("⏰ 启动定时清空功能: %s, 间隔1分钟", req.TokenAddress)

	globalCleanupManager.timer = time.NewTicker(1 * time.Minute)
	globalCleanupManager.running = true
	globalCleanupManager.tokenAddress = req.TokenAddress

	go func() {
		defer func() {
			globalCleanupManager.mutex.Lock()
			globalCleanupManager.running = false
			globalCleanupManager.mutex.Unlock()
		}()

		for {
			select {
			case <-globalCleanupManager.timer.C:
				log.Printf("🧹 执行定时清空: %s", req.TokenAddress)

				if err := forceCleanToken(req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, req.PricePrecision); err != nil {
					log.Printf("❌ 定时清空失败: %v", err)
				} else {
					log.Printf("✅ 定时清空完成: %s", req.TokenAddress)

					// 🔧 新增：检查是否应该停止定时清空
					// 如果清空成功且代币价值<1 USDT，停止定时清空
					if shouldStopCleanup(req.TokenAddress, req.Csrftoken, req.Cookie) {
						log.Printf("⏹️ 代币价值已低于1 USDT，停止定时清空: %s", req.TokenAddress)
						return
					}
				}
			case <-globalCleanupManager.stopChan:
				log.Printf("⏹️ 定时清空收到停止信号")
				return
			}
		}
	}()

	log.Printf("✅ 定时清空已启动: %s", req.TokenAddress)
}

// stopTokenCleanup 停止指定token的定时清空
func stopTokenCleanup(tokenAddress string) {
	globalCleanupManager.mutex.Lock()
	defer globalCleanupManager.mutex.Unlock()

	if !globalCleanupManager.running {
		return
	}

	if globalCleanupManager.tokenAddress != tokenAddress {
		return
	}

	log.Printf("⏹️ 停止定时清空: %s (清空完成)", tokenAddress)

	// 发送停止信号
	select {
	case globalCleanupManager.stopChan <- true:
	default:
	}

	// 停止定时器
	if globalCleanupManager.timer != nil {
		globalCleanupManager.timer.Stop()
	}

	globalCleanupManager.running = false
	globalCleanupManager.tokenAddress = ""
}

// shouldStopCleanup 检查是否应该停止定时清空
func shouldStopCleanup(tokenAddress, csrftoken, cookie string) bool {
	// 查询当前余额
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {
		log.Printf("⚠️ 检查停止清空时余额查询失败: %v", err)
		return false
	}

	totalAmount, _ := strconv.ParseFloat(balance.Amount, 64)
	if totalAmount <= 0 {
		return true
	}

	// 获取代币价格计算价值
	marketPrice, err := price.GetTokenPriceWithPrecision(tokenAddress, DEFAULT_CHAIN_ID, 8)
	if err != nil {
		log.Printf("⚠️ 检查停止清空时价格获取失败: %v", err)
		// 如果无法获取价格，按数量判断
		return totalAmount <= 1.0
	}

	tokenValue := totalAmount * marketPrice
	log.Printf("📊 停止清空检查: %.6f × %.8f = %.2f USDT", totalAmount, marketPrice, tokenValue)

	return tokenValue < 1.0
}

// hangOrderAtQian8Loss 千8挂单策略
func hangOrderAtQian8Loss(req *TradeRequest, buyPrice, sellTokenAmount float64, startTime time.Time) TradeResponse {
	log.Printf("📌 [%s] 开始千8挂单策略", req.AccountID)

	// 检查代币数量是否为0
	if sellTokenAmount <= 0 {
		log.Printf("⚠️ [%s] 千8挂单代币数量为0，跳过卖单 - 数量: %.0f", req.AccountID, sellTokenAmount)
		return TradeResponse{
			Success: false,
			Message: "千8挂单代币数量为0，无法下单",
		}
	}

	// 获取当前市场价
	currentMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {

		return TradeResponse{Success: false, Message: "千8挂单获取价格失败"}
	}
	currentMarketPrice = adjustPricePrecision(currentMarketPrice, req.PricePrecision)

	// 计算千8磨损的价格
	qian8Price := buyPrice * 0.992 // 千分之8磨损
	qian8Price = adjustPricePrecision(qian8Price, req.PricePrecision)

	log.Printf("💰 [%s] 千8挂单价格: %.12f (买入价: %.12f, 市场价: %.12f)",
		req.AccountID, qian8Price, buyPrice, currentMarketPrice)
		
	// 🚨 修改：提高最低价值阈值，避免币安拒绝下单
	tokenValue := qian8Price * sellTokenAmount
	if tokenValue < 1.0 { // 如果代币总价值低于1.0 USDT，则中断卖出
		log.Printf("💸 [%s] 代币价值过低(%.8f USDT < 1.0 USDT)，币安会拒绝下单，跳过千8挂单操作", 
			req.AccountID, tokenValue)
		log.Printf("📊 [%s] 代币详情: 数量=%.6f, 价格=%.8f", req.AccountID, sellTokenAmount, qian8Price)
		// 更新最后交易时间，确保系统可以继续其他操作
		updateLastTradeTime(req.AccountID)
		return TradeResponse{
			Success:     true, // 标记为成功，避免系统继续尝试卖出
			Message:     "代币价值过低(< 1.0 USDT)，币安会拒绝下单，跳过千8挂单操作",
			BuyPrice:    buyPrice,
			SellPrice:   qian8Price,
			TokenAmount: sellTokenAmount,
			Profit:      0,
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}
	}

	// 调整代币数量以符合币安规定
	adjustedTokenAmount := adjustTokenAmountForBinance(qian8Price, sellTokenAmount, req.PricePrecision)
	if adjustedTokenAmount != sellTokenAmount {
		log.Printf("🔢 [%s] 千8挂单数量调整: %.0f → %.0f", req.AccountID, sellTokenAmount, adjustedTokenAmount)
		sellTokenAmount = adjustedTokenAmount
	}

	// 挂千8磨损价格的卖单
	hangingSellOrderID, hangingSuccess := placeOrderWithRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      qian8Price,
		Quantity:   sellTokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellTokenAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, req.AccountID)

	if hangingSuccess {
		log.Printf("📌 [%s] 千8挂单成功: %s, 价格: %.12f", req.AccountID, hangingSellOrderID, qian8Price)

		// 🔧 优化：启动异步监控千8挂单，等待2分钟
		log.Printf("⏰ [%s] 启动异步监控千8挂单，等待2分钟", req.AccountID)
		go monitorQian8OrderAsync(req, hangingSellOrderID, qian8Price, sellTokenAmount, buyPrice, startTime, req.USDTAmount)

		// 立即返回，不阻塞主线程
		return TradeResponse{
			Success:     true,
			Message:     "千8挂单已提交，异步监控中",
			BuyPrice:    buyPrice,
			SellPrice:   qian8Price,
			TokenAmount: sellTokenAmount,
			Profit:      0, // 暂时未知，异步处理中
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}

	} else {

		// 挂单失败，尝试市场价强制卖出
		forceSellOrderID, forceSellSuccess := placeOrderWithRetry(OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      currentMarketPrice,
			Quantity:   sellTokenAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellTokenAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: req.Csrftoken,
			Cookie:    req.Cookie,
		}, req.AccountID)

		if forceSellSuccess {
			log.Printf("✅ [%s] 市场价强制卖出成功: %s", req.AccountID, forceSellOrderID)

			// 记录市场价强制卖出信息 (不重复统计)
			usdtAmount := currentMarketPrice * sellTokenAmount
			profit := (currentMarketPrice - buyPrice) * sellTokenAmount
			log.Printf("📊 [%s] 市场价强制卖出: %.0f个代币, %.6f USDT, 利润: %.6f USDT",
				req.AccountID, sellTokenAmount, usdtAmount, profit)

			return TradeResponse{
				Success:     true,
				Message:     "市场价强制卖出",
				BuyPrice:    buyPrice,
				SellPrice:   currentMarketPrice,
				TokenAmount: sellTokenAmount,
				Profit:      profit,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		} else {
			log.Printf("🚨 [%s] 市场价强制卖出也失败，执行最终极限清理", req.AccountID)

			// 🔧 增强：使用极限价格强制卖出
			extremePrice := currentMarketPrice * 0.85 // 市场价的85%
			extremePrice = adjustPricePrecision(extremePrice, req.PricePrecision)

			log.Printf("💰 [%s] 极限清理价格: %.12f (市场价85%%)", req.AccountID, extremePrice)

			extremeOrderID, extremeSuccess := placeOrderWithLimitedRetry(OrderRequest{
				BaseAsset:  req.BaseAsset,
				QuoteAsset: "USDT",
				Side:       "SELL",
				Price:      extremePrice,
				Quantity:   sellTokenAmount,
				PaymentDetails: []PaymentDetail{{
					Amount:            sellTokenAmount,
					AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
					PaymentWalletType: "ALPHA",
				}},
				Csrftoken: req.Csrftoken,
				Cookie:    req.Cookie,
			}, "EXTREME_CLEANUP", 2)

			if extremeSuccess && extremeOrderID != "" {
				log.Printf("✅ [%s] 极限清理订单成功: %s", req.AccountID, extremeOrderID)

				// 等待成交确认
				extremeTime := time.Now().UnixMilli()
				for i := 0; i < 15; i++ { // 等待3秒
					time.Sleep(200 * time.Millisecond)
					if checkSellOrderHistory(extremeTime, req.Csrftoken, req.Cookie) {
						log.Printf("✅ [%s] 极限清理成交确认", req.AccountID)

						extremeProfit := (extremePrice - buyPrice) * sellTokenAmount
						return TradeResponse{
							Success:     true,
							Message:     "极限清理成功",
							BuyPrice:    buyPrice,
							SellPrice:   extremePrice,
							TokenAmount: sellTokenAmount,
							Profit:      extremeProfit,
							ExecuteTime: time.Since(startTime).Milliseconds(),
						}
					}
				}

				// 如果3秒内未成交，取消订单
				log.Printf("⏰ [%s] 极限清理未及时成交，取消订单", req.AccountID)
				cancelOrder(extremeOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie)
			}

			// 极限清理也失败，启动异步兜底
			log.Printf("🚨 [%s] 极限清理失败，启动异步兜底", req.AccountID)
			go asyncFastDecrement(req, buyPrice, sellTokenAmount, currentMarketPrice)

			// 返回特殊状态，表示已启动异步处理
			marketLoss := (buyPrice - currentMarketPrice) * sellTokenAmount
			return TradeResponse{
				Success:     true, // 标记为成功，因为已启动异步处理
				Message:     "极限清理失败，已启动异步兜底",
				BuyPrice:    buyPrice,
				SellPrice:   currentMarketPrice,
				TokenAmount: sellTokenAmount,
				Profit:      -marketLoss,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		}
	}
}

// executeDecrementRetry 执行递减重试策略
func executeDecrementRetry(req *TradeRequest, buyPrice, sellTokenAmount, currentMarketPrice float64, startTime time.Time) TradeResponse {
	log.Printf("🔄 [%s] 开始递减重试策略", req.AccountID)

	// 首先检查代币是否已被其他流程卖出
	if isTokenSold(req.AccountID, req.TokenAddress) {
		status := getTokenSellStatus(req.AccountID, req.TokenAddress)
		log.Printf("✅ [%s] 代币已被其他流程卖出，停止递减重试 - 方式: %s, 价格: %.8f, 数量: %.6f", 
			req.AccountID, status.SoldBy, status.SoldPrice, status.SoldAmount)
		
		// 返回成功响应，避免继续尝试卖出
		return TradeResponse{
			Success:     true,
			Message:     fmt.Sprintf("代币已被%s方式卖出，无需递减重试", status.SoldBy),
			BuyPrice:    buyPrice,
			SellPrice:   status.SoldPrice,
			TokenAmount: status.SoldAmount,
			Profit:      (status.SoldPrice - buyPrice) * status.SoldAmount,
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}
	}
	
	// 检查代币数量是否为0
	if sellTokenAmount <= 0 {
		log.Printf("⚠️ [%s] 递减重试代币数量为0，跳过卖单 - 数量: %.0f", req.AccountID, sellTokenAmount)
		return TradeResponse{
			Success: false,
			Message: "递减重试代币数量为0，无法下单",
		}
	}
	
	// 再次检查代币余额，确认是否还有可卖代币
	tokenBalance, balErr := getTokenBalance(req.TokenAddress, req.Csrftoken, req.Cookie)
	if balErr == nil {
		freeAmount, _ := strconv.ParseFloat(tokenBalance.Free, 64)
		if freeAmount <= 0 {
			log.Printf("✅ [%s] 代币余额为0，可能已被卖出，停止递减重试", req.AccountID)
			// 设置代币已卖出状态，但卖出方式未知
			setTokenSold(req.AccountID, req.TokenAddress, "unknown", currentMarketPrice, sellTokenAmount)
			return TradeResponse{
				Success:     true,
				Message:     "代币余额为0，无需递减重试",
				BuyPrice:    buyPrice,
				SellPrice:   currentMarketPrice,
				TokenAmount: sellTokenAmount,
				Profit:      (currentMarketPrice - buyPrice) * sellTokenAmount,
				ExecuteTime: time.Since(startTime).Milliseconds(),
			}
		} else if freeAmount < sellTokenAmount {
			log.Printf("📊 [%s] 递减重试更新卖出数量: %.6f → %.6f (使用当前可用余额)", 
				req.AccountID, sellTokenAmount, freeAmount)
			sellTokenAmount = freeAmount
		}
	}

	// 🔧 递减策略：4步递减
	decrementSteps := []float64{
		0.0002, // 万2（第1步）
		0.001,  // 千1（第2步）
		0.005,  // 千5（第3步）
		0.1,    // 百10（第4步，最终限制）
	}

	currentSellPrice := 0.0
	for step, decrementRate := range decrementSteps {
		// 按递减率计算价格
		if currentMarketPrice > buyPrice {
			// 有利润情况：从市场价-万1开始递减
			currentSellPrice = currentMarketPrice * (0.9999 - decrementRate)
		} else {
			// 等价情况：从买入价-万1开始递减
			currentSellPrice = buyPrice * (0.9999 - decrementRate)
		}
		currentSellPrice = adjustPricePrecision(currentSellPrice, req.PricePrecision)

		// 计算当前磨损
		currentLoss := (buyPrice - currentSellPrice) / buyPrice

		// 计算递减幅度的万分比表示
		decrementWanFen := decrementRate * 10000
		lossWanFen := currentLoss * 10000

		if currentLoss < 0 {
			profitWanFen := -lossWanFen
			log.Printf("💰 [%s] 递减第%d步(%.0f万分): %.12f, 利润: %.2f万分",
				req.AccountID, step+1, decrementWanFen, currentSellPrice, profitWanFen)
		} else {
			log.Printf("💰 [%s] 递减第%d步(%.0f万分): %.12f, 磨损: %.2f万分",
				req.AccountID, step+1, decrementWanFen, currentSellPrice, lossWanFen)
		}

		// 调整代币数量以符合币安规定
		adjustedTokenAmount := adjustTokenAmountForBinance(currentSellPrice, sellTokenAmount, req.PricePrecision)
		if adjustedTokenAmount != sellTokenAmount {
			log.Printf("🔢 [%s] 递减第%d步数量调整: %.0f → %.0f",
				req.AccountID, step+1, sellTokenAmount, adjustedTokenAmount)
			sellTokenAmount = adjustedTokenAmount
		}

		// 下单
		newSellOrderID, newSellSuccess := placeOrderWithRetry(OrderRequest{
			BaseAsset:  req.BaseAsset,
			QuoteAsset: "USDT",
			Side:       "SELL",
			Price:      currentSellPrice,
			Quantity:   sellTokenAmount,
			PaymentDetails: []PaymentDetail{{
				Amount:            sellTokenAmount,
				AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
				PaymentWalletType: "ALPHA",
			}},
			Csrftoken: req.Csrftoken,
			Cookie:    req.Cookie,
		}, req.AccountID)

		if !newSellSuccess {
			if newSellOrderID == "INSUFFICIENT_BALANCE" {
				log.Printf("✅ [%s] 递减第%d步检测到余额不足错误，代币可能已被其他操作卖出，视为成功", req.AccountID, step+1)
				
				// 设置代币已卖出状态
				setTokenSold(req.AccountID, req.TokenAddress, "auto_sold", currentSellPrice, sellTokenAmount)
				
				// 更新最后交易时间
				updateLastTradeTime(req.AccountID)
				
				// 返回成功响应
				return TradeResponse{
					Success:     true,
					Message:     "检测到余额不足错误，代币可能已被其他操作卖出",
					BuyPrice:    buyPrice,
					SellPrice:   currentSellPrice,
					TokenAmount: sellTokenAmount,
					Profit:      (currentSellPrice - buyPrice) * sellTokenAmount,
					ExecuteTime: time.Since(startTime).Milliseconds(),
				}
			} else {
				continue
			}
		}

		// 🔥 刷量优化：减少递减等待时间
		newSellTime := time.Now().UnixMilli()
		var waitTime int
		if isVolumeMode {
			waitTime = 2 // 刷量模式：400ms快速检查
		} else {
			waitTime = 3 // 正常模式：600ms
		}

		for i := 0; i < waitTime; i++ { // 动态等待时间
			time.Sleep(200 * time.Millisecond)

			filledQuantity, remainingQuantity := checkOrderFillStatus(newSellTime, req.Csrftoken, req.Cookie)
			if filledQuantity > 0 {
				if remainingQuantity == 0 {
					// 全部成交
					log.Printf("✅ [%s] 递减重试全部成交 (第%d步): %.0f 个", req.AccountID, step+1, filledQuantity)
					profit := (currentSellPrice - buyPrice) * sellTokenAmount
					executeTime := time.Since(startTime).Milliseconds()

					// 记录递减重试成交信息 (不重复统计)
					usdtAmount := currentSellPrice * sellTokenAmount
					log.Printf("📊 [%s] 递减重试成交: %.0f个代币, %.6f USDT, 利润: %.6f USDT",
						req.AccountID, sellTokenAmount, usdtAmount, profit)

					return TradeResponse{
						Success:     true,
						Message:     fmt.Sprintf("递减重试成交 (第%d步)", step+1),
						BuyPrice:    buyPrice,
						SellPrice:   currentSellPrice,
						TokenAmount: sellTokenAmount,
						Profit:      profit,
						ExecuteTime: executeTime,
					}
				} else {
					// 部分成交，取消订单并处理剩余代币
					log.Printf("⚠️ [%s] 递减重试部分成交 (第%d步): %.0f/%.0f 个，剩余: %.0f 个",
						req.AccountID, step+1, filledQuantity, sellTokenAmount, remainingQuantity)

					// 取消当前订单
					cancelOrderWithVerification(newSellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie, req.AccountID)

					// 记录已成交的部分 (不重复统计)
					filledUSDTAmount := currentSellPrice * filledQuantity
					filledProfit := (currentSellPrice - buyPrice) * filledQuantity
					log.Printf("📊 [%s] 递减重试部分成交: %.0f个代币, %.6f USDT, 利润: %.6f USDT",
						req.AccountID, filledQuantity, filledUSDTAmount, filledProfit)

					// 处理剩余代币
					partialResult := handlePartialFill(req, buyPrice, remainingQuantity, startTime)

					// 返回部分成交的结果
					return TradeResponse{
						Success:     partialResult.Success,
						Message:     fmt.Sprintf("递减重试部分成交 (第%d步)，剩余已处理", step+1),
						BuyPrice:    buyPrice,
						SellPrice:   currentSellPrice,
						TokenAmount: filledQuantity, // 返回已成交的数量
						Profit:      filledProfit,
						ExecuteTime: time.Since(startTime).Milliseconds(),
					}
				}
			}
		}

		// 动态等待时间内未成交，取消订单继续下一步递减
		if isVolumeMode {
			log.Printf("⏰ [%s] 递减第%d步400ms未成交，取消订单: %s", req.AccountID, step+1, newSellOrderID)
		} else {
			log.Printf("⏰ [%s] 递减第%d步600ms未成交，取消订单: %s", req.AccountID, step+1, newSellOrderID)
		}
		cancelOrderWithVerification(newSellOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie, req.AccountID)
	}

	// 🔧 增强：递减重试完成后，先尝试最终强制卖出
	log.Printf("⚠️ [%s] 递减重试完成但未成交，尝试最终强制卖出", req.AccountID)

	// 获取当前市场价
	currentMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {
		log.Printf("⚠️ [%s] 无法获取市场价，使用极低价格强制卖出", req.AccountID)
		currentMarketPrice = 0.00000001
	}

	// 使用市场价的90%进行最终强制卖出
	ultimatePrice := currentMarketPrice * 0.9
	ultimatePrice = adjustPricePrecision(ultimatePrice, req.PricePrecision)

	log.Printf("💰 [%s] 最终强制卖出价格: %.12f (市场价90%%)", req.AccountID, ultimatePrice)

	// 执行最终强制卖出
	finalOrderID, finalSuccess := placeOrderWithLimitedRetry(OrderRequest{
		BaseAsset:  req.BaseAsset,
		QuoteAsset: "USDT",
		Side:       "SELL",
		Price:      ultimatePrice,
		Quantity:   sellTokenAmount,
		PaymentDetails: []PaymentDetail{{
			Amount:            sellTokenAmount,
			AmountStr:         fmt.Sprintf("%.0f", sellTokenAmount),
			PaymentWalletType: "ALPHA",
		}},
		Csrftoken: req.Csrftoken,
		Cookie:    req.Cookie,
	}, "ULTIMATE_FORCE_SELL", 2)

	if finalSuccess && finalOrderID != "" {
		log.Printf("✅ [%s] 最终强制卖出订单成功: %s", req.AccountID, finalOrderID)

		// 等待成交确认
		finalSellTime := time.Now().UnixMilli()
		for i := 0; i < 10; i++ { // 等待2秒
			time.Sleep(200 * time.Millisecond)
			if checkSellOrderHistory(finalSellTime, req.Csrftoken, req.Cookie) {
				log.Printf("✅ [%s] 最终强制卖出成交确认", req.AccountID)

				// 计算最终结果
				finalProfit := (ultimatePrice - buyPrice) * sellTokenAmount
				return TradeResponse{
					Success:     true,
					Message:     "最终强制卖出成功",
					BuyPrice:    buyPrice,
					SellPrice:   ultimatePrice,
					TokenAmount: sellTokenAmount,
					Profit:      finalProfit,
					ExecuteTime: time.Since(startTime).Milliseconds(),
				}
			}
		}

		// 如果2秒内未成交，取消订单
		log.Printf("⏰ [%s] 最终强制卖出未及时成交，取消订单", req.AccountID)
		cancelOrder(finalOrderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie)
	}

	// 最终强制卖出也失败，进入千8挂单作为兜底
	log.Printf("⏰ [%s] 最终强制卖出失败，进入千8挂单兜底", req.AccountID)
	return hangOrderAtQian8Loss(req, buyPrice, sellTokenAmount, startTime)
}

// adjustTokenAmountForBinance 调整代币数量以符合币安规定
func adjustTokenAmountForBinance(price, originalQuantity float64, precision int) float64 {
	// 币安要求：
	// 1. 数量必须是整数
	// 2. 价格×数量必须精确到8位小数
	// 3. 最小金额要求（通常0.00000001 USDT）

	// 从原数量开始，逐步减少直到符合要求
	for reduction := 0.0; reduction <= 0.15; reduction += 0.002 { // 最多减少15%，每次0.2%
		adjustedQuantity := math.Floor(originalQuantity * (1.0 - reduction))
		if adjustedQuantity < 1 {
			break
		}

		// 计算期望金额，精确到8位小数
		expectedValue := price * adjustedQuantity
		expectedValue = math.Round(expectedValue*100000000) / 100000000

		// 验证币安要求
		if expectedValue >= 0.00000001 && // 最小金额要求
			adjustedQuantity == math.Floor(adjustedQuantity) && // 数量必须是整数
			expectedValue > 0 { // 金额必须大于0

			// 进一步验证：反向计算确保精度匹配
			reverseCheck := expectedValue / adjustedQuantity
			priceDiff := math.Abs(reverseCheck - price)
			if priceDiff < 0.000000001 { // 价格差异在可接受范围内
				return adjustedQuantity
			}
		}
	}

	// 如果都不行，返回保守的数量
	conservativeQuantity := math.Floor(originalQuantity * 0.95) // 减少5%
	if conservativeQuantity < 1 {
		conservativeQuantity = math.Floor(originalQuantity * 0.99) // 减少1%
	}
	if conservativeQuantity < 1 {
		conservativeQuantity = originalQuantity // 保持原数量
	}

	return conservativeQuantity
}

// cancelOrderWithVerification 取消订单并验证是否成功
func cancelOrderWithVerification(orderID, symbol, csrftoken, cookie string, accountID string) bool {

	// 尝试取消订单，最多3次
	cancelSuccess := false
	for attempt := 1; attempt <= 3; attempt++ {

		if cancelOrder(orderID, symbol, csrftoken, cookie) {

			// 验证订单是否真的被取消
			if checkOrderCanceled(orderID, csrftoken, cookie) {

				cancelSuccess = true
				break
			} else {
				log.Printf("⚠️ [%s] 订单取消API成功但订单仍活跃，继续重试: %s", accountID, orderID)
			}
		}

		if attempt < 3 {
			time.Sleep(300 * time.Millisecond) // 增加重试间隔
		}
	}

	if !cancelSuccess {

		// 尝试取消所有订单作为兜底
		if cancelAllOrders(csrftoken, cookie) {

			time.Sleep(500 * time.Millisecond) // 等待取消完成

			// 再次验证订单是否被取消
			if checkOrderCanceled(orderID, csrftoken, cookie) {

				return true
			} else {

				return false
			}
		} else {

			return false
		}
	}

	log.Printf("✅ [%s] 订单取消完成，代币已释放: %s", accountID, orderID)
	return true
}

// checkOrderFillStatus 检查订单成交状态，返回已成交数量和剩余数量
func checkOrderFillStatus(orderTime int64, csrftoken, cookie string) (float64, float64) {
	// 检查订单历史来确定成交状态
	if checkSellOrderHistory(orderTime, csrftoken, cookie) {
		// 如果在历史中找到，说明全部成交
		return 1.0, 0.0 // 返回示意值，实际应该返回真实数量
	}

	// 未找到成交记录，说明未成交
	return 0.0, 1.0 // 返回示意值，实际应该返回真实数量
}

// handlePartialFill 处理部分成交的剩余代币
func handlePartialFill(req *TradeRequest, buyPrice, remainingQuantity float64, startTime time.Time) TradeResponse {

	// 获取当前市场价
	currentMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {

		// 价格获取失败，启动异步递减处理剩余代币
		go asyncFastDecrement(req, buyPrice, remainingQuantity, buyPrice*0.99)
		return TradeResponse{
			Success:     true,
			Message:     "部分成交，剩余代币已启动异步处理",
			BuyPrice:    buyPrice,
			SellPrice:   buyPrice * 0.99,
			TokenAmount: remainingQuantity,
			Profit:      (buyPrice*0.99 - buyPrice) * remainingQuantity,
			ExecuteTime: time.Since(startTime).Milliseconds(),
		}
	}
	currentMarketPrice = adjustPricePrecision(currentMarketPrice, req.PricePrecision)

	// 计算当前磨损
	currentLoss := (buyPrice - currentMarketPrice) / buyPrice

	if currentLoss > 0.1 { // 磨损 > 百10

		// 磨损超过百10，使用千8挂单策略处理剩余代币
		return hangOrderAtQian8Loss(req, buyPrice, remainingQuantity, startTime)
	} else {

		// 磨损不大，直接按市场价卖出剩余代币
		return sellAtMarketPrice(req, buyPrice, remainingQuantity, currentMarketPrice, startTime)
	}
}

// monitorQian8OrderAsync 异步监控千8挂单
func monitorQian8OrderAsync(req *TradeRequest, orderID string, qian8Price, sellTokenAmount, buyPrice float64, startTime time.Time, originalUSDTAmount float64) {
	log.Printf("⏰ [%s] 开始异步监控千8挂单: %s", req.AccountID, orderID)

	// 设置千8挂单监控状态
	setAccountAsyncState(req.AccountID, "hanging", true)
	defer setAccountAsyncState(req.AccountID, "hanging", false)

	// 🔧 优化：监控2分钟（从3分钟缩短到2分钟）
	for i := 0; i < 120; i++ { // 2分钟，每1秒检查一次
		time.Sleep(1 * time.Second)

		if checkSellOrderHistory(time.Now().UnixMilli()-1000, req.Csrftoken, req.Cookie) {
			log.Printf("✅ [%s] 千8挂单异步成交: %s", req.AccountID, orderID)

			// 千8挂单完成，只更新最后交易时间，不计算交易额和交易次数
			buyInCost := buyPrice * sellTokenAmount
			sellOutRevenue := qian8Price * sellTokenAmount
			netLoss := buyInCost - sellOutRevenue // 净损益
			updateLastTradeTime(req.AccountID)

			if netLoss > 0 {
				log.Printf("📊 [%s] 千8挂单统计: 交易额=%.6f, 买入成本=%.6f, 卖出收入=%.6f, 亏损=%.6f",
					req.AccountID, buyInCost, buyInCost, sellOutRevenue, netLoss)
			} else {
				log.Printf("📊 [%s] 千8挂单统计: 交易额=%.6f, 买入成本=%.6f, 卖出收入=%.6f, 盈利=%.6f",
					req.AccountID, buyInCost, buyInCost, sellOutRevenue, -netLoss)
			}

			log.Printf("🎉 [%s] 千8挂单异步监控完成", req.AccountID)
			return
		}
	}

	// 🔧 优化：2分钟未成交，取消挂单并进入后续处理

	// 取消挂单
	cancelSuccess := false
	for attempt := 1; attempt <= 3; attempt++ {
		if cancelOrder(orderID, req.BaseAsset+"USDT", req.Csrftoken, req.Cookie) {
			cancelSuccess = true
			break
		} else {

			if attempt < 3 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	if !cancelSuccess {
		// 尝试取消所有订单作为兜底
		cancelAllOrders(req.Csrftoken, req.Cookie)
		time.Sleep(500 * time.Millisecond) // 等待取消完成
	}

	// 获取最新市场价
	latestMarketPrice, err := price.GetTokenPriceWithPrecision(req.TokenAddress, getChainID(req), req.PricePrecision)
	if err != nil {
		latestMarketPrice = qian8Price * 0.99 // 使用估算价格
	}
	latestMarketPrice = adjustPricePrecision(latestMarketPrice, req.PricePrecision)

	// 启动市场价强制卖出 (异步)
	executeAsyncMarketSell(req, buyPrice, sellTokenAmount, latestMarketPrice, startTime)
}

// executeAsyncMarketSell 执行异步市场价卖出
func executeAsyncMarketSell(req *TradeRequest, buyPrice, sellTokenAmount, marketPrice float64, startTime time.Time) {
	// 使用现有的市场价卖出逻辑，但不返回结果
	response := sellAtMarketPrice(req, buyPrice, sellTokenAmount, marketPrice, startTime)

	if response.Success {
		log.Printf("✅ [%s] 异步市场价卖出成功", req.AccountID)
		// 🔧 修正：卖出操作不需要统计交易额，买入交易额已在买入时统计
	} else {
		// 如果市场价卖出失败，启动异步递减万1
		log.Printf("🔄 [%s] 异步市场价失败，启动异步递减万1", req.AccountID)
		go asyncFastDecrement(req, buyPrice, sellTokenAmount, marketPrice)
	}
}

// hasLockedTokens 检查指定token是否有锁定余额
func hasLockedTokens(tokenAddress, csrftoken, cookie, accountID string) bool {
	balance, err := getTokenBalance(tokenAddress, csrftoken, cookie)
	if err != nil {
		return true // 查询失败时保守处理，假设有锁定
	}

	lockedAmount, _ := strconv.ParseFloat(balance.Locked, 64)

	if lockedAmount > 0 {
		log.Printf("🔒 [%s] 检测到代币锁定: %.6f %s", accountID, lockedAmount, balance.Symbol)
		return true
	} else {
		return false
	}
}

// performGlobalTokenCheck 执行全局代币检查
func performGlobalTokenCheck() {
	log.Printf("🌍 开始全局代币检查...")

	// 获取所有已注册的账户认证信息
	authMutex.RLock()
	accounts := make(map[string]*AccountAuth)
	for accountID, auth := range globalAccountAuths {
		accounts[accountID] = auth
	}
	authMutex.RUnlock()

	if len(accounts) == 0 {
		log.Printf("🌍 没有已注册的账户，跳过全局检查")
		return
	}

	checkedCount := 0
	foundCount := 0

	// 检查每个账户
	for accountID, auth := range accounts {
		log.Printf("🔍 [%s] 检查账户代币余额...", accountID)

		// 检查是否有停止的监控任务需要重启
		autoSellMutex.Lock()
		for taskID, task := range autoSellManager.tasks {
			if strings.Contains(taskID, accountID) && !task.IsActive {
				// 检查该代币是否仍有余额
				balance, err := getTokenBalance(task.TokenAddress, auth.Csrftoken, auth.Cookie)
				if err == nil && balance != nil {
					freeAmount, _ := strconv.ParseFloat(balance.Free, 64)
					if freeAmount > 1.0 {
						// 检查代币价值
						currentPrice, priceErr := price.GetTokenPriceWithPrecision(task.TokenAddress, getTaskChainID(task), 8)
						if priceErr == nil {
							tokenValue := freeAmount * currentPrice
							if tokenValue >= 1.0 { // 价值>=1 USDT的代币
								foundCount++
								log.Printf("💰 [%s] 全局检查发现停止的监控任务有高价值代币: %s, 数量: %.6f, 价值: %.4f USDT",
									accountID, task.TokenAddress, freeAmount, tokenValue)

								// 重新启动监控任务
								task.IsActive = true
								task.StopChan = make(chan bool, 1)
								task.LastCheck = time.Now()

								go runAutoSellTask(task)
								log.Printf("🔄 [%s] 重新启动监控任务: %s", accountID, task.TokenAddress)
							}
						}
					}
				}
				checkedCount++
			}
		}
		autoSellMutex.Unlock()
	}

	log.Printf("🌍 全局代币检查完成: 检查了%d个任务，重启了%d个监控", checkedCount, foundCount)
}

// resetGlobalStateForNextTrade 重置全局状态，为下次交易做准备
func resetGlobalStateForNextTrade() {
	log.Printf("🔄 重置全局状态，准备接收新的交易请求")

	// 重置全局统计（保留历史数据，但清零当前交易数据）
	globalMutex.Lock()
	globalStats.TotalTargetVolume = 0
	globalStats.TotalCurrentVolume = 0
	globalStats.CompletionRate = 0
	globalStats.ActiveAccounts = 0
	globalMutex.Unlock()

	// 清理账号异步状态
	asyncStateMutex.Lock()
	for accountID := range accountAsyncStates {
		delete(accountAsyncStates, accountID)
	}
	asyncStateMutex.Unlock()

	// 🔧 增强：安全清理循环账号状态
	loopMutex.Lock()
	// 先发送停止信号给所有循环
	for accountID, stopChan := range loopingAccounts {
		select {
		case stopChan <- true:
			log.Printf("🛑 发送停止信号给循环账号: %s", accountID)
		default:
		}
	}
	// 等待一段时间让循环自然结束
	loopMutex.Unlock()
	time.Sleep(2 * time.Second)

	// 再清理状态
	loopMutex.Lock()
	loopingAccounts = make(map[string]chan bool)
	loopMutex.Unlock()

	// 🔧 增强：安全清理自动卖单管理器
	autoSellMutex.Lock()
	// 先停止所有任务
	for taskID, task := range autoSellManager.tasks {
		if task.IsActive {
			task.IsActive = false
			select {
			case task.StopChan <- true:
				log.Printf("🛑 停止自动卖单任务: %s", taskID)
			default:
			}
		}
	}
	autoSellMutex.Unlock()
	time.Sleep(1 * time.Second)

	// 再清理状态
	autoSellMutex.Lock()
	autoSellManager.tasks = make(map[string]*AutoSellTask)
	autoSellMutex.Unlock()

	// 🔧 新增：清理代币处理状态
	processingMutex.Lock()
	processingTokens = make(map[string]bool)
	processingMutex.Unlock()

	// 🚨 新增：清理账号统计数据，防止异步任务读取到旧数据
	statsMutex.Lock()
	for accountID := range accountStats {
		delete(accountStats, accountID)
	}
	statsMutex.Unlock()

	// 重置全局停止状态，允许新的任务执行
	globalStopMutex.Lock()
	isGlobalStopping = false
	globalStopTime = time.Time{}
	globalStopMutex.Unlock()

	// 🚨 新增：等待一小段时间，确保所有异步任务都能看到重置状态
	time.Sleep(100 * time.Millisecond)
}

// 🚀 处理速度模式设置
func handleSpeedMode(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == "GET" {
		// 查询当前速度模式
		currentMode := getCurrentSpeedMode()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"speed_mode":  currentMode.Name,
			"description": currentMode.Description,
			"config": map[string]interface{}{
				"trade_interval_seconds":   int(currentMode.TradeInterval.Seconds()),
				"max_orders_per_second":    currentMode.MaxOrdersPerSecond,
				"max_api_calls_per_second": currentMode.MaxAPICallsPerSecond,
				"api_call_interval_ms":     int(currentMode.APICallInterval.Nanoseconds() / 1000000),
				"volume_api_interval_ms":   int(currentMode.VolumeAPIInterval.Nanoseconds() / 1000000),
				"global_api_limit":         currentMode.GlobalAPILimit,
			},
			"成功":   true,
			"速度模式": currentMode.Name,
			"描述":   currentMode.Description,
		})
		return
	}

	if r.Method == "POST" {
		var req struct {
			SpeedMode string `json:"speed_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Invalid request format",
				"成功":      false,
				"消息":      "请求格式无效",
			})
			return
		}

		// 验证速度模式
		if req.SpeedMode == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Speed mode is required",
				"成功":      false,
				"消息":      "速度模式不能为空",
			})
			return
		}

		// 应用速度模式
		applySpeedMode(req.SpeedMode)
		currentMode := getCurrentSpeedMode()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"message":     fmt.Sprintf("Speed mode applied: %s", currentMode.Name),
			"speed_mode":  currentMode.Name,
			"description": currentMode.Description,
			"成功":          true,
			"消息":          fmt.Sprintf("速度模式已应用: %s", currentMode.Name),
			"速度模式":        currentMode.Name,
			"描述":          currentMode.Description,
		})
		return
	}
}

// 🚀 处理速度模式列表查询
func handleSpeedModes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 构建速度模式列表
	modes := make([]map[string]interface{}, 0)
	for key, mode := range speedModes {
		modes = append(modes, map[string]interface{}{
			"key":         key,
			"name":        mode.Name,
			"description": mode.Description,
			"config": map[string]interface{}{
				"trade_interval_seconds":   int(mode.TradeInterval.Seconds()),
				"max_orders_per_second":    mode.MaxOrdersPerSecond,
				"max_api_calls_per_second": mode.MaxAPICallsPerSecond,
				"api_call_interval_ms":     int(mode.APICallInterval.Nanoseconds() / 1000000),
				"volume_api_interval_ms":   int(mode.VolumeAPIInterval.Nanoseconds() / 1000000),
				"global_api_limit":         mode.GlobalAPILimit,
			},
		})
	}

	// 获取当前模式
	currentMode := getCurrentSpeedMode()
	currentKey := ""
	for key, mode := range speedModes {
		if mode.Name == currentMode.Name {
			currentKey = key
			break
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      true,
		"modes":        modes,
		"current_mode": currentKey,
		"成功":           true,
		"模式列表":         modes,
		"当前模式":         currentKey,
	})

	log.Printf("✅ 全局状态重置完成，服务已准备好接收新的交易请求")
}

// handleCompleteCleanup 处理完整清理请求（暂停+清理挂单+清理代币残留）
func handleCompleteCleanup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "只支持POST方法",
		})
		return
	}

	// 解析请求
	var req struct {
		AccountID      string `json:"account_id"`
		Csrftoken      string `json:"csrftoken"`
		Cookie         string `json:"cookie"`
		TokenAddress   string `json:"token_address"`   // 可选，指定要清理的代币地址
		BaseAsset      string `json:"base_asset"`      // 可选，指定要清理的基础资产
		PauseDuration  int    `json:"pause_duration"`  // 暂停时长（秒），默认300秒
		PricePrecision int    `json:"price_precision"` // 价格精度，默认8位
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "请求格式无效",
		})
		return
	}

	if req.AccountID == "" || req.Csrftoken == "" || req.Cookie == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "缺少必要参数: account_id, csrftoken, cookie",
		})
		return
	}

	// 设置默认值
	if req.PauseDuration <= 0 {
		req.PauseDuration = 300 // 默认暂停5分钟
	}
	if req.PricePrecision <= 0 {
		req.PricePrecision = 8 // 默认8位精度
	}

	// 1. 设置账户暂停状态
	duration := time.Duration(req.PauseDuration) * time.Second
	setAccountPauseStatus(req.AccountID, duration, "complete_cleanup")
	log.Printf("⏸️ [%s] 完整清理：设置暂停状态 %d 秒", req.AccountID, req.PauseDuration)

	// 2. 强制清理所有挂单
	cleanedCount := forceCleanupAllOrders(req.AccountID, req.Csrftoken, req.Cookie)
	log.Printf("🧹 [%s] 完整清理：清理了 %d 个挂单", req.AccountID, cleanedCount)

	// 3. 清理指定代币残留（如果提供了代币地址）
	tokenCleanResult := "未指定代币，跳过清理"
	if req.TokenAddress != "" {
		// 如果提供了代币地址但没有提供baseAsset，尝试推断
		if req.BaseAsset == "" {
			req.BaseAsset = inferBaseAssetFromTokenAddress(req.TokenAddress)
			log.Printf("🔍 [%s] 完整清理：推断基础资产为 %s", req.AccountID, req.BaseAsset)
		}

		// 执行代币清理
		err := forceCleanToken(req.TokenAddress, req.BaseAsset, req.Csrftoken, req.Cookie, req.PricePrecision)
		if err != nil {
			tokenCleanResult = fmt.Sprintf("清理失败: %v", err)
			log.Printf("❌ [%s] 完整清理：代币清理失败 %s: %v", req.AccountID, req.TokenAddress, err)
		} else {
			tokenCleanResult = "清理成功"
			log.Printf("✅ [%s] 完整清理：代币清理成功 %s", req.AccountID, req.TokenAddress)
		}
	}

	// 4. 返回结果
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "完整清理操作已执行",
		"results": map[string]interface{}{
			"account_id":       req.AccountID,
			"pause_status":     "已暂停",
			"pause_duration":   req.PauseDuration,
			"pause_end":        time.Now().Add(duration).Unix(),
			"orders_cleaned":   cleanedCount,
			"token_clean":      tokenCleanResult,
			"token_address":    req.TokenAddress,
			"base_asset":       req.BaseAsset,
			"price_precision":  req.PricePrecision,
		},
	})
}
