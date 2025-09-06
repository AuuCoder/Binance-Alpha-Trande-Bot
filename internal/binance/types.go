package binance

import "time"

// TradeRequest 交易请求
type TradeRequest struct {
	TokenAddress   string  `json:"token_address"`
	USDTAmount     float64 `json:"usdt_amount"`
	BaseAsset      string  `json:"base_asset"`
	Csrftoken      string  `json:"csrftoken"`
	Cookie         string  `json:"cookie"`
	AccountID      string  `json:"account_id"`
	PricePrecision int     `json:"price_precision"`
}

// TradeResponse 交易响应
type TradeResponse struct {
	Success     bool    `json:"success"`
	Message     string  `json:"message"`
	BuyPrice    float64 `json:"buy_price"`
	SellPrice   float64 `json:"sell_price"`
	TokenAmount float64 `json:"token_amount"`
	Profit      float64 `json:"profit"`
	ExecuteTime int64   `json:"execute_time_ms"`
	TradeID     string  `json:"trade_id"`
	Timestamp   int64   `json:"timestamp"`
}

// AutoTradeRequest 自动交易请求
type AutoTradeRequest struct {
	TradeRequest TradeRequest `json:"trade_request"`
	TargetVolume float64      `json:"target_volume"`
	AutoLoop     bool         `json:"auto_loop"`
	MinDelay     int          `json:"min_delay"`
	MaxDelay     int          `json:"max_delay"`
}

// OrderRequest 订单请求
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

// PaymentDetail 支付详情
type PaymentDetail struct {
	Amount            float64 `json:"amount"`
	AmountStr         string  `json:"amountStr"`
	PaymentWalletType string  `json:"paymentWalletType"`
}

// OrderResponse 订单响应
type OrderResponse struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	Success bool        `json:"success"`
}

// AccountStats 账户统计
type AccountStats struct {
	AccountID     string    `json:"account_id"`
	TotalVolume   float64   `json:"total_volume"`
	TargetVolume  float64   `json:"target_volume"`
	TradeCount    int       `json:"trade_count"`
	TotalLoss     float64   `json:"total_loss"`
	TotalLossRate float64   `json:"total_loss_rate"`
	IsLooping     bool      `json:"is_looping"`
	LastTradeTime time.Time `json:"last_trade_time"`
	AssignedNode  string    `json:"assigned_node"`
}

// GlobalStats 全局统计
type GlobalStats struct {
	TotalTargetVolume  float64 `json:"total_target_volume"`
	TotalCurrentVolume float64 `json:"total_current_volume"`
	TotalLoss          float64 `json:"total_loss"`
	TotalLossRate      float64 `json:"total_loss_rate"`
	CompletionRate     float64 `json:"completion_rate"`
	ActiveAccounts     int     `json:"active_accounts"`
	ProjectedLoss100k  float64 `json:"projected_loss_100k"`
	ActiveNodes        int     `json:"active_nodes"`
}

// NodeStats 节点统计
type NodeStats struct {
	NodeID         string  `json:"node_id"`
	TotalTrades    int     `json:"total_trades"`
	TotalVolume    float64 `json:"total_volume"`
	ActiveAccounts int     `json:"active_accounts"`
	SuccessRate    float64 `json:"success_rate"`
	AvgExecuteTime float64 `json:"avg_execute_time"`
	Status         string  `json:"status"`
	CPUUsage       float64 `json:"cpu_usage"`
	MemoryUsage    float64 `json:"memory_usage"`
	Uptime         int64   `json:"uptime"`
	LastHeartbeat  int64   `json:"last_heartbeat"`
}

// TradeResult 交易结果
type TradeResult struct {
	TradeID     string    `json:"trade_id"`
	Success     bool      `json:"success"`
	Message     string    `json:"message"`
	BuyPrice    float64   `json:"buy_price"`
	SellPrice   float64   `json:"sell_price"`
	TokenAmount float64   `json:"token_amount"`
	Profit      float64   `json:"profit"`
	ExecuteTime int64     `json:"execute_time_ms"`
	Timestamp   time.Time `json:"timestamp"`
	AccountID   string    `json:"account_id"`
	NodeID      string    `json:"node_id"`
}

// TaskInfo 任务信息
type TaskInfo struct {
	TaskID        string           `json:"task_id"`
	AccountID     string           `json:"account_id"`
	NodeID        string           `json:"node_id"`
	Status        string           `json:"status"` // running, stopped, completed, error
	Request       AutoTradeRequest `json:"request"`
	StartTime     time.Time        `json:"start_time"`
	EndTime       *time.Time       `json:"end_time,omitempty"`
	TradeCount    int              `json:"trade_count"`
	TotalVolume   float64          `json:"total_volume"`
	TotalProfit   float64          `json:"total_profit"`
	LastTradeTime *time.Time       `json:"last_trade_time,omitempty"`
}

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID        string               `json:"node_id"`
	NodeType      string               `json:"node_type"` // master, slave
	Address       string               `json:"address"`
	Status        string               `json:"status"` // online, offline, busy
	StartTime     time.Time            `json:"start_time"`
	LastHeartbeat time.Time            `json:"last_heartbeat"`
	ActiveTasks   map[string]*TaskInfo `json:"active_tasks"`
	Stats         NodeStats            `json:"stats"`
	Metadata      map[string]string    `json:"metadata"`
}

// Command 命令结构
type Command struct {
	CommandID   string      `json:"command_id"`
	CommandType string      `json:"command_type"` // execute_trade, start_auto, stop_auto, get_status
	TargetNode  string      `json:"target_node"`  // 目标节点ID，空表示任意节点
	Payload     interface{} `json:"payload"`
	Timestamp   int64       `json:"timestamp"`
	Priority    int         `json:"priority"`    // 优先级 1-10
	ExpireTime  int64       `json:"expire_time"` // 过期时间
}

// CommandResult 命令结果
type CommandResult struct {
	CommandID string      `json:"command_id"`
	Success   bool        `json:"success"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data"`
	NodeID    string      `json:"node_id"`
	Timestamp int64       `json:"timestamp"`
}

// PriceInfo 价格信息
type PriceInfo struct {
	TokenAddress string    `json:"token_address"`
	Price        float64   `json:"price"`
	Timestamp    time.Time `json:"timestamp"`
	Source       string    `json:"source"`
}

// HealthStatus 健康状态
type HealthStatus struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Uptime    time.Duration     `json:"uptime"`
	Services  map[string]string `json:"services"` // 各服务状态
}
