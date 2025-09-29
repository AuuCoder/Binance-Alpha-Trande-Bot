package price

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"math"
	"math/rand"

	"github.com/gorilla/websocket"
)

// PriceClient 价格客户端
type PriceClient struct {
	wsURL       string
	backupURLs  []string // 🔧 新增：备用WebSocket地址
	conn        *websocket.Conn
	mu          sync.RWMutex
	writeMu     sync.Mutex // 🔧 新增：WebSocket写入锁，防止并发写入
	subscribers map[string][]chan float64
	isRunning   bool
	stopChan    chan struct{}

	// 🚀 优化：价格缓存系统
	priceCache map[string]float64
	cacheTime  map[string]time.Time

	// 🚀 新增：持久订阅管理
	persistentSubscriptions map[string]bool // 跟踪已建立的持久订阅
	subscriptionMutex       sync.RWMutex    // 订阅管理锁

	// 🚀 新增：智能缓存配置
	cacheMaxAge       time.Duration // 缓存最大有效期
	emergencyCacheAge time.Duration // 紧急情况下的缓存有效期

	// 🔧 重连机制相关字段
	isReconnecting       bool
	reconnectAttempts    int
	maxReconnectAttempts int
	reconnectDelay       time.Duration
	maxReconnectDelay    time.Duration
	lastConnectTime      time.Time
	networkCheckHosts    []string
}

// TokenConfig 代币配置
type TokenConfig struct {
	ContractAddress string
	ChainID         string
	Symbol          string
}

// PriceMode 价格模式枚举
type PriceMode string

const (
	PriceModeLimit    PriceMode = "limit"    // 限价模式
	PriceModeMarket   PriceMode = "market"   // 链上模式
	PriceModeCombined PriceMode = "combined" // 链上+限价模式
	PriceModeAuto     PriceMode = "auto"     // 自动选择模式（默认）
)

// getKlineInterval 根据链ID获取对应的K线时间间隔
func getKlineInterval(chainID string) string {
	switch chainID {
	case "56": // BSC
		return "1s" // BSC链使用1秒间隔
	case "CT_501": // Solana链
		return "1s" // Solana链使用1秒间隔
	default:
		return "15m" // 其他链默认使用15分钟间隔
	}
}

// buildSubscriptionParam 构建订阅参数
func buildSubscriptionParam(contractAddress, chainID string, mode PriceMode) string {
	interval := getKlineInterval(chainID)
	
	// 统一使用新格式: came@{address}@{chainID}@kline_{interval}
	// 不再根据模式区分不同格式，而是在sendSubscription中处理多个订阅
	return fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)
}

// NewPriceClient 创建价格客户端
func NewPriceClient() *PriceClient {
	return &PriceClient{
		// 🔧 修正：使用正确的WebSocket URL
		wsURL:       "wss://nbstream.binance.com/w3w/wsa/stream",
		subscribers: make(map[string][]chan float64),
		stopChan:    make(chan struct{}),
		priceCache:  make(map[string]float64),
		cacheTime:   make(map[string]time.Time),

		// 🚀 新增：持久订阅管理
		persistentSubscriptions: make(map[string]bool),

			// 🚀 新增：智能缓存配置
	cacheMaxAge:       180 * time.Second, // 正常缓存3分钟，极大减少请求频率
	emergencyCacheAge: 30 * time.Minute,  // 紧急情况缓存30分钟

		// 🔧 重连配置
		maxReconnectAttempts: 5, // 减少重连次数
		reconnectDelay:       2 * time.Second,
		maxReconnectDelay:    30 * time.Second, // 减少最大延迟
		networkCheckHosts: []string{
			"8.8.8.8:53", // Google DNS
			"1.1.1.1:53", // Cloudflare DNS
		},
	}
}

// Connect 连接WebSocket
func (pc *PriceClient) Connect() error {
	// 🔧 新增：连接前检查网络
	if !pc.checkNetworkConnectivity() {
		return fmt.Errorf("network connectivity check failed")
	}

	// 🚨 新增：启动健康检查协程
	go pc.startHealthCheck()

	// 🚀 最快连接参数：优化所有超时时间
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second // 快速握手：5秒
	dialer.ReadBufferSize = 16384             // 增大读缓冲区：16KB
	dialer.WriteBufferSize = 16384            // 增大写缓冲区：16KB
	dialer.EnableCompression = true           // 启用压缩

	// 设置请求头，模拟浏览器行为
	headers := make(map[string][]string)
	headers["User-Agent"] = []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
	headers["Origin"] = []string{"https://www.binance.com"}

	conn, _, err := dialer.Dial(pc.wsURL, headers)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %v", err)
	}

	// 🔧 币安官方要求：设置合理的超时时间
	conn.SetReadDeadline(time.Now().Add(70 * time.Second))  // 70秒读超时（给PING/PONG留余量）
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second)) // 10秒写超时

	// 🔧 币安官方要求：处理服务器发送的PING，回复PONG
	conn.SetPingHandler(func(appData string) error {
		log.Printf("📨 收到服务器PING，立即回复PONG")
		// 必须在1分钟内回复PONG，payload与PING一致
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		// 🔧 使用安全写入方法防止并发写入
		err := pc.safeWriteMessage(websocket.PongMessage, []byte(appData))
		if err != nil {
			return err
		}
		// 重置读超时
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))

		return nil
	})

	// 保留PONG处理器（处理我们发送PING的回复，虽然现在不主动发PING）
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		return nil
	})

	// 🔧 获取写入锁，确保连接设置的原子性
	pc.writeMu.Lock()
	pc.mu.Lock()
	pc.conn = conn
	pc.isRunning = true
	pc.lastConnectTime = time.Now()
	pc.reconnectAttempts = 0 // 重置重连次数
	pc.isReconnecting = false
	pc.mu.Unlock()
	pc.writeMu.Unlock()

	go pc.readLoop()
	// 移除复杂的连接监控，让WebSocket自然处理连接状态

	return nil
}

// 🔧 新增：检查网络连通性
func (pc *PriceClient) checkNetworkConnectivity() bool {
	for _, host := range pc.networkCheckHosts {
		if pc.testSingleHost(host) {
			return true
		}
	}
	return false
}

// 🔧 新增：测试单个主机连通性
func (pc *PriceClient) testSingleHost(host string) bool {
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// Disconnect 断开连接
func (pc *PriceClient) Disconnect() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.isRunning = false
	close(pc.stopChan)

	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}

	// 关闭所有订阅通道
	for _, channels := range pc.subscribers {
		for _, ch := range channels {
			close(ch)
		}
	}
	pc.subscribers = make(map[string][]chan float64)
}

// SubscribePrice 订阅代币价格（使用默认模式）
func (pc *PriceClient) SubscribePrice(contractAddress, chainID string) (<-chan float64, error) {
	return pc.SubscribePriceWithMode(contractAddress, chainID, PriceModeAuto)
}

// SubscribePriceWithMode 订阅代币价格（指定模式）
func (pc *PriceClient) SubscribePriceWithMode(contractAddress, chainID string, mode PriceMode) (<-chan float64, error) {
	if !pc.isRunning {
		return nil, fmt.Errorf("client is not connected")
	}

	// 构建订阅参数
	param := buildSubscriptionParam(contractAddress, chainID, mode)

	// 创建价格通道
	priceChan := make(chan float64, 100)

	pc.mu.Lock()
	// 添加到订阅列表
	pc.subscribers[param] = append(pc.subscribers[param], priceChan)
	isFirstSubscriber := len(pc.subscribers[param]) == 1
	pc.mu.Unlock()

	// 如果是第一个订阅者，发送订阅请求
	if isFirstSubscriber {
		if err := pc.sendSubscription(param); err != nil {
			pc.mu.Lock()
			// 移除失败的订阅
			channels := pc.subscribers[param]
			for i, ch := range channels {
				if ch == priceChan {
					pc.subscribers[param] = append(channels[:i], channels[i+1:]...)
					break
				}
			}
			if len(pc.subscribers[param]) == 0 {
				delete(pc.subscribers, param)
			}
			pc.mu.Unlock()
			close(priceChan)
			return nil, err
		}
	}

	log.Printf("✅ [价格订阅] 订阅成功: %s", param)
	return priceChan, nil
}

// GetPrice 获取单次价格（优化版）
func (pc *PriceClient) GetPrice(contractAddress, chainID string, timeout time.Duration) (float64, error) {
	return pc.GetPriceWithPrecision(contractAddress, chainID, timeout, 8) // 默认8位精度
}

// GetPriceWithMode 获取指定模式的价格
func (pc *PriceClient) GetPriceWithMode(contractAddress, chainID string, timeout time.Duration, mode PriceMode) (float64, error) {
	return pc.GetPriceWithPrecisionAndMode(contractAddress, chainID, timeout, 8, mode)
}

// GetPriceWithPrecision 获取指定精度的价格 - 增强容错版本（使用默认模式）
func (pc *PriceClient) GetPriceWithPrecision(contractAddress, chainID string, timeout time.Duration, precision int) (float64, error) {
	return pc.GetPriceWithPrecisionAndMode(contractAddress, chainID, timeout, precision, PriceModeAuto)
}

// GetPriceWithPrecisionAndMode 获取指定精度和模式的价格 - 增强容错版本
func (pc *PriceClient) GetPriceWithPrecisionAndMode(contractAddress, chainID string, timeout time.Duration, precision int, mode PriceMode) (float64, error) {

	// 构建订阅参数
	param := buildSubscriptionParam(contractAddress, chainID, mode)

	// 🚀 优化：智能缓存检查
	if cachedPrice, valid := pc.getValidCachedPrice(param, contractAddress); valid {
		return cachedPrice, nil
	}

	// 🚀 优化：确保持久订阅存在（只订阅一次）
	if err := pc.ensureSubscription(param); err != nil {
		// 订阅失败，尝试使用紧急缓存
		if cachedPrice, valid := pc.getEmergencyCachedPrice(param); valid {

			return cachedPrice, nil
		}
		return 0, fmt.Errorf("订阅失败且无可用缓存: %v", err)
	}

	// 等待价格更新
	priceChan := make(chan float64, 1)
	pc.mu.Lock()
	pc.subscribers[param] = append(pc.subscribers[param], priceChan)
	pc.mu.Unlock()

	// 使用局部变量避免defer问题
	success := false
	var finalPrice float64

	func() {
		defer func() {
			// 清理临时订阅通道
			pc.mu.Lock()
			if channels, exists := pc.subscribers[param]; exists {
				for i, ch := range channels {
					if ch == priceChan {
						pc.subscribers[param] = append(channels[:i], channels[i+1:]...)
						break
					}
				}
			}
			pc.mu.Unlock()
			close(priceChan)
		}()

		select {
		case price := <-priceChan:
			log.Printf("✅ [价格获取] 实时价格获取成功: %.*f (Token: %s)", precision, price, contractAddress)
			finalPrice = price
			success = true
		case <-time.After(timeout):

		}
	}()

	if success {
		return finalPrice, nil
	}

	// 超时后尝试使用紧急缓存
	if cachedPrice, valid := pc.getEmergencyCachedPrice(param); valid {
		return cachedPrice, nil
	}

	return 0, fmt.Errorf("价格获取超时且无可用缓存")
}

// 🚀 新增：智能缓存检查
func (pc *PriceClient) getValidCachedPrice(param, contractAddress string) (float64, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	cachedPrice, exists := pc.priceCache[param]
	if !exists {
		return 0, false
	}

	cacheTime, timeExists := pc.cacheTime[param]
	if !timeExists {
		return 0, false
	}

	cacheAge := time.Since(cacheTime)
	maxAge := pc.cacheMaxAge

	// 如果连接断开，使用更长的缓存时间
	if !pc.isRunning {
		maxAge = pc.emergencyCacheAge

	}

	if cacheAge < maxAge {

		return cachedPrice, true
	}

	return 0, false
}

// 🚀 新增：紧急缓存检查（连接失败时使用）
func (pc *PriceClient) getEmergencyCachedPrice(param string) (float64, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	cachedPrice, exists := pc.priceCache[param]
	if !exists {
		return 0, false
	}

	cacheTime, timeExists := pc.cacheTime[param]
	if !timeExists {
		return 0, false
	}

	// 紧急情况下，使用更长时间的缓存
	cacheAge := time.Since(cacheTime)
	if cacheAge < pc.emergencyCacheAge {

		return cachedPrice, true
	}

	return 0, false
}

// 🚀 新增：确保订阅存在（避免重复订阅）
func (pc *PriceClient) ensureSubscription(param string) error {
	// 检查是否已有持久订阅
	pc.subscriptionMutex.RLock()
	hasSubscription := pc.persistentSubscriptions[param]
	pc.subscriptionMutex.RUnlock()

	if hasSubscription {
		// 已有订阅，直接返回
		return nil
	}

	// 需要创建新订阅
	pc.subscriptionMutex.Lock()
	defer pc.subscriptionMutex.Unlock()

	// 双重检查，防止并发创建
	if pc.persistentSubscriptions[param] {
		return nil
	}

	// 确保连接可用
	if err := pc.ensureConnection(); err != nil {
		return fmt.Errorf("连接不可用: %v", err)
	}

	// 发送订阅请求
	if err := pc.sendSubscription(param); err != nil {
		return fmt.Errorf("发送订阅失败: %v", err)
	}

	// 标记为已订阅
	pc.persistentSubscriptions[param] = true
	log.Printf("✅ [持久订阅] 订阅创建成功: %s", param)

	return nil
}

// 🚀 新增：确保连接可用
func (pc *PriceClient) ensureConnection() error {
	pc.mu.RLock()
	isRunning := pc.isRunning
	pc.mu.RUnlock()

	if !isRunning {

		if err := pc.Connect(); err != nil {
			return fmt.Errorf("重新连接失败: %v", err)
		}
	}

	return nil
}

// ensurePersistentSubscription 确保持久订阅存在（保留兼容性）
func (pc *PriceClient) ensurePersistentSubscription(param string) error {
	// 🚨 修复：如果连接断开，尝试重新连接
	if !pc.isRunning {

		if err := pc.Connect(); err != nil {
			return fmt.Errorf("重新连接失败: %v", err)
		}
	}

	return pc.sendSubscription(param)
}

// sendSubscription 发送订阅请求
func (pc *PriceClient) sendSubscription(param string) error {
	// 构建自定义订阅请求
	// 支持格式: ["came@allTokens@ticker24","alpha_372usdt@aggTrade","came@0xa2be3e48170a60119b5f0400c65f65f3158fbeee@56@kline_1s"]
	var params []string
	
	// 添加全局订阅
	params = append(params, "came@allTokens@ticker24")
	
	// 如果param是代币地址格式，则添加自定义订阅
	if strings.Contains(param, "came@") {
		// 直接使用构建好的参数
		params = append(params, param)
		
		// 解析参数中的代币地址和链ID
		parts := strings.Split(param, "@")
		if len(parts) >= 3 {
			contractAddress := parts[1]
			chainID := parts[2]
			
			// 添加aggTrade订阅
			if chainID == "56" { // 如果是BSC链
				params = append(params, fmt.Sprintf("alpha_%susdt@aggTrade", contractAddress[:6]))
			}
		}
	}

	// 构建订阅请求
	req := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     1, // 固定使用ID 1
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	pc.mu.RLock()
	conn := pc.conn
	isRunning := pc.isRunning
	pc.mu.RUnlock()

	if conn == nil || !isRunning {
		return fmt.Errorf("connection is not established")
	}

	// 使用写入锁防止并发写入
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	// 再次检查连接状态，防止在获取锁期间连接被关闭
	pc.mu.RLock()
	currentConn := pc.conn
	currentRunning := pc.isRunning
	pc.mu.RUnlock()

	if currentConn == nil || !currentRunning || currentConn != conn {
		return fmt.Errorf("connection changed during write operation")
	}

	log.Printf("📤 发送订阅请求: %s", string(data))
	return conn.WriteMessage(websocket.TextMessage, data)
}

// safeWriteMessage 安全写入消息，防止并发写入
func (pc *PriceClient) safeWriteMessage(messageType int, data []byte) error {
	// 🔧 对于 PONG 消息，使用超时机制避免长时间阻塞
	done := make(chan error, 1)

	go func() {
		pc.writeMu.Lock()
		defer pc.writeMu.Unlock()

		pc.mu.RLock()
		conn := pc.conn
		isRunning := pc.isRunning
		pc.mu.RUnlock()

		if conn == nil || !isRunning {
			done <- fmt.Errorf("connection is not available")
			return
		}

		done <- conn.WriteMessage(messageType, data)
	}()

	// 等待写入完成或超时
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		return fmt.Errorf("write operation timeout")
	}
}

// readLoop 读取WebSocket消息
func (pc *PriceClient) readLoop() {
	log.Printf("🔄 启动WebSocket读取循环")
	defer func() {
		pc.mu.Lock()
		pc.isRunning = false
		pc.mu.Unlock()
		log.Printf("🛑 WebSocket读取循环结束")
	}()

	// 添加连接状态日志
	log.Printf("📊 当前连接状态: isRunning=%v, 持久订阅数=%d", pc.isRunning, len(pc.persistentSubscriptions))

	for {
		pc.mu.RLock()
		conn := pc.conn
		running := pc.isRunning
		pc.mu.RUnlock()

		if !running || conn == nil {
			log.Printf("⚠️ 连接已关闭或不可用，退出读取循环")
			return
		}

		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))

		// 读取消息
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err) {
				log.Printf("❌ WebSocket意外关闭: %v", err)
			} else if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("❌ WebSocket读取错误: %v", err)
			}

			// 检查是否应该尝试重新连接
			pc.mu.RLock()
			reconnecting := pc.isReconnecting
			pc.mu.RUnlock()

			if !reconnecting {
				// 避免重复重连
				go pc.handleReconnect()
			}
			return
		}

		// 处理消息
		pc.handleMessage(message)
	}
}

// handleReconnect 处理重连逻辑
func (pc *PriceClient) handleReconnect() {
	// 获取锁，防止并发重连
	pc.mu.Lock()
	
	// 检查是否已经在重连中
	if pc.isReconnecting {
		pc.mu.Unlock()
		log.Printf("⚠️ 已有重连过程在进行中，跳过本次重连")
		return
	}
	
	// 设置重连标志
	pc.isReconnecting = true
	pc.mu.Unlock()
	
	// 重连完成后，无论成功与否，都要重置重连标志
	defer func() {
		pc.mu.Lock()
		pc.isReconnecting = false
		pc.mu.Unlock()
	}()
	
	// 记录重连开始
	log.Printf("🔄 开始重连 (尝试 %d/%d)", pc.reconnectAttempts+1, pc.maxReconnectAttempts)
	
	// 检查是否超过最大重试次数
	if pc.reconnectAttempts >= pc.maxReconnectAttempts {
		log.Printf("❌ 达到最大重连次数 (%d)，停止重连", pc.maxReconnectAttempts)
		return
	}
	
	// 计算重连延迟（指数退避）
	delay := pc.reconnectDelay * time.Duration(math.Pow(2, float64(pc.reconnectAttempts)))
	if delay > pc.maxReconnectDelay {
		delay = pc.maxReconnectDelay
	}
	
	// 添加随机抖动，避免多个客户端同时重连
	jitter := time.Duration(rand.Int63n(int64(delay / 4)))
	delay = delay + jitter
	
	log.Printf("⏳ 等待 %v 后重连", delay)
	time.Sleep(delay)
	
	// 断开旧连接
	pc.mu.Lock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
	pc.isRunning = false
	pc.reconnectAttempts++ // 增加重连计数
	pc.mu.Unlock()
	
	// 尝试重新连接
	err := pc.Connect()
	if err != nil {
		log.Printf("❌ 重连失败: %v", err)
		
		// 如果还有重试次数，递归调用自己
		if pc.reconnectAttempts < pc.maxReconnectAttempts {
			go pc.handleReconnect()
		} else {
			log.Printf("❌ 所有重连尝试均失败，放弃重连")
		}
		return
	}
	
	log.Printf("✅ 重连成功")
	
	// 重置重连计数
	pc.mu.Lock()
	pc.reconnectAttempts = 0
	pc.mu.Unlock()
}

// reconnect 重新连接
func (pc *PriceClient) reconnect() error {
	// 🔧 先获取写入锁，确保没有正在进行的写入操作
	pc.writeMu.Lock()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	defer pc.writeMu.Unlock()

	// 关闭旧连接
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}

	// 🔧 优化：使用与Connect相同的连接参数
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second
	dialer.ReadBufferSize = 8192
	dialer.WriteBufferSize = 8192
	dialer.EnableCompression = true

	// 设置请求头
	headers := make(map[string][]string)
	headers["User-Agent"] = []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
	headers["Origin"] = []string{"https://www.binance.com"}

	// 建立新连接
	conn, _, err := dialer.Dial(pc.wsURL, headers)
	if err != nil {
		return fmt.Errorf("failed to reconnect to WebSocket: %v", err)
	}

	// 🔧 设置与Connect相同的连接参数
	conn.SetReadDeadline(time.Now().Add(70 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// 🔧 币安官方要求：处理服务器发送的PING，回复PONG
	conn.SetPingHandler(func(appData string) error {
		log.Printf("📨 收到服务器PING，立即回复PONG")
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		// 🔧 使用安全写入方法防止并发写入
		err := pc.safeWriteMessage(websocket.PongMessage, []byte(appData))
		if err != nil {
			log.Printf("❌ 回复PONG失败: %v", err)
			return err
		}
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		log.Printf("✅ PONG回复成功")
		return nil
	})

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		return nil
	})

	pc.conn = conn
	pc.isRunning = true
	pc.lastConnectTime = time.Now()

	// 重新启动读取和连接监控
	go pc.readLoop()
	go pc.connectionMonitor() // 🔧 改为连接监控

	// 重新订阅所有之前的订阅
	go pc.resubscribeAll()

	return nil
}

// resubscribeAll 重新订阅所有之前的订阅
func (pc *PriceClient) resubscribeAll() {
	// 获取重连前的订阅列表
	pc.subscriptionMutex.Lock()
	
	// 打印当前订阅状态
	log.Printf("📊 重连前订阅状态: 持久订阅数=%d", len(pc.persistentSubscriptions))
	
	// 获取所有持久订阅参数，并去重
	uniqueParams := make(map[string]bool)
	for param := range pc.persistentSubscriptions {
		uniqueParams[param] = true
	}
	
	// 将去重后的参数转换为切片
	params := make([]string, 0, len(uniqueParams))
	for param := range uniqueParams {
		params = append(params, param)
	}
	
	// 重置持久订阅状态，准备重新建立
	pc.persistentSubscriptions = make(map[string]bool)
	pc.subscriptionMutex.Unlock()
	
	// 如果没有需要重新订阅的参数，直接返回
	if len(params) == 0 {
		log.Printf("ℹ️ 没有需要重新订阅的参数，跳过重新订阅")
		return
	}
	
	log.Printf("🔄 开始重新订阅，共%d个参数", len(params))
	
	// 重新建立所有持久订阅
	successCount := 0
	for i, param := range params {
		log.Printf("🔄 [%d/%d] 重新订阅: %s", i+1, len(params), param)
		
		if err := pc.sendSubscription(param); err != nil {
			log.Printf("❌ 重新订阅失败: %s, 错误: %v", param, err)
		} else {
			// 标记为成功订阅
			pc.subscriptionMutex.Lock()
			pc.persistentSubscriptions[param] = true
			pc.subscriptionMutex.Unlock()
			
			successCount++
			log.Printf("✅ [%d/%d] 重新订阅成功: %s", i+1, len(params), param)
		}
		
		// 添加订阅间隔，避免请求过于频繁
		time.Sleep(200 * time.Millisecond)
	}
	
	log.Printf("📊 重新订阅完成: 成功=%d, 失败=%d, 总数=%d", 
		successCount, len(params)-successCount, len(params))
}

// connectionMonitor 连接监控（替代主动PING）
func (pc *PriceClient) connectionMonitor() {
	// 🔧 币安官方：不主动发送PING，只监控连接状态
	// 服务器会每20秒发送PING，我们只需要回复PONG
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查连接状态
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pc.mu.RLock()
			conn := pc.conn
			running := pc.isRunning
			lastConnectTime := pc.lastConnectTime
			pc.mu.RUnlock()

			if !running || conn == nil {
				return
			}

			// 检查连接是否超过24小时（币安限制）
			if time.Since(lastConnectTime) > 23*time.Hour {

				go pc.handleReconnect()
				return
			}

		case <-pc.stopChan:
			return
		}
	}
}

// handleMessage 处理WebSocket消息
func (pc *PriceClient) handleMessage(message []byte) {

	// 尝试解析消息
	var data map[string]interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		log.Printf("❌ 解析消息失败: %v, 消息内容: %s", err, truncateMessage(string(message), 100))
		return
	}

	// 处理ping/pong消息
	if _, ok := data["ping"]; ok {
		// 收到ping，回复pong
		log.Printf("📌 收到ping消息，回复pong")
		pc.handlePingPong(data)
		return
	}

	// 处理订阅确认消息
	if _, ok := data["result"]; ok {
		// 订阅确认，不需要处理
		log.Printf("✅ 订阅确认: %s", truncateMessage(string(message), 100))
		return
	}

	// 处理错误消息
	if errMsg, ok := data["error"]; ok {
		log.Printf("❌ 订阅错误: %v", errMsg)
		return
	}

	// 处理价格更新消息
	if stream, ok := data["stream"]; ok {
		streamStr, _ := stream.(string)
		
		// 处理K线数据
		if strings.Contains(streamStr, "kline_") {
			pc.handleKlineMessage(data)
			return
		}
		
		// 处理aggTrade数据
		if strings.Contains(streamStr, "@aggTrade") {
			pc.handleAggTradeMessage(data)
			return
		}
		
		// 处理ticker24数据
		if strings.Contains(streamStr, "@ticker24") {
			pc.handleTickerMessage(data)
			return
		}

		// 处理其他已知stream类型
		if strings.Contains(streamStr, "@bookTicker") || 
		   strings.Contains(streamStr, "@depth") || 
		   strings.Contains(streamStr, "@trade") {
			// 这些是已知的其他stream类型，我们可以安全地忽略
			log.Printf("ℹ️ 已知但未处理的stream类型: %s", streamStr)
			return
		}
	}

	// 处理其他类型的消息
	log.Printf("⚠️ 未知消息类型，但将继续处理: %s", truncateMessage(string(message), 100))
	// 不要因为未知消息类型而断开连接，只记录日志
}

// handlePingPong 处理ping/pong消息
func (pc *PriceClient) handlePingPong(data map[string]interface{}) {
	if ping, ok := data["ping"]; ok {
		// 构造pong响应
		pongResponse := map[string]interface{}{
			"pong": ping,
		}
		
		responseBytes, err := json.Marshal(pongResponse)
		if err != nil {
			log.Printf("❌ 构造pong响应失败: %v", err)
			return
		}
		
		// 发送pong响应
		pc.writeMu.Lock()
		defer pc.writeMu.Unlock()
		
		if pc.conn != nil {
			pc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := pc.conn.WriteMessage(websocket.TextMessage, responseBytes); err != nil {
				log.Printf("❌ 发送pong响应失败: %v", err)
			} else {
				log.Printf("✅ 发送pong响应成功")
			}
		}
	}
}

// truncateMessage 截断消息以便于日志显示
func truncateMessage(message string, maxLength int) string {
	if len(message) <= maxLength {
		return message
	}
	return message[:maxLength] + "..."
}

// handleKlineMessage 处理K线消息
func (pc *PriceClient) handleKlineMessage(data map[string]interface{}) {
	// 调试输出，帮助分析数据结构
	// log.Printf("🔍 K线数据: %+v", data)
	
	// 检查是否有data字段（WebSocket消息的标准格式）
	dataObj, hasData := data["data"].(map[string]interface{})
	if !hasData {
		// 如果没有data字段，可能是直接的K线数据
		dataObj = data
	}
	
	// 解析K线数据
	klineData, ok := dataObj["k"].(map[string]interface{})
	if !ok {
		log.Printf("❌ K线数据格式错误: 找不到k字段")
		// log.Printf("❌ 收到的数据: %+v", dataObj)
		return
	}
	
	// 提取收盘价
	closePrice, ok := klineData["c"].(string)
	if !ok {
		log.Printf("❌ 无法解析收盘价")
		return
	}
	
	// 提取合约地址和链ID
	var contractAddress, chainID string
	
	// 尝试从ca字段获取
	if caInfo, ok := dataObj["ca"].(string); ok {
		contractAddress, chainID = extractContractAddressAndChainID(caInfo)
	} else {
		// 尝试从stream字段获取
		if stream, ok := data["stream"].(string); ok {
			parts := strings.Split(stream, "@")
			if len(parts) >= 3 {
				// 格式: came@0x123...@56@kline_1s
				contractAddress = parts[1]
				chainID = parts[2]
			}
		}
	}
	
	// 如果仍然无法获取合约地址和链ID，记录错误并返回
	if contractAddress == "" || chainID == "" {
		log.Printf("❌ 无法从K线数据中提取合约地址和链ID")
		return
	}
	
	// 转换价格为float64
	price, err := strconv.ParseFloat(closePrice, 64)
	if err != nil {
		log.Printf("❌ 价格转换错误: %v", err)
		return
	}
	
	// 更新订阅价格
	pc.updateSubscriptionPrice(contractAddress, chainID, price)
	
	// 计算价格变动（如果有开盘价）
	var priceChange float64 = 0
	if openPrice, ok := klineData["o"].(string); ok {
		open, err := strconv.ParseFloat(openPrice, 64)
		if err == nil && open > 0 {
			priceChange = (price - open) / open * 100
		}
	}
	
	// 更新市场状态
	updateMarketCondition(contractAddress, chainID, price, priceChange)
	
	// log.Printf("✅ K线价格更新成功: %s@%s = %.8f (变动: %.2f%%)", contractAddress, chainID, price, priceChange)
}

// handleAggTradeMessage 处理聚合交易消息
func (pc *PriceClient) handleAggTradeMessage(data map[string]interface{}) {
	// 获取stream信息
	stream, _ := data["stream"].(string)
	if stream == "" {
		log.Printf("❌ aggTrade消息缺少stream字段")
		return
	}
	
	// 检查是否有data字段
	dataObj, ok := data["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ aggTrade消息缺少data字段")
		return
	}

	// 解析价格 - aggTrade格式中价格在"p"字段
	priceStr, ok := dataObj["p"].(string)
	if !ok {
		log.Printf("❌ aggTrade消息缺少价格字段p")
		return
	}

	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		log.Printf("❌ 解析aggTrade价格失败: %v", err)
		return
	}

	// 提取代币标识
	parts := strings.Split(stream, "@")
	if len(parts) < 2 {
		log.Printf("❌ aggTrade stream格式错误: %s", stream)
		return
	}
	
	tokenSymbol := parts[0] // 例如 alpha_372usdt
	
	// 处理两种可能的格式
	var contractAddress, chainID string
	
	// 格式1: alpha_XXXusdt@aggTrade
	if strings.HasPrefix(tokenSymbol, "alpha_") && strings.HasSuffix(tokenSymbol, "usdt") {
		addrPrefix := strings.TrimPrefix(tokenSymbol, "alpha_")
		addrPrefix = strings.TrimSuffix(addrPrefix, "usdt")
		
		// 在这种情况下，我们没有明确的合约地址和链ID
		// 尝试从订阅参数中匹配
		pc.mu.RLock()
		for param, subscribers := range pc.subscribers {
			// 检查是否是相关的合约地址
			if strings.Contains(param, "@"+addrPrefix) {
				// 更新缓存
				pc.priceCache[param] = price
				pc.cacheTime[param] = time.Now()
				
				// 通知订阅者
				for _, ch := range subscribers {
					select {
					case ch <- price:
					default:
						// 通道已满或已关闭，忽略
					}
				}
				
				// 尝试从参数中提取合约地址和链ID
				paramParts := strings.Split(param, "@")
				if len(paramParts) >= 3 {
					contractAddress = paramParts[1]
					chainID = paramParts[2]
					// 更新市场状态
					updateMarketCondition(contractAddress, chainID, price, 0) // 无法获取涨跌幅
				}
			}
		}
		pc.mu.RUnlock()
	} else {
		// 格式2: 可能是其他格式，记录日志但不处理
		log.Printf("⚠️ 未处理的aggTrade格式: %s", stream)
	}
}

// handleTickerMessage 处理24小时行情消息
func (pc *PriceClient) handleTickerMessage(data map[string]interface{}) {
	// 检查是否有data字段
	dataObj, ok := data["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ ticker消息缺少data字段")
		return
	}
	
	// 检查是否有tickerList事件
	if eventType, ok := dataObj["e"].(string); !ok || eventType != "tickerList" {
		log.Printf("⚠️ ticker消息不是tickerList事件: %v", eventType)
		return
	}
	
	// 解析24小时行情数据
	tickerList, ok := dataObj["d"].([]interface{})
	if !ok {
		log.Printf("❌ 无法解析ticker列表")
		return
	}
	
	processedCount := 0
	for _, item := range tickerList {
		ticker, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		
		// 提取合约地址和链ID
		caInfo, ok := ticker["ca"].(string)
		if !ok {
			log.Printf("⚠️ ticker项缺少ca字段")
			continue
		}
		
		contractAddress, chainID := extractContractAddressAndChainID(caInfo)
		
		// 提取价格
		priceStr, ok := ticker["p"].(string)
		if !ok {
			log.Printf("⚠️ ticker项缺少p字段")
			continue
		}
		
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("⚠️ ticker价格解析失败: %v", err)
			continue
		}
		
		// 提取24小时涨跌幅
		priceChangeStr, ok := ticker["pc24"].(string)
		if !ok {
			log.Printf("⚠️ ticker项缺少pc24字段")
			priceChangeStr = "0"
		}
		
		priceChange, err := strconv.ParseFloat(priceChangeStr, 64)
		if err != nil {
			priceChange = 0
		}
		
		// 更新订阅价格
		pc.updateSubscriptionPrice(contractAddress, chainID, price)
		
		// 更新市场状态
		updateMarketCondition(contractAddress, chainID, price, priceChange)
		processedCount++
	}
	

}

// parseFloatValue 解析浮点数值
func (pc *PriceClient) parseFloatValue(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("无法解析价格值: %v (类型: %T)", value, value)
	}
}

// GetCachedPrice 获取缓存价格（非阻塞，使用默认模式）
func (pc *PriceClient) GetCachedPrice(contractAddress, chainID string) (float64, bool) {
	return pc.GetCachedPriceWithMode(contractAddress, chainID, PriceModeAuto)
}

// GetCachedPriceWithMode 获取指定模式的缓存价格（非阻塞）
func (pc *PriceClient) GetCachedPriceWithMode(contractAddress, chainID string, mode PriceMode) (float64, bool) {
	// 构建订阅参数
	param := buildSubscriptionParam(contractAddress, chainID, mode)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if cachedPrice, exists := pc.priceCache[param]; exists {
		if cacheTime, timeExists := pc.cacheTime[param]; timeExists {
			// 5秒内的缓存认为有效
			if time.Since(cacheTime) < 5*time.Second {
				return cachedPrice, true
			}
		}
	}

	return 0, false
}

// ClearCache 清理所有价格缓存
func (pc *PriceClient) ClearCache() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	log.Printf("🧹 [价格缓存] 清理所有缓存，当前缓存数量: %d", len(pc.priceCache))

	// 清空缓存
	pc.priceCache = make(map[string]float64)
	pc.cacheTime = make(map[string]time.Time)

	log.Printf("✅ [价格缓存] 缓存清理完成")
}

// CleanExpiredCache 清理过期缓存
func (pc *PriceClient) CleanExpiredCache() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	now := time.Now()
	for param, cacheTime := range pc.cacheTime {
		if now.Sub(cacheTime) > 30*time.Second { // 30秒后清理
			delete(pc.priceCache, param)
			delete(pc.cacheTime, param)
		}
	}
}

// parseFloat 解析浮点数
func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

// 🚨 新增：健康检查和自动恢复机制
func (pc *PriceClient) startHealthCheck() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	log.Printf("🏥 启动WebSocket健康检查，每30秒检查一次")

	for {
		select {
		case <-ticker.C:
			pc.performHealthCheck()
		case <-pc.stopChan:
			log.Printf("🏥 健康检查已停止")
			return
		}
	}
}

// performHealthCheck 执行健康检查
func (pc *PriceClient) performHealthCheck() {
	pc.mu.RLock()
	isRunning := pc.isRunning
	isReconnecting := pc.isReconnecting
	lastConnectTime := pc.lastConnectTime
	conn := pc.conn
	pc.mu.RUnlock()

	// 检查连接状态
	if !isRunning && !isReconnecting {

		go pc.handleReconnect()
		return
	}

	// 检查连接是否长时间无响应
	if isRunning && conn != nil {
		timeSinceConnect := time.Since(lastConnectTime)
		if timeSinceConnect > 25*time.Hour { // 超过25小时强制重连

			go pc.handleReconnect()
			return
		}

		// 检查是否有最近的价格更新
		pc.mu.RLock()
		hasRecentData := false
		cutoffTime := time.Now().Add(-5 * time.Minute)
		for _, cacheTime := range pc.cacheTime {
			if cacheTime.After(cutoffTime) {
				hasRecentData = true
				break
			}
		}
		pc.mu.RUnlock()

		if !hasRecentData && timeSinceConnect > 2*time.Minute {

			go pc.handleReconnect()
			return
		}
	}

}

// 🚨 新增：强制重置连接状态（紧急恢复）
func (pc *PriceClient) ForceReset() error {

	pc.mu.Lock()
	pc.isRunning = false
	pc.isReconnecting = false
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
	pc.mu.Unlock()

	// 等待1秒让旧连接完全关闭
	time.Sleep(1 * time.Second)

	// 重新连接
	return pc.Connect()
}

// MarketCondition 市场状态
type MarketCondition struct {
	TokenAddress  string    // 代币地址
	ChainID       string    // 链ID
	CurrentPrice  float64   // 当前价格
	PriceChange   float64   // 价格变动百分比（24小时）
	IsExtreme     bool      // 是否极端行情
	ExtremeReason string    // 极端行情原因
	UpdatedAt     time.Time // 更新时间
}

// 全局市场状态缓存
var (
	marketConditions     = make(map[string]*MarketCondition) // 代币地址@链ID -> 市场状态
	marketConditionMutex sync.RWMutex
	
	// 极端行情检测参数
	extremeDropThreshold    = -15.0  // 极端下跌阈值（百分比）
	extremeVolatileThreshold = 10.0  // 极端波动阈值（百分比）
	priceHistoryWindow      = 5      // 价格历史窗口大小
	volatilityCheckInterval = 60     // 波动性检查间隔（秒）
)

// 价格历史记录
type PriceHistory struct {
	Prices     []float64  // 历史价格
	Timestamps []int64    // 时间戳
	mutex      sync.Mutex
}

var priceHistories = make(map[string]*PriceHistory) // 代币地址@链ID -> 价格历史

// 添加价格到历史记录
func addPriceToHistory(key string, price float64, timestamp int64) {
	if _, exists := priceHistories[key]; !exists {
		priceHistories[key] = &PriceHistory{
			Prices:     make([]float64, 0, priceHistoryWindow),
			Timestamps: make([]int64, 0, priceHistoryWindow),
		}
	}
	
	history := priceHistories[key]
	history.mutex.Lock()
	defer history.mutex.Unlock()
	
	history.Prices = append(history.Prices, price)
	history.Timestamps = append(history.Timestamps, timestamp)
	
	// 保持窗口大小
	if len(history.Prices) > priceHistoryWindow {
		history.Prices = history.Prices[1:]
		history.Timestamps = history.Timestamps[1:]
	}
}

// 检查短期价格波动
func checkPriceVolatility(key string) (bool, float64) {
	history, exists := priceHistories[key]
	if !exists || len(history.Prices) < 2 {
		return false, 0
	}
	
	history.mutex.Lock()
	defer history.mutex.Unlock()
	
	if len(history.Prices) < 2 {
		return false, 0
	}
	
	// 计算最大波动幅度
	minPrice := history.Prices[0]
	maxPrice := history.Prices[0]
	
	for _, price := range history.Prices {
		if price < minPrice {
			minPrice = price
		}
		if price > maxPrice {
			maxPrice = price
		}
	}
	
	// 没有价格或价格接近零，无法计算波动
	if minPrice <= 0.0000001 {
		return false, 0
	}
	
	volatilityPercent := (maxPrice - minPrice) / minPrice * 100
	isVolatile := volatilityPercent > extremeVolatileThreshold
	
	return isVolatile, volatilityPercent
}

// 更新市场状态
func updateMarketCondition(tokenAddress, chainID string, currentPrice float64, priceChange float64) {
	key := tokenAddress + "@" + chainID
	
	// 添加价格到历史记录
	addPriceToHistory(key, currentPrice, time.Now().Unix())
	
	marketConditionMutex.Lock()
	defer marketConditionMutex.Unlock()
	
	condition, exists := marketConditions[key]
	if !exists {
		condition = &MarketCondition{
			TokenAddress: tokenAddress,
			ChainID:     chainID,
			UpdatedAt:   time.Now(),
		}
		marketConditions[key] = condition
	}
	
	condition.CurrentPrice = currentPrice
	condition.PriceChange = priceChange
	condition.UpdatedAt = time.Now()
	
	// 不再检查24小时极端下跌
	// isExtremeDrop := priceChange < extremeDropThreshold
	
	// 只检查短期价格波动
	isVolatile, volatilityPercent := checkPriceVolatility(key)
	
	// 更新极端行情状态 - 只基于短期价格波动
	if isVolatile {
		condition.IsExtreme = true
		condition.ExtremeReason = fmt.Sprintf("价格剧烈波动: %.2f%%", volatilityPercent)
	} else {
		condition.IsExtreme = false
		condition.ExtremeReason = ""
	}
}

// GetMarketCondition 获取代币的市场状态
func GetMarketCondition(tokenAddress, chainID string) *MarketCondition {
	key := tokenAddress + "@" + chainID
	
	marketConditionMutex.RLock()
	defer marketConditionMutex.RUnlock()
	
	if condition, exists := marketConditions[key]; exists {
		return condition
	}
	
	return &MarketCondition{
		TokenAddress: tokenAddress,
		ChainID:     chainID,
		IsExtreme:   false,
	}
}

// IsExtremeMarket 检查是否处于极端市场状态
func IsExtremeMarket(tokenAddress, chainID string) (bool, string) {
	condition := GetMarketCondition(tokenAddress, chainID)
	return condition.IsExtreme, condition.ExtremeReason
}

// SetExtremeThresholds 设置极端行情阈值
func SetExtremeThresholds(dropThreshold, volatileThreshold float64) {
	marketConditionMutex.Lock()
	defer marketConditionMutex.Unlock()
	
	extremeDropThreshold = dropThreshold
	extremeVolatileThreshold = volatileThreshold
}

// 从合约地址字符串中提取地址和链ID
func extractContractAddressAndChainID(caInfo string) (string, string) {
	parts := strings.Split(caInfo, "@")
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	return caInfo, "56" // 默认BSC链
}

// updateSubscriptionPrice 更新订阅价格并通知订阅者
func (pc *PriceClient) updateSubscriptionPrice(contractAddress, chainID string, price float64) {
	// 构建可能的订阅参数格式
	possibleParams := []string{
		fmt.Sprintf("came@%s@%s@kline_1s", contractAddress, chainID),
		fmt.Sprintf("came@%s@%s@kline_15m", contractAddress, chainID),
	}
	
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	
	for _, param := range possibleParams {
		if subscribers, exists := pc.subscribers[param]; exists {
			// 更新缓存
			pc.mu.RUnlock()
			pc.mu.Lock()
			pc.priceCache[param] = price
			pc.cacheTime[param] = time.Now()
			pc.mu.Unlock()
			pc.mu.RLock()
			
			// 通知订阅者
			for _, ch := range subscribers {
				select {
				case ch <- price:
				default:
					// 通道已满或已关闭，忽略
				}
			}
		}
	}
}
