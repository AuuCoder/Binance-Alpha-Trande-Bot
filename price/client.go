package price

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

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
	// 添加价格缓存
	priceCache map[string]float64
	cacheTime  map[string]time.Time

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

// getKlineInterval 根据链ID获取对应的K线时间间隔
func getKlineInterval(chainID string) string {
	switch chainID {
	case "56": // BSC
		return "1s"
	case "CT_195": // 自定义链
		return "1m"
	default:
		return "1s" // 默认使用1秒间隔
	}
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
			log.Printf("❌ 回复PONG失败: %v", err)
			return err
		}
		// 重置读超时
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		log.Printf("✅ PONG回复成功")
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

	log.Printf("✅ Price client connected to: %s", pc.wsURL)
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

// SubscribePrice 订阅代币价格
func (pc *PriceClient) SubscribePrice(contractAddress, chainID string) (<-chan float64, error) {
	if !pc.isRunning {
		return nil, fmt.Errorf("client is not connected")
	}

	// 构建订阅参数，根据链ID选择时间间隔
	interval := getKlineInterval(chainID)
	param := fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)

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

	log.Printf("Subscribed to price for: %s", param)
	return priceChan, nil
}

// GetPrice 获取单次价格（优化版）
func (pc *PriceClient) GetPrice(contractAddress, chainID string, timeout time.Duration) (float64, error) {
	return pc.GetPriceWithPrecision(contractAddress, chainID, timeout, 8) // 默认8位精度
}

// GetPriceWithPrecision 获取指定精度的价格 - 增强容错版本
func (pc *PriceClient) GetPriceWithPrecision(contractAddress, chainID string, timeout time.Duration, precision int) (float64, error) {
	log.Printf("🔍 [价格获取] 输入参数 - Token地址: %s, 链ID: %s, 精度: %d", contractAddress, chainID, precision)
	// 根据链ID选择时间间隔
	interval := getKlineInterval(chainID)
	param := fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)
	log.Printf("📡 [价格获取] WebSocket订阅参数: %s (间隔: %s)", param, interval)

	// 1. 先检查缓存（扩展到30秒内的价格认为是可用的）
	pc.mu.RLock()
	if cachedPrice, exists := pc.priceCache[param]; exists {
		if cacheTime, timeExists := pc.cacheTime[param]; timeExists {
			cacheAge := time.Since(cacheTime)
			// 🚨 修复：如果连接断开，使用更旧的缓存（最多30秒）
			maxCacheAge := 5 * time.Second
			if !pc.isRunning {
				maxCacheAge = 30 * time.Second
				log.Printf("⚠️ WebSocket未运行，使用较旧缓存")
			}

			if cacheAge < maxCacheAge {
				pc.mu.RUnlock()
				log.Printf("📋 [价格获取] 使用缓存价格: %.*f (缓存时间: %v, Token: %s)", precision, cachedPrice, cacheAge, contractAddress)
				return cachedPrice, nil
			}
		}
	}
	pc.mu.RUnlock()

	// 2. 检查是否已有订阅
	pc.mu.RLock()
	hasSubscription := len(pc.subscribers[param]) > 0
	pc.mu.RUnlock()

	// 🚨 修复：增加重试机制，最多尝试3次获取价格
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			log.Printf("🔄 [价格获取] 第%d次尝试获取价格", attempt)
		}

		if !hasSubscription {
			// 3. 没有订阅，创建持久订阅
			log.Printf("📡 Creating persistent subscription for %s (尝试 %d/%d)", param, attempt, maxAttempts)
			if err := pc.ensurePersistentSubscription(param); err != nil {
				if attempt == maxAttempts {
					// 🚨 修复：最后一次失败时，返回缓存价格（如果有的话）
					pc.mu.RLock()
					if cachedPrice, exists := pc.priceCache[param]; exists {
						pc.mu.RUnlock()
						log.Printf("⚠️ [价格获取] 订阅失败，使用历史缓存价格: %.*f", precision, cachedPrice)
						return cachedPrice, nil
					}
					pc.mu.RUnlock()
					return 0, fmt.Errorf("订阅失败且无缓存价格: %v", err)
				}
				log.Printf("⚠️ [价格获取] 订阅失败，等待2秒后重试: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}

		// 4. 等待价格更新
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
				log.Printf("⏰ [价格获取] 获取价格超时 (尝试 %d/%d)", attempt, maxAttempts)
			}
		}()

		if success {
			return finalPrice, nil
		}

		// 如果不是最后一次尝试，等待后重试
		if attempt < maxAttempts {
			time.Sleep(1 * time.Second)
		}
	}

	// 🚨 修复：所有尝试都失败时，返回最后的缓存价格
	pc.mu.RLock()
	if cachedPrice, exists := pc.priceCache[param]; exists {
		pc.mu.RUnlock()
		log.Printf("⚠️ [价格获取] 所有尝试失败，使用最后缓存价格: %.*f", precision, cachedPrice)
		return cachedPrice, nil
	}
	pc.mu.RUnlock()

	return 0, fmt.Errorf("价格获取失败，已尝试%d次且无缓存价格", maxAttempts)
}

// ensurePersistentSubscription 确保持久订阅存在
func (pc *PriceClient) ensurePersistentSubscription(param string) error {
	// 🚨 修复：如果连接断开，尝试重新连接
	if !pc.isRunning {
		log.Printf("⚠️ WebSocket未运行，尝试重新连接...")
		if err := pc.Connect(); err != nil {
			return fmt.Errorf("重新连接失败: %v", err)
		}
	}

	return pc.sendSubscription(param)
}

// sendSubscription 发送订阅请求
func (pc *PriceClient) sendSubscription(param string) error {
	req := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{param},
		"id":     time.Now().Unix(),
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

	// 🔧 使用写入锁防止并发写入
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

// readLoop 读取消息循环
func (pc *PriceClient) readLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Price client read loop panic: %v", r)
		}
	}()

	for {
		pc.mu.RLock()
		conn := pc.conn
		running := pc.isRunning
		pc.mu.RUnlock()

		if !running || conn == nil {
			break
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("❌ WebSocket read error: %v", err)
			// 触发重连
			go pc.handleReconnect()
			break
		}

		pc.handleMessage(message)
	}
}

// handleReconnect 处理重连 - 增强重连策略，永不放弃
func (pc *PriceClient) handleReconnect() {
	pc.mu.Lock()
	if pc.isReconnecting {
		pc.mu.Unlock()
		return // 已经在重连中
	}
	pc.isReconnecting = true
	pc.mu.Unlock()

	log.Printf("🔄 WebSocket连接断开，开始重连...")

	// 🚨 修复：增强重连策略，永不放弃，使用指数退避
	attempt := 1
	baseDelay := 5 * time.Second
	maxDelay := 5 * time.Minute

	for {
		log.Printf("🔄 WebSocket重连尝试第%d次", attempt)

		if attempt > 1 {
			// 计算退避延迟：5s, 10s, 20s, 40s, 80s, 160s, 300s(max)
			delay := time.Duration(attempt-1) * baseDelay
			if delay > maxDelay {
				delay = maxDelay
			}
			log.Printf("⏳ 等待 %v 后重连", delay)
			time.Sleep(delay)
		}

		// 尝试重连
		if err := pc.reconnect(); err != nil {
			log.Printf("❌ 重连失败 (第%d次尝试): %v", attempt, err)
			attempt++

			// 🚨 修复：每10次失败后重置连接状态，防止永久失效
			if attempt%10 == 0 {
				log.Printf("🔧 第%d次重连失败，重置连接状态", attempt)
				pc.mu.Lock()
				pc.isRunning = false
				if pc.conn != nil {
					pc.conn.Close()
					pc.conn = nil
				}
				pc.mu.Unlock()
			}
			continue
		}

		log.Printf("✅ WebSocket重连成功 (第%d次尝试)", attempt)
		pc.mu.Lock()
		pc.isReconnecting = false
		pc.mu.Unlock()

		// 重连成功后重新启动读取循环
		go pc.readLoop()
		return
	}
}

// 重复函数已删除，使用上面的定义

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
	pc.mu.Lock()

	// 🔧 修复：转换旧格式到新格式
	newSubscribers := make(map[string][]chan float64)

	for oldParam, channels := range pc.subscribers {
		// 检查是否是旧格式 (came@...@kline_1s)
		if strings.Contains(oldParam, "came@") && strings.Contains(oldParam, "kline_1s") {
			// 转换为新格式 (w3w@...@ticker24h)
			newParam := strings.Replace(oldParam, "came@", "w3w@", 1)
			newParam = strings.Replace(newParam, "@kline_1s", "@ticker24h", 1)

			log.Printf("🔄 转换订阅格式: %s → %s", oldParam, newParam)
			newSubscribers[newParam] = channels
		} else {
			// 保持新格式不变
			newSubscribers[oldParam] = channels
		}
	}

	// 更新订阅列表
	pc.subscribers = newSubscribers

	// 获取所有参数
	params := make([]string, 0, len(pc.subscribers))
	for param := range pc.subscribers {
		params = append(params, param)
	}
	pc.mu.Unlock()

	// 重新订阅
	for _, param := range params {
		if err := pc.sendSubscription(param); err != nil {
			log.Printf("❌ 重新订阅失败: %s, 错误: %v", param, err)
		} else {
			log.Printf("✅ 重新订阅成功: %s", param)
		}
		time.Sleep(100 * time.Millisecond) // 避免发送过快
	}
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
				log.Printf("⏰ 连接已超过23小时，主动重连以符合币安24小时限制")
				go pc.handleReconnect()
				return
			}

			log.Printf("💓 连接监控：状态正常，已连接 %v", time.Since(lastConnectTime).Round(time.Minute))
		case <-pc.stopChan:
			return
		}
	}
}

// handleMessage 处理消息
func (pc *PriceClient) handleMessage(message []byte) {
	var response map[string]interface{}
	if err := json.Unmarshal(message, &response); err != nil {
		return
	}

	// 处理订阅确认
	if id, exists := response["id"]; exists {
		if response["error"] != nil {
			log.Printf("Subscription error: %v", response["error"])
		} else {
			log.Printf("Subscription confirmed: ID=%v", id)
		}
		return
	}

	// 处理价格数据
	if stream, exists := response["stream"].(string); exists {
		if dataField, exists := response["data"].(map[string]interface{}); exists {
			if klineField, exists := dataField["k"].(map[string]interface{}); exists {
				if closePrice, exists := klineField["c"]; exists {
					var price float64
					switch v := closePrice.(type) {
					case float64:
						price = v
					case string:
						// 如果是字符串，尝试解析
						if parsed, err := parseFloat(v); err == nil {
							price = parsed
						}
					}

					if price > 0 {
						pc.broadcastPrice(stream, price)
					}
				}
			}
		}
	}
}

// broadcastPrice 广播价格到所有订阅者
func (pc *PriceClient) broadcastPrice(stream string, price float64) {
	pc.mu.Lock()
	// 更新价格缓存
	pc.priceCache[stream] = price
	pc.cacheTime[stream] = time.Now()

	channels, exists := pc.subscribers[stream]
	pc.mu.Unlock()

	if !exists {
		return
	}

	// 非阻塞发送价格到所有订阅通道
	for _, ch := range channels {
		select {
		case ch <- price:
		default:
			// 通道满了，跳过这次发送
		}
	}
}

// GetCachedPrice 获取缓存价格（非阻塞）
func (pc *PriceClient) GetCachedPrice(contractAddress, chainID string) (float64, bool) {
	// 根据链ID选择时间间隔
	interval := getKlineInterval(chainID)
	param := fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)

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
		log.Printf("🚨 健康检查：WebSocket未运行且未在重连，尝试重新连接")
		go pc.handleReconnect()
		return
	}

	// 检查连接是否长时间无响应
	if isRunning && conn != nil {
		timeSinceConnect := time.Since(lastConnectTime)
		if timeSinceConnect > 25*time.Hour { // 超过25小时强制重连
			log.Printf("🚨 健康检查：连接时间过长(%v)，强制重连", timeSinceConnect)
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
			log.Printf("🚨 健康检查：5分钟内无价格更新，可能连接异常，尝试重连")
			go pc.handleReconnect()
			return
		}
	}

	log.Printf("💚 健康检查：WebSocket状态正常 (运行: %v, 重连中: %v, 连接时长: %v)",
		isRunning, isReconnecting, time.Since(lastConnectTime).Round(time.Minute))
}

// 🚨 新增：强制重置连接状态（紧急恢复）
func (pc *PriceClient) ForceReset() error {
	log.Printf("🔧 强制重置WebSocket连接状态")

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
