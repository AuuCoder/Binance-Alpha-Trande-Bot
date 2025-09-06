package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// ConnectionStatus 连接状态
type ConnectionStatus int

const (
	StatusDisconnected ConnectionStatus = iota
	StatusConnecting
	StatusConnected
	StatusReconnecting
)

func (s ConnectionStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "Disconnected"
	case StatusConnecting:
		return "Connecting"
	case StatusConnected:
		return "Connected"
	case StatusReconnecting:
		return "Reconnecting"
	default:
		return "Unknown"
	}
}

// ConnectionManager 网络连接管理器
type ConnectionManager struct {
	status               ConnectionStatus
	mutex                sync.RWMutex
	lastConnectTime      time.Time
	lastDisconnectTime   time.Time
	reconnectAttempts    int
	maxReconnectAttempts int
	reconnectDelay       time.Duration
	maxReconnectDelay    time.Duration

	// 网络检测配置
	testHosts   []string
	testTimeout time.Duration

	// 回调函数
	onConnected    func()
	onDisconnected func()
	onReconnecting func(attempt int)

	// 控制通道
	stopChan      chan struct{}
	monitorTicker *time.Ticker
	isMonitoring  bool
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		status:               StatusDisconnected,
		maxReconnectAttempts: 10,
		reconnectDelay:       2 * time.Second,
		maxReconnectDelay:    60 * time.Second,
		testHosts: []string{
			"8.8.8.8:53",         // Google DNS
			"1.1.1.1:53",         // Cloudflare DNS
			"114.114.114.114:53", // 114 DNS
		},
		testTimeout:   5 * time.Second,
		stopChan:      make(chan struct{}),
		monitorTicker: time.NewTicker(10 * time.Second), // 每10秒检测一次
	}
}

// SetCallbacks 设置回调函数
func (cm *ConnectionManager) SetCallbacks(onConnected, onDisconnected func(), onReconnecting func(int)) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.onConnected = onConnected
	cm.onDisconnected = onDisconnected
	cm.onReconnecting = onReconnecting
}

// GetStatus 获取连接状态
func (cm *ConnectionManager) GetStatus() ConnectionStatus {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.status
}

// IsConnected 是否已连接
func (cm *ConnectionManager) IsConnected() bool {
	return cm.GetStatus() == StatusConnected
}

// StartMonitoring 开始网络监控
func (cm *ConnectionManager) StartMonitoring() {
	cm.mutex.Lock()
	if cm.isMonitoring {
		cm.mutex.Unlock()
		return
	}
	cm.isMonitoring = true
	cm.mutex.Unlock()

	log.Printf("🌐 网络连接监控已启动")

	// 立即检测一次网络状态
	go cm.checkNetworkStatus()

	// 启动定期监控
	go cm.monitorLoop()
}

// StopMonitoring 停止网络监控
func (cm *ConnectionManager) StopMonitoring() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.isMonitoring {
		return
	}

	cm.isMonitoring = false
	close(cm.stopChan)
	cm.monitorTicker.Stop()

	log.Printf("🌐 网络连接监控已停止")
}

// monitorLoop 监控循环
func (cm *ConnectionManager) monitorLoop() {
	for {
		select {
		case <-cm.monitorTicker.C:
			go cm.checkNetworkStatus()
		case <-cm.stopChan:
			return
		}
	}
}

// checkNetworkStatus 检查网络状态
func (cm *ConnectionManager) checkNetworkStatus() {
	isConnected := cm.testNetworkConnectivity()

	cm.mutex.Lock()
	currentStatus := cm.status
	cm.mutex.Unlock()

	if isConnected {
		if currentStatus != StatusConnected {
			cm.handleConnected()
		}
	} else {
		if currentStatus == StatusConnected {
			cm.handleDisconnected()
		}
	}
}

// testNetworkConnectivity 测试网络连通性
func (cm *ConnectionManager) testNetworkConnectivity() bool {
	for _, host := range cm.testHosts {
		if cm.testSingleHost(host) {
			return true
		}
	}
	return false
}

// testSingleHost 测试单个主机连通性
func (cm *ConnectionManager) testSingleHost(host string) bool {
	conn, err := net.DialTimeout("tcp", host, cm.testTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// handleConnected 处理连接成功
func (cm *ConnectionManager) handleConnected() {
	cm.mutex.Lock()
	oldStatus := cm.status
	cm.status = StatusConnected
	cm.lastConnectTime = time.Now()
	cm.reconnectAttempts = 0 // 重置重连次数
	cm.mutex.Unlock()

	if oldStatus != StatusConnected {
		log.Printf("✅ 网络连接已恢复")
		if cm.onConnected != nil {
			go cm.onConnected()
		}
	}
}

// handleDisconnected 处理连接断开
func (cm *ConnectionManager) handleDisconnected() {
	cm.mutex.Lock()
	oldStatus := cm.status
	cm.status = StatusDisconnected
	cm.lastDisconnectTime = time.Now()
	cm.mutex.Unlock()

	if oldStatus == StatusConnected {
		log.Printf("❌ 网络连接已断开")
		if cm.onDisconnected != nil {
			go cm.onDisconnected()
		}

		// 启动重连
		go cm.startReconnect()
	}
}

// startReconnect 开始重连
func (cm *ConnectionManager) startReconnect() {
	cm.mutex.Lock()
	if cm.status == StatusReconnecting {
		cm.mutex.Unlock()
		return // 已经在重连中
	}
	cm.status = StatusReconnecting
	cm.mutex.Unlock()

	log.Printf("🔄 开始网络重连...")

	for {
		cm.mutex.Lock()
		attempts := cm.reconnectAttempts
		maxAttempts := cm.maxReconnectAttempts
		cm.mutex.Unlock()

		if attempts >= maxAttempts {
			log.Printf("❌ 网络重连失败，已达到最大重试次数 (%d)", maxAttempts)
			cm.mutex.Lock()
			cm.status = StatusDisconnected
			cm.mutex.Unlock()
			return
		}

		cm.mutex.Lock()
		cm.reconnectAttempts++
		currentAttempt := cm.reconnectAttempts
		cm.mutex.Unlock()

		log.Printf("🔄 网络重连尝试 %d/%d", currentAttempt, maxAttempts)

		if cm.onReconnecting != nil {
			go cm.onReconnecting(currentAttempt)
		}

		// 测试网络连通性
		if cm.testNetworkConnectivity() {
			cm.handleConnected()
			return
		}

		// 计算重连延迟（指数退避）
		delay := cm.calculateReconnectDelay(currentAttempt)
		log.Printf("⏳ 等待 %v 后进行下次重连", delay)

		select {
		case <-time.After(delay):
			continue
		case <-cm.stopChan:
			return
		}
	}
}

// calculateReconnectDelay 计算重连延迟（指数退避）
func (cm *ConnectionManager) calculateReconnectDelay(attempt int) time.Duration {
	delay := cm.reconnectDelay * time.Duration(1<<uint(attempt-1)) // 2^(attempt-1)
	if delay > cm.maxReconnectDelay {
		delay = cm.maxReconnectDelay
	}
	return delay
}

// GetConnectionInfo 获取连接信息
func (cm *ConnectionManager) GetConnectionInfo() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	info := map[string]interface{}{
		"status":                 cm.status.String(),
		"reconnect_attempts":     cm.reconnectAttempts,
		"max_reconnect_attempts": cm.maxReconnectAttempts,
		"is_monitoring":          cm.isMonitoring,
	}

	if !cm.lastConnectTime.IsZero() {
		info["last_connect_time"] = cm.lastConnectTime.Format("2006-01-02 15:04:05")
		if cm.status == StatusConnected {
			info["connected_duration"] = time.Since(cm.lastConnectTime).String()
		}
	}

	if !cm.lastDisconnectTime.IsZero() {
		info["last_disconnect_time"] = cm.lastDisconnectTime.Format("2006-01-02 15:04:05")
		if cm.status != StatusConnected {
			info["disconnected_duration"] = time.Since(cm.lastDisconnectTime).String()
		}
	}

	return info
}

// CreateHTTPClientWithRetry 创建带重试的HTTP客户端
func (cm *ConnectionManager) CreateHTTPClientWithRetry(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// 检查网络状态
			if !cm.IsConnected() {
				return nil, fmt.Errorf("network is disconnected")
			}

			dialer := &net.Dialer{
				Timeout:   timeout,
				KeepAlive: 30 * time.Second,
			}
			return dialer.DialContext(ctx, network, addr)
		},
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   false,
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}
