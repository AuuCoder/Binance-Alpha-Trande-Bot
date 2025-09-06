package master

import (
	"alpha-autosell-bot/internal/task"
	"fmt"
	"log"
	"sync"
	"time"
)

// AutoRestartManager 自动重启管理器
type AutoRestartManager struct {
	server          *Server
	monitoringTasks map[string]*MonitoringTask
	mutex           sync.RWMutex
	stopChan        chan struct{}
	isRunning       bool
}

// MonitoringTask 监控任务
type MonitoringTask struct {
	NodeID         string                 `json:"node_id"`
	AccountID      string                 `json:"account_id"`
	TaskParams     map[string]interface{} `json:"task_params"`
	LastTradeTime  time.Time              `json:"last_trade_time"`
	IsWaiting      bool                   `json:"is_waiting"`
	OriginalVolume float64                `json:"original_volume"`
	RestartCount   int                    `json:"restart_count"`
}

// NewAutoRestartManager 创建自动重启管理器
func NewAutoRestartManager(server *Server) *AutoRestartManager {
	return &AutoRestartManager{
		server:          server,
		monitoringTasks: make(map[string]*MonitoringTask),
		stopChan:        make(chan struct{}),
	}
}

// Start 启动自动重启监控
func (arm *AutoRestartManager) Start() {
	arm.mutex.Lock()
	if arm.isRunning {
		arm.mutex.Unlock()
		return
	}
	arm.isRunning = true
	arm.mutex.Unlock()

	log.Printf("🔄 自动重启监控器已启动")

	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			arm.checkTimeouts()
		case <-arm.stopChan:
			log.Printf("🛑 自动重启监控器已停止")
			return
		}
	}
}

// Stop 停止自动重启监控
func (arm *AutoRestartManager) Stop() {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()

	if arm.isRunning {
		close(arm.stopChan)
		arm.isRunning = false
	}
}

// AddMonitoring 添加监控任务
func (arm *AutoRestartManager) AddMonitoring(nodeID, accountID string, taskParams map[string]interface{}) {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()

	key := fmt.Sprintf("%s-%s", nodeID, accountID)

	// 获取目标交易额
	targetVolume := float64(0)
	if vol, ok := taskParams["target_volume"].(float64); ok {
		targetVolume = vol
	}

	task := &MonitoringTask{
		NodeID:         nodeID,
		AccountID:      accountID,
		TaskParams:     taskParams,
		LastTradeTime:  time.Now(),
		IsWaiting:      false,
		OriginalVolume: targetVolume,
		RestartCount:   0,
	}

	arm.monitoringTasks[key] = task
	log.Printf("📊 开始监控: %s - %s (目标交易额: %.2f)", nodeID, accountID, targetVolume)
}

// RemoveMonitoring 移除监控任务
func (arm *AutoRestartManager) RemoveMonitoring(nodeID, accountID string) {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()

	key := fmt.Sprintf("%s-%s", nodeID, accountID)
	if task, exists := arm.monitoringTasks[key]; exists {
		delete(arm.monitoringTasks, key)
		log.Printf("🗑️ 停止监控: %s - %s (重启次数: %d)", nodeID, accountID, task.RestartCount)
	}
}

// UpdateLastTradeTime 更新最后交易时间
func (arm *AutoRestartManager) UpdateLastTradeTime(nodeID, accountID string) {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()

	key := fmt.Sprintf("%s-%s", nodeID, accountID)
	if task, exists := arm.monitoringTasks[key]; exists {
		task.LastTradeTime = time.Now()
		task.IsWaiting = false // 重置等待状态
	}
}

// checkTimeouts 检查超时任务
func (arm *AutoRestartManager) checkTimeouts() {
	arm.mutex.RLock()
	tasks := make([]*MonitoringTask, 0, len(arm.monitoringTasks))
	for _, task := range arm.monitoringTasks {
		tasks = append(tasks, task)
	}
	arm.mutex.RUnlock()

	now := time.Now()
	timeoutThreshold := 8 * time.Minute // 8分钟超时

	for _, task := range tasks {
		if !task.IsWaiting && now.Sub(task.LastTradeTime) > timeoutThreshold {
			go arm.handleTimeout(task)
		}
	}
}

// handleTimeout 处理超时任务
func (arm *AutoRestartManager) handleTimeout(task *MonitoringTask) {
	arm.mutex.Lock()
	task.IsWaiting = true
	task.RestartCount++
	arm.mutex.Unlock()

	log.Printf("🚨 检测到超时: %s - %s (第%d次重启)", task.NodeID, task.AccountID, task.RestartCount)

	// 1. 停止任务
	if err := arm.stopFlashTradeTask(task.NodeID, task.AccountID); err != nil {
		log.Printf("❌ 停止任务失败: %v", err)
	} else {
		log.Printf("🛑 任务已停止: %s - %s", task.NodeID, task.AccountID)
	}

	// 2. 等待3分钟
	log.Printf("⏳ 等待3分钟后重启: %s - %s", task.NodeID, task.AccountID)
	time.Sleep(3 * time.Minute)

	// 3. 重启任务
	arm.restartTask(task)
}

// stopFlashTradeTask 停止Flash Trade任务
func (arm *AutoRestartManager) stopFlashTradeTask(nodeID, accountID string) error {
	// 获取任务管理器
	taskManager := arm.server.GetTaskManager()

	// 查找并停止相关任务
	tasks := taskManager.GetAllTasks()
	for _, task := range tasks {
		// 检查是否是Flash Trade任务且匹配节点和账号
		if task.Type == "flash_trade" {
			// 检查任务是否包含指定的节点和账号
			if contains(task.TargetNodes, nodeID) &&
				(len(task.TargetAccounts) == 0 || contains(task.TargetAccounts, accountID)) {
				if err := taskManager.StopTask(task.ID); err != nil {
					return fmt.Errorf("停止任务 %s 失败: %v", task.ID, err)
				}
			}
		}
	}

	return nil
}

// restartTask 重启任务
func (arm *AutoRestartManager) restartTask(task *MonitoringTask) {
	// 获取当前已刷交易额
	currentVolume := arm.getCurrentVolume(task.NodeID, task.AccountID)

	// 计算剩余交易额
	remainingVolume := task.OriginalVolume - currentVolume
	if remainingVolume <= 0 {
		log.Printf("✅ 任务已完成: %s - %s (总交易额: %.2f)",
			task.NodeID, task.AccountID, currentVolume)
		arm.RemoveMonitoring(task.NodeID, task.AccountID)
		return
	}

	// 更新任务参数
	newParams := make(map[string]interface{})
	for k, v := range task.TaskParams {
		newParams[k] = v
	}
	newParams["target_volume"] = remainingVolume

	// 重新启动任务
	if err := arm.startFlashTradeTask(newParams); err != nil {
		log.Printf("❌ 重启任务失败: %s - %s, 错误: %v", task.NodeID, task.AccountID, err)
	} else {
		log.Printf("🔄 任务已重启: %s - %s, 剩余交易额: %.2f",
			task.NodeID, task.AccountID, remainingVolume)

		arm.mutex.Lock()
		task.LastTradeTime = time.Now()
		task.IsWaiting = false
		arm.mutex.Unlock()
	}
}

// getCurrentVolume 获取当前交易额（累积）
func (arm *AutoRestartManager) getCurrentVolume(nodeID, accountID string) float64 {
	// 获取节点的Flash Trade统计
	nodeStats := arm.server.GetNodesFlashTradeStats(nodeID)

	if nodeData, exists := nodeStats[nodeID]; exists {
		if nodeMap, ok := nodeData.(map[string]interface{}); ok {
			if accountStats, ok := nodeMap["账户统计"].(map[string]interface{}); ok {
				if accountData, ok := accountStats[accountID].(map[string]interface{}); ok {
					// 🔧 修改：优先使用累积统计中的总完成交易额
					if cumulativeStats, ok := accountData["cumulative_stats"].(map[string]interface{}); ok {
						if completedVolume, ok := cumulativeStats["completed_volume"].(float64); ok {
							return completedVolume
						}
					}

					// 如果没有累积统计，使用普通交易额
					if volume, ok := accountData["总交易额"].(float64); ok {
						return volume
					}
				}
			}
		}
	}

	return 0
}

// startFlashTradeTask 启动Flash Trade任务
func (arm *AutoRestartManager) startFlashTradeTask(params map[string]interface{}) error {
	// 获取任务管理器
	taskManager := arm.server.GetTaskManager()

	// 构建Flash Trade任务参数
	tokenAddress, _ := params["token_address"].(string)
	usdtAmount, _ := params["usdt_amount"].(float64)
	baseAsset, _ := params["base_asset"].(string)
	targetVolume, _ := params["target_volume"].(float64)
	autoLoop, _ := params["auto_loop"].(bool)
	pricePrecision, _ := params["price_precision"].(float64)
	nodeIDs, _ := params["node_ids"].([]string)
	accountID, _ := params["account_id"].(string)

	// 创建通用任务
	newTask := &task.UniversalTask{
		Name:           fmt.Sprintf("Auto-Restart Flash Trade - %s", tokenAddress),
		Type:           "flash_trade",
		TargetNodes:    nodeIDs,
		TargetAccounts: []string{accountID},
		Parameters: map[string]interface{}{
			"token_address":   tokenAddress,
			"usdt_amount":     usdtAmount,
			"base_asset":      baseAsset,
			"target_volume":   targetVolume,
			"auto_loop":       autoLoop,
			"price_precision": pricePrecision,
		},
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	// 创建任务
	err := taskManager.CreateTask(newTask)
	if err != nil {
		return fmt.Errorf("创建任务失败: %v", err)
	}

	log.Printf("🚀 自动重启任务已创建: %s (剩余交易额: %.2f)", newTask.ID, targetVolume)
	return nil
}

// GetMonitoringStatus 获取监控状态
func (arm *AutoRestartManager) GetMonitoringStatus() map[string]*MonitoringTask {
	arm.mutex.RLock()
	defer arm.mutex.RUnlock()

	result := make(map[string]*MonitoringTask)
	for key, task := range arm.monitoringTasks {
		result[key] = task
	}
	return result
}

// EnableGlobalMonitoring 启用全局监控
func (arm *AutoRestartManager) EnableGlobalMonitoring() int {
	// 获取所有活跃的Flash Trade账号
	activeAccounts := arm.getActiveFlashTradeAccounts()

	count := 0
	for _, account := range activeAccounts {
		// 为每个活跃账号启用监控
		taskParams := map[string]interface{}{
			"token_address":   account.TokenAddress,
			"usdt_amount":     100.0,
			"base_asset":      "USDT",
			"target_volume":   account.TargetVolume,
			"auto_loop":       true,
			"price_precision": 8.0,
		}

		arm.AddMonitoring(account.NodeID, account.AccountID, taskParams)
		count++
	}

	log.Printf("🔄 全局自动重启已启用，监控 %d 个账号", count)
	return count
}

// DisableGlobalMonitoring 禁用全局监控
func (arm *AutoRestartManager) DisableGlobalMonitoring() int {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()

	count := len(arm.monitoringTasks)

	// 清空所有监控任务
	for _, task := range arm.monitoringTasks {
		log.Printf("🗑️ 停止监控: %s - %s", task.NodeID, task.AccountID)
	}

	arm.monitoringTasks = make(map[string]*MonitoringTask)

	log.Printf("🔄 全局自动重启已禁用，停止监控 %d 个账号", count)
	return count
}

// getActiveFlashTradeAccounts 获取所有活跃的Flash Trade账号
func (arm *AutoRestartManager) getActiveFlashTradeAccounts() []ActiveAccount {
	var activeAccounts []ActiveAccount

	// 简化实现：返回默认的活跃账号信息
	// 实际使用时可以通过其他方式获取活跃账号列表
	log.Printf("🔍 获取活跃Flash Trade账号（简化实现）")

	return activeAccounts
}

// ActiveAccount 活跃账号信息
type ActiveAccount struct {
	NodeID       string
	AccountID    string
	TokenAddress string
	TargetVolume float64
}

// 辅助函数
func getStringValue(m map[string]interface{}, key, defaultValue string) string {
	if value, ok := m[key].(string); ok {
		return value
	}
	return defaultValue
}

func getFloatValue(m map[string]interface{}, key string, defaultValue float64) float64 {
	if value, ok := m[key].(float64); ok {
		return value
	}
	return defaultValue
}

// contains 检查切片是否包含指定元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
