package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"alpha-autosell-bot/internal/account"
	"github.com/go-redis/redis/v8"
)

// AutoSellNode 自动卖出节点信息
type AutoSellNode struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Status    string    `json:"status"` // online, offline, busy
	LastPing  time.Time `json:"last_ping"`
	TaskCount int       `json:"task_count"`
}

// AutoSellExecutor 自动卖出任务执行器
type AutoSellExecutor struct {
	slaveNodes  map[string]*AutoSellNode // nodeID -> node info
	mutex       sync.RWMutex
	redisClient *redis.Client
}

// NewAutoSellExecutor 创建自动卖出任务执行器
func NewAutoSellExecutor(redisClient *redis.Client) *AutoSellExecutor {
	executor := &AutoSellExecutor{
		slaveNodes:  make(map[string]*AutoSellNode),
		redisClient: redisClient,
	}

	// 启动节点健康检查
	go executor.startHealthCheck()

	return executor
}

// RegisterSlaveNode 注册被控端节点
func (e *AutoSellExecutor) RegisterSlaveNode(nodeID, address string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.slaveNodes[nodeID] = &AutoSellNode{
		NodeID:    nodeID,
		Address:   address,
		Status:    "unknown",
		LastPing:  time.Time{},
		TaskCount: 0,
	}

	log.Printf("📡 注册自动卖出被控端节点: %s -> %s", nodeID, address)

	// 立即检查节点状态
	go e.checkNodeHealth(nodeID)
}

// RegisterSlaveNodes 批量注册被控端节点
func (e *AutoSellExecutor) RegisterSlaveNodes(nodes map[string]string) {
	for nodeID, address := range nodes {
		e.RegisterSlaveNode(nodeID, address)
	}
}

// GetAvailableNodes 获取可用节点
func (e *AutoSellExecutor) GetAvailableNodes() []*AutoSellNode {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	var availableNodes []*AutoSellNode
	for _, node := range e.slaveNodes {
		if node.Status == "online" {
			availableNodes = append(availableNodes, node)
		}
	}

	return availableNodes
}

// SelectBestNode 选择最佳节点（负载最低）
func (e *AutoSellExecutor) SelectBestNode() *AutoSellNode {
	availableNodes := e.GetAvailableNodes()
	if len(availableNodes) == 0 {
		return nil
	}

	// 选择任务数最少的节点
	bestNode := availableNodes[0]
	for _, node := range availableNodes[1:] {
		if node.TaskCount < bestNode.TaskCount {
			bestNode = node
		}
	}

	return bestNode
}

// startHealthCheck 启动节点健康检查
func (e *AutoSellExecutor) startHealthCheck() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for range ticker.C {
		e.mutex.RLock()
		nodeIDs := make([]string, 0, len(e.slaveNodes))
		for nodeID := range e.slaveNodes {
			nodeIDs = append(nodeIDs, nodeID)
		}
		e.mutex.RUnlock()

		// 并发检查所有节点
		for _, nodeID := range nodeIDs {
			go e.checkNodeHealth(nodeID)
		}
	}
}

// checkNodeHealth 检查节点健康状态
func (e *AutoSellExecutor) checkNodeHealth(nodeID string) {
	e.mutex.RLock()
	node, exists := e.slaveNodes[nodeID]
	if !exists {
		e.mutex.RUnlock()
		return
	}
	address := node.Address
	e.mutex.RUnlock()

	// 调用节点的健康检查接口
	url := fmt.Sprintf("http://%s/health", address)
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		e.updateNodeStatus(nodeID, "offline", 0)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// 尝试获取任务数量
		taskCount := e.getNodeTaskCount(address)
		e.updateNodeStatus(nodeID, "online", taskCount)
	} else {
		e.updateNodeStatus(nodeID, "offline", 0)
	}
}

// getNodeTaskCount 获取节点任务数量
func (e *AutoSellExecutor) getNodeTaskCount(address string) int {
	url := fmt.Sprintf("http://%s/tasks", address)
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return 0
	}

	if tasks, ok := response["tasks"].([]interface{}); ok {
		return len(tasks)
	}

	return 0
}

// updateNodeStatus 更新节点状态
func (e *AutoSellExecutor) updateNodeStatus(nodeID, status string, taskCount int) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if node, exists := e.slaveNodes[nodeID]; exists {
		oldStatus := node.Status
		node.Status = status
		node.LastPing = time.Now()
		node.TaskCount = taskCount

		if oldStatus != status {
			log.Printf("📡 节点状态变更: %s %s -> %s (任务数: %d)", nodeID, oldStatus, status, taskCount)
		}
	}
}

// Execute 执行自动卖出任务
func (e *AutoSellExecutor) Execute(task *UniversalTask, accountManager *account.UniversalAccountManager) error {
	log.Printf("🚀 开始执行自动卖出任务: %s", task.ID)
	log.Printf("🔧 [调试] 任务参数: %+v", task.Parameters)
	log.Printf("🔧 [调试] 目标账号: %v", task.TargetAccounts)

	// 解析任务参数
	tokenAddress, ok := task.Parameters["token_address"].(string)
	if !ok {
		return fmt.Errorf("缺少必要参数: token_address")
	}

	monitorAmount, ok := task.Parameters["monitor_amount"].(float64)
	if !ok {
		return fmt.Errorf("缺少必要参数: monitor_amount")
	}

	baseAsset, _ := task.Parameters["base_asset"].(string)
	if baseAsset == "" {
		baseAsset = "ALPHA_251" // 默认值
	}

	// 更新任务进度
	task.Progress.TotalSteps = len(task.TargetAccounts)
	task.Progress.CompletedSteps = 0
	task.Progress.CurrentStep = "启动自动卖出监控"

	successCount := 0
	failureCount := 0

	// 为每个目标账号启动监控任务
	for _, accountID := range task.TargetAccounts {
		log.Printf("💰 [%s] 启动自动卖出监控: 代币=%s, 监控数量=%.6f", accountID, tokenAddress, monitorAmount)

		// 获取账号信息（验证账号存在并获取分配节点）
		account, err := accountManager.GetAccount(accountID)
		if err != nil {
			log.Printf("❌ [%s] 获取账号信息失败: %v", accountID, err)
			failureCount++
			continue
		}

		// 确定目标节点
		var targetNode string
		if account.AssignedNode != "" {
			// 账号已分配给特定节点
			targetNode = account.AssignedNode
			log.Printf("🎯 [%s] 账号已分配给节点: %s", accountID, targetNode)
		} else {
			// 账号未分配，发送给所有节点（让被控端自己判断）
			targetNode = ""
			log.Printf("🌐 [%s] 账号未分配节点，发送给所有在线节点", accountID)
		}

		// 通过 Redis 发送自动卖出命令到被控端
		success := e.sendAutoSellCommand(accountID, tokenAddress, monitorAmount, baseAsset, targetNode)
		if success {
			log.Printf("✅ [%s] 自动卖出监控启动成功", accountID)
			successCount++

			// 更新账号进度
			task.Progress.AccountProgress[accountID] = AccountProgress{
				AccountID:     accountID,
				Status:        "monitoring",
				Progress:      100.0,
				CurrentAction: "监控中",
				StartTime:     time.Now(),
				LastUpdate:    time.Now(),
				Stats: map[string]interface{}{
					"token_address":  tokenAddress,
					"monitor_amount": monitorAmount,
					"base_asset":     baseAsset,
				},
			}
		} else {
			log.Printf("❌ [%s] 自动卖出监控启动失败", accountID)
			failureCount++

			// 更新账号进度
			task.Progress.AccountProgress[accountID] = AccountProgress{
				AccountID:     accountID,
				Status:        "failed",
				Progress:      0.0,
				CurrentAction: "启动失败",
				StartTime:     time.Now(),
				LastUpdate:    time.Now(),
				ErrorMessage:  "调用被控端接口失败",
			}
		}

		task.Progress.CompletedSteps++
		task.Progress.Percentage = float64(task.Progress.CompletedSteps) / float64(task.Progress.TotalSteps) * 100
	}

	// 更新任务结果
	task.Results["success_count"] = successCount
	task.Results["failure_count"] = failureCount
	task.Results["total_accounts"] = len(task.TargetAccounts)
	task.Results["completion_time"] = time.Now().Format(time.RFC3339)

	if failureCount > 0 {
		task.Results["status"] = "partial_success"
		task.Results["message"] = fmt.Sprintf("部分成功: %d成功, %d失败", successCount, failureCount)
	} else {
		task.Results["status"] = "success"
		task.Results["message"] = fmt.Sprintf("全部成功: %d个账号的自动卖出监控已启动", successCount)
	}

	log.Printf("✅ 自动卖出任务执行完成: %s (成功: %d, 失败: %d)", task.ID, successCount, failureCount)
	return nil
}

// sendAutoSellCommand 通过 Redis 发送自动卖出命令
func (e *AutoSellExecutor) sendAutoSellCommand(accountID, tokenAddress string, monitorAmount float64, baseAsset, targetNode string) bool {
	if e.redisClient == nil {
		log.Printf("❌ Redis客户端未初始化")
		return false
	}

	// 构建命令载荷（让被控端自己获取认证信息，就像Flash Trade一样）
	payload := map[string]interface{}{
		"action":         "start",
		"account_id":     accountID,
		"token_address":  tokenAddress,
		"monitor_amount": monitorAmount,
		"base_asset":     baseAsset,
	}

	// 构建 binance.Command 格式的命令
	command := map[string]interface{}{
		"command_id":   fmt.Sprintf("autosell_%s_%d", accountID, time.Now().Unix()),
		"command_type": "start_monitor",
		"target_node":  targetNode, // 指定目标节点，空表示所有节点
		"payload":      payload,
		"timestamp":    time.Now().Unix(),
	}

	// 序列化命令
	commandJSON, err := json.Marshal(command)
	if err != nil {
		log.Printf("❌ 序列化自动卖出命令失败: %v", err)
		return false
	}

	// 发送到 Redis 频道
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = e.redisClient.Publish(ctx, "autosell_commands", string(commandJSON)).Err()
	if err != nil {
		log.Printf("❌ 发送自动卖出命令失败: %v", err)
		return false
	}

	log.Printf("✅ 自动卖出命令已发送: 账号=%s, 代币=%s, 数量=%.6f, 目标节点=%s", accountID, tokenAddress, monitorAmount, targetNode)
	return true
}

// getOnlineNodes 获取在线节点列表（简化实现）
func (e *AutoSellExecutor) getOnlineNodes() []string {
	// 这里可以从Redis或其他地方获取在线节点列表
	// 简化实现：返回默认节点
	return []string{"default"}
}

// callSlaveMonitorAPI 调用被控端的监控接口
func (e *AutoSellExecutor) callSlaveMonitorAPI(nodeAddress, accountID, tokenAddress string, monitorAmount float64, baseAsset string) bool {
	url := fmt.Sprintf("http://%s/monitor", nodeAddress)

	// 构建请求参数
	requestData := map[string]interface{}{
		"account_id":     accountID,
		"token_address":  tokenAddress,
		"monitor_amount": monitorAmount,
		"base_asset":     baseAsset,
	}

	// 序列化请求体
	requestBody, err := json.Marshal(requestData)
	if err != nil {
		log.Printf("❌ 序列化请求参数失败: %v", err)
		return false
	}

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		log.Printf("❌ 创建HTTP请求失败: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 调用被控端接口失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ 读取响应失败: %v", err)
		return false
	}

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("❌ 解析响应失败: %v", err)
		return false
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		log.Printf("📡 被控端响应成功: %s", response["message"])
		return true
	} else {
		log.Printf("📡 被控端响应失败: %s", response["message"])
		return false
	}
}

// Validate 验证任务参数
func (e *AutoSellExecutor) Validate(parameters map[string]interface{}) error {
	// 验证必要参数
	if _, ok := parameters["token_address"]; !ok {
		return fmt.Errorf("缺少必要参数: token_address")
	}

	if _, ok := parameters["monitor_amount"]; !ok {
		return fmt.Errorf("缺少必要参数: monitor_amount")
	}

	// 验证参数类型
	if _, ok := parameters["token_address"].(string); !ok {
		return fmt.Errorf("参数类型错误: token_address 必须是字符串")
	}

	if _, ok := parameters["monitor_amount"].(float64); !ok {
		return fmt.Errorf("参数类型错误: monitor_amount 必须是数字")
	}

	return nil
}

// GetDefaultParameters 获取默认参数
func (e *AutoSellExecutor) GetDefaultParameters() map[string]interface{} {
	return map[string]interface{}{
		"token_address":  "",
		"monitor_amount": 1.0,
		"base_asset":     "ALPHA_251",
	}
}
