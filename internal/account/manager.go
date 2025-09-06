package account

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// UniversalAccount 通用账号结构
type UniversalAccount struct {
	// 基本信息
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"` // active, inactive, expired, error

	// 认证信息
	Csrftoken string `json:"csrftoken"`
	Cookie    string `json:"cookie"`

	// 时间管理
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	ExpiresAt time.Time `json:"expires_at"` // Cookie有效期 (4.5天)
	LastUsed  time.Time `json:"last_used"`

	// 分配信息
	AssignedNode string `json:"assigned_node"` // 当前分配的节点

	// 任务使用记录
	TaskHistory []TaskUsage `json:"task_history"`

	// 统计信息
	TotalUsage    int       `json:"total_usage"`     // 总使用次数
	SuccessRate   float64   `json:"success_rate"`    // 成功率
	LastErrorMsg  string    `json:"last_error_msg"`  // 最后错误信息
	LastErrorTime time.Time `json:"last_error_time"` // 最后错误时间

	// 扩展字段 - 支持不同任务类型的特定配置
	Extensions map[string]interface{} `json:"extensions"`
}

// TaskUsage 任务使用记录
type TaskUsage struct {
	TaskID    string    `json:"task_id"`
	TaskType  string    `json:"task_type"` // flash_trade, auto_sell, etc.
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Status    string    `json:"status"` // running, completed, failed
	Result    string    `json:"result"`
	NodeID    string    `json:"node_id"`
}

// UniversalAccountManager 通用账号管理器
type UniversalAccountManager struct {
	accounts     map[string]*UniversalAccount
	nodeAccounts map[string][]string // 节点ID -> 账号ID列表
	mutex        sync.RWMutex
	dataFile     string
	redisClient  *redis.Client // Redis 客户端
	useRedis     bool          // 是否使用 Redis 存储
}

// NewUniversalAccountManager 创建通用账号管理器（文件存储）
func NewUniversalAccountManager(dataFile string) *UniversalAccountManager {
	manager := &UniversalAccountManager{
		accounts:     make(map[string]*UniversalAccount),
		nodeAccounts: make(map[string][]string),
		dataFile:     dataFile,
		useRedis:     false,
	}

	// 确保数据目录存在
	if err := os.MkdirAll(filepath.Dir(dataFile), 0755); err != nil {
		log.Printf("❌ 创建数据目录失败: %v", err)
	}

	// 加载现有数据
	manager.loadFromFile()

	return manager
}

// NewUniversalAccountManagerWithRedis 创建使用 Redis 的账号管理器
func NewUniversalAccountManagerWithRedis(redisClient *redis.Client) *UniversalAccountManager {
	manager := &UniversalAccountManager{
		accounts:     make(map[string]*UniversalAccount),
		nodeAccounts: make(map[string][]string),
		redisClient:  redisClient,
		useRedis:     true,
	}

	// 从 Redis 加载现有数据
	if err := manager.loadFromRedis(); err != nil {
		log.Printf("⚠️ 从 Redis 加载账号数据失败: %v", err)
	}

	// 加载节点账号映射
	if err := manager.loadNodeAccountsFromRedis(); err != nil {
		log.Printf("⚠️ 从 Redis 加载节点账号映射失败: %v", err)
	}

	return manager
}

// AddAccount 添加账号
func (m *UniversalAccountManager) AddAccount(account *UniversalAccount) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.accounts[account.ID]; exists {
		return fmt.Errorf("账号ID已存在: %s", account.ID)
	}

	// 设置默认值
	now := time.Now()
	account.CreatedAt = now
	account.UpdatedAt = now
	account.ExpiresAt = now.Add(4*24*time.Hour + 12*time.Hour) // 4.5天
	account.Status = "active"
	account.TaskHistory = []TaskUsage{}
	account.Extensions = make(map[string]interface{})

	if account.Name == "" {
		account.Name = account.ID
	}

	m.accounts[account.ID] = account

	// 如果指定了节点，添加到节点账号列表
	if account.AssignedNode != "" {
		m.assignAccountToNode(account.ID, account.AssignedNode)
	}

	// 保存到存储
	if m.useRedis {
		return m.saveToRedis(account.ID, account)
	}
	return m.saveToFile()
}

// UpdateAccount 更新账号
func (m *UniversalAccountManager) UpdateAccount(accountID string, updates *UniversalAccount) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	account, exists := m.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	// 记录旧的节点分配，用于后续同步
	oldAssignedNode := account.AssignedNode

	// 更新字段
	if updates.Name != "" {
		account.Name = updates.Name
	}
	if updates.Description != "" {
		account.Description = updates.Description
	}
	if updates.Csrftoken != "" {
		account.Csrftoken = updates.Csrftoken
		// 更新Cookie时重新设置过期时间
		account.ExpiresAt = time.Now().Add(4*24*time.Hour + 12*time.Hour)
	}
	if updates.Cookie != "" {
		account.Cookie = updates.Cookie
		// 更新Cookie时重新设置过期时间
		account.ExpiresAt = time.Now().Add(4*24*time.Hour + 12*time.Hour)
	}
	if updates.Status != "" {
		account.Status = updates.Status
	}

	// 处理节点分配更新
	nodeChanged := false
	if updates.AssignedNode != account.AssignedNode {
		// 从旧节点移除
		if account.AssignedNode != "" {
			m.removeAccountFromNode(accountID, account.AssignedNode)
		}

		// 分配到新节点（如果指定了新节点）
		if updates.AssignedNode != "" {
			m.assignAccountToNode(accountID, updates.AssignedNode)
		}

		account.AssignedNode = updates.AssignedNode
		nodeChanged = true
	}

	account.UpdatedAt = time.Now()

	// 保存到存储
	var err error
	if m.useRedis {
		err = m.saveToRedis(account.ID, account)
	} else {
		err = m.saveToFile()
	}

	// 如果保存成功且节点发生变化，返回节点变化信息
	if err == nil && nodeChanged {
		// 这里可以通过回调或者其他方式通知节点变化
		// 暂时通过日志记录
		log.Printf("📋 账号 %s 节点分配已变更: %s -> %s", accountID, oldAssignedNode, account.AssignedNode)
	}

	return err
}

// DeleteAccount 删除账号
func (m *UniversalAccountManager) DeleteAccount(accountID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.accounts[accountID]; !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	// 从所有节点中移除
	for nodeID := range m.nodeAccounts {
		m.removeAccountFromNode(accountID, nodeID)
	}

	delete(m.accounts, accountID)

	// 从存储中删除
	if m.useRedis {
		err := m.deleteFromRedis(accountID)
		if err != nil {
			return err
		}
		// 同时更新节点账号映射
		return m.saveNodeAccountsToRedis()
	}
	return m.saveToFile()
}

// GetAccount 获取账号
func (m *UniversalAccountManager) GetAccount(accountID string) (*UniversalAccount, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	account, exists := m.accounts[accountID]
	if !exists {
		return nil, fmt.Errorf("账号不存在: %s", accountID)
	}

	return account, nil
}

// GetAllAccounts 获取所有账号
func (m *UniversalAccountManager) GetAllAccounts() map[string]*UniversalAccount {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]*UniversalAccount)
	for id, account := range m.accounts {
		result[id] = account
	}

	return result
}

// GetNodeAccounts 获取节点的账号列表
func (m *UniversalAccountManager) GetNodeAccounts(nodeID string) []*UniversalAccount {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var accounts []*UniversalAccount
	if accountIDs, exists := m.nodeAccounts[nodeID]; exists {
		for _, accountID := range accountIDs {
			if account, exists := m.accounts[accountID]; exists {
				accounts = append(accounts, account)
			}
		}
	}

	return accounts
}

// AssignAccountToNode 将账号分配给节点
func (m *UniversalAccountManager) AssignAccountToNode(accountID, nodeID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	account, exists := m.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	// 从旧节点移除
	if account.AssignedNode != "" {
		m.removeAccountFromNode(accountID, account.AssignedNode)
	}

	// 分配到新节点
	account.AssignedNode = nodeID
	m.assignAccountToNode(accountID, nodeID)

	// 保存到存储
	if m.useRedis {
		err := m.saveToRedis(account.ID, account)
		if err != nil {
			return err
		}
		// 节点账号映射已在 saveToRedis 中更新
		return nil
	}
	return m.saveToFile()
}

// assignAccountToNode 内部方法：将账号添加到节点
func (m *UniversalAccountManager) assignAccountToNode(accountID, nodeID string) {
	if m.nodeAccounts[nodeID] == nil {
		m.nodeAccounts[nodeID] = []string{}
	}

	// 检查是否已存在
	for _, id := range m.nodeAccounts[nodeID] {
		if id == accountID {
			return
		}
	}

	m.nodeAccounts[nodeID] = append(m.nodeAccounts[nodeID], accountID)
}

// removeAccountFromNode 内部方法：从节点移除账号
func (m *UniversalAccountManager) removeAccountFromNode(accountID, nodeID string) {
	if accounts, exists := m.nodeAccounts[nodeID]; exists {
		for i, id := range accounts {
			if id == accountID {
				m.nodeAccounts[nodeID] = append(accounts[:i], accounts[i+1:]...)
				break
			}
		}
	}
}

// RecordTaskUsage 记录任务使用
func (m *UniversalAccountManager) RecordTaskUsage(accountID string, usage TaskUsage) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	account, exists := m.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	account.TaskHistory = append(account.TaskHistory, usage)
	account.TotalUsage++
	account.LastUsed = time.Now()
	account.UpdatedAt = time.Now()

	// 计算成功率
	successCount := 0
	for _, task := range account.TaskHistory {
		if task.Status == "completed" {
			successCount++
		}
	}
	account.SuccessRate = float64(successCount) / float64(len(account.TaskHistory)) * 100

	// 保存到存储
	if m.useRedis {
		return m.saveToRedis(account.ID, account)
	}
	return m.saveToFile()
}

// GetExpiredAccounts 获取过期账号
func (m *UniversalAccountManager) GetExpiredAccounts() []*UniversalAccount {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var expired []*UniversalAccount
	now := time.Now()

	for _, account := range m.accounts {
		if account.ExpiresAt.Before(now) && account.Status == "active" {
			expired = append(expired, account)
		}
	}

	return expired
}

// GetAccountsNearExpiry 获取即将过期的账号
func (m *UniversalAccountManager) GetAccountsNearExpiry(hours int) []*UniversalAccount {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var nearExpiry []*UniversalAccount
	now := time.Now()
	threshold := now.Add(time.Duration(hours) * time.Hour)

	for _, account := range m.accounts {
		if account.ExpiresAt.Before(threshold) && account.ExpiresAt.After(now) && account.Status == "active" {
			nearExpiry = append(nearExpiry, account)
		}
	}

	return nearExpiry
}

// loadFromFile 从文件加载数据
func (m *UniversalAccountManager) loadFromFile() error {
	if _, err := os.Stat(m.dataFile); os.IsNotExist(err) {
		return nil // 文件不存在，跳过加载
	}

	data, err := ioutil.ReadFile(m.dataFile)
	if err != nil {
		return err
	}

	var fileData struct {
		Accounts     map[string]*UniversalAccount `json:"accounts"`
		NodeAccounts map[string][]string          `json:"node_accounts"`
	}

	if err := json.Unmarshal(data, &fileData); err != nil {
		return err
	}

	m.accounts = fileData.Accounts
	m.nodeAccounts = fileData.NodeAccounts

	if m.accounts == nil {
		m.accounts = make(map[string]*UniversalAccount)
	}
	if m.nodeAccounts == nil {
		m.nodeAccounts = make(map[string][]string)
	}

	log.Printf("✅ 加载账号数据: %d个账号", len(m.accounts))
	return nil
}

// saveToFile 保存数据到文件
func (m *UniversalAccountManager) saveToFile() error {
	fileData := struct {
		Accounts     map[string]*UniversalAccount `json:"accounts"`
		NodeAccounts map[string][]string          `json:"node_accounts"`
	}{
		Accounts:     m.accounts,
		NodeAccounts: m.nodeAccounts,
	}

	data, err := json.MarshalIndent(fileData, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(m.dataFile, data, 0644)
}

// Redis 存储相关方法

// loadFromRedis 从 Redis 加载所有账号数据
func (m *UniversalAccountManager) loadFromRedis() error {
	if m.redisClient == nil {
		return fmt.Errorf("Redis 客户端未初始化")
	}

	ctx := context.Background()

	// 获取所有账号 key
	keys, err := m.redisClient.Keys(ctx, "account:*").Result()
	if err != nil {
		return fmt.Errorf("获取账号 keys 失败: %v", err)
	}

	log.Printf("📡 从 Redis 加载账号数据，找到 %d 个账号", len(keys))

	for _, key := range keys {
		// 获取账号数据
		data, err := m.redisClient.Get(ctx, key).Result()
		if err != nil {
			log.Printf("⚠️ 获取账号数据失败 %s: %v", key, err)
			continue
		}

		// 解析账号数据
		var account UniversalAccount
		if err := json.Unmarshal([]byte(data), &account); err != nil {
			log.Printf("⚠️ 解析账号数据失败 %s: %v", key, err)
			continue
		}

		// 添加到内存
		m.accounts[account.ID] = &account
	}

	log.Printf("✅ 从 Redis 加载账号数据完成: %d个账号", len(m.accounts))
	return nil
}

// saveToRedis 保存单个账号到 Redis
func (m *UniversalAccountManager) saveToRedis(accountID string, account *UniversalAccount) error {
	if m.redisClient == nil {
		return fmt.Errorf("Redis 客户端未初始化")
	}

	ctx := context.Background()
	key := fmt.Sprintf("account:%s", accountID)

	// 序列化账号数据
	data, err := json.Marshal(account)
	if err != nil {
		return fmt.Errorf("序列化账号数据失败: %v", err)
	}

	// 保存到 Redis
	if err := m.redisClient.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("保存账号到 Redis 失败: %v", err)
	}

	// 同时保存节点账号映射关系
	if err := m.saveNodeAccountsToRedis(); err != nil {
		log.Printf("⚠️ 保存节点账号映射失败: %v", err)
	}

	log.Printf("✅ 账号已保存到 Redis: %s", accountID)
	return nil
}

// deleteFromRedis 从 Redis 删除账号
func (m *UniversalAccountManager) deleteFromRedis(accountID string) error {
	if m.redisClient == nil {
		return fmt.Errorf("Redis 客户端未初始化")
	}

	ctx := context.Background()
	key := fmt.Sprintf("account:%s", accountID)

	if err := m.redisClient.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("从 Redis 删除账号失败: %v", err)
	}

	log.Printf("✅ 账号已从 Redis 删除: %s", accountID)
	return nil
}

// GetAccountFromRedis 直接从 Redis 获取账号（供被控端使用）
func GetAccountFromRedis(redisClient *redis.Client, accountID string) (*UniversalAccount, error) {
	ctx := context.Background()
	key := fmt.Sprintf("account:%s", accountID)

	data, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("账号不存在: %s", accountID)
		}
		return nil, fmt.Errorf("获取账号失败: %v", err)
	}

	var account UniversalAccount
	if err := json.Unmarshal([]byte(data), &account); err != nil {
		return nil, fmt.Errorf("解析账号数据失败: %v", err)
	}

	return &account, nil
}

// saveNodeAccountsToRedis 保存节点账号映射到 Redis
func (m *UniversalAccountManager) saveNodeAccountsToRedis() error {
	if m.redisClient == nil {
		return fmt.Errorf("Redis 客户端未初始化")
	}

	ctx := context.Background()
	key := "node_accounts_mapping"

	// 序列化节点账号映射
	data, err := json.Marshal(m.nodeAccounts)
	if err != nil {
		return fmt.Errorf("序列化节点账号映射失败: %v", err)
	}

	// 保存到 Redis
	if err := m.redisClient.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("保存节点账号映射到 Redis 失败: %v", err)
	}

	return nil
}

// loadNodeAccountsFromRedis 从 Redis 加载节点账号映射
func (m *UniversalAccountManager) loadNodeAccountsFromRedis() error {
	if m.redisClient == nil {
		return fmt.Errorf("Redis 客户端未初始化")
	}

	ctx := context.Background()
	key := "node_accounts_mapping"

	// 获取节点账号映射数据
	data, err := m.redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// 数据不存在，初始化为空映射
			m.nodeAccounts = make(map[string][]string)
			return nil
		}
		return fmt.Errorf("获取节点账号映射失败: %v", err)
	}

	// 解析节点账号映射
	if err := json.Unmarshal([]byte(data), &m.nodeAccounts); err != nil {
		return fmt.Errorf("解析节点账号映射失败: %v", err)
	}

	if m.nodeAccounts == nil {
		m.nodeAccounts = make(map[string][]string)
	}

	return nil
}

// GetAllAccountsFromRedis 从 Redis 获取所有账号（供被控端使用）
func GetAllAccountsFromRedis(redisClient *redis.Client) (map[string]*UniversalAccount, error) {
	ctx := context.Background()

	// 获取所有账号 key
	keys, err := redisClient.Keys(ctx, "account:*").Result()
	if err != nil {
		return nil, fmt.Errorf("获取账号 keys 失败: %v", err)
	}

	accounts := make(map[string]*UniversalAccount)

	for _, key := range keys {
		// 获取账号数据
		data, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			log.Printf("⚠️ 获取账号数据失败 %s: %v", key, err)
			continue
		}

		// 解析账号数据
		var account UniversalAccount
		if err := json.Unmarshal([]byte(data), &account); err != nil {
			log.Printf("⚠️ 解析账号数据失败 %s: %v", key, err)
			continue
		}

		accounts[account.ID] = &account
	}

	return accounts, nil
}
