package slave

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Account 账号信息
type Account struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Csrftoken   string    `json:"csrftoken"`
	Cookie      string    `json:"cookie"`
	Status      string    `json:"status"` // active, inactive, expired, error
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	LastUsed    time.Time `json:"last_used"`
	Description string    `json:"description"`

	// 新增字段 - Cookie有效期管理
	ExpiresAt time.Time `json:"expires_at"` // Cookie有效期 (4.5天)

	// 交易状态跟踪
	IsLooping     bool   `json:"is_looping"`      // 是否正在循环交易
	CurrentToken  string `json:"current_token"`   // 当前交易的代币地址
	CurrentTaskID string `json:"current_task_id"` // 当前任务ID

	// 交易统计
	TotalVolume    float64 `json:"total_volume"`    // 总交易量
	TargetVolume   float64 `json:"target_volume"`   // 目标交易量
	TradeCount     int     `json:"trade_count"`     // 交易次数
	TotalLoss      float64 `json:"total_loss"`      // 总磨损 (USDT)
	TotalLossRate  float64 `json:"total_loss_rate"` // 总磨损率 (万分比)
	CompletionRate float64 `json:"completion_rate"` // 完成率 (%)

	// 时间记录
	TaskStartTime time.Time `json:"task_start_time"` // 任务开始时间
	LastTradeTime time.Time `json:"last_trade_time"` // 最后交易时间
}

// AccountManager 账号管理器
type AccountManager struct {
	accounts   map[string]*Account
	mutex      sync.RWMutex
	configFile string
	nodeID     string
}

// AccountConfig 账号配置文件结构
type AccountConfig struct {
	NodeID   string              `json:"node_id"`
	Accounts map[string]*Account `json:"accounts"`
	Version  string              `json:"version"`
	UpdateAt time.Time           `json:"update_at"`
}

// NewAccountManager 创建账号管理器
func NewAccountManager(nodeID, configFile string) *AccountManager {
	am := &AccountManager{
		accounts:   make(map[string]*Account),
		configFile: configFile,
		nodeID:     nodeID,
	}

	// 加载现有配置
	am.loadConfig()

	return am
}

// AddAccount 添加账号
func (am *AccountManager) AddAccount(account *Account) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	// 验证账号信息
	if account.ID == "" {
		return fmt.Errorf("账号ID不能为空")
	}

	if account.Csrftoken == "" || account.Cookie == "" {
		return fmt.Errorf("csrftoken和cookie不能为空")
	}

	// 检查是否已存在
	if _, exists := am.accounts[account.ID]; exists {
		return fmt.Errorf("账号ID已存在: %s", account.ID)
	}

	// 设置默认值
	now := time.Now()
	account.CreatedAt = now
	account.UpdatedAt = now
	account.LastUsed = now
	account.Status = "active"

	// 设置Cookie有效期为4.5天
	account.ExpiresAt = now.Add(4*24*time.Hour + 12*time.Hour) // 4.5天

	if account.Name == "" {
		account.Name = account.ID
	}

	// 添加到内存
	am.accounts[account.ID] = account

	// 保存到文件
	if err := am.saveConfig(); err != nil {
		delete(am.accounts, account.ID)
		return fmt.Errorf("保存配置失败: %v", err)
	}

	log.Printf("✅ 账号添加成功: %s (%s)", account.ID, account.Name)
	return nil
}

// UpdateAccount 更新账号
func (am *AccountManager) UpdateAccount(accountID string, updates *Account) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	account, exists := am.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	// 更新字段
	if updates.Name != "" {
		account.Name = updates.Name
	}
	if updates.Csrftoken != "" {
		account.Csrftoken = updates.Csrftoken
	}
	if updates.Cookie != "" {
		account.Cookie = updates.Cookie
	}
	if updates.Status != "" {
		account.Status = updates.Status
	}
	if updates.Description != "" {
		account.Description = updates.Description
	}

	account.UpdatedAt = time.Now()

	// 保存配置
	if err := am.saveConfig(); err != nil {
		return fmt.Errorf("保存配置失败: %v", err)
	}

	log.Printf("✅ 账号更新成功: %s", accountID)
	return nil
}

// DeleteAccount 删除账号
func (am *AccountManager) DeleteAccount(accountID string) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	if _, exists := am.accounts[accountID]; !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	delete(am.accounts, accountID)

	// 保存配置
	if err := am.saveConfig(); err != nil {
		return fmt.Errorf("保存配置失败: %v", err)
	}

	log.Printf("✅ 账号删除成功: %s", accountID)
	return nil
}

// GetAccount 获取账号
func (am *AccountManager) GetAccount(accountID string) (*Account, error) {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	account, exists := am.accounts[accountID]
	if !exists {
		return nil, fmt.Errorf("账号不存在: %s", accountID)
	}

	// 返回副本
	accountCopy := *account
	return &accountCopy, nil
}

// GetAllAccounts 获取所有账号
func (am *AccountManager) GetAllAccounts() map[string]*Account {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	result := make(map[string]*Account)
	for id, account := range am.accounts {
		accountCopy := *account
		result[id] = &accountCopy
	}

	return result
}

// GetActiveAccounts 获取活跃账号
func (am *AccountManager) GetActiveAccounts() map[string]*Account {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	result := make(map[string]*Account)
	for id, account := range am.accounts {
		if account.Status == "active" {
			accountCopy := *account
			result[id] = &accountCopy
		}
	}

	return result
}

// UpdateLastUsed 更新最后使用时间
func (am *AccountManager) UpdateLastUsed(accountID string) {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	if account, exists := am.accounts[accountID]; exists {
		account.LastUsed = time.Now()
		am.saveConfig() // 异步保存，忽略错误
	}
}

// ValidateAccount 验证账号有效性
func (am *AccountManager) ValidateAccount(accountID string) error {
	account, err := am.GetAccount(accountID)
	if err != nil {
		return err
	}

	if account.Status != "active" {
		return fmt.Errorf("账号未激活: %s", accountID)
	}

	if account.Csrftoken == "" || account.Cookie == "" {
		return fmt.Errorf("账号认证信息不完整: %s", accountID)
	}

	return nil
}

// loadConfig 加载配置文件
func (am *AccountManager) loadConfig() error {
	if _, err := os.Stat(am.configFile); os.IsNotExist(err) {
		log.Printf("📋 配置文件不存在，创建新配置: %s", am.configFile)
		return am.saveConfig()
	}

	data, err := ioutil.ReadFile(am.configFile)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config AccountConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("解析配置文件失败: %v", err)
	}

	// 检查节点ID是否匹配
	if config.NodeID != "" && config.NodeID != am.nodeID {
		log.Printf("⚠️ 配置文件节点ID不匹配: 期望%s, 实际%s", am.nodeID, config.NodeID)
	}

	am.accounts = config.Accounts
	if am.accounts == nil {
		am.accounts = make(map[string]*Account)
	}

	log.Printf("✅ 加载配置成功: %d个账号", len(am.accounts))
	return nil
}

// saveConfig 保存配置文件
func (am *AccountManager) saveConfig() error {
	config := AccountConfig{
		NodeID:   am.nodeID,
		Accounts: am.accounts,
		Version:  "1.0",
		UpdateAt: time.Now(),
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %v", err)
	}

	// 确保目录存在
	dir := filepath.Dir(am.configFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %v", err)
	}

	if err := ioutil.WriteFile(am.configFile, data, 0600); err != nil {
		return fmt.Errorf("写入配置文件失败: %v", err)
	}

	return nil
}

// GetAccountCount 获取账号数量
func (am *AccountManager) GetAccountCount() (total, active int) {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	total = len(am.accounts)
	for _, account := range am.accounts {
		if account.Status == "active" {
			active++
		}
	}

	return total, active
}

// SetAccountStatus 设置账号状态
func (am *AccountManager) SetAccountStatus(accountID, status string) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	account, exists := am.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	account.Status = status
	account.UpdatedAt = time.Now()

	return am.saveConfig()
}

// GetNodeID 获取节点ID
func (am *AccountManager) GetNodeID() string {
	return am.nodeID
}

// UpdateAccountStats 更新账号交易统计
func (am *AccountManager) UpdateAccountStats(accountID string, stats *AccountStats) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	account, exists := am.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	// 更新统计数据
	account.TotalVolume = stats.TotalVolume
	account.TargetVolume = stats.TargetVolume
	account.TradeCount = stats.TradeCount
	account.TotalLoss = stats.TotalLoss
	account.TotalLossRate = stats.TotalLossRate
	account.IsLooping = stats.IsLooping
	account.CurrentToken = stats.CurrentToken
	account.CurrentTaskID = stats.CurrentTaskID
	account.LastTradeTime = stats.LastTradeTime
	account.TaskStartTime = stats.TaskStartTime

	// 计算完成率
	if account.TargetVolume > 0 {
		account.CompletionRate = (account.TotalVolume / account.TargetVolume) * 100
	}

	account.UpdatedAt = time.Now()
	account.LastUsed = time.Now()

	// 保存到文件
	return am.saveConfig()
}

// SetAccountLooping 设置账号循环状态
func (am *AccountManager) SetAccountLooping(accountID string, isLooping bool, token string, taskID string) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	account, exists := am.accounts[accountID]
	if !exists {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	account.IsLooping = isLooping
	account.CurrentToken = token
	account.CurrentTaskID = taskID

	if isLooping {
		account.TaskStartTime = time.Now()
	}

	account.UpdatedAt = time.Now()
	account.LastUsed = time.Now()

	return am.saveConfig()
}

// CheckExpiredAccounts 检查过期账号
func (am *AccountManager) CheckExpiredAccounts() []*Account {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	var expiredAccounts []*Account
	now := time.Now()

	for _, account := range am.accounts {
		if account.ExpiresAt.Before(now) && account.Status == "active" {
			// 更新状态为过期
			account.Status = "expired"
			expiredAccounts = append(expiredAccounts, account)
		}
	}

	if len(expiredAccounts) > 0 {
		// 保存更新
		am.saveConfig()
	}

	return expiredAccounts
}

// GetAccountsNearExpiry 获取即将过期的账号 (24小时内)
func (am *AccountManager) GetAccountsNearExpiry() []*Account {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	var nearExpiryAccounts []*Account
	now := time.Now()
	threshold := now.Add(24 * time.Hour) // 24小时后

	for _, account := range am.accounts {
		if account.ExpiresAt.Before(threshold) && account.ExpiresAt.After(now) && account.Status == "active" {
			nearExpiryAccounts = append(nearExpiryAccounts, account)
		}
	}

	return nearExpiryAccounts
}

// GetActiveLoopingAccounts 获取正在循环交易的账号
func (am *AccountManager) GetActiveLoopingAccounts() []*Account {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	var loopingAccounts []*Account
	for _, account := range am.accounts {
		if account.IsLooping && account.Status == "active" {
			loopingAccounts = append(loopingAccounts, account)
		}
	}

	return loopingAccounts
}

// AccountStats 账号统计结构
type AccountStats struct {
	TotalVolume   float64   `json:"total_volume"`
	TargetVolume  float64   `json:"target_volume"`
	TradeCount    int       `json:"trade_count"`
	TotalLoss     float64   `json:"total_loss"`
	TotalLossRate float64   `json:"total_loss_rate"`
	IsLooping     bool      `json:"is_looping"`
	CurrentToken  string    `json:"current_token"`
	CurrentTaskID string    `json:"current_task_id"`
	LastTradeTime time.Time `json:"last_trade_time"`
	TaskStartTime time.Time `json:"task_start_time"`
}
