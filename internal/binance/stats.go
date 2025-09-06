package binance

import (
	"log"
	"sync"
	"time"
)

// StatsManager 统计管理器
type StatsManager struct {
	mu              sync.RWMutex
	accountStats    map[string]*AccountStats
	globalStats     *GlobalStats
	nodeStats       map[string]*NodeStats
	tradeResults    map[string][]*TradeResult
	processedTrades map[string]bool // 防重复统计
	startTime       time.Time
}

// NewStatsManager 创建统计管理器
func NewStatsManager() *StatsManager {
	return &StatsManager{
		accountStats: make(map[string]*AccountStats),
		globalStats: &GlobalStats{
			ActiveNodes: 1,
		},
		nodeStats:       make(map[string]*NodeStats),
		tradeResults:    make(map[string][]*TradeResult),
		processedTrades: make(map[string]bool),
		startTime:       time.Now(),
	}
}

// InitAccountStats 初始化账户统计
func (sm *StatsManager) InitAccountStats(accountID string, targetVolume float64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.accountStats[accountID]; !exists {
		sm.accountStats[accountID] = &AccountStats{
			AccountID:     accountID,
			TargetVolume:  targetVolume,
			TotalVolume:   0,
			TradeCount:    0,
			TotalLoss:     0,
			TotalLossRate: 0,
			IsLooping:     false,
			LastTradeTime: time.Now(),
		}
		sm.updateGlobalStats()
	}
}

// UpdateAccountStats 更新账户统计
func (sm *StatsManager) UpdateAccountStats(accountID string, volume, profit float64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stats, exists := sm.accountStats[accountID]
	if !exists {
		stats = &AccountStats{
			AccountID: accountID,
		}
		sm.accountStats[accountID] = stats
	}

	// 更新账户统计
	stats.TotalVolume += volume
	stats.TradeCount++
	stats.LastTradeTime = time.Now()

	// 计算损失（profit为负数表示损失）
	if profit < 0 {
		stats.TotalLoss += -profit
	}

	// 计算损失率（万分比）
	if stats.TotalVolume > 0 {
		stats.TotalLossRate = (stats.TotalLoss / stats.TotalVolume) * 10000
	}

	sm.updateGlobalStats()
}

// UpdateAccountStatsWithID 带交易ID的统计更新（防重复）
func (sm *StatsManager) UpdateAccountStatsWithID(accountID string, volume, loss float64, tradeID string) {
	// 🔧 防重复统计检查
	if tradeID != "" {
		if sm.isTradeProcessed(tradeID) {
			log.Printf("⚠️ [%s] 交易已统计，跳过重复统计: %s", accountID, tradeID)
			return
		}
		sm.markTradeProcessed(tradeID)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	stats, exists := sm.accountStats[accountID]
	if !exists {
		stats = &AccountStats{
			AccountID: accountID,
		}
		sm.accountStats[accountID] = stats
	}

	// 更新账户统计
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

	sm.updateGlobalStats()
}

// isTradeProcessed 检查交易是否已处理
func (sm *StatsManager) isTradeProcessed(tradeID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.processedTrades[tradeID]
}

// markTradeProcessed 标记交易已处理
func (sm *StatsManager) markTradeProcessed(tradeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.processedTrades[tradeID] = true
}

// SetAccountLooping 设置账户循环状态
func (sm *StatsManager) SetAccountLooping(accountID string, isLooping bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if stats, exists := sm.accountStats[accountID]; exists {
		stats.IsLooping = isLooping
	}
}

// SetAccountNode 设置账户分配的节点
func (sm *StatsManager) SetAccountNode(accountID, nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if stats, exists := sm.accountStats[accountID]; exists {
		stats.AssignedNode = nodeID
	}
}

// AddTradeResult 添加交易结果
func (sm *StatsManager) AddTradeResult(result *TradeResult) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	results := sm.tradeResults[result.AccountID]
	results = append(results, result)

	// 保持最近100条记录
	if len(results) > 100 {
		results = results[len(results)-100:]
	}

	sm.tradeResults[result.AccountID] = results
}

// GetAccountStats 获取账户统计
func (sm *StatsManager) GetAccountStats(accountID string) *AccountStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if stats, exists := sm.accountStats[accountID]; exists {
		// 返回副本
		statsCopy := *stats
		return &statsCopy
	}

	return nil
}

// GetAllAccountStats 获取所有账户统计
func (sm *StatsManager) GetAllAccountStats() map[string]*AccountStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]*AccountStats)
	for id, stats := range sm.accountStats {
		statsCopy := *stats
		result[id] = &statsCopy
	}

	return result
}

// GetGlobalStats 获取全局统计
func (sm *StatsManager) GetGlobalStats() *GlobalStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 返回副本
	statsCopy := *sm.globalStats
	return &statsCopy
}

// UpdateNodeStats 更新节点统计
func (sm *StatsManager) UpdateNodeStats(nodeID string, stats *NodeStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.nodeStats[nodeID] = stats
	sm.updateGlobalStats()
}

// GetNodeStats 获取节点统计
func (sm *StatsManager) GetNodeStats(nodeID string) *NodeStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if stats, exists := sm.nodeStats[nodeID]; exists {
		statsCopy := *stats
		return &statsCopy
	}

	return nil
}

// GetAllNodeStats 获取所有节点统计
func (sm *StatsManager) GetAllNodeStats() map[string]*NodeStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]*NodeStats)
	for id, stats := range sm.nodeStats {
		statsCopy := *stats
		result[id] = &statsCopy
	}

	return result
}

// GetTradeResults 获取交易结果
func (sm *StatsManager) GetTradeResults(accountID string, limit int) []*TradeResult {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	results, exists := sm.tradeResults[accountID]
	if !exists {
		return []*TradeResult{}
	}

	// 返回最近的记录
	if limit > 0 && len(results) > limit {
		start := len(results) - limit
		return results[start:]
	}

	return results
}

// updateGlobalStats 更新全局统计（内部方法，需要持有锁）
func (sm *StatsManager) updateGlobalStats() {
	var totalTargetVolume, totalCurrentVolume, totalLoss float64
	var activeAccounts int

	for _, stats := range sm.accountStats {
		totalTargetVolume += stats.TargetVolume
		totalCurrentVolume += stats.TotalVolume
		totalLoss += stats.TotalLoss
		if stats.IsLooping {
			activeAccounts++
		}
	}

	// 计算完成率
	var completionRate float64
	if totalTargetVolume > 0 {
		completionRate = (totalCurrentVolume / totalTargetVolume) * 100
	}

	// 计算总损失率（万分比）
	var totalLossRate float64
	if totalCurrentVolume > 0 {
		totalLossRate = (totalLoss / totalCurrentVolume) * 10000
	}

	// 计算10万交易额预计损失
	var projectedLoss100k float64
	if totalCurrentVolume > 0 {
		projectedLoss100k = (totalLoss / totalCurrentVolume) * 100000
	}

	sm.globalStats.TotalTargetVolume = totalTargetVolume
	sm.globalStats.TotalCurrentVolume = totalCurrentVolume
	sm.globalStats.TotalLoss = totalLoss
	sm.globalStats.TotalLossRate = totalLossRate
	sm.globalStats.CompletionRate = completionRate
	sm.globalStats.ActiveAccounts = activeAccounts
	sm.globalStats.ProjectedLoss100k = projectedLoss100k
	sm.globalStats.ActiveNodes = len(sm.nodeStats)
}

// Reset 重置统计
func (sm *StatsManager) Reset() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.accountStats = make(map[string]*AccountStats)
	sm.globalStats = &GlobalStats{
		ActiveNodes: 1,
	}
	sm.nodeStats = make(map[string]*NodeStats)
	sm.tradeResults = make(map[string][]*TradeResult)
	sm.startTime = time.Now()
}

// GetUptime 获取运行时间
func (sm *StatsManager) GetUptime() time.Duration {
	return time.Since(sm.startTime)
}

// RemoveAccount 移除账户统计
func (sm *StatsManager) RemoveAccount(accountID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.accountStats, accountID)
	delete(sm.tradeResults, accountID)
	sm.updateGlobalStats()
}

// RemoveNode 移除节点统计
func (sm *StatsManager) RemoveNode(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.nodeStats, nodeID)
	sm.updateGlobalStats()
}
