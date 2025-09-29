package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// 注意：交易参数存储在account_params.go中
// 这里不再重复定义accountTradeParams和tradeParamsMutex

// handleAccountControl 处理账号暂停和恢复功能
func handleAccountControl(w http.ResponseWriter, r *http.Request) {
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
		Mode string `json:"mode"` // "pause" 或 "resume"
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "请求格式无效",
		})
		return
	}

	// 验证必要参数
	if req.Mode != "pause" && req.Mode != "resume" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "mode参数必须为'pause'或'resume'",
		})
		return
	}

	var success bool
	if req.Mode == "pause" {
		// 执行暂停操作
		success = pauseCurrentAccount()
		} else {
		// 执行恢复操作
		success = resumeCurrentAccount()
	}

	// 返回结果
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
		"mode":    req.Mode,
		"message": func() string {
		if !success {
				return "操作失败"
			}
			if req.Mode == "pause" {
				return "账号已暂停"
			}
			return "账号已恢复"
		}(),
	})
}

// pauseCurrentAccount 暂停当前实例的账号交易活动
func pauseCurrentAccount() bool {
	log.Printf("⏸️ 开始执行账号暂停流程...")
	
	// 获取当前运行的账号
	authMutex.RLock()
	var currentAccounts []string
	for accountID := range globalAccountAuths {
		currentAccounts = append(currentAccounts, accountID)
		}
		authMutex.RUnlock()

	if len(currentAccounts) == 0 {
		log.Printf("⚠️ 没有找到正在运行的账号")
		return false
	}

	// 对每个账号执行暂停操作
	// 通常情况下，每个实例只有一个账号
	for _, accountID := range currentAccounts {
		// 1. 设置账号暂停状态（永久暂停）
		setAccountPauseStatus(accountID, 365*24*time.Hour, "manual_pause")
		
		// 2. 停止循环交易
	loopMutex.Lock()
		if stopChan, exists := loopingAccounts[accountID]; exists {
			log.Printf("⏹️ [%s] 停止循环交易", accountID)
			// 发送停止信号
		select {
		case stopChan <- true:
				log.Printf("✅ [%s] 已发送停止信号", accountID)
		default:
				log.Printf("⚠️ [%s] 停止通道已满或已关闭", accountID)
		}
			// 从循环账号列表中移除
			delete(loopingAccounts, accountID)
	}
	loopMutex.Unlock()

		// 3. 清理挂单
		auth, ok := getAccountAuth(accountID)
		if !ok || auth == nil {
			log.Printf("⚠️ [%s] 获取账号认证信息失败，无法清理挂单", accountID)
			continue
		}

		// 3.1 首先标记账号为暂停状态（在内存中）
		// 这样可以确保其他异步操作检查到账号已暂停
		setAccountAsyncState(accountID, "paused", true)
		log.Printf("🔒 [%s] 已设置暂停状态标记，阻止新的异步操作", accountID)
		
		// 3.2 等待一小段时间，确保正在进行的异步操作有机会检查暂停状态
		time.Sleep(500 * time.Millisecond)

		// 4. 尝试清理挂单
		cleanedCount := forceCleanupAllOrders(accountID, auth.Csrftoken, auth.Cookie)
		log.Printf("🧹 [%s] 暂停流程：清理了 %d 个挂单", accountID, cleanedCount)
		
		// 5. 尝试清理代币
		tokensMutex.RLock()
		tokenAddress := currentTradeTokens[accountID]
		tokensMutex.RUnlock()
		
		if tokenAddress != "" {
			log.Printf("💰 [%s] 暂停流程：尝试清理代币 %s", accountID, tokenAddress)
			
			// 5.1 尝试从保存的交易参数中获取BaseAsset
			var baseAsset string
			
			// 首先查找保存的交易参数
			tradeParamsMutex.RLock()
			savedParams, hasSavedParams := accountTradeParams[accountID]
			tradeParamsMutex.RUnlock()
			
			if hasSavedParams && savedParams != nil && savedParams.BaseAsset != "" {
				// 使用保存的BaseAsset
				baseAsset = savedParams.BaseAsset
				log.Printf("📝 [%s] 使用保存的BaseAsset: %s", accountID, baseAsset)
			} else {
				// 如果没有保存的参数，尝试推断
				baseAsset = inferBaseAssetFromTokenAddress(tokenAddress)
				log.Printf("🔍 [%s] 推断BaseAsset: %s", accountID, baseAsset)
				
				// 特殊处理FROGGIE代币
				if strings.ToLower(tokenAddress) == "0xa45f5eb48cecd034751651aeeda6271bd5df8888" && baseAsset != "ALPHA_386" {
					baseAsset = "ALPHA_386" // 强制设置FROGGIE代币的BaseAsset为ALPHA_386
					log.Printf("🔧 [%s] 检测到FROGGIE代币，强制设置BaseAsset为ALPHA_386", accountID)
				}
			}
			
			if baseAsset != "" {
				// 注意：forceCleanToken的第一个参数是代币地址，第二个参数是BaseAsset
				err := forceCleanToken(tokenAddress, baseAsset, auth.Csrftoken, auth.Cookie, 8)
				if err != nil {
					log.Printf("⚠️ [%s] 暂停流程：清理代币失败 - %v", accountID, err)
				} else {
					log.Printf("✅ [%s] 暂停流程：代币清理成功", accountID)
				}
			} else {
				log.Printf("⚠️ [%s] 暂停流程：无法确定BaseAsset，跳过清理代币", accountID)
			}
		} else {
			log.Printf("⚠️ [%s] 暂停流程：没有找到当前交易的代币地址", accountID)
		}
		
		log.Printf("✅ [%s] 账号暂停流程完成", accountID)
	}

	return true
}

// resumeCurrentAccount 恢复当前实例的账号交易活动
func resumeCurrentAccount() bool {
	log.Printf("▶️ 开始执行账号恢复流程...")
	
	// 获取当前运行的账号
		authMutex.RLock()
	var currentAccounts []string
	for accountID := range globalAccountAuths {
		currentAccounts = append(currentAccounts, accountID)
		}
		authMutex.RUnlock()

	if len(currentAccounts) == 0 {
		log.Printf("⚠️ 没有找到正在运行的账号")
		return false
	}

	// 对每个账号执行恢复操作
	// 通常情况下，每个实例只有一个账号
	for _, accountID := range currentAccounts {
		// 1. 清除账号暂停状态
		clearAccountPauseStatus(accountID)
		
		// 2. 清除暂停标记
		setAccountAsyncState(accountID, "paused", false)
		
		// 3. 尝试获取保存的交易参数（会自动计算剩余目标交易量）
		savedParams := getAccountTradeParams(accountID)
		
		if savedParams != nil {
			log.Printf("✅ [%s] 找到保存的交易参数，使用原有参数恢复交易", accountID)
			
			// 创建新的停止通道
	loopMutex.Lock()
			if _, exists := loopingAccounts[accountID]; exists {
				// 如果已经存在，先清理旧的
				log.Printf("🔄 [%s] 清理已存在的交易循环", accountID)
				delete(loopingAccounts, accountID)
			}
			
			// 创建新的停止通道
			stopChan := make(chan bool, 1)
			loopingAccounts[accountID] = stopChan
	loopMutex.Unlock()

			// 设置账号为活跃状态
			statsMutex.Lock()
			if stats, exists := accountStats[accountID]; exists {
				stats.IsLooping = true
			}
			statsMutex.Unlock()
			
			// 启动交易循环
			go func(req *TradeRequest, stopChan chan bool) {
				// 启用自动循环模式
				req.AutoLoop = true
				
				log.Printf("🔄 [%s] 恢复流程：使用保存的参数重新启动交易循环", accountID)
				log.Printf("📝 [%s] 恢复参数: 代币=%s, 基础资产=%s, 金额=%.2f, 速度=%s, 目标量=%.2f", 
					accountID, req.TokenAddress, req.BaseAsset, req.USDTAmount, req.SpeedMode, req.TargetVolume)
				
				// 重新启动自动清理功能
				startTokenCleanupIfNotRunning(req)
				
				// 启动交易循环
				executeTradeWithCancel(req, stopChan)
			}(savedParams, stopChan)
			
			log.Printf("✅ [%s] 账号恢复流程完成，交易循环已使用原有参数重新启动", accountID)
	} else {
			// 如果没有保存的参数，使用默认参数
			log.Printf("⚠️ [%s] 没有找到保存的交易参数，使用默认参数恢复交易", accountID)
			
			// 获取账号认证信息
			auth, ok := getAccountAuth(accountID)
			if !ok || auth == nil {
				log.Printf("⚠️ [%s] 获取账号认证信息失败，无法重启交易循环", accountID)
				continue
			}
			
			// 获取账号统计信息
	statsMutex.RLock()
			stats, hasStats := accountStats[accountID]
	statsMutex.RUnlock()

			if !hasStats || stats == nil {
				log.Printf("⚠️ [%s] 没有找到账号统计信息，无法恢复交易状态", accountID)
			continue
		}

			// 获取当前交易的代币地址
			tokensMutex.RLock()
			tokenAddress := currentTradeTokens[accountID]
			tokensMutex.RUnlock()
			
			if tokenAddress == "" {
				log.Printf("⚠️ [%s] 没有找到当前交易的代币地址，使用默认代币", accountID)
				// 使用默认值
				tokenAddress = "0xa45f5eb48cecd034751651aeeda6271bd5df8888" // 默认代币地址
			}
			
			// 尝试从保存的交易参数中获取BaseAsset
			var baseAsset string = inferBaseAssetFromTokenAddress(tokenAddress)
			var usdtAmount float64 = 10.0 // 默认交易金额
			var speedMode string = "normal" // 默认速度模式
			
			// 尝试从其他地方获取交易参数
			log.Printf("🔍 [%s] 尝试从其他来源获取交易参数", accountID)
			
			// 构造交易请求
			tradeReq := &TradeRequest{
				AccountID:      accountID,
				Csrftoken:      auth.Csrftoken,
				Cookie:         auth.Cookie,
				TokenAddress:   tokenAddress,
				BaseAsset:      baseAsset,
				USDTAmount:     usdtAmount,
				MinDelay:       1,
				MaxDelay:       2,
				PricePrecision: 8,
				SpeedMode:      speedMode,
				TargetVolume:   stats.TargetVolume, // 使用原有目标
			}
			
			// 创建新的停止通道
	loopMutex.Lock()
			if _, exists := loopingAccounts[accountID]; exists {
				// 如果已经存在，先清理旧的
				log.Printf("🔄 [%s] 清理已存在的交易循环", accountID)
				delete(loopingAccounts, accountID)
			}
			
			// 创建新的停止通道
			stopChan := make(chan bool, 1)
			loopingAccounts[accountID] = stopChan
	loopMutex.Unlock()

			// 设置账号为活跃状态
	statsMutex.Lock()
			if stats, exists := accountStats[accountID]; exists {
				stats.IsLooping = true
	}
	statsMutex.Unlock()

			// 启动交易循环
			go func(req *TradeRequest, stopChan chan bool) {
				log.Printf("🔄 [%s] 恢复流程：使用默认参数重新启动交易循环", accountID)
				log.Printf("📝 [%s] 恢复参数: 代币=%s, 基础资产=%s, 金额=%.2f, 速度=%s", 
					accountID, req.TokenAddress, req.BaseAsset, req.USDTAmount, req.SpeedMode)
				
				// 重新启动自动清理功能
				startTokenCleanupIfNotRunning(req)
				
				// 启动交易循环
				executeTradeWithCancel(req, stopChan)
			}(tradeReq, stopChan)
			
			log.Printf("✅ [%s] 账号恢复流程完成，交易循环已使用默认参数重新启动", accountID)
		}
	}

	return true
}

// getAccountCurrentToken 获取账号当前正在交易的代币地址
func getAccountCurrentToken(accountID string) string {
	// 1. 从当前交易代币记录中获取
	tokensMutex.RLock()
	if tokenAddress, exists := currentTradeTokens[accountID]; exists {
		tokensMutex.RUnlock()
		return tokenAddress
	}
	tokensMutex.RUnlock()
	
	return ""
} 