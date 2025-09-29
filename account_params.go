package main

import (
	"log"
	"sync"
)

// 账号交易参数映射
var (
	// 保存每个账号的交易参数
	accountTradeParams = make(map[string]*TradeRequest)
	// 账号交易参数锁
	tradeParamsMutex sync.RWMutex
)

// saveAccountTradeParams 保存账号的交易参数
func saveAccountTradeParams(req *TradeRequest) {
	if req == nil || req.AccountID == "" {
		return
	}
	
	// 确保BaseAsset有值
	if req.BaseAsset == "" && req.TokenAddress != "" {
		req.BaseAsset = inferBaseAssetFromTokenAddress(req.TokenAddress)
		log.Printf("🔧 [%s] 设置BaseAsset: %s (根据TokenAddress推断)", req.AccountID, req.BaseAsset)
	}
	
	// 更新currentTradeTokens，确保保存代币地址
	if req.TokenAddress != "" {
		tokensMutex.Lock()
		currentTradeTokens[req.AccountID] = req.TokenAddress
		tokensMutex.Unlock()
		log.Printf("💾 [%s] 更新当前交易代币: %s", req.AccountID, req.TokenAddress)
	}
	
	tradeParamsMutex.Lock()
	defer tradeParamsMutex.Unlock()
	
	// 创建参数副本
	paramsCopy := &TradeRequest{
		AccountID:      req.AccountID,
		Csrftoken:      req.Csrftoken,
		Cookie:         req.Cookie,
		TokenAddress:   req.TokenAddress,
		BaseAsset:      req.BaseAsset,
		USDTAmount:     req.USDTAmount,
		MinDelay:       req.MinDelay,
		MaxDelay:       req.MaxDelay,
		PricePrecision: req.PricePrecision,
		SpeedMode:      req.SpeedMode,
		TargetVolume:   req.TargetVolume,
		PriceMode:      req.PriceMode,
		ChainID:        req.ChainID,
	}
	
	// 保存参数
	accountTradeParams[req.AccountID] = paramsCopy
	log.Printf("💾 [%s] 已保存交易参数: 代币=%s, 基础资产=%s, 金额=%.2f, 目标量=%.2f, 速度=%s", 
		req.AccountID, req.TokenAddress, req.BaseAsset, req.USDTAmount, req.TargetVolume, req.SpeedMode)
}

// getAccountTradeParams 获取账号的交易参数
func getAccountTradeParams(accountID string) *TradeRequest {
	tradeParamsMutex.RLock()
	defer tradeParamsMutex.RUnlock()
	
	if params, exists := accountTradeParams[accountID]; exists && params != nil {
		// 创建参数副本
		paramsCopy := &TradeRequest{
			AccountID:      params.AccountID,
			Csrftoken:      params.Csrftoken,
			Cookie:         params.Cookie,
			TokenAddress:   params.TokenAddress,
			BaseAsset:      params.BaseAsset,
			USDTAmount:     params.USDTAmount,
			MinDelay:       params.MinDelay,
			MaxDelay:       params.MaxDelay,
			PricePrecision: params.PricePrecision,
			SpeedMode:      params.SpeedMode,
			TargetVolume:   params.TargetVolume,
			PriceMode:      params.PriceMode,
			ChainID:        params.ChainID,
		}
		
		// 获取已交易的金额
		statsMutex.RLock()
		stats, hasStats := accountStats[accountID]
		statsMutex.RUnlock()
		
		if hasStats && stats != nil {
			// 计算剩余目标交易量
			tradedVolume := stats.TotalVolume
			remainingVolume := params.TargetVolume - tradedVolume
			if remainingVolume < 0 {
				remainingVolume = 0
			}
			
			log.Printf("📊 [%s] 恢复交易参数: 已交易=%.2f, 目标=%.2f, 剩余=%.2f", 
				accountID, tradedVolume, params.TargetVolume, remainingVolume)
			
			// 更新目标交易量为剩余量
			paramsCopy.TargetVolume = remainingVolume
		}
		
		return paramsCopy
	}
	
	return nil
} 