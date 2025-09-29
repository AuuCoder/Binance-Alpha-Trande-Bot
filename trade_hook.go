package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

// 原始的handleTrade函数指针
var originalHandleTrade func(http.ResponseWriter, *http.Request)

// 初始化函数，在程序启动时执行
func init() {
	// 保存原始的handleTrade函数
	http.HandleFunc("/trade", func(w http.ResponseWriter, r *http.Request) {
		// 这是我们的钩子函数，会在每次/trade请求时执行
		if r.Method == "POST" {
			var req TradeRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				// 获取用户标识
				firstName, err := getUserFirstName(req.Csrftoken, req.Cookie)
				if err == nil {
					req.AccountID = firstName
					
					// 特殊处理FROGGIE代币
					if strings.ToLower(req.TokenAddress) == "0xa45f5eb48cecd034751651aeeda6271bd5df8888" {
						req.BaseAsset = "ALPHA_386" // 强制设置FROGGIE代币的BaseAsset为ALPHA_386
						log.Printf("🔧 [%s] 检测到FROGGIE代币，强制设置BaseAsset为ALPHA_386", req.AccountID)
					}
					
					// 保存完整的交易参数
					saveTradeParams(&req)
					
					// 打印参数信息
					log.Printf("💾 [%s] 已保存/trade参数: 代币=%s, 基础资产=%s, 金额=%.2f, 速度=%s", 
						req.AccountID, req.TokenAddress, req.BaseAsset, req.USDTAmount, req.SpeedMode)
				}
			}
			
			// 重置请求体，因为我们已经读取过了
			r.Body = http.MaxBytesReader(w, r.Body, 0)
		}
		
		// 调用原始的handleTrade函数
		handleTrade(w, r)
	})
	
	log.Printf("🔄 已安装/trade请求参数保存钩子")
}

// saveTradeParams 保存交易参数
func saveTradeParams(req *TradeRequest) {
	if req == nil || req.AccountID == "" {
		return
	}
	
	// 保存代币地址到currentTradeTokens
	if req.TokenAddress != "" {
		tokensMutex.Lock()
		currentTradeTokens[req.AccountID] = req.TokenAddress
		tokensMutex.Unlock()
	}
	
	// 保存完整的交易参数
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
} 