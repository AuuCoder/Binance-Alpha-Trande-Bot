package main

import (
	"sync"
)

// 全局变量，用于存储交易参数
var (
	accountTradeParams = make(map[string]*TradeRequest) // 账号交易参数映射
	tradeParamsMutex   sync.RWMutex                     // 账号交易参数锁
) 