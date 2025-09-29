package price

import (
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	globalClient *PriceClient
	clientMutex  sync.Mutex
	clientOnce   sync.Once
)

// GetGlobalClient 获取全局价格客户端
func GetGlobalClient() *PriceClient {
	clientOnce.Do(func() {
		globalClient = NewPriceClient()
		if err := globalClient.Connect(); err != nil {
			panic("Failed to connect price client: " + err.Error())
		}
	})
	return globalClient
}

// GetTokenPrice 获取代币价格（简化接口）
func GetTokenPrice(contractAddress string) (float64, error) {
	return GetTokenPriceWithChain(contractAddress, "56") // 默认BSC链
}

// GetTokenPriceWithChain 获取指定链上的代币价格
func GetTokenPriceWithChain(contractAddress, chainID string) (float64, error) {
	client := GetGlobalClient()
	return client.GetPrice(contractAddress, chainID, 10*time.Second)
}

// GetTokenPriceWithMode 获取指定模式的代币价格
func GetTokenPriceWithMode(contractAddress, chainID string, mode PriceMode) (float64, error) {
	client := GetGlobalClient()
	return client.GetPriceWithMode(contractAddress, chainID, 10*time.Second, mode)
}

// GetTokenPriceWithPrecision 获取指定精度的代币价格（带重试）- 优化版本
func GetTokenPriceWithPrecision(contractAddress, chainID string, precision int) (float64, error) {
	return GetTokenPriceWithPrecisionAndMode(contractAddress, chainID, precision, PriceModeAuto)
}

// GetTokenPriceWithPrecisionAndMode 获取指定精度和模式的代币价格（带重试）- 优化版本
func GetTokenPriceWithPrecisionAndMode(contractAddress, chainID string, precision int, mode PriceMode) (float64, error) {
	client := GetGlobalClient()

	log.Printf("🔍 [API] 开始获取价格 - Token: %s, 链: %s, 精度: %d, 模式: %s", contractAddress, chainID, precision, mode)

	// 🚨 修复：增加重试次数，减少等待时间，提高成功率
	maxAttempts := 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 🚨 修复：增加超时时间，给WebSocket更多时间恢复
		timeout := 15 * time.Second
		if attempt > 2 {
			timeout = 30 * time.Second // 后续尝试使用更长超时
		}

		price, err := client.GetPriceWithPrecisionAndMode(contractAddress, chainID, timeout, precision, mode)
		if err == nil {
			if attempt > 1 {
				log.Printf("✅ 价格获取成功 (第%d次尝试): %.*f (模式: %s)", attempt, precision, price, mode)
			}
			return price, nil
		}

		log.Printf("⚠️ 价格获取失败 (尝试 %d/%d, 模式: %s): %v", attempt, maxAttempts, mode, err)

		// 🚨 修复：大幅减少等待时间，提高响应速度
		if attempt < maxAttempts {
			var waitTime time.Duration
			switch attempt {
			case 1:
				waitTime = 3 * time.Second // 第一次失败等待3秒
			case 2:
				waitTime = 10 * time.Second // 第二次失败等待10秒
			case 3:
				waitTime = 30 * time.Second // 第三次失败等待30秒
			default:
				waitTime = 60 * time.Second // 后续失败等待1分钟
			}
			log.Printf("⏳ 价格获取失败，等待 %v 后重试 (尝试 %d/%d)", waitTime, attempt+1, maxAttempts)
			time.Sleep(waitTime)
		}
	}

	// 🚨 修复：最后尝试强制重置连接
	log.Printf("🔧 所有重试失败，尝试强制重置WebSocket连接")
	if err := client.ForceReset(); err != nil {
		log.Printf("❌ 强制重置失败: %v", err)
	} else {
		// 重置后再试一次
		log.Printf("🔄 强制重置成功，最后尝试获取价格")
		if price, err := client.GetPriceWithPrecisionAndMode(contractAddress, chainID, 30*time.Second, precision, mode); err == nil {
			log.Printf("✅ 强制重置后价格获取成功: %.*f (模式: %s)", precision, price, mode)
			return price, nil
		}
	}

	return 0, fmt.Errorf("价格获取失败，已达到最大重试次数(%d)，包括强制重置，模式: %s", maxAttempts, mode)
}

// 🚨 新增：紧急价格获取函数（用于关键时刻）
func GetTokenPriceEmergency(contractAddress, chainID string, precision int) (float64, error) {
	return GetTokenPriceEmergencyWithMode(contractAddress, chainID, precision, PriceModeAuto)
}

// GetTokenPriceEmergencyWithMode 紧急价格获取函数（指定模式）
func GetTokenPriceEmergencyWithMode(contractAddress, chainID string, precision int, mode PriceMode) (float64, error) {
	log.Printf("🚨 紧急价格获取模式启动 (模式: %s)", mode)
	client := GetGlobalClient()

	// 先尝试获取缓存价格
	if price, exists := client.GetCachedPriceWithMode(contractAddress, chainID, mode); exists {
		log.Printf("📋 紧急模式：使用缓存价格 %.*f (模式: %s)", precision, price, mode)
		return price, nil
	}

	// 强制重置连接
	if err := client.ForceReset(); err != nil {
		return 0, fmt.Errorf("紧急模式：强制重置失败 %v", err)
	}

	// 使用较长超时时间获取价格
	price, err := client.GetPriceWithPrecisionAndMode(contractAddress, chainID, 60*time.Second, precision, mode)
	if err != nil {
		return 0, fmt.Errorf("紧急模式：价格获取失败 %v", err)
	}

	log.Printf("✅ 紧急模式：价格获取成功 %.*f (模式: %s)", precision, price, mode)
	return price, nil
}

// SubscribeTokenPrice 订阅代币价格（简化接口）
func SubscribeTokenPrice(contractAddress string) (<-chan float64, error) {
	return SubscribeTokenPriceWithChain(contractAddress, "56") // 默认BSC链
}

// SubscribeTokenPriceWithChain 订阅指定链上的代币价格
func SubscribeTokenPriceWithChain(contractAddress, chainID string) (<-chan float64, error) {
	client := GetGlobalClient()
	return client.SubscribePrice(contractAddress, chainID)
}

// SubscribeTokenPriceWithMode 订阅指定模式的代币价格
func SubscribeTokenPriceWithMode(contractAddress, chainID string, mode PriceMode) (<-chan float64, error) {
	client := GetGlobalClient()
	return client.SubscribePriceWithMode(contractAddress, chainID, mode)
}

// CloseGlobalClient 关闭全局客户端
func CloseGlobalClient() {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if globalClient != nil {
		globalClient.Disconnect()
		globalClient = nil
	}
}

// GetTokenPriceFromWebSocket 从WebSocket获取代币价格（优先使用长连接）
func GetTokenPriceFromWebSocket(contractAddress, chainID string) (float64, error) {
	// 直接使用现有的GetTokenPriceWithMode函数，使用Auto模式
	// 这样会优先使用WebSocket缓存的价格
	price, err := GetTokenPriceWithMode(contractAddress, chainID, PriceModeAuto)
	if err == nil && price > 0 {
		return price, nil
	}
	
	// 如果从缓存获取失败，返回错误
	return 0, fmt.Errorf("无法从WebSocket缓存获取价格: %v", err)
}
