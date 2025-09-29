package main

import (
	"encoding/json"
	"fmt"
	"time"

	"alpha-autosell-bot/price"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("🌐 多链代币价格获取测试")
	fmt.Println("========================================")

	// 测试代币配置
	testTokens := []struct {
		address     string
		symbol      string
		chainID     string
		chainName   string
		priceMode   price.PriceMode
	}{
		{
			address:   "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			symbol:    "ALPHA_372",
			chainID:   "56",
			chainName: "BSC",
			priceMode: price.PriceModeMarket,
		},
		{
			address:   "0x6cfffa5bfd4277a04d83307feedfe2d18d944dd2",
			symbol:    "ALPHA_373",
			chainID:   "56",
			chainName: "BSC",
			priceMode: price.PriceModeLimit,
		},
		{
			address:   "SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL",
			symbol:    "ALPHA_360",
			chainID:   "CT_501",
			chainName: "Solana",
			priceMode: price.PriceModeCombined,
		},
	}

	fmt.Printf("📍 测试 %d 个不同链的代币价格获取\n", len(testTokens))
	fmt.Println()

	// 初始化价格客户端
	fmt.Println("🔧 初始化价格客户端...")
	client := price.GetGlobalClient()
	
	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 测试每个代币的价格获取
	testMultiChainTokenPricing(client, testTokens)

	// 测试同一代币的不同价格模式
	fmt.Println("========================================")
	fmt.Println("🔀 测试同一代币不同价格模式")
	fmt.Println("========================================")
	
	// 测试BSC代币的所有模式
	testAllModesForToken(client, testTokens[0])
	
	// 测试Solana代币的所有模式
	testAllModesForToken(client, testTokens[2])

	fmt.Println()
	fmt.Println("🎉 多链代币价格获取测试完成！")
}

// testMultiChainTokenPricing 测试多链代币价格获取
func testMultiChainTokenPricing(client *price.PriceClient, tokens []struct {
	address     string
	symbol      string
	chainID     string
	chainName   string
	priceMode   price.PriceMode
}) {
	fmt.Println("========================================")
	fmt.Println("💰 测试多链代币价格获取")
	fmt.Println("========================================")

	results := make(map[string]interface{})

	for i, token := range tokens {
		fmt.Printf("🔍 测试代币 %d: %s (%s链)\n", i+1, token.symbol, token.chainName)
		fmt.Printf("   地址: %s\n", token.address)
		fmt.Printf("   链ID: %s\n", token.chainID)
		fmt.Printf("   模式: %s\n", token.priceMode)

		// 获取价格
		startTime := time.Now()
		tokenPrice, err := client.GetPriceWithMode(
			token.address,   // 动态代币地址
			token.chainID,   // 动态链ID
			15*time.Second,  // 超时时间
			token.priceMode, // 动态模式
		)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 获取失败: %v\n", err)
			results[token.symbol] = map[string]interface{}{
				"success":     false,
				"error":       err.Error(),
				"duration_ms": duration.Milliseconds(),
				"chain":       token.chainName,
				"chain_id":    token.chainID,
			}
		} else {
			fmt.Printf("   ✅ 获取成功: %.8f USDT\n", tokenPrice)
			fmt.Printf("   ⏱️  耗时: %v\n", duration)
			
			// 计算买单价格（模拟实际使用）
			var buyPrice float64
			switch token.priceMode {
			case price.PriceModeMarket:
				buyPrice = tokenPrice * 1.0002 // 万二
			case price.PriceModeLimit:
				buyPrice = tokenPrice * 1.0001 // 万一
			default:
				buyPrice = tokenPrice * 1.0015 // 万一点五
			}
			
			fmt.Printf("   💰 买单价格: %.8f USDT\n", buyPrice)
			
			results[token.symbol] = map[string]interface{}{
				"success":      true,
				"base_price":   tokenPrice,
				"buy_price":    buyPrice,
				"duration_ms":  duration.Milliseconds(),
				"chain":        token.chainName,
				"chain_id":     token.chainID,
				"token_address": token.address,
				"price_mode":   string(token.priceMode),
			}
		}

		fmt.Println()
		time.Sleep(1 * time.Second) // 避免请求过于频繁
	}

	// 输出结果摘要
	fmt.Println("📊 多链代币价格获取结果:")
	jsonResults, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(jsonResults))
	fmt.Println()
}

// testAllModesForToken 测试单个代币的所有价格模式
func testAllModesForToken(client *price.PriceClient, token struct {
	address     string
	symbol      string
	chainID     string
	chainName   string
	priceMode   price.PriceMode
}) {
	fmt.Printf("🎯 测试代币: %s (%s链)\n", token.symbol, token.chainName)
	fmt.Printf("   地址: %s\n", token.address)
	fmt.Printf("   链ID: %s\n", token.chainID)
	fmt.Println()

	modes := []struct {
		mode price.PriceMode
		name string
		desc string
	}{
		{price.PriceModeMarket, "链上模式", "真实成交价格"},
		{price.PriceModeLimit, "限价模式", "限价订单价格"},
		{price.PriceModeCombined, "综合模式", "综合价格信息"},
		{price.PriceModeAuto, "自动模式", "系统自动选择"},
	}

	for _, mode := range modes {
		fmt.Printf("🔍 测试 %s (%s)\n", mode.name, mode.desc)

		startTime := time.Now()
		tokenPrice, err := client.GetPriceWithMode(token.address, token.chainID, 10*time.Second, mode.mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功: %.8f USDT (%v)\n", tokenPrice, duration)
		}

		fmt.Println()
		time.Sleep(500 * time.Millisecond)
	}
}
