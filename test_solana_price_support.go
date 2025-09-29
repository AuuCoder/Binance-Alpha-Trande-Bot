package main

import (
	"encoding/json"
	"fmt"
	"time"

	"alpha-autosell-bot/price"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("🌟 Solana链价格获取兼容性测试")
	fmt.Println("========================================")

	// 测试Solana链的代币
	solanaTokens := []struct {
		address     string
		name        string
		chainID     string
		priceMode   price.PriceMode
		description string
	}{
		{
			address:     "SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx",
			name:        "Solana Token 1",
			chainID:     "CT_501",
			priceMode:   price.PriceModeCombined,
			description: "综合模式 (came@SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx@CT_501@kline_15m)",
		},
		{
			address:     "SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL",
			name:        "Solana Token 2",
			chainID:     "CT_501",
			priceMode:   price.PriceModeLimit,
			description: "限价模式 (came@SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL@CT_501@limit@kline_15m)",
		},
		{
			address:     "SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx",
			name:        "Solana Token 1",
			chainID:     "CT_501",
			priceMode:   price.PriceModeMarket,
			description: "链上模式 (came@SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx@CT_501@market@kline_15m)",
		},
	}

	// BSC链代币作为对比
	bscTokens := []struct {
		address     string
		name        string
		chainID     string
		priceMode   price.PriceMode
		description string
	}{
		{
			address:     "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			name:        "BSC ALPHA",
			chainID:     "56",
			priceMode:   price.PriceModeMarket,
			description: "BSC链上模式 (came@0xa2be3e48170a60119b5f0400c65f65f3158fbeee@56@market@kline_15m)",
		},
	}

	fmt.Printf("📍 测试 %d 个Solana代币 + %d 个BSC代币\n", len(solanaTokens), len(bscTokens))
	fmt.Println()

	// 初始化价格客户端
	fmt.Println("🔧 初始化价格客户端...")
	client := price.GetGlobalClient()
	
	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 测试Solana链价格获取
	fmt.Println("========================================")
	fmt.Println("🌟 测试Solana链价格获取")
	fmt.Println("========================================")
	
	solanaResults := testTokenPricing(client, solanaTokens, "Solana")

	// 测试BSC链价格获取（对比）
	fmt.Println("========================================")
	fmt.Println("🔗 测试BSC链价格获取（对比）")
	fmt.Println("========================================")
	
	bscResults := testTokenPricing(client, bscTokens, "BSC")

	// 测试跨链价格模式切换
	testCrossChainModeSwitch(client)

	// 输出完整结果
	fmt.Println("========================================")
	fmt.Println("📊 完整测试结果")
	fmt.Println("========================================")
	
	allResults := map[string]interface{}{
		"solana_results": solanaResults,
		"bsc_results":    bscResults,
	}
	
	jsonResults, _ := json.MarshalIndent(allResults, "", "  ")
	fmt.Println(string(jsonResults))

	fmt.Println()
	fmt.Println("🎉 Solana链价格获取兼容性测试完成！")
}

// testTokenPricing 测试代币价格获取
func testTokenPricing(client *price.PriceClient, tokens []struct {
	address     string
	name        string
	chainID     string
	priceMode   price.PriceMode
	description string
}, chainName string) map[string]interface{} {
	
	results := make(map[string]interface{})

	for i, token := range tokens {
		fmt.Printf("🔍 测试%s代币 %d: %s\n", chainName, i+1, token.name)
		fmt.Printf("   地址: %s\n", token.address)
		fmt.Printf("   链ID: %s\n", token.chainID)
		fmt.Printf("   模式: %s\n", token.priceMode)
		fmt.Printf("   描述: %s\n", token.description)

		// 获取价格
		startTime := time.Now()
		tokenPrice, err := client.GetPriceWithPrecisionAndMode(
			token.address,     // 动态代币地址
			token.chainID,     // 动态链ID
			10*time.Second,    // 超时时间
			8,                 // 精度
			token.priceMode,   // 动态模式
		)
		duration := time.Since(startTime)

		testKey := fmt.Sprintf("%s_%s_%s", chainName, token.name, token.priceMode)

		if err != nil {
			fmt.Printf("   ❌ 获取失败: %v\n", err)
			results[testKey] = map[string]interface{}{
				"success":      false,
				"error":        err.Error(),
				"duration_ms":  duration.Milliseconds(),
				"chain_id":     token.chainID,
				"price_mode":   string(token.priceMode),
				"token_address": token.address,
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
			
			results[testKey] = map[string]interface{}{
				"success":       true,
				"base_price":    tokenPrice,
				"buy_price":     buyPrice,
				"duration_ms":   duration.Milliseconds(),
				"chain_id":      token.chainID,
				"price_mode":    string(token.priceMode),
				"token_address": token.address,
			}
		}

		fmt.Println()
		time.Sleep(1 * time.Second) // 避免请求过于频繁
	}

	return results
}

// testCrossChainModeSwitch 测试跨链价格模式切换
func testCrossChainModeSwitch(client *price.PriceClient) {
	fmt.Println("========================================")
	fmt.Println("🔄 测试跨链价格模式切换")
	fmt.Println("========================================")

	// 测试场景：同一个客户端连续获取不同链的价格
	testCases := []struct {
		address   string
		chainID   string
		mode      price.PriceMode
		chainName string
	}{
		{
			address:   "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			chainID:   "56",
			mode:      price.PriceModeMarket,
			chainName: "BSC",
		},
		{
			address:   "SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx",
			chainID:   "CT_501",
			mode:      price.PriceModeLimit,
			chainName: "Solana",
		},
		{
			address:   "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			chainID:   "56",
			mode:      price.PriceModeCombined,
			chainName: "BSC",
		},
		{
			address:   "SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL",
			chainID:   "CT_501",
			mode:      price.PriceModeMarket,
			chainName: "Solana",
		},
	}

	fmt.Printf("🎯 测试跨链切换场景 (%d个测试)\n", len(testCases))
	fmt.Println()

	for i, testCase := range testCases {
		fmt.Printf("🔍 测试 %d: %s链 %s模式\n", i+1, testCase.chainName, testCase.mode)
		fmt.Printf("   地址: %s\n", testCase.address)
		fmt.Printf("   链ID: %s\n", testCase.chainID)

		startTime := time.Now()
		tokenPrice, err := client.GetPriceWithMode(testCase.address, testCase.chainID, 8*time.Second, testCase.mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功: %.8f USDT (%v)\n", tokenPrice, duration)
		}

		fmt.Println()
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("✅ 跨链价格模式切换测试完成")
}
