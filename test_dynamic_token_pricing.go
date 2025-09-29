package main

import (
	"encoding/json"
	"fmt"
	"time"

	"alpha-autosell-bot/price"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("🔄 动态代币价格获取测试")
	fmt.Println("========================================")

	// 测试多个不同的代币地址
	testTokens := []struct {
		address     string
		name        string
		chainID     string
		priceMode   price.PriceMode
		precision   int
	}{
		{
			address:   "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			name:      "ALPHA",
			chainID:   "56",
			priceMode: price.PriceModeMarket,
			precision: 8,
		},
		{
			address:   "0x9ec02756a559700d8d9e79ece56809f7bcc5dc27",
			name:      "Token2",
			chainID:   "56",
			priceMode: price.PriceModeLimit,
			precision: 6,
		},
		{
			address:   "0xa5346f91a767b89a0363a4309c8e6c5adc0c4a59",
			name:      "Token3",
			chainID:   "56",
			priceMode: price.PriceModeCombined,
			precision: 10,
		},
	}

	fmt.Printf("📍 测试 %d 个不同代币的动态价格获取\n", len(testTokens))
	fmt.Println()

	// 初始化价格客户端
	fmt.Println("🔧 初始化价格客户端...")
	client := price.GetGlobalClient()
	
	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 测试动态代币价格获取
	testDynamicTokenPricing(client, testTokens)

	// 测试同一代币不同模式
	testSameTokenDifferentModes(client, testTokens[0])

	// 测试动态精度
	testDynamicPrecision(client, testTokens[0])

	fmt.Println()
	fmt.Println("🎉 动态代币价格获取测试完成！")
}

// testDynamicTokenPricing 测试动态代币价格获取
func testDynamicTokenPricing(client *price.PriceClient, tokens []struct {
	address     string
	name        string
	chainID     string
	priceMode   price.PriceMode
	precision   int
}) {
	fmt.Println("========================================")
	fmt.Println("🔄 测试动态代币价格获取")
	fmt.Println("========================================")

	results := make(map[string]interface{})

	for i, token := range tokens {
		fmt.Printf("🔍 测试代币 %d: %s\n", i+1, token.name)
		fmt.Printf("   地址: %s\n", token.address)
		fmt.Printf("   链ID: %s\n", token.chainID)
		fmt.Printf("   模式: %s\n", token.priceMode)
		fmt.Printf("   精度: %d位\n", token.precision)

		// 获取价格
		startTime := time.Now()
		price, err := client.GetPriceWithPrecisionAndMode(
			token.address,   // 动态代币地址
			token.chainID,   // 动态链ID
			token.precision, // 动态精度
			token.priceMode, // 动态模式
		)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 获取失败: %v\n", err)
			results[token.name] = map[string]interface{}{
				"success":     false,
				"error":       err.Error(),
				"duration_ms": duration.Milliseconds(),
			}
		} else {
			fmt.Printf("   ✅ 获取成功: %.*f USDT\n", token.precision, price)
			fmt.Printf("   ⏱️  耗时: %v\n", duration)
			
			// 计算买单价格（模拟实际使用）
			var buyPrice float64
			switch token.priceMode {
			case price.PriceModeMarket:
				buyPrice = price * 1.0002 // 万二
			case price.PriceModeLimit:
				buyPrice = price * 1.0001 // 万一
			default:
				buyPrice = price * 1.0015 // 万一点五
			}
			
			fmt.Printf("   💰 买单价格: %.*f USDT\n", token.precision, buyPrice)
			
			results[token.name] = map[string]interface{}{
				"success":      true,
				"base_price":   price,
				"buy_price":    buyPrice,
				"duration_ms":  duration.Milliseconds(),
				"token_address": token.address,
				"chain_id":     token.chainID,
				"price_mode":   string(token.priceMode),
				"precision":    token.precision,
			}
		}

		fmt.Println()
		time.Sleep(1 * time.Second) // 避免请求过于频繁
	}

	// 输出结果摘要
	fmt.Println("📊 动态代币价格获取结果:")
	jsonResults, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(jsonResults))
	fmt.Println()
}

// testSameTokenDifferentModes 测试同一代币不同模式
func testSameTokenDifferentModes(client *price.PriceClient, token struct {
	address     string
	name        string
	chainID     string
	priceMode   price.PriceMode
	precision   int
}) {
	fmt.Println("========================================")
	fmt.Println("🔀 测试同一代币不同价格模式")
	fmt.Println("========================================")

	modes := []struct {
		mode price.PriceMode
		name string
	}{
		{price.PriceModeMarket, "链上模式"},
		{price.PriceModeLimit, "限价模式"},
		{price.PriceModeCombined, "综合模式"},
		{price.PriceModeAuto, "自动模式"},
	}

	fmt.Printf("🎯 代币: %s (%s)\n", token.name, token.address)
	fmt.Println()

	for _, mode := range modes {
		fmt.Printf("🔍 测试 %s (%s)\n", mode.name, mode.mode)

		startTime := time.Now()
		price, err := client.GetPriceWithMode(token.address, token.chainID, 5*time.Second, mode.mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功: %.8f USDT (%v)\n", price, duration)
		}

		fmt.Println()
		time.Sleep(500 * time.Millisecond)
	}
}

// testDynamicPrecision 测试动态精度
func testDynamicPrecision(client *price.PriceClient, token struct {
	address     string
	name        string
	chainID     string
	priceMode   price.PriceMode
	precision   int
}) {
	fmt.Println("========================================")
	fmt.Println("🔢 测试动态价格精度")
	fmt.Println("========================================")

	precisions := []int{4, 6, 8, 10, 12}

	fmt.Printf("🎯 代币: %s (%s)\n", token.name, token.address)
	fmt.Printf("🔧 模式: %s\n", token.priceMode)
	fmt.Println()

	for _, precision := range precisions {
		fmt.Printf("🔍 测试 %d位精度\n", precision)

		startTime := time.Now()
		price, err := client.GetPriceWithPrecisionAndMode(
			token.address,
			token.chainID,
			precision,        // 动态精度
			token.priceMode,
		)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功: %.*f USDT (%v)\n", precision, price, duration)
		}

		fmt.Println()
		time.Sleep(300 * time.Millisecond)
	}
}
