package main

import (
	"encoding/json"
	"fmt"
	"time"

	"alpha-autosell-bot/price"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("🎯 买单价格优化测试程序")
	fmt.Println("========================================")

	// 测试代币地址
	tokenAddress := "0xa2be3e48170a60119b5f0400c65f65f3158fbeee"
	chainID := "56" // BSC链

	fmt.Printf("📍 测试代币: %s\n", tokenAddress)
	fmt.Printf("🔗 链ID: %s\n", chainID)
	fmt.Println()

	// 初始化价格客户端
	fmt.Println("🔧 初始化价格客户端...")
	client := price.GetGlobalClient()
	
	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 测试不同价格模式的买单策略
	testBuyPriceStrategies(client, tokenAddress, chainID)

	// 测试价格获取稳定性
	testPriceStability(client, tokenAddress, chainID)

	// 测试降级策略
	testFallbackStrategy(client, tokenAddress, chainID)

	fmt.Println()
	fmt.Println("🎉 买单价格优化测试完成！")
}

// testBuyPriceStrategies 测试不同价格模式的买单策略
func testBuyPriceStrategies(client *price.PriceClient, tokenAddress, chainID string) {
	fmt.Println("========================================")
	fmt.Println("💰 测试买单价格策略")
	fmt.Println("========================================")

	modes := []struct {
		mode        price.PriceMode
		name        string
		description string
		buyStrategy string
	}{
		{price.PriceModeMarket, "链上模式", "真实成交价格", "加万二(+0.02%)"},
		{price.PriceModeLimit, "限价模式", "限价订单价格", "加万一(+0.01%)"},
		{price.PriceModeCombined, "综合模式", "综合价格信息", "加万一点五(+0.015%)"},
		{price.PriceModeAuto, "自动模式", "系统自动选择", "智能调整"},
	}

	results := make(map[string]interface{})

	for _, modeInfo := range modes {
		fmt.Printf("🔍 测试 %s (%s)\n", modeInfo.name, modeInfo.description)
		fmt.Printf("   买单策略: %s\n", modeInfo.buyStrategy)

		// 获取基础价格
		startTime := time.Now()
		basePrice, err := client.GetPriceWithMode(tokenAddress, chainID, 10*time.Second, modeInfo.mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 价格获取失败: %v\n", err)
			results[string(modeInfo.mode)] = map[string]interface{}{
				"success":     false,
				"error":       err.Error(),
				"duration_ms": duration.Milliseconds(),
			}
			continue
		}

		// 计算买单价格
		var buyPrice float64
		var markup float64

		switch modeInfo.mode {
		case price.PriceModeMarket:
			markup = 0.0002 // 万二
			buyPrice = basePrice * (1 + markup)
		case price.PriceModeLimit:
			markup = 0.0001 // 万一
			buyPrice = basePrice * (1 + markup)
		case price.PriceModeCombined:
			markup = 0.0015 // 万一点五
			buyPrice = basePrice * (1 + markup)
		default:
			markup = 0.0002 // 默认万二
			buyPrice = basePrice * (1 + markup)
		}

		fmt.Printf("   ✅ 基础价格: %.8f USDT\n", basePrice)
		fmt.Printf("   💰 买单价格: %.8f USDT (加价%.4f%%)\n", buyPrice, markup*100)
		fmt.Printf("   ⏱️  获取耗时: %v\n", duration)

		results[string(modeInfo.mode)] = map[string]interface{}{
			"success":      true,
			"base_price":   basePrice,
			"buy_price":    buyPrice,
			"markup_pct":   markup * 100,
			"duration_ms":  duration.Milliseconds(),
		}

		fmt.Println()
		time.Sleep(1 * time.Second) // 避免请求过于频繁
	}

	// 输出策略对比
	fmt.Println("📊 买单策略对比:")
	jsonResults, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(jsonResults))
	fmt.Println()
}

// testPriceStability 测试价格获取稳定性
func testPriceStability(client *price.PriceClient, tokenAddress, chainID string) {
	fmt.Println("========================================")
	fmt.Println("📈 测试价格获取稳定性")
	fmt.Println("========================================")

	// 连续获取10次价格，测试稳定性
	mode := price.PriceModeMarket // 使用推荐的买单模式
	prices := make([]float64, 0, 10)
	durations := make([]int64, 0, 10)

	fmt.Printf("🔄 连续获取10次价格 (模式: %s)...\n", mode)

	for i := 1; i <= 10; i++ {
		startTime := time.Now()
		price, err := client.GetPriceWithMode(tokenAddress, chainID, 5*time.Second, mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 第%d次获取失败: %v\n", i, err)
			continue
		}

		prices = append(prices, price)
		durations = append(durations, duration.Milliseconds())
		fmt.Printf("   ✅ 第%d次: %.8f USDT (%dms)\n", i, price, duration.Milliseconds())

		time.Sleep(500 * time.Millisecond) // 间隔500ms
	}

	if len(prices) > 0 {
		// 计算统计信息
		var sum, minPrice, maxPrice float64
		var totalDuration int64

		minPrice = prices[0]
		maxPrice = prices[0]

		for i, p := range prices {
			sum += p
			totalDuration += durations[i]
			if p < minPrice {
				minPrice = p
			}
			if p > maxPrice {
				maxPrice = p
			}
		}

		avgPrice := sum / float64(len(prices))
		avgDuration := totalDuration / int64(len(prices))
		priceVariation := ((maxPrice - minPrice) / avgPrice) * 100

		fmt.Printf("\n📊 稳定性统计:\n")
		fmt.Printf("   📈 平均价格: %.8f USDT\n", avgPrice)
		fmt.Printf("   📉 最低价格: %.8f USDT\n", minPrice)
		fmt.Printf("   📊 最高价格: %.8f USDT\n", maxPrice)
		fmt.Printf("   📈 价格波动: %.4f%%\n", priceVariation)
		fmt.Printf("   ⏱️  平均耗时: %dms\n", avgDuration)
		fmt.Printf("   ✅ 成功率: %.1f%% (%d/%d)\n", float64(len(prices))/10*100, len(prices), 10)
	}

	fmt.Println()
}

// testFallbackStrategy 测试降级策略
func testFallbackStrategy(client *price.PriceClient, tokenAddress, chainID string) {
	fmt.Println("========================================")
	fmt.Println("🔄 测试价格获取降级策略")
	fmt.Println("========================================")

	// 模拟降级策略的执行顺序
	fallbackModes := []struct {
		mode        price.PriceMode
		name        string
		description string
	}{
		{price.PriceModeMarket, "首选模式", "链上真实成交价"},
		{price.PriceModeCombined, "降级模式1", "综合价格信息"},
		{price.PriceModeLimit, "降级模式2", "限价订单价格"},
	}

	fmt.Println("🎯 买单价格获取降级策略测试:")
	fmt.Println("   1. 首选: Market模式 (真实成交价)")
	fmt.Println("   2. 降级1: Combined模式 (综合信息)")
	fmt.Println("   3. 降级2: Limit模式 (限价订单)")
	fmt.Println()

	for i, modeInfo := range fallbackModes {
		fmt.Printf("🔍 测试 %s - %s\n", modeInfo.name, modeInfo.description)

		startTime := time.Now()
		price, err := client.GetPriceWithMode(tokenAddress, chainID, 5*time.Second, modeInfo.mode)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("   ❌ 失败: %v\n", err)
			if i < len(fallbackModes)-1 {
				fmt.Printf("   🔄 尝试降级到下一个模式...\n")
			} else {
				fmt.Printf("   🚨 所有模式都失败，买单无法执行\n")
			}
		} else {
			fmt.Printf("   ✅ 成功: %.8f USDT (%v)\n", price, duration)
			fmt.Printf("   🎯 买单策略: 使用此价格进行买单\n")
			break
		}

		fmt.Println()
	}

	fmt.Println("✅ 降级策略测试完成")
}
