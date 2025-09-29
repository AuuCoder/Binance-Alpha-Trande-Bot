package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// 测试分布式系统的多链支持
func main() {
	log.Println("🧪 开始测试分布式系统多链支持")

	// 测试用例
	testCases := []struct {
		name         string
		tokenAddress string
		chainID      string
		priceMode    string
		description  string
	}{
		{
			name:         "BSC_ALPHA_372",
			tokenAddress: "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
			chainID:      "56",
			priceMode:    "market",
			description:  "BSC链 ALPHA_372 代币 - Market模式",
		},
		{
			name:         "BSC_ALPHA_373",
			tokenAddress: "0x6cfffa5bfd4277a04d83307feedfe2d18d944dd2",
			chainID:      "56",
			priceMode:    "limit",
			description:  "BSC链 ALPHA_373 代币 - Limit模式",
		},
		{
			name:         "Solana_ALPHA_360",
			tokenAddress: "SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL",
			chainID:      "CT_501",
			priceMode:    "combined",
			description:  "Solana链 ALPHA_360 代币 - Combined模式",
		},
	}

	// 等待服务启动
	log.Println("⏳ 等待服务启动...")
	time.Sleep(3 * time.Second)

	// 测试每个用例
	for i, testCase := range testCases {
		log.Printf("\n🔍 测试 %d/%d: %s", i+1, len(testCases), testCase.description)
		log.Printf("   代币地址: %s", testCase.tokenAddress)
		log.Printf("   链ID: %s", testCase.chainID)
		log.Printf("   价格模式: %s", testCase.priceMode)

		success := testFlashTradeDistribution(testCase.tokenAddress, testCase.chainID, testCase.priceMode)
		if success {
			log.Printf("✅ 测试 %s 成功", testCase.name)
		} else {
			log.Printf("❌ 测试 %s 失败", testCase.name)
		}

		// 测试间隔
		if i < len(testCases)-1 {
			log.Println("⏳ 等待下一个测试...")
			time.Sleep(2 * time.Second)
		}
	}

	log.Println("\n🎉 分布式多链支持测试完成！")
}

// testFlashTradeDistribution 测试Flash Trade分布式任务
func testFlashTradeDistribution(tokenAddress, chainID, priceMode string) bool {
	// 构建请求
	requestBody := map[string]interface{}{
		"node_id":         "",                    // 空表示所有节点
		"account_id":      "",                    // 空表示所有账号
		"token_address":   tokenAddress,
		"usdt_amount":     10.0,                  // 小额测试
		"base_asset":      "ALPHA_TEST",
		"target_volume":   50.0,
		"auto_loop":       false,                 // 测试时不启用循环
		"price_precision": 8,
		"chain_id":        chainID,               // 🔧 新增：链ID
		"price_mode":      priceMode,             // 🔧 新增：价格模式
		"force_start":     true,                  // 强制启动
		"stop_existing":   true,                  // 停止现有任务
	}

	// 转换为JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("❌ JSON序列化失败: %v", err)
		return false
	}

	// 发送请求到master节点
	masterURL := "http://localhost:28080/api/flash-trade/start"
	req, err := http.NewRequest("POST", masterURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ 创建HTTP请求失败: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ 发送请求失败: %v", err)
		return false
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ 读取响应失败: %v", err)
		return false
	}

	log.Printf("📡 Master节点响应: %s", string(body))

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("❌ 解析响应失败: %v", err)
		return false
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		log.Printf("✅ Flash Trade任务分发成功")
		
		// 获取任务ID
		if taskID, ok := response["task_id"].(string); ok {
			log.Printf("   任务ID: %s", taskID)
		}
		
		// 获取影响的节点
		if affectedNodes, ok := response["affected_nodes"].([]interface{}); ok {
			log.Printf("   影响节点: %v", affectedNodes)
		}
		
		// 获取影响的账号
		if affectedAccounts, ok := response["affected_accounts"].([]interface{}); ok {
			log.Printf("   影响账号: %v", affectedAccounts)
		}
		
		return true
	} else {
		message, _ := response["message"].(string)
		log.Printf("❌ Flash Trade任务分发失败: %s", message)
		return false
	}
}

// testPriceRetrieval 测试价格获取（可选）
func testPriceRetrieval(tokenAddress, chainID, priceMode string) bool {
	log.Printf("🔍 测试价格获取: 代币=%s, 链=%s, 模式=%s", tokenAddress, chainID, priceMode)
	
	// 这里可以添加直接调用价格服务的测试
	// 暂时返回true表示跳过
	return true
}
