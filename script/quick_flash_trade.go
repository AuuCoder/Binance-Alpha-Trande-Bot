package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// QuickFlashTradeRequest 快速Flash Trade请求结构体
type QuickFlashTradeRequest struct {
	NodeID         string  `json:"node_id"`
	AccountID      string  `json:"account_id"`
	TokenAddress   string  `json:"token_address"`
	USDTAmount     float64 `json:"usdt_amount"`
	BaseAsset      string  `json:"base_asset"`
	TargetVolume   float64 `json:"target_volume"`
	AutoLoop       bool    `json:"auto_loop"`
	PricePrecision int32   `json:"price_precision"`
	ChainID        string  `json:"chain_id"`
	PriceMode      string  `json:"price_mode"`
	SpeedMode      string  `json:"speed_mode"`      // 🚀 新增：速度模式
	ForceStart     bool    `json:"force_start"`
	StopExisting   bool    `json:"stop_existing"`
}

// PresetConfig 预设配置
type PresetConfig struct {
	Name         string
	Description  string
	ChainID      string
	PriceMode    string
	SpeedMode    string  // 🚀 新增：速度模式
	USDTAmount   float64
	TargetVolume float64
	BaseAsset    string
}

// 预设配置列表
var presetConfigs = []PresetConfig{
	{
		Name:         "BSC_MARKET_SMALL",
		Description:  "BSC链 - Market模式 - 小额测试 - 慢速模式",
		ChainID:      "56",
		PriceMode:    "market",
		SpeedMode:    "slow",     // 🚀 新增：默认慢速模式
		USDTAmount:   10,
		TargetVolume: 50,
		BaseAsset:    "ALPHA_TEST",
	},
	{
		Name:         "BSC_MARKET_NORMAL",
		Description:  "BSC链 - Market模式 - 正常交易 - 普通模式",
		ChainID:      "56",
		PriceMode:    "market",
		SpeedMode:    "normal",   // 🚀 新增：普通模式
		USDTAmount:   100,
		TargetVolume: 1000,
		BaseAsset:    "ALPHA_251",
	},
	{
		Name:         "BSC_LIMIT_NORMAL",
		Description:  "BSC链 - Limit模式 - 正常交易 - 普通模式",
		ChainID:      "56",
		PriceMode:    "limit",
		SpeedMode:    "normal",   // 🚀 新增：普通模式
		USDTAmount:   100,
		TargetVolume: 1000,
		BaseAsset:    "ALPHA_251",
	},
	{
		Name:         "SOLANA_COMBINED_NORMAL",
		Description:  "Solana链 - Combined模式 - 正常交易 - 普通模式",
		ChainID:      "CT_501",
		PriceMode:    "combined",
		SpeedMode:    "normal",   // 🚀 新增：普通模式
		USDTAmount:   100,
		TargetVolume: 1000,
		BaseAsset:    "ALPHA_251",
	},
	{
		Name:         "SOLANA_MARKET_NORMAL",
		Description:  "Solana链 - Market模式 - 正常交易 - 普通模式",
		ChainID:      "CT_501",
		PriceMode:    "market",
		SpeedMode:    "normal",   // 🚀 新增：普通模式
		USDTAmount:   100,
		TargetVolume: 1000,
		BaseAsset:    "ALPHA_251",
	},
}

func main() {
	fmt.Println("========================================")
	fmt.Println("⚡ 快速Flash Trade任务下发脚本 v1.0")
	fmt.Println("========================================")
	fmt.Println("🚀 支持预设配置，快速下发常用任务")
	fmt.Println()

	// 检查命令行参数
	if len(os.Args) < 2 {
		showUsage()
		return
	}

	tokenAddress := os.Args[1]
	if tokenAddress == "" {
		fmt.Println("❌ 代币地址不能为空")
		showUsage()
		return
	}

	// 选择预设配置
	var selectedPreset *PresetConfig
	if len(os.Args) >= 3 {
		presetName := os.Args[2]
		for i, preset := range presetConfigs {
			if preset.Name == presetName {
				selectedPreset = &presetConfigs[i]
				break
			}
		}
		if selectedPreset == nil {
			fmt.Printf("❌ 未找到预设配置: %s\n", presetName)
			showPresets()
			return
		}
	} else {
		// 默认使用BSC Market模式
		selectedPreset = &presetConfigs[1] // BSC_MARKET_NORMAL
	}

	// 构建请求
	request := &QuickFlashTradeRequest{
		NodeID:         "",                           // 所有节点
		AccountID:      "",                           // 所有账号
		TokenAddress:   tokenAddress,
		USDTAmount:     selectedPreset.USDTAmount,
		BaseAsset:      selectedPreset.BaseAsset,
		TargetVolume:   selectedPreset.TargetVolume,
		AutoLoop:       true,                         // 默认启用自动循环
		PricePrecision: 8,                            // 默认8位精度
		ChainID:        selectedPreset.ChainID,
		PriceMode:      selectedPreset.PriceMode,
		SpeedMode:      selectedPreset.SpeedMode,     // 🚀 新增：传递速度模式
		ForceStart:     true,                         // 强制启动
		StopExisting:   true,                         // 停止现有任务
	}

	// 显示任务信息
	fmt.Printf("📋 任务信息:\n")
	fmt.Printf("   预设配置: %s\n", selectedPreset.Name)
	fmt.Printf("   配置描述: %s\n", selectedPreset.Description)
	fmt.Printf("   代币地址: %s\n", request.TokenAddress)
	fmt.Printf("   链ID: %s (%s)\n", request.ChainID, getChainName(request.ChainID))
	fmt.Printf("   价格模式: %s\n", request.PriceMode)
	fmt.Printf("   速度模式: %s (%s)\n", request.SpeedMode, getSpeedModeDescription(request.SpeedMode))
	fmt.Printf("   USDT金额: %.2f\n", request.USDTAmount)
	fmt.Printf("   目标交易量: %.2f\n", request.TargetVolume)
	fmt.Printf("   基础资产: %s\n", request.BaseAsset)
	fmt.Println()

	// 发送任务
	fmt.Println("📡 正在发送Flash Trade任务...")
	success := sendQuickFlashTradeTask(request)
	
	if success {
		fmt.Println("✅ Flash Trade任务下发成功！")
	} else {
		fmt.Println("❌ Flash Trade任务下发失败！")
		os.Exit(1)
	}
}

// showUsage 显示使用说明
func showUsage() {
	fmt.Println("📖 使用说明:")
	fmt.Println("   ./quick_flash_trade <代币地址> [预设配置名称]")
	fmt.Println()
	fmt.Println("📝 示例:")
	fmt.Println("   ./quick_flash_trade 0xa2be3e48170a60119b5f0400c65f65f3158fbeee")
	fmt.Println("   ./quick_flash_trade 0xa2be3e48170a60119b5f0400c65f65f3158fbeee BSC_MARKET_NORMAL")
	fmt.Println("   ./quick_flash_trade SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL SOLANA_MARKET_NORMAL")
	fmt.Println()
	showPresets()
}

// showPresets 显示预设配置
func showPresets() {
	fmt.Println("🔧 可用预设配置:")
	for i, preset := range presetConfigs {
		fmt.Printf("   %d. %s - %s\n", i+1, preset.Name, preset.Description)
		fmt.Printf("      链: %s, 价格模式: %s, 速度模式: %s, 金额: %.0f USDT, 目标量: %.0f\n",
			preset.ChainID, preset.PriceMode, preset.SpeedMode, preset.USDTAmount, preset.TargetVolume)
	}
	fmt.Println()
}

// getChainName 获取链名称
func getChainName(chainID string) string {
	switch chainID {
	case "56":
		return "BSC"
	case "CT_501":
		return "Solana"
	default:
		return "未知"
	}
}

// 🚀 getSpeedModeDescription 获取速度模式描述
func getSpeedModeDescription(mode string) string {
	switch mode {
	case "fast":
		return "快速模式 - 高效率，中等风控风险"
	case "normal":
		return "普通模式 - 平衡效率与安全，推荐使用"
	case "slow":
		return "慢速模式 - 极度保守，最低风控风险"
	default:
		return "未知"
	}
}

// sendQuickFlashTradeTask 发送快速Flash Trade任务
func sendQuickFlashTradeTask(req *QuickFlashTradeRequest) bool {
	// 构建请求URL
	masterURL := "http://localhost:28080/api/flash-trade/start"
	
	// 转换为JSON
	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("❌ JSON序列化失败: %v\n", err)
		return false
	}

	fmt.Printf("🔧 发送请求到: %s\n", masterURL)
	fmt.Printf("📦 请求数据: %s\n", string(jsonData))

	// 创建HTTP请求
	httpReq, err := http.NewRequest("POST", masterURL, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("❌ 创建HTTP请求失败: %v\n", err)
		return false
	}

	httpReq.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		fmt.Printf("❌ 发送请求失败: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ 读取响应失败: %v\n", err)
		return false
	}

	fmt.Printf("📡 响应状态码: %d\n", resp.StatusCode)
	fmt.Printf("📦 响应内容: %s\n", string(body))

	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		fmt.Printf("❌ 解析响应失败: %v\n", err)
		return false
	}

	// 检查响应状态
	if success, ok := response["success"].(bool); ok && success {
		fmt.Printf("✅ 任务下发成功!\n")
		if taskID, ok := response["task_id"].(string); ok {
			fmt.Printf("   任务ID: %s\n", taskID)
		}
		if affectedNodes, ok := response["affected_nodes"].([]interface{}); ok && len(affectedNodes) > 0 {
			fmt.Printf("   影响节点: %v\n", affectedNodes)
		}
		if affectedAccounts, ok := response["affected_accounts"].([]interface{}); ok && len(affectedAccounts) > 0 {
			fmt.Printf("   影响账号: %v\n", affectedAccounts)
		}
		return true
	} else {
		message, _ := response["message"].(string)
		fmt.Printf("❌ 任务下发失败: %s\n", message)
		return false
	}
}
