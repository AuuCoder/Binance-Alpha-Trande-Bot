package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// DistributedFlashTradeRequest 分布式Flash Trade请求结构体
type DistributedFlashTradeRequest struct {
	NodeID         string  `json:"node_id"`         // 目标节点ID，空表示所有节点
	AccountID      string  `json:"account_id"`      // 目标账号ID，空表示所有账号
	TokenAddress   string  `json:"token_address"`   // 代币地址
	USDTAmount     float64 `json:"usdt_amount"`     // USDT交易金额
	BaseAsset      string  `json:"base_asset"`      // 基础资产
	TargetVolume   float64 `json:"target_volume"`   // 目标交易额
	AutoLoop       bool    `json:"auto_loop"`       // 是否自动循环
	PricePrecision int32   `json:"price_precision"` // 价格精度位数
	ChainID        string  `json:"chain_id"`        // 区块链ID
	PriceMode      string  `json:"price_mode"`      // 价格模式
	ForceStart     bool    `json:"force_start"`     // 是否强制启动
	StopExisting   bool    `json:"stop_existing"`   // 是否停止现有任务
}

// DistributedFlashTradeResponse 分布式Flash Trade响应结构体
type DistributedFlashTradeResponse struct {
	Success          bool     `json:"success"`
	Message          string   `json:"message"`
	TaskID           string   `json:"task_id"`
	AffectedNodes    []string `json:"affected_nodes"`
	AffectedAccounts []string `json:"affected_accounts"`
}

func main() {
	fmt.Println("========================================")
	fmt.Println("🚀 分布式Flash Trade任务下发脚本 v1.0")
	fmt.Println("========================================")
	fmt.Println("📡 支持多链 (BSC + Solana) 和多价格模式")
	fmt.Println()

	// 获取用户输入
	request, err := getDistributedTaskInput()
	if err != nil {
		fmt.Printf("❌ 获取用户输入失败: %v\n", err)
		return
	}

	// 显示任务参数确认
	displayTaskConfirmation(request)

	// 确认执行
	if !confirmExecution() {
		fmt.Println("❌ 用户取消执行")
		return
	}

	// 发送分布式任务
	fmt.Println("📡 正在发送分布式Flash Trade任务...")
	success := sendDistributedFlashTradeTask(request)
	
	if success {
		fmt.Println("✅ 分布式Flash Trade任务下发成功！")
	} else {
		fmt.Println("❌ 分布式Flash Trade任务下发失败！")
	}
}

// getDistributedTaskInput 获取分布式任务输入参数
func getDistributedTaskInput() (*DistributedFlashTradeRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	req := &DistributedFlashTradeRequest{}

	// 获取Master节点地址
	fmt.Print("请输入Master节点地址 (默认localhost:28080): ")
	masterAddr, _ := reader.ReadString('\n')
	masterAddr = strings.TrimSpace(masterAddr)
	if masterAddr == "" {
		masterAddr = "localhost:28080"
	}

	// 获取目标节点ID
	fmt.Print("请输入目标节点ID (空表示所有节点): ")
	nodeID, _ := reader.ReadString('\n')
	req.NodeID = strings.TrimSpace(nodeID)

	// 获取目标账号ID
	fmt.Print("请输入目标账号ID (空表示所有账号): ")
	accountID, _ := reader.ReadString('\n')
	req.AccountID = strings.TrimSpace(accountID)

	// 获取代币地址
	fmt.Print("请输入代币地址 (token_address): ")
	tokenAddress, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	req.TokenAddress = strings.TrimSpace(tokenAddress)
	if req.TokenAddress == "" {
		return nil, fmt.Errorf("代币地址不能为空")
	}

	// 获取USDT金额
	fmt.Print("请输入USDT金额 (usdt_amount, 默认100): ")
	usdtAmountStr, _ := reader.ReadString('\n')
	usdtAmountStr = strings.TrimSpace(usdtAmountStr)
	if usdtAmountStr == "" {
		req.USDTAmount = 100
	} else {
		req.USDTAmount, err = strconv.ParseFloat(usdtAmountStr, 64)
		if err != nil {
			return nil, fmt.Errorf("USDT金额格式错误: %v", err)
		}
	}

	// 获取基础资产
	fmt.Print("请输入基础资产 (base_asset, 默认ALPHA_251): ")
	baseAsset, _ := reader.ReadString('\n')
	req.BaseAsset = strings.TrimSpace(baseAsset)
	if req.BaseAsset == "" {
		req.BaseAsset = "ALPHA_251"
	}

	// 获取目标交易量
	fmt.Print("请输入目标交易量 (target_volume, 默认1000): ")
	targetVolumeStr, _ := reader.ReadString('\n')
	targetVolumeStr = strings.TrimSpace(targetVolumeStr)
	if targetVolumeStr == "" {
		req.TargetVolume = 1000
	} else {
		req.TargetVolume, err = strconv.ParseFloat(targetVolumeStr, 64)
		if err != nil {
			return nil, fmt.Errorf("目标交易量格式错误: %v", err)
		}
	}

	// 获取链ID
	fmt.Print("请输入链ID (56=BSC, CT_501=Solana, 默认56): ")
	chainID, _ := reader.ReadString('\n')
	req.ChainID = strings.TrimSpace(chainID)
	if req.ChainID == "" {
		req.ChainID = "56"
	}

	// 获取价格模式
	fmt.Print("请输入价格模式 (limit/market/combined/auto, 默认market): ")
	priceMode, _ := reader.ReadString('\n')
	req.PriceMode = strings.TrimSpace(priceMode)
	if req.PriceMode == "" {
		req.PriceMode = "market"
	}
	// 验证价格模式
	validModes := []string{"limit", "market", "combined", "auto"}
	isValidMode := false
	for _, mode := range validModes {
		if req.PriceMode == mode {
			isValidMode = true
			break
		}
	}
	if !isValidMode {
		return nil, fmt.Errorf("无效的价格模式: %s，支持的模式: %v", req.PriceMode, validModes)
	}

	// 获取价格精度
	fmt.Print("请输入价格精度 (price_precision, 默认8): ")
	precisionStr, _ := reader.ReadString('\n')
	precisionStr = strings.TrimSpace(precisionStr)
	if precisionStr == "" {
		req.PricePrecision = 8
	} else {
		precision, err := strconv.Atoi(precisionStr)
		if err != nil {
			return nil, fmt.Errorf("价格精度格式错误: %v", err)
		}
		req.PricePrecision = int32(precision)
	}

	// 获取自动循环设置
	fmt.Print("是否启用自动循环 (auto_loop, Y/n, 默认Y): ")
	autoLoopStr, _ := reader.ReadString('\n')
	autoLoopStr = strings.ToLower(strings.TrimSpace(autoLoopStr))
	req.AutoLoop = autoLoopStr == "" || autoLoopStr == "y" || autoLoopStr == "yes"

	// 获取强制启动设置
	fmt.Print("是否强制启动 (force_start, y/N, 默认N): ")
	forceStartStr, _ := reader.ReadString('\n')
	forceStartStr = strings.ToLower(strings.TrimSpace(forceStartStr))
	req.ForceStart = forceStartStr == "y" || forceStartStr == "yes"

	// 获取停止现有任务设置
	fmt.Print("是否停止现有任务 (stop_existing, y/N, 默认N): ")
	stopExistingStr, _ := reader.ReadString('\n')
	stopExistingStr = strings.ToLower(strings.TrimSpace(stopExistingStr))
	req.StopExisting = stopExistingStr == "y" || stopExistingStr == "yes"

	return req, nil
}

// displayTaskConfirmation 显示任务参数确认
func displayTaskConfirmation(req *DistributedFlashTradeRequest) {
	fmt.Println()
	fmt.Println("📋 分布式Flash Trade任务参数确认:")
	fmt.Printf("   目标节点: %s\n", getDisplayValue(req.NodeID, "所有节点"))
	fmt.Printf("   目标账号: %s\n", getDisplayValue(req.AccountID, "所有账号"))
	fmt.Printf("   代币地址: %s\n", req.TokenAddress)
	fmt.Printf("   USDT金额: %.2f\n", req.USDTAmount)
	fmt.Printf("   基础资产: %s\n", req.BaseAsset)
	fmt.Printf("   目标交易量: %.2f\n", req.TargetVolume)
	fmt.Printf("   链ID: %s (%s)\n", req.ChainID, getChainName(req.ChainID))
	fmt.Printf("   价格模式: %s (%s)\n", req.PriceMode, getPriceModeDescription(req.PriceMode))
	fmt.Printf("   价格精度: %d\n", req.PricePrecision)
	fmt.Printf("   自动循环: %t\n", req.AutoLoop)
	fmt.Printf("   强制启动: %t\n", req.ForceStart)
	fmt.Printf("   停止现有: %t\n", req.StopExisting)
	fmt.Println()
}

// getDisplayValue 获取显示值
func getDisplayValue(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
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

// getPriceModeDescription 获取价格模式描述
func getPriceModeDescription(mode string) string {
	switch mode {
	case "limit":
		return "限价模式"
	case "market":
		return "市价模式 - 推荐买单"
	case "combined":
		return "综合模式"
	case "auto":
		return "自动模式"
	default:
		return "未知"
	}
}

// confirmExecution 确认执行
func confirmExecution() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("确认执行分布式Flash Trade任务? (Y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.ToLower(strings.TrimSpace(confirm))
	return confirm == "" || confirm == "y" || confirm == "yes"
}

// sendDistributedFlashTradeTask 发送分布式Flash Trade任务
func sendDistributedFlashTradeTask(req *DistributedFlashTradeRequest) bool {
	// 构建请求URL
	masterURL := "http://localhost:28080/api/flash-trade/start"
	
	// 转换为JSON
	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("❌ JSON序列化失败: %v\n", err)
		return false
	}

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

	fmt.Printf("📡 Master节点响应 (状态码: %d):\n", resp.StatusCode)
	fmt.Printf("   响应内容: %s\n", string(body))

	// 解析响应
	var response DistributedFlashTradeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		fmt.Printf("❌ 解析响应失败: %v\n", err)
		return false
	}

	// 检查响应状态
	if response.Success {
		fmt.Printf("✅ 任务下发成功!\n")
		fmt.Printf("   任务ID: %s\n", response.TaskID)
		if len(response.AffectedNodes) > 0 {
			fmt.Printf("   影响节点: %v\n", response.AffectedNodes)
		}
		if len(response.AffectedAccounts) > 0 {
			fmt.Printf("   影响账号: %v\n", response.AffectedAccounts)
		}
		return true
	} else {
		fmt.Printf("❌ 任务下发失败: %s\n", response.Message)
		return false
	}
}
