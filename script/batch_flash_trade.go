package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NodeConfig 节点配置结构体
type NodeConfig struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IP        string `json:"ip"`
	Csrftoken string `json:"csrftoken"`
	Cookie    string `json:"cookie"`
}

// FlashTradeRequest 交易请求结构体
type FlashTradeRequest struct {
	TokenAddress   string  `json:"token_address"`
	USDTAmount     float64 `json:"usdt_amount"`
	TargetVolume   float64 `json:"target_volume"`
	BaseAsset      string  `json:"base_asset"`
	ChainID        string  `json:"chain_id"`        // 链ID支持
	PriceMode      string  `json:"price_mode"`      // 价格模式
	SpeedMode      string  `json:"speed_mode"`      // 速度模式
	PricePrecision int     `json:"price_precision"`
	AutoLoop       bool    `json:"auto_loop"`
	Csrftoken      string  `json:"csrftoken"`
	Cookie         string  `json:"cookie"`
}

// FlashTradeResponse 交易响应结构体
type FlashTradeResponse struct {
	AccountID string `json:"account_id"`
	Message   string `json:"message"`
	Status    string `json:"status"`
	Success   bool   `json:"success"`
	成功        bool   `json:"成功"`
	消息        string `json:"消息"`
	状态        string `json:"状态"`
	账户ID      string `json:"账户ID"`
}

// TaskResult 任务执行结果
type TaskResult struct {
	Node    NodeConfig
	Success bool
	Message string
	Retries int
}

func main() {
	fmt.Println("========================================")
	fmt.Println("🚀 批量下发Flash Trade任务脚本 v2.0")
	fmt.Println("========================================")
	fmt.Println("📡 支持多链 (BSC + Solana) 和多价格模式")
	fmt.Println()

	// 显示运行环境信息
	if execPath, err := os.Executable(); err == nil {
		fmt.Printf("📍 程序位置: %s\n", execPath)
		fmt.Printf("📁 程序目录: %s\n", filepath.Dir(execPath))
	}

	if workDir, err := os.Getwd(); err == nil {
		fmt.Printf("💼 工作目录: %s\n", workDir)
	}

	fmt.Println()

	// 读取配置文件
	nodes, err := loadConfigFile("config.json")
	if err != nil {
		fmt.Printf("❌ 读取配置文件失败: %v\n", err)
		return
	}

	if len(nodes) == 0 {
		fmt.Println("❌ 配置文件中没有找到节点信息")
		return
	}

	fmt.Printf("✅ 成功加载 %d 个节点配置\n", len(nodes))
	for i, node := range nodes {
		fmt.Printf("   %d. %s (%s) - %s\n", i+1, node.Name, node.ID, node.IP)
	}
	fmt.Println()

	// 获取用户输入的交易参数
	tradeParams, err := getTradeInput()
	if err != nil {
		fmt.Printf("❌ 获取用户输入失败: %v\n", err)
		return
	}

	// 显示交易参数确认
	fmt.Println("📋 交易参数确认:")
	fmt.Printf("   代币地址: %s\n", tradeParams.TokenAddress)
	fmt.Printf("   USDT金额: %.2f\n", tradeParams.USDTAmount)
	fmt.Printf("   目标交易量: %.2f\n", tradeParams.TargetVolume)
	fmt.Printf("   基础资产: %s\n", tradeParams.BaseAsset)
	fmt.Printf("   链ID: %s\n", tradeParams.ChainID)
	fmt.Printf("   价格模式: %s\n", tradeParams.PriceMode)
	fmt.Printf("   速度模式: %s\n", tradeParams.SpeedMode)
	fmt.Printf("   价格精度: %d\n", tradeParams.PricePrecision)
	fmt.Printf("   自动循环: %t\n", tradeParams.AutoLoop)
	fmt.Println()

	// 选择要执行的节点
	selectedNodes := selectNodeList(nodes)
	if len(selectedNodes) == 0 {
		fmt.Println("❌ 未选择任何节点，程序退出")
		return
	}

	// 确认执行
	if !confirmExecute() {
		fmt.Println("❌ 用户取消执行")
		return
	}

	// 批量发送交易请求（多线程并发）
	fmt.Println("🎯 开始批量发送交易请求（多线程并发）...")
	fmt.Printf("📊 节点总数: %d，最大重试次数: 5\n", len(selectedNodes))
	fmt.Println("========================================")

	// 创建结果通道和等待组
	resultChan := make(chan TaskResult, len(selectedNodes))
	var wg sync.WaitGroup

	// 启动协程处理每个节点
	for i, node := range selectedNodes {
		wg.Add(1)
		go func(index int, n NodeConfig) {
			defer wg.Done()

			fmt.Printf("🚀 [%d/%d] 开始处理节点: %s (%s)\n", index+1, len(selectedNodes), n.Name, n.IP)

			// 为当前节点设置认证信息
			nodeTradeParams := *tradeParams // 复制参数
			nodeTradeParams.Csrftoken = n.Csrftoken
			nodeTradeParams.Cookie = n.Cookie

			// 发送交易请求（带重试）
			result := sendTradeWithRetry(n, &nodeTradeParams, 5)
			resultChan <- result

			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 交易请求成功 (重试%d次)\n",
					index+1, len(selectedNodes), n.Name, result.Retries)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 交易请求失败 (重试%d次): %s\n",
					index+1, len(selectedNodes), n.Name, result.Retries, result.Message)
			}
		}(i, node)
	}

	// 等待所有协程完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	var results []TaskResult
	successCount := 0
	failCount := 0

	for result := range resultChan {
		results = append(results, result)
		if result.Success {
			successCount++
		} else {
			failCount++
		}
	}

	// 导出失败记录
	if failCount > 0 {
		err := exportFailRecords(results, tradeParams)
		if err != nil {
			fmt.Printf("⚠️  导出失败记录时出错: %v\n", err)
		} else {
			fmt.Printf("📄 失败记录已导出到 fail.txt\n")
		}
	}

	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量执行结果:")
	fmt.Printf("   总节点数: %d\n", len(selectedNodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", failCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(selectedNodes))*100)

	// 显示重试统计
	totalRetries := 0
	for _, result := range results {
		totalRetries += result.Retries
	}
	fmt.Printf("   总重试次数: %d\n", totalRetries)
	fmt.Printf("   平均重试次数: %.1f\n", float64(totalRetries)/float64(len(selectedNodes)))

	fmt.Println("========================================")

	if failCount > 0 {
		fmt.Printf("⚠️  %d 个节点执行失败，详细信息已保存到 fail.txt\n", failCount)
		fmt.Println("💡 请检查失败节点的状态和网络连接")
	} else {
		fmt.Println("🎉 所有节点交易请求发送成功！")
	}

	fmt.Println("\n🏁 批量下发任务执行完毕")
	fmt.Println("按任意键退出...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

// loadConfigFile 从JSON文件加载节点配置
func loadConfigFile(filename string) ([]NodeConfig, error) {
	// 获取可执行文件的目录
	execPath, err := os.Executable()
	if err != nil {
		execPath = ""
	}
	execDir := filepath.Dir(execPath)

	// 优先在可执行文件同目录查找配置文件
	searchPaths := []string{
		filepath.Join(execDir, filename), // 可执行文件目录（优先）
		filename,                         // 当前目录（备用）
	}

	var configPath string
	var found bool

	for _, path := range searchPaths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			configPath = path
			found = true
			fmt.Printf("✅ 找到配置文件: %s\n", path)
			break
		}
	}

	if !found {
		fmt.Printf("❌ 在以下位置都未找到配置文件 %s:\n", filename)
		for _, path := range searchPaths {
			if path != "" {
				fmt.Printf("   - %s\n", path)
			}
		}
		return nil, fmt.Errorf("配置文件 %s 不存在", filename)
	}

	// 读取文件内容
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	// 尝试两种格式解析JSON
	var nodes []NodeConfig
	
	// 首先尝试直接解析为节点数组
	err = json.Unmarshal(data, &nodes)
	if err == nil && len(nodes) > 0 {
		fmt.Println("✅ 使用数组格式配置文件")
	} else {
		// 如果失败，尝试解析为包含nodes字段的对象
		var config struct {
			Nodes []NodeConfig `json:"nodes"`
		}
		
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("解析配置文件失败: %v", err)
		}
		
		nodes = config.Nodes
		fmt.Println("✅ 使用对象格式配置文件")
	}
	
	if len(nodes) == 0 {
		return nil, fmt.Errorf("配置文件中没有节点信息")
	}

	return nodes, nil
}

// getTradeInput 获取用户输入的交易参数
func getTradeInput() (*FlashTradeRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	req := &FlashTradeRequest{}

	// 获取代币地址
	fmt.Print("请输入代币地址 (token_address): ")
	tokenAddress, _ := reader.ReadString('\n')
	req.TokenAddress = strings.TrimSpace(tokenAddress)
	if req.TokenAddress == "" {
		return nil, fmt.Errorf("代币地址不能为空")
	}

	// 获取USDT金额
	fmt.Print("请输入USDT金额 (usdt_amount): ")
	usdtAmountStr, _ := reader.ReadString('\n')
	usdtAmountStr = strings.TrimSpace(usdtAmountStr)
	usdtAmount, err := strconv.ParseFloat(usdtAmountStr, 64)
	if err != nil {
		return nil, fmt.Errorf("USDT金额格式错误: %v", err)
	}
	req.USDTAmount = usdtAmount

	// 获取目标交易量
	fmt.Print("请输入目标交易量 (target_volume): ")
	targetVolumeStr, _ := reader.ReadString('\n')
	targetVolumeStr = strings.TrimSpace(targetVolumeStr)
	targetVolume, err := strconv.ParseFloat(targetVolumeStr, 64)
	if err != nil {
		return nil, fmt.Errorf("目标交易量格式错误: %v", err)
	}
	req.TargetVolume = targetVolume

	// 获取基础资产
	fmt.Print("请输入基础资产 (base_asset，默认ALPHA_372): ")
	baseAsset, _ := reader.ReadString('\n')
	req.BaseAsset = strings.TrimSpace(baseAsset)
	if req.BaseAsset == "" {
		req.BaseAsset = "ALPHA_372"
	}

	// 获取链ID
	fmt.Print("请输入链ID (chain_id，默认56): ")
	chainID, _ := reader.ReadString('\n')
	req.ChainID = strings.TrimSpace(chainID)
	if req.ChainID == "" {
		req.ChainID = "56"
	}

	// 获取价格模式
	fmt.Print("请输入价格模式 (price_mode，默认auto): ")
	priceMode, _ := reader.ReadString('\n')
	req.PriceMode = strings.TrimSpace(priceMode)
	if req.PriceMode == "" {
		req.PriceMode = "auto"
	}

	// 获取速度模式
	fmt.Print("请输入速度模式 (speed_mode，默认normal): ")
	speedMode, _ := reader.ReadString('\n')
	req.SpeedMode = strings.TrimSpace(speedMode)
	if req.SpeedMode == "" {
		req.SpeedMode = "normal"
	}

	// 获取价格精度
	fmt.Print("请输入价格精度 (price_precision，默认8): ")
	precisionStr, _ := reader.ReadString('\n')
	precisionStr = strings.TrimSpace(precisionStr)
	if precisionStr == "" {
		req.PricePrecision = 8
	} else {
		precision, err := strconv.Atoi(precisionStr)
		if err != nil {
			return nil, fmt.Errorf("价格精度格式错误: %v", err)
		}
		req.PricePrecision = precision
	}

	// 获取是否自动循环
	fmt.Print("是否自动循环 (auto_loop，y/n，默认y): ")
	autoLoopStr, _ := reader.ReadString('\n')
	autoLoopStr = strings.TrimSpace(autoLoopStr)
	if autoLoopStr == "" || strings.ToLower(autoLoopStr) == "y" {
		req.AutoLoop = true
	} else {
		req.AutoLoop = false
	}

	return req, nil
}

// selectNodeList 选择要执行的节点
func selectNodeList(nodes []NodeConfig) []NodeConfig {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("🔍 请选择要执行的节点:")
	fmt.Println("   1. 全部节点")
	fmt.Println("   2. 选择特定节点")
	fmt.Print("请输入选择 [1]: ")
	
	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)
	
	if choice == "" || choice == "1" {
		fmt.Println("✅ 已选择: 全部节点")
		return nodes
	}
	
	if choice == "2" {
		fmt.Println("📋 可用节点列表:")
		for i, node := range nodes {
			fmt.Printf("   %d. %s (%s) - %s\n", i+1, node.Name, node.ID, node.IP)
		}
		
		fmt.Print("请输入要选择的节点编号 (多个节点用逗号分隔，如 1,3,5): ")
		indexesStr, _ := reader.ReadString('\n')
		indexesStr = strings.TrimSpace(indexesStr)
		
		if indexesStr == "" {
			fmt.Println("⚠️ 未选择任何节点，默认使用全部节点")
			return nodes
		}
		
		// 解析选择的节点索引
		parts := strings.Split(indexesStr, ",")
		selectedIndexes := make(map[int]bool)
		
		for _, part := range parts {
			part = strings.TrimSpace(part)
			index, err := strconv.Atoi(part)
			if err != nil || index < 1 || index > len(nodes) {
				fmt.Printf("⚠️ 忽略无效的节点编号: %s\n", part)
				continue
			}
			selectedIndexes[index-1] = true
		}
		
		// 构建选定的节点列表
		var selectedNodes []NodeConfig
		for i, node := range nodes {
			if selectedIndexes[i] {
				selectedNodes = append(selectedNodes, node)
			}
		}
		
		fmt.Printf("✅ 已选择 %d 个节点\n", len(selectedNodes))
		return selectedNodes
	}
	
	fmt.Println("⚠️ 无效的选择，默认使用全部节点")
	return nodes
}

// confirmExecute 确认执行
func confirmExecute() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\n⚠️ 确认执行以上操作? (y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	return strings.ToLower(confirm) == "y" || confirm == ""
}

// sendTradeWithRetry 发送交易请求（带重试）
func sendTradeWithRetry(node NodeConfig, tradeParams *FlashTradeRequest, maxRetries int) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "",
		Retries: 0,
	}

	var err error
	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			result.Retries++
			retryDelay := time.Duration(500*i) * time.Millisecond
			time.Sleep(retryDelay)
			fmt.Printf("🔄 [%s] 第%d次重试发送交易请求...\n", node.Name, i)
		}

		success, message := sendTradeRequest(node, tradeParams)
		if success {
			result.Success = true
			result.Message = message
			return result
		}

		err = fmt.Errorf(message)
	}

	result.Message = fmt.Sprintf("重试%d次后失败: %v", maxRetries, err)
	return result
}

// sendTradeRequest 发送交易请求
func sendTradeRequest(node NodeConfig, tradeParams *FlashTradeRequest) (bool, string) {
	url := fmt.Sprintf("%s/trade", node.IP)
	
	// 构建请求体
	reqBody, err := json.Marshal(tradeParams)
	if err != nil {
		return false, fmt.Sprintf("序列化请求失败: %v", err)
	}

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return false, fmt.Sprintf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Sprintf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Sprintf("读取响应失败: %v", err)
	}

	// 解析响应
	var tradeResp FlashTradeResponse
	if err := json.Unmarshal(body, &tradeResp); err != nil {
		return false, fmt.Sprintf("解析响应失败: %v", err)
	}

	if tradeResp.Success || tradeResp.成功 {
		return true, "交易请求发送成功"
	}

	message := tradeResp.Message
	if message == "" {
		message = tradeResp.消息
	}
	
	return false, fmt.Sprintf("交易请求失败: %s", message)
}

// exportFailRecords 导出失败节点记录
func exportFailRecords(results []TaskResult, tradeParams *FlashTradeRequest) error {
	// 创建失败记录文件
	file, err := os.Create("fail.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入标题
	file.WriteString("========================================\n")
	file.WriteString("Flash Trade 批量下发失败记录\n")
	file.WriteString("========================================\n\n")

	// 写入交易参数
	file.WriteString("📋 交易参数:\n")
	file.WriteString(fmt.Sprintf("   代币地址: %s\n", tradeParams.TokenAddress))
	file.WriteString(fmt.Sprintf("   USDT金额: %.2f\n", tradeParams.USDTAmount))
	file.WriteString(fmt.Sprintf("   目标交易量: %.2f\n", tradeParams.TargetVolume))
	file.WriteString(fmt.Sprintf("   基础资产: %s\n", tradeParams.BaseAsset))
	file.WriteString(fmt.Sprintf("   链ID: %s\n", tradeParams.ChainID))
	file.WriteString(fmt.Sprintf("   价格模式: %s\n", tradeParams.PriceMode))
	file.WriteString(fmt.Sprintf("   速度模式: %s\n", tradeParams.SpeedMode))
	file.WriteString(fmt.Sprintf("   价格精度: %d\n", tradeParams.PricePrecision))
	file.WriteString(fmt.Sprintf("   自动循环: %t\n\n", tradeParams.AutoLoop))

	// 写入失败记录
	file.WriteString("❌ 失败节点列表:\n")
	failCount := 0
	for _, result := range results {
		if !result.Success {
			failCount++
			file.WriteString(fmt.Sprintf("%d. %s (%s) - %s\n", failCount, result.Node.Name, result.Node.ID, result.Node.IP))
			file.WriteString(fmt.Sprintf("   错误信息: %s\n", result.Message))
			file.WriteString(fmt.Sprintf("   重试次数: %d\n\n", result.Retries))
		}
	}

	file.WriteString(fmt.Sprintf("总计 %d 个节点失败\n", failCount))
	file.WriteString("========================================\n")
	file.WriteString(fmt.Sprintf("生成时间: %s\n", time.Now().Format("2006-01-02 15:04:05")))

	return nil
} 