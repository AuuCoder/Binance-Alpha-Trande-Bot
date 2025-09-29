package main

import (
	"bufio"
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

// TradeRequest 交易请求结构体
type TradeRequest struct {
	TokenAddress   string  `json:"token_address"`
	USDTAmount     float64 `json:"usdt_amount"`
	TargetVolume   float64 `json:"target_volume"`
	BaseAsset      string  `json:"base_asset"`
	ChainID        string  `json:"chain_id"`        // 🔧 链ID支持
	PriceMode      string  `json:"price_mode"`      // 🔧 新增：价格模式
	PricePrecision int     `json:"price_precision"`
	AutoLoop       bool    `json:"auto_loop"`
	Csrftoken      string  `json:"csrftoken"`
	Cookie         string  `json:"cookie"`
}

// TradeResponse 交易响应结构体
type TradeResponse struct {
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
	fmt.Println("🚀 Flash Trade 批量交易脚本 v2.0")
	fmt.Println("========================================")

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
	nodes, err := loadConfig("config.json")
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
	tradeParams, err := getUserInput()
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
	fmt.Printf("   价格模式: %s\n", tradeParams.PriceMode)  // 🔧 新增
	fmt.Printf("   价格精度: %d\n", tradeParams.PricePrecision)
	fmt.Printf("   自动循环: %t\n", tradeParams.AutoLoop)
	fmt.Println()

	// 确认执行
	if !confirmExecution() {
		fmt.Println("❌ 用户取消执行")
		return
	}

	// 批量发送交易请求（多线程并发）
	fmt.Println("🎯 开始批量发送交易请求（多线程并发）...")
	fmt.Printf("📊 节点总数: %d，最大重试次数: 5\n", len(nodes))
	fmt.Println("========================================")

	// 创建结果通道和等待组
	resultChan := make(chan TaskResult, len(nodes))
	var wg sync.WaitGroup

	// 启动协程处理每个节点
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n NodeConfig) {
			defer wg.Done()

			fmt.Printf("🚀 [%d/%d] 开始处理节点: %s (%s)\n", index+1, len(nodes), n.Name, n.IP)

			// 为当前节点设置认证信息
			nodeTradeParams := *tradeParams // 复制参数
			nodeTradeParams.Csrftoken = n.Csrftoken
			nodeTradeParams.Cookie = n.Cookie

			// 发送交易请求（带重试）
			result := sendTradeRequestWithRetry(n, &nodeTradeParams, 5)
			resultChan <- result

			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 交易请求成功 (重试%d次)\n",
					index+1, len(nodes), n.Name, result.Retries)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 交易请求失败 (重试%d次): %s\n",
					index+1, len(nodes), n.Name, result.Retries, result.Message)
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
		err := exportFailedNodes(results, tradeParams)
		if err != nil {
			fmt.Printf("⚠️  导出失败记录时出错: %v\n", err)
		} else {
			fmt.Printf("📄 失败记录已导出到 fail.txt\n")
		}
	}

	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量执行结果:")
	fmt.Printf("   总节点数: %d\n", len(nodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", failCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(nodes))*100)

	// 显示重试统计
	totalRetries := 0
	for _, result := range results {
		totalRetries += result.Retries
	}
	fmt.Printf("   总重试次数: %d\n", totalRetries)
	fmt.Printf("   平均重试次数: %.1f\n", float64(totalRetries)/float64(len(nodes)))

	fmt.Println("========================================")

	if failCount > 0 {
		fmt.Printf("⚠️  %d 个节点执行失败，详细信息已保存到 fail.txt\n", failCount)
		fmt.Println("💡 请检查失败节点的状态和网络连接")
	} else {
		fmt.Println("🎉 所有节点交易请求发送成功！")
	}
}

// loadConfig 从JSON文件加载节点配置
func loadConfig(filename string) ([]NodeConfig, error) {
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

	// 验证配置并输出认证信息
	fmt.Println("🔐 节点认证信息:")
	for i, node := range nodes {
		if node.IP == "" {
			return nil, fmt.Errorf("节点 %d 缺少IP地址", i+1)
		}
		if node.Name == "" {
			nodes[i].Name = fmt.Sprintf("节点-%d", i+1)
		}

		// 输出每个节点的认证信息
		fmt.Printf("   节点 %d (%s - %s):\n", i+1, nodes[i].Name, node.IP)

		fmt.Println()
	}

	return nodes, nil
}

// getUserInput 获取用户输入的交易参数
func getUserInput() (*TradeRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	req := &TradeRequest{}

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

	// 获取基础资产
	fmt.Print("请输入基础资产 (base_asset): ")
	baseAsset, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	req.BaseAsset = strings.TrimSpace(baseAsset)
	if req.BaseAsset == "" {
		return nil, fmt.Errorf("基础资产不能为空")
	}

	// 获取USDT金额
	fmt.Print("请输入USDT金额 (usdt_amount, 默认80): ")
	usdtAmountStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	usdtAmountStr = strings.TrimSpace(usdtAmountStr)
	if usdtAmountStr == "" {
		req.USDTAmount = 80 // 默认值
	} else {
		req.USDTAmount, err = strconv.ParseFloat(usdtAmountStr, 64)
		if err != nil {
			return nil, fmt.Errorf("USDT金额格式错误: %v", err)
		}
	}

	// 获取目标交易量
	fmt.Print("请输入目标交易量 (target_volume, 默认68000): ")
	targetVolumeStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	targetVolumeStr = strings.TrimSpace(targetVolumeStr)
	if targetVolumeStr == "" {
		req.TargetVolume = 68000 // 默认值
	} else {
		req.TargetVolume, err = strconv.ParseFloat(targetVolumeStr, 64)
		if err != nil {
			return nil, fmt.Errorf("目标交易量格式错误: %v", err)
		}
	}

	// 获取链ID
	fmt.Print("请输入链ID (chain_id, 56=BSC/CT_501=Solana, 默认56): ")
	chainID, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	req.ChainID = strings.TrimSpace(chainID)
	if req.ChainID == "" {
		req.ChainID = "56" // 默认值
	}

	// 🔧 新增：获取价格模式
	fmt.Print("请输入价格模式 (price_mode, limit/market/combined/auto, 默认market): ")
	priceMode, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	req.PriceMode = strings.TrimSpace(priceMode)
	if req.PriceMode == "" {
		req.PriceMode = "market" // 默认值，推荐买单使用
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
	precisionStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	precisionStr = strings.TrimSpace(precisionStr)
	if precisionStr == "" {
		req.PricePrecision = 8 // 默认值
	} else {
		req.PricePrecision, err = strconv.Atoi(precisionStr)
		if err != nil {
			return nil, fmt.Errorf("价格精度格式错误: %v", err)
		}
	}

	// 获取自动循环设置
	fmt.Print("是否启用自动循环 (auto_loop, Y/n, 默认Y): ")
	autoLoopStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	autoLoopStr = strings.ToLower(strings.TrimSpace(autoLoopStr))
	if autoLoopStr == "" {
		req.AutoLoop = true // 默认值
	} else {
		req.AutoLoop = autoLoopStr == "y" || autoLoopStr == "yes"
	}

	return req, nil
}

// confirmExecution 确认执行
func confirmExecution() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("确认执行批量交易? (y/N): ")
	confirm, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	confirm = strings.ToLower(strings.TrimSpace(confirm))
	return confirm == "y" || confirm == "yes"
}

// sendTradeRequestWithRetry 发送交易请求到指定节点（带重试机制）
func sendTradeRequestWithRetry(node NodeConfig, tradeParams *TradeRequest, maxRetries int) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "",
		Retries: 0,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		result.Retries = attempt

		success, message := sendSingleTradeRequest(node, tradeParams)

		if success {
			result.Success = true
			result.Message = message
			return result
		}

		// 如果不是最后一次尝试，等待2秒后重试
		if attempt < maxRetries-1 {
			fmt.Printf("🔄 节点 %s 第%d次尝试失败，2秒后重试: %s\n",
				node.Name, attempt+1, message)

			time.Sleep(2 * time.Second)
		} else {
			// 最后一次尝试失败
			result.Message = message
			fmt.Printf("💥 节点 %s 达到最大重试次数(%d)，最终失败: %s\n",
				node.Name, maxRetries, message)
		}
	}

	return result
}

// sendSingleTradeRequest 发送单次交易请求
func sendSingleTradeRequest(node NodeConfig, tradeParams *TradeRequest) (bool, string) {
	// 🔧 手动构建JSON，避免Cookie被转义
	jsonPayload := fmt.Sprintf(`{
        "token_address": "%s",
        "usdt_amount": %f,
        "target_volume": %f,
        "base_asset": "%s",
        "chain_id": "%s",
        "price_mode": "%s",
        "price_precision": %d,
        "auto_loop": %t,
        "csrftoken": "%s",
        "cookie": "%s"
    }`,
		tradeParams.TokenAddress,
		tradeParams.USDTAmount,
		tradeParams.TargetVolume,
		tradeParams.BaseAsset,
		tradeParams.ChainID,
		tradeParams.PriceMode,  // 🔧 新增：价格模式
		tradeParams.PricePrecision,
		tradeParams.AutoLoop,
		tradeParams.Csrftoken,
		tradeParams.Cookie, // 直接插入，不转义
	)

	// 🔧 调试输出
	fmt.Printf("🔍 [调试] 节点 %s 发送的JSON长度: %d\n", node.Name, len(jsonPayload))
	fmt.Printf("   Cookie在JSON中的状态: %s\n", jsonPayload[strings.Index(jsonPayload, `"cookie"`):min(len(jsonPayload), strings.Index(jsonPayload, `"cookie"`)+200)])

	// 构建请求URL
	url := fmt.Sprintf("http://%s:8080/trade", node.IP)

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, strings.NewReader(jsonPayload))
	if err != nil {
		return false, fmt.Sprintf("创建HTTP请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Sprintf("发送HTTP请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Sprintf("读取响应失败: %v", err)
	}

	fmt.Printf("   HTTP状态码: %d\n", resp.StatusCode)
	fmt.Printf("   响应内容: %s\n", string(body))

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("HTTP错误 %d: %s", resp.StatusCode, string(body))
	}

	// 尝试解析响应JSON
	var tradeResp TradeResponse
	if err := json.Unmarshal(body, &tradeResp); err != nil {
		return true, string(body)
	}

	if tradeResp.Success {
		return true, fmt.Sprintf("账户: %s, 状态: %s, 消息: %s",
			tradeResp.AccountID, tradeResp.Status, tradeResp.Message)
	} else {
		return false, fmt.Sprintf("账户: %s, 消息: %s",
			tradeResp.AccountID, tradeResp.Message)
	}
}

// exportFailedNodes 导出失败的节点信息到文件
func exportFailedNodes(results []TaskResult, tradeParams *TradeRequest) error {
	// 过滤出失败的结果
	var failedResults []TaskResult
	for _, result := range results {
		if !result.Success {
			failedResults = append(failedResults, result)
		}
	}

	if len(failedResults) == 0 {
		return nil // 没有失败的节点
	}

	// 确定失败记录文件的保存位置
	var failFilePath string

	// 尝试保存在可执行文件目录
	if execPath, err := os.Executable(); err == nil {
		execDir := filepath.Dir(execPath)
		failFilePath = filepath.Join(execDir, "fail.txt")
	} else {
		// 如果获取可执行文件路径失败，保存在当前目录
		failFilePath = "fail.txt"
	}

	// 创建失败记录文件
	file, err := os.Create(failFilePath)
	if err != nil {
		return fmt.Errorf("创建失败记录文件失败: %v", err)
	}
	defer file.Close()

	fmt.Printf("📄 失败记录将保存到: %s\n", failFilePath)

	// 写入文件头信息
	fmt.Fprintf(file, "Flash Trade 批量交易失败记录\n")
	fmt.Fprintf(file, "生成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(file, "交易参数:\n")
	fmt.Fprintf(file, "  代币地址: %s\n", tradeParams.TokenAddress)
	fmt.Fprintf(file, "  USDT金额: %.2f\n", tradeParams.USDTAmount)
	fmt.Fprintf(file, "  目标交易量: %.2f\n", tradeParams.TargetVolume)
	fmt.Fprintf(file, "  基础资产: %s\n", tradeParams.BaseAsset)
	fmt.Fprintf(file, "  链ID: %s\n", tradeParams.ChainID)
	fmt.Fprintf(file, "  价格精度: %d\n", tradeParams.PricePrecision)
	fmt.Fprintf(file, "  自动循环: %t\n", tradeParams.AutoLoop)
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "失败节点详情:\n")
	fmt.Fprintf(file, "========================================\n")

	// 写入失败节点信息
	for i, result := range failedResults {
		fmt.Fprintf(file, "\n[%d] 节点信息:\n", i+1)
		fmt.Fprintf(file, "  ID: %s\n", result.Node.ID)
		fmt.Fprintf(file, "  名称: %s\n", result.Node.Name)
		fmt.Fprintf(file, "  IP: %s\n", result.Node.IP)
		fmt.Fprintf(file, "  重试次数: %d\n", result.Retries)
		fmt.Fprintf(file, "  失败原因: %s\n", result.Message)
		fmt.Fprintf(file, "  CSRF Token: %s\n", result.Node.Csrftoken)
		fmt.Fprintf(file, "  Cookie: %s\n", result.Node.Cookie)

		// 生成重试命令
		fmt.Fprintf(file, "  重试命令:\n")
		fmt.Fprintf(file, "    curl -X POST http://%s:8080/trade \\\n", result.Node.IP)
		fmt.Fprintf(file, "      -H \"Content-Type: application/json\" \\\n")
		fmt.Fprintf(file, "      -d '{\n")
		fmt.Fprintf(file, "        \"token_address\": \"%s\",\n", tradeParams.TokenAddress)
		fmt.Fprintf(file, "        \"usdt_amount\": %.2f,\n", tradeParams.USDTAmount)
		fmt.Fprintf(file, "        \"target_volume\": %.2f,\n", tradeParams.TargetVolume)
		fmt.Fprintf(file, "        \"base_asset\": \"%s\",\n", tradeParams.BaseAsset)
		fmt.Fprintf(file, "        \"chain_id\": \"%s\",\n", tradeParams.ChainID)
		fmt.Fprintf(file, "        \"price_mode\": \"%s\",\n", tradeParams.PriceMode)  // 🔧 新增
		fmt.Fprintf(file, "        \"price_precision\": %d,\n", tradeParams.PricePrecision)
		fmt.Fprintf(file, "        \"auto_loop\": %t,\n", tradeParams.AutoLoop)
		fmt.Fprintf(file, "        \"csrftoken\": \"%s\",\n", result.Node.Csrftoken)
		fmt.Fprintf(file, "        \"cookie\": \"%s\"\n", result.Node.Cookie)
		fmt.Fprintf(file, "      }'\n")

		if i < len(failedResults)-1 {
			fmt.Fprintf(file, "\n----------------------------------------\n")
		}
	}

	// 写入文件尾信息
	fmt.Fprintf(file, "\n========================================\n")
	fmt.Fprintf(file, "总失败节点数: %d\n", len(failedResults))
	fmt.Fprintf(file, "建议检查项:\n")
	fmt.Fprintf(file, "1. 网络连接是否正常\n")
	fmt.Fprintf(file, "2. Flash Trade 服务是否运行在端口 8080\n")
	fmt.Fprintf(file, "3. CSRF Token 和 Cookie 是否有效\n")
	fmt.Fprintf(file, "4. 节点IP地址是否正确\n")
	fmt.Fprintf(file, "5. 防火墙是否阻止了连接\n")

	return nil
}

// printUsage 打印使用说明
func printUsage() {
	fmt.Println("使用说明:")
	fmt.Println("1. 确保 config.json 文件存在且格式正确")
	fmt.Println("2. 确保所有节点的 Flash Trade 服务正在运行")
	fmt.Println("3. 按提示输入交易参数")
	fmt.Println("4. 确认后将批量发送交易请求")
	fmt.Println()
	fmt.Println("config.json 格式示例:")
	fmt.Println(`[
  {
    "id": "slave-d14083a4",
    "name": "代聚林",
    "ip": "10.9.0.1",
    "csrftoken": "your_csrf_token",
    "cookie": "your_cookie"
  },
  {
    "id": "slave-abc123",
    "name": "李四",
    "ip": "10.9.0.2",
    "csrftoken": "your_csrf_token",
    "cookie": "your_cookie"
  }
]`)
}
