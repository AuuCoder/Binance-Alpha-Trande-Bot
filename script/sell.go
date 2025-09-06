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

// SellNodeConfig 卖出监控节点配置结构体
type SellNodeConfig struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	IP            string  `json:"ip"`
	Csrftoken     string  `json:"csrftoken"`
	Cookie        string  `json:"cookie"`
	TokenAddress  string  `json:"token_address"`
	MonitorAmount float64 `json:"monitor_amount"`
	BaseAsset     string  `json:"base_asset"`
}

// SellMonitorRequest 卖出监控请求结构体
type SellMonitorRequest struct {
	AccountID     string  `json:"account_id"`
	TokenAddress  string  `json:"token_address"`
	MonitorAmount float64 `json:"monitor_amount"`
	Csrftoken     string  `json:"csrftoken"`
	Cookie        string  `json:"cookie"`
	BaseAsset     string  `json:"base_asset"`
}

// SellStopRequest 停止监控请求结构体
type SellStopRequest struct {
	AccountID    string `json:"account_id"`
	TokenAddress string `json:"token_address"`
}

// SellResponse 卖出监控响应结构体
type SellResponse struct {
	Success   bool                   `json:"success"`
	Message   string                 `json:"message"`
	TaskID    string                 `json:"task_id"`
	Results   map[string]interface{} `json:"results"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

// SellTaskResult 卖出任务执行结果
type SellTaskResult struct {
	Node    SellNodeConfig
	Success bool
	Message string
	TaskID  string
	Retries int
}

// MonitorParams 监控参数
type MonitorParams struct {
	TokenAddress  string  `json:"token_address"`
	MonitorAmount float64 `json:"monitor_amount"`
	BaseAsset     string  `json:"base_asset"`
}

func main() {
	fmt.Println("========================================")
	fmt.Println("🤖 Alpha AutoSell 批量监控脚本 v1.0")
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

	// 显示操作选择菜单
	fmt.Println("请选择操作:")
	fmt.Println("1. 批量开始监控")
	fmt.Println("2. 批量停止监控")
	fmt.Println("3. 查看使用说明")

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("请输入选择 (1-3): ")
	choice, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("❌ 读取输入失败: %v\n", err)
		return
	}

	choice = strings.TrimSpace(choice)
	switch choice {
	case "1":
		startBatchMonitoring()
	case "2":
		stopBatchMonitoring()
	case "3":
		printUsage()
	default:
		fmt.Println("❌ 无效选择，请输入 1-3")
	}
}

// startBatchMonitoring 批量开始监控
func startBatchMonitoring() {
	fmt.Println("\n🚀 批量开始监控")
	fmt.Println("========================================")

	// 读取配置文件
	nodes, err := loadSellConfig("sell_config.json")
	if err != nil {
		fmt.Printf("❌ 读取配置文件失败: %v\n", err)
		fmt.Println("💡 请确保 sell_config.json 文件存在且格式正确")
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

	// 获取用户输入的监控参数
	monitorParams, err := getUserMonitorInput()
	if err != nil {
		fmt.Printf("❌ 获取用户输入失败: %v\n", err)
		return
	}

	// 显示监控参数确认
	fmt.Println("📋 监控参数确认:")
	fmt.Printf("   代币地址: %s\n", monitorParams.TokenAddress)
	fmt.Printf("   监控数量: %.6f\n", monitorParams.MonitorAmount)
	fmt.Printf("   基础资产: %s\n", monitorParams.BaseAsset)
	fmt.Println()

	// 应用监控参数到所有节点
	for i := range nodes {
		nodes[i].TokenAddress = monitorParams.TokenAddress
		nodes[i].MonitorAmount = monitorParams.MonitorAmount
		nodes[i].BaseAsset = monitorParams.BaseAsset
	}

	// 确认执行
	if !confirmExecution("确认开始批量监控") {
		fmt.Println("❌ 用户取消执行")
		return
	}

	// 批量发送监控请求（多线程并发）
	fmt.Println("🎯 开始批量发送监控请求（多线程并发）...")
	fmt.Printf("📊 节点总数: %d，最大重试次数: 5\n", len(nodes))
	fmt.Println("========================================")

	// 创建结果通道和等待组
	resultChan := make(chan SellTaskResult, len(nodes))
	var wg sync.WaitGroup

	// 启动协程处理每个节点
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n SellNodeConfig) {
			defer wg.Done()

			fmt.Printf("🚀 [%d/%d] 开始处理节点: %s (%s)\n", index+1, len(nodes), n.Name, n.IP)

			// 构建监控请求
			monitorReq := &SellMonitorRequest{
				AccountID:     n.Name, // 使用 name 字段作为 account_id
				TokenAddress:  n.TokenAddress,
				MonitorAmount: n.MonitorAmount,
				Csrftoken:     n.Csrftoken,
				Cookie:        n.Cookie,
				BaseAsset:     n.BaseAsset,
			}

			// 发送监控请求（带重试）
			result := sendMonitorRequestWithRetry(n, monitorReq, 5)
			resultChan <- result

			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 监控启动成功 (重试%d次) - TaskID: %s\n",
					index+1, len(nodes), n.Name, result.Retries, result.TaskID)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 监控启动失败 (重试%d次): %s\n",
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
	var results []SellTaskResult
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
		err := exportFailedSellNodes(results, "start")
		if err != nil {
			fmt.Printf("⚠️  导出失败记录时出错: %v\n", err)
		} else {
			fmt.Printf("📄 失败记录已导出到 sell_fail.txt\n")
		}
	}

	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量监控启动结果:")
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
		fmt.Printf("⚠️  %d 个节点监控启动失败，详细信息已保存到 sell_fail.txt\n", failCount)
		fmt.Println("💡 请检查失败节点的状态和网络连接")
	} else {
		fmt.Println("🎉 所有节点监控启动成功！")
	}
}

// stopBatchMonitoring 批量停止监控
func stopBatchMonitoring() {
	fmt.Println("\n🛑 批量停止监控")
	fmt.Println("========================================")

	// 读取配置文件
	nodes, err := loadSellConfig("sell_config.json")
	if err != nil {
		fmt.Printf("❌ 读取配置文件失败: %v\n", err)
		fmt.Println("💡 请确保 sell_config.json 文件存在且格式正确")
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

	// 获取用户输入的代币地址
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("请输入要停止监控的代币地址 (token_address): ")
	tokenAddress, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("❌ 读取输入失败: %v\n", err)
		return
	}
	tokenAddress = strings.TrimSpace(tokenAddress)
	if tokenAddress == "" {
		fmt.Println("❌ 代币地址不能为空")
		return
	}

	fmt.Printf("📋 将停止监控代币地址: %s\n", tokenAddress)
	fmt.Println()

	// 应用代币地址到所有节点
	for i := range nodes {
		nodes[i].TokenAddress = tokenAddress
	}

	// 确认执行
	if !confirmExecution("确认停止批量监控") {
		fmt.Println("❌ 用户取消执行")
		return
	}

	// 批量发送停止请求（多线程并发）
	fmt.Println("🎯 开始批量发送停止请求（多线程并发）...")
	fmt.Printf("📊 节点总数: %d，最大重试次数: 3\n", len(nodes))
	fmt.Println("========================================")

	// 创建结果通道和等待组
	resultChan := make(chan SellTaskResult, len(nodes))
	var wg sync.WaitGroup

	// 启动协程处理每个节点
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n SellNodeConfig) {
			defer wg.Done()

			fmt.Printf("🛑 [%d/%d] 开始停止节点: %s (%s)\n", index+1, len(nodes), n.Name, n.IP)

			// 构建停止请求
			stopReq := &SellStopRequest{
				AccountID:    n.Name, // 使用 name 字段作为 account_id
				TokenAddress: n.TokenAddress,
			}

			// 发送停止请求（带重试）
			result := sendStopRequestWithRetry(n, stopReq, 3)
			resultChan <- result

			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 监控停止成功 (重试%d次)\n",
					index+1, len(nodes), n.Name, result.Retries)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 监控停止失败 (重试%d次): %s\n",
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
	var results []SellTaskResult
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
		err := exportFailedSellNodes(results, "stop")
		if err != nil {
			fmt.Printf("⚠️  导出失败记录时出错: %v\n", err)
		} else {
			fmt.Printf("📄 失败记录已导出到 sell_fail.txt\n")
		}
	}

	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量监控停止结果:")
	fmt.Printf("   总节点数: %d\n", len(nodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", failCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(nodes))*100)

	fmt.Println("========================================")

	if failCount > 0 {
		fmt.Printf("⚠️  %d 个节点监控停止失败，详细信息已保存到 sell_fail.txt\n", failCount)
		fmt.Println("💡 请检查失败节点的状态和网络连接")
	} else {
		fmt.Println("🎉 所有节点监控停止成功！")
	}
}

// loadSellConfig 从JSON文件加载卖出监控节点配置
func loadSellConfig(filename string) ([]SellNodeConfig, error) {
	// 获取可执行文件的目录
	execPath, err := os.Executable()
	if err != nil {
		execPath = ""
	}
	execDir := filepath.Dir(execPath)

	// 获取当前工作目录
	workDir, err := os.Getwd()
	if err != nil {
		workDir = ""
	}

	// 尝试多个位置查找配置文件
	searchPaths := []string{
		filepath.Join(execDir, filename),                  // 可执行文件目录（优先）
		filepath.Join(filepath.Dir(os.Args[0]), filename), // 程序启动目录
		filename,                         // 当前目录
		filepath.Join(workDir, filename), // 工作目录（最后）
	}

	var configPath string
	var found bool

	for _, path := range searchPaths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			// 验证配置文件格式是否正确
			if isValidSellConfig(path) {
				configPath = path
				found = true
				fmt.Printf("✅ 找到有效配置文件: %s\n", path)
				break
			} else {
				fmt.Printf("⚠️  跳过格式不匹配的文件: %s\n", path)
			}
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

	// 解析JSON
	var nodes []SellNodeConfig
	if err := json.Unmarshal(data, &nodes); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
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
		if node.BaseAsset == "" {
			nodes[i].BaseAsset = "ALPHA_251" // 默认基础资产
		}
		if node.MonitorAmount <= 0 {
			nodes[i].MonitorAmount = 1.0 // 默认监控数量
		}

		// 输出每个节点的认证信息
		fmt.Printf("   节点 %d (%s - %s):\n", i+1, nodes[i].Name, node.IP)
		fmt.Printf("     代币地址: %s\n", node.TokenAddress)
		fmt.Printf("     监控数量: %.6f\n", nodes[i].MonitorAmount)
		fmt.Printf("     基础资产: %s\n", nodes[i].BaseAsset)
		fmt.Println()
	}

	return nodes, nil
}

// isValidSellConfig 验证配置文件是否是有效的卖出监控配置格式
func isValidSellConfig(filepath string) bool {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		fmt.Printf("⚠️  读取文件失败: %v\n", err)
		return false
	}

	// 尝试解析为卖出监控配置数组
	var nodes []SellNodeConfig
	if err := json.Unmarshal(data, &nodes); err != nil {
		fmt.Printf("⚠️  JSON解析失败: %v\n", err)
		return false
	}

	// 检查是否至少有一个节点
	if len(nodes) == 0 {
		fmt.Printf("⚠️  配置文件中没有节点信息\n")
		return false
	}

	// 验证第一个节点是否包含必要字段
	firstNode := nodes[0]

	// 检查必需字段
	missingFields := []string{}
	if firstNode.IP == "" {
		missingFields = append(missingFields, "ip")
	}
	if firstNode.ID == "" {
		missingFields = append(missingFields, "id")
	}
	if firstNode.Csrftoken == "" {
		missingFields = append(missingFields, "csrftoken")
	}
	if firstNode.Cookie == "" {
		missingFields = append(missingFields, "cookie")
	}

	// 如果缺少必需字段，输出详细信息
	if len(missingFields) > 0 {
		fmt.Printf("⚠️  第一个节点缺少必需字段: %v\n", missingFields)
		fmt.Printf("💡  提示: token_address 字段可以在运行时手动输入\n")
		return false
	}

	// 检查是否包含卖出监控配置特有的字段（更宽松的验证）
	hasSellFields := firstNode.ID != "" && firstNode.Csrftoken != "" && firstNode.Cookie != ""

	if !hasSellFields {
		fmt.Printf("⚠️  配置文件格式不符合卖出监控要求\n")
	}

	return hasSellFields
}

// confirmExecution 确认执行
func confirmExecution(action string) bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("%s? (y/N): ", action)
	confirm, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	confirm = strings.ToLower(strings.TrimSpace(confirm))
	return confirm == "y" || confirm == "yes"
}

// sendMonitorRequestWithRetry 发送监控请求到指定节点（带重试机制）
func sendMonitorRequestWithRetry(node SellNodeConfig, monitorReq *SellMonitorRequest, maxRetries int) SellTaskResult {
	result := SellTaskResult{
		Node:    node,
		Success: false,
		Message: "",
		TaskID:  "",
		Retries: 0,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		result.Retries = attempt

		success, message, taskID := sendSingleMonitorRequest(node, monitorReq)

		if success {
			result.Success = true
			result.Message = message
			result.TaskID = taskID
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

// sendSingleMonitorRequest 发送单次监控请求
func sendSingleMonitorRequest(node SellNodeConfig, monitorReq *SellMonitorRequest) (bool, string, string) {
	// 构建请求URL
	url := fmt.Sprintf("http://%s:8081/monitor", node.IP)

	// 序列化请求体
	requestBody, err := json.Marshal(monitorReq)
	if err != nil {
		return false, fmt.Sprintf("序列化请求参数失败: %v", err), ""
	}

	// 🔧 调试输出
	fmt.Printf("🔍 [调试] 节点 %s 发送的JSON: %s\n", node.Name, string(requestBody))

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return false, fmt.Sprintf("创建HTTP请求失败: %v", err), ""
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
		return false, fmt.Sprintf("发送HTTP请求失败: %v", err), ""
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Sprintf("读取响应失败: %v", err), ""
	}

	fmt.Printf("   HTTP状态码: %d\n", resp.StatusCode)
	fmt.Printf("   响应内容: %s\n", string(body))

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("HTTP错误 %d: %s", resp.StatusCode, string(body)), ""
	}

	// 尝试解析响应JSON
	var sellResp SellResponse
	if err := json.Unmarshal(body, &sellResp); err != nil {
		return true, string(body), ""
	}

	if sellResp.Success {
		return true, fmt.Sprintf("监控启动成功: %s", sellResp.Message), sellResp.TaskID
	} else {
		return false, fmt.Sprintf("监控启动失败: %s", sellResp.Message), ""
	}
}

// sendStopRequestWithRetry 发送停止请求到指定节点（带重试机制）
func sendStopRequestWithRetry(node SellNodeConfig, stopReq *SellStopRequest, maxRetries int) SellTaskResult {
	result := SellTaskResult{
		Node:    node,
		Success: false,
		Message: "",
		TaskID:  "",
		Retries: 0,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		result.Retries = attempt

		success, message := sendSingleStopRequest(node, stopReq)

		if success {
			result.Success = true
			result.Message = message
			return result
		}

		// 如果不是最后一次尝试，等待1秒后重试
		if attempt < maxRetries-1 {
			fmt.Printf("🔄 节点 %s 第%d次停止尝试失败，1秒后重试: %s\n",
				node.Name, attempt+1, message)

			time.Sleep(1 * time.Second)
		} else {
			// 最后一次尝试失败
			result.Message = message
			fmt.Printf("💥 节点 %s 达到最大重试次数(%d)，停止失败: %s\n",
				node.Name, maxRetries, message)
		}
	}

	return result
}

// sendSingleStopRequest 发送单次停止请求
func sendSingleStopRequest(node SellNodeConfig, stopReq *SellStopRequest) (bool, string) {
	// 构建请求URL
	url := fmt.Sprintf("http://%s:8081/stop", node.IP)

	// 序列化请求体
	requestBody, err := json.Marshal(stopReq)
	if err != nil {
		return false, fmt.Sprintf("序列化请求参数失败: %v", err)
	}

	// 🔧 调试输出
	fmt.Printf("🔍 [调试] 节点 %s 发送停止请求: %s\n", node.Name, string(requestBody))

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return false, fmt.Sprintf("创建HTTP请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 15 * time.Second,
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
	var sellResp SellResponse
	if err := json.Unmarshal(body, &sellResp); err != nil {
		return true, string(body)
	}

	if sellResp.Success {
		return true, fmt.Sprintf("监控停止成功: %s", sellResp.Message)
	} else {
		return false, fmt.Sprintf("监控停止失败: %s", sellResp.Message)
	}
}

// exportFailedSellNodes 导出失败的卖出监控节点信息到文件
func exportFailedSellNodes(results []SellTaskResult, operation string) error {
	// 过滤出失败的结果
	var failedResults []SellTaskResult
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
		failFilePath = filepath.Join(execDir, "sell_fail.txt")
	} else {
		// 如果获取可执行文件路径失败，保存在当前目录
		failFilePath = "sell_fail.txt"
	}

	// 创建失败记录文件
	file, err := os.Create(failFilePath)
	if err != nil {
		return fmt.Errorf("创建失败记录文件失败: %v", err)
	}
	defer file.Close()

	fmt.Printf("📄 失败记录将保存到: %s\n", failFilePath)

	// 写入文件头信息
	fmt.Fprintf(file, "Alpha AutoSell 批量监控失败记录\n")
	fmt.Fprintf(file, "生成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(file, "操作类型: %s\n", operation)
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "失败节点详情:\n")
	fmt.Fprintf(file, "========================================\n")

	// 写入失败节点信息
	for i, result := range failedResults {
		fmt.Fprintf(file, "\n[%d] 节点信息:\n", i+1)
		fmt.Fprintf(file, "  ID: %s\n", result.Node.ID)
		fmt.Fprintf(file, "  名称(账户ID): %s\n", result.Node.Name)
		fmt.Fprintf(file, "  IP: %s\n", result.Node.IP)
		fmt.Fprintf(file, "  代币地址: %s\n", result.Node.TokenAddress)
		fmt.Fprintf(file, "  监控数量: %.6f\n", result.Node.MonitorAmount)
		fmt.Fprintf(file, "  基础资产: %s\n", result.Node.BaseAsset)
		fmt.Fprintf(file, "  重试次数: %d\n", result.Retries)
		fmt.Fprintf(file, "  失败原因: %s\n", result.Message)
		fmt.Fprintf(file, "  CSRF Token: %s\n", result.Node.Csrftoken)
		fmt.Fprintf(file, "  Cookie: %s\n", result.Node.Cookie)

		// 生成重试命令
		if operation == "start" {
			fmt.Fprintf(file, "  重试命令 (启动监控):\n")
			fmt.Fprintf(file, "    curl -X POST http://%s:8081/monitor \\\n", result.Node.IP)
			fmt.Fprintf(file, "      -H \"Content-Type: application/json\" \\\n")
			fmt.Fprintf(file, "      -d '{\n")
			fmt.Fprintf(file, "        \"account_id\": \"%s\",\n", result.Node.Name) // 使用 name 作为 account_id
			fmt.Fprintf(file, "        \"token_address\": \"%s\",\n", result.Node.TokenAddress)
			fmt.Fprintf(file, "        \"monitor_amount\": %.6f,\n", result.Node.MonitorAmount)
			fmt.Fprintf(file, "        \"csrftoken\": \"%s\",\n", result.Node.Csrftoken)
			fmt.Fprintf(file, "        \"cookie\": \"%s\",\n", result.Node.Cookie)
			fmt.Fprintf(file, "        \"base_asset\": \"%s\"\n", result.Node.BaseAsset)
			fmt.Fprintf(file, "      }'\n")
		} else if operation == "stop" {
			fmt.Fprintf(file, "  重试命令 (停止监控):\n")
			fmt.Fprintf(file, "    curl -X POST http://%s:8081/stop \\\n", result.Node.IP)
			fmt.Fprintf(file, "      -H \"Content-Type: application/json\" \\\n")
			fmt.Fprintf(file, "      -d '{\n")
			fmt.Fprintf(file, "        \"account_id\": \"%s\",\n", result.Node.Name) // 使用 name 作为 account_id
			fmt.Fprintf(file, "        \"token_address\": \"%s\"\n", result.Node.TokenAddress)
			fmt.Fprintf(file, "      }'\n")
		}

		if i < len(failedResults)-1 {
			fmt.Fprintf(file, "\n----------------------------------------\n")
		}
	}

	// 写入文件尾信息
	fmt.Fprintf(file, "\n========================================\n")
	fmt.Fprintf(file, "总失败节点数: %d\n", len(failedResults))
	fmt.Fprintf(file, "建议检查项:\n")
	fmt.Fprintf(file, "1. 网络连接是否正常\n")
	fmt.Fprintf(file, "2. Alpha AutoSell 服务是否运行在端口 8081\n")
	fmt.Fprintf(file, "3. CSRF Token 和 Cookie 是否有效\n")
	fmt.Fprintf(file, "4. 节点IP地址是否正确\n")
	fmt.Fprintf(file, "5. 防火墙是否阻止了连接\n")
	fmt.Fprintf(file, "6. 代币地址是否正确\n")
	fmt.Fprintf(file, "7. 监控数量是否合理\n")

	return nil
}

// printUsage 打印使用说明
func printUsage() {
	fmt.Println("\n📖 Alpha AutoSell 批量监控脚本使用说明")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Println("🎯 功能说明:")
	fmt.Println("1. 批量开始监控 - 对多个节点同时启动自动卖出监控")
	fmt.Println("2. 批量停止监控 - 对多个节点同时停止自动卖出监控")
	fmt.Println()
	fmt.Println("📋 配置文件要求:")
	fmt.Println("配置文件名: sell_config.json")
	fmt.Println("配置文件格式示例:")
	fmt.Println(`[
  {
    "id": "account-001",
    "name": "测试账户1",
    "ip": "192.168.1.100",
    "csrftoken": "your_csrf_token_here",
    "cookie": "your_cookie_here"
  }
]`)
	fmt.Println()
	fmt.Println("🔧 配置文件字段说明:")
	fmt.Println("  id           - 节点标识ID（必需）")
	fmt.Println("  name         - 账户名称，用作account_id（必需）")
	fmt.Println("  ip           - 节点IP地址（必需）")
	fmt.Println("  csrftoken    - CSRF令牌（必需）")
	fmt.Println("  cookie       - Cookie信息（必需）")
	fmt.Println()
	fmt.Println("💬 运行时输入参数:")
	fmt.Println("  token_address  - 代币合约地址（运行时输入）")
	fmt.Println("  monitor_amount - 监控数量（运行时输入，默认1.0）")
	fmt.Println("  base_asset     - 基础资产（运行时输入，默认ALPHA_251）")
	fmt.Println()
	fmt.Println("⚠️  注意事项:")
	fmt.Println("1. 确保所有节点的 Alpha AutoSell 服务正在运行（端口8081）")
	fmt.Println("2. 确保网络连接正常，防火墙允许访问")
	fmt.Println("3. CSRF Token 和 Cookie 必须有效且未过期")
	fmt.Println("4. 代币地址必须是有效的合约地址")
	fmt.Println("5. 监控数量应该大于0")
	fmt.Println()
	fmt.Println("📁 文件位置:")
	fmt.Println("配置文件会在以下位置按顺序查找:")
	fmt.Println("1. 可执行文件目录")
	fmt.Println("2. 程序启动目录")
	fmt.Println("3. 当前目录")
	fmt.Println("4. 工作目录")
	fmt.Println()
	fmt.Println("📊 执行结果:")
	fmt.Println("- 成功和失败的统计信息会显示在控制台")
	fmt.Println("- 失败的节点详情会保存到 sell_fail.txt 文件")
	fmt.Println("- 失败记录包含重试命令，方便手动调试")
	fmt.Println()
	fmt.Println("🚀 开始使用:")
	fmt.Println("1. 准备好 sell_config.json 配置文件")
	fmt.Println("2. 运行程序并选择相应操作")
	fmt.Println("3. 确认执行后等待批量处理完成")
	fmt.Println("========================================")
}

// getUserMonitorInput 获取用户输入的监控参数
func getUserMonitorInput() (*MonitorParams, error) {
	reader := bufio.NewReader(os.Stdin)
	params := &MonitorParams{}

	// 获取代币地址
	fmt.Print("请输入代币地址 (token_address): ")
	tokenAddress, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	params.TokenAddress = strings.TrimSpace(tokenAddress)
	if params.TokenAddress == "" {
		return nil, fmt.Errorf("代币地址不能为空")
	}

	// 获取监控数量
	fmt.Print("请输入监控数量 (monitor_amount, 默认1.0): ")
	monitorAmountStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	monitorAmountStr = strings.TrimSpace(monitorAmountStr)
	if monitorAmountStr == "" {
		params.MonitorAmount = 1.0 // 默认值
	} else {
		amount, err := strconv.ParseFloat(monitorAmountStr, 64)
		if err != nil {
			fmt.Printf("⚠️  监控数量格式错误，使用默认值: 1.0\n")
			params.MonitorAmount = 1.0
		} else if amount <= 0 {
			fmt.Printf("⚠️  监控数量必须大于0，使用默认值: 1.0\n")
			params.MonitorAmount = 1.0
		} else {
			params.MonitorAmount = amount
		}
	}

	// 获取基础资产
	fmt.Print("请输入基础资产 (base_asset, 默认ALPHA_251): ")
	baseAsset, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	params.BaseAsset = strings.TrimSpace(baseAsset)
	if params.BaseAsset == "" {
		params.BaseAsset = "ALPHA_251" // 默认值
	}

	return params, nil
}
