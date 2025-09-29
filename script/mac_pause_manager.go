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

// CleanupRequest 清理请求结构体
type CleanupRequest struct {
	AccountID      string `json:"account_id"`
	TokenAddress   string `json:"token_address,omitempty"`   // 可选，指定要清理的代币地址
	BaseAsset      string `json:"base_asset,omitempty"`      // 可选，指定要清理的基础资产
	PauseDuration  int    `json:"pause_duration"`            // 暂停时长（秒），默认300秒
	PricePrecision int    `json:"price_precision,omitempty"` // 价格精度，默认8位
	Csrftoken      string `json:"csrftoken"`
	Cookie         string `json:"cookie"`
}

// CleanupResponse 清理响应结构体
type CleanupResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Results map[string]interface{} `json:"results"`
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
	fmt.Println("🧹 Flash Trade 账户管理脚本 v2.0 (Mac版)")
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

	// 选择操作模式
	mode := selectMode()
	if mode == "" {
		fmt.Println("❌ 未选择操作模式，程序退出")
		return
	}

	// 选择要执行的节点
	selectedNodes := selectNodeList(nodes)
	if len(selectedNodes) == 0 {
		fmt.Println("❌ 未选择任何节点，程序退出")
		return
	}

	// 根据模式执行不同操作
	switch mode {
	case "pause":
		// 获取暂停/清理参数
		cleanupParams, err := getUserInput()
		if err != nil {
			fmt.Printf("❌ 获取参数失败: %v\n", err)
			return
		}

		// 确认执行
		if !confirmExecute(mode, cleanupParams) {
			fmt.Println("❌ 用户取消执行")
			return
		}

		// 执行批量暂停/清理
		executeBatchCleanup(selectedNodes, cleanupParams)

	case "resume":
		// 确认执行
		if !confirmResumeExecute(mode) {
			fmt.Println("❌ 用户取消执行")
			return
		}

		// 执行批量恢复
		executeBatchResume(selectedNodes)
	}

	fmt.Println("\n🏁 操作执行完毕")
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
	configData, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	// 尝试两种格式解析JSON
	var nodes []NodeConfig
	
	// 首先尝试直接解析为节点数组
	err = json.Unmarshal(configData, &nodes)
	if err == nil && len(nodes) > 0 {
		fmt.Println("✅ 使用数组格式配置文件")
	} else {
		// 如果失败，尝试解析为包含nodes字段的对象
		var config struct {
			Nodes []NodeConfig `json:"nodes"`
		}
		
		if err := json.Unmarshal(configData, &config); err != nil {
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

// selectMode 选择操作模式
func selectMode() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("🔍 请选择操作模式:")
	fmt.Println("   1. 暂停/清理账户 (pause)")
	fmt.Println("   2. 恢复账户 (resume)")
	fmt.Print("请输入选择 [1]: ")
	
	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)
	
	if choice == "" || choice == "1" {
		fmt.Println("✅ 已选择: 暂停/清理账户模式")
		return "pause"
	}
	
	if choice == "2" {
		fmt.Println("✅ 已选择: 恢复账户模式")
		return "resume"
	}
	
	fmt.Println("❌ 无效的选择")
	return ""
}

// getUserInput 获取用户输入的清理参数
func getUserInput() (CleanupRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	var cleanupParams CleanupRequest

	// 默认值
	cleanupParams.PauseDuration = 300     // 默认暂停5分钟
	cleanupParams.PricePrecision = 8      // 默认精度8位

	fmt.Println("📝 请输入清理参数 (直接回车使用默认值)")
	fmt.Println("----------------------------------------")

	// 获取账户ID（必填）
	fmt.Print("🆔 账户ID (必填): ")
	accountID, _ := reader.ReadString('\n')
	cleanupParams.AccountID = strings.TrimSpace(accountID)
	
	if cleanupParams.AccountID == "" {
		fmt.Println("❌ 错误: 账户ID不能为空")
		return cleanupParams, fmt.Errorf("账户ID不能为空")
	}

	// 获取代币地址（可选）
	fmt.Print("🔑 代币地址 (可选): ")
	tokenAddress, _ := reader.ReadString('\n')
	cleanupParams.TokenAddress = strings.TrimSpace(tokenAddress)

	// 如果提供了代币地址，询问基础资产
	if cleanupParams.TokenAddress != "" {
		fmt.Print("💱 基础资产 (可选，如不提供将自动推断): ")
		baseAsset, _ := reader.ReadString('\n')
		cleanupParams.BaseAsset = strings.TrimSpace(baseAsset)
	}

	// 获取暂停时长
	fmt.Print("⏱️ 暂停时长 (秒，默认300): ")
	pauseDurationStr, _ := reader.ReadString('\n')
	pauseDurationStr = strings.TrimSpace(pauseDurationStr)
	if pauseDurationStr != "" {
		pauseDuration, err := strconv.Atoi(pauseDurationStr)
		if err == nil && pauseDuration > 0 {
			cleanupParams.PauseDuration = pauseDuration
		}
	}

	// 如果提供了代币地址，询问价格精度
	if cleanupParams.TokenAddress != "" {
		fmt.Print("🔢 价格精度 (默认8): ")
		precisionStr, _ := reader.ReadString('\n')
		precisionStr = strings.TrimSpace(precisionStr)
		if precisionStr != "" {
			precision, err := strconv.Atoi(precisionStr)
			if err == nil && precision > 0 {
				cleanupParams.PricePrecision = precision
			}
		}
	}

	fmt.Println("----------------------------------------")
	return cleanupParams, nil
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
func confirmExecute(mode string, params CleanupRequest) bool {
	reader := bufio.NewReader(os.Stdin)
	
	fmt.Println("\n📋 操作确认:")
	fmt.Printf("   模式: %s\n", mode)
	if params.TokenAddress != "" {
		fmt.Printf("   代币地址: %s\n", params.TokenAddress)
		if params.BaseAsset != "" {
			fmt.Printf("   基础资产: %s\n", params.BaseAsset)
		}
		fmt.Printf("   价格精度: %d\n", params.PricePrecision)
	}
	fmt.Printf("   暂停时长: %d秒\n", params.PauseDuration)
	
	fmt.Print("\n⚠️ 确认执行以上操作? (y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	return strings.ToLower(confirm) == "y" || confirm == ""
}

// confirmResumeExecute 确认恢复执行
func confirmResumeExecute(mode string) bool {
	reader := bufio.NewReader(os.Stdin)
	
	fmt.Println("\n📋 操作确认:")
	fmt.Printf("   模式: %s\n", mode)
	fmt.Printf("   操作: 恢复所有被暂停的账户\n")
	
	fmt.Print("\n⚠️ 确认执行以上操作? (y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	return strings.ToLower(confirm) == "y" || confirm == ""
}

// executeBatchCleanup 执行批量暂停/清理
func executeBatchCleanup(nodes []NodeConfig, params CleanupRequest) {
	fmt.Println("\n🚀 开始执行批量暂停/清理...")
	fmt.Printf("📊 节点总数: %d\n", len(nodes))
	fmt.Println("========================================")
	
	// 创建结果通道和等待组
	resultChan := make(chan TaskResult, len(nodes))
	var wg sync.WaitGroup
	
	// 启动协程处理每个节点
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n NodeConfig) {
			defer wg.Done()
			
			fmt.Printf("🔄 [%d/%d] 开始处理节点: %s (%s)\n", index+1, len(nodes), n.Name, n.IP)
			
			// 为当前节点设置认证信息
			nodeParams := params
			nodeParams.Csrftoken = n.Csrftoken
			nodeParams.Cookie = n.Cookie
			
			// 执行暂停/清理操作
			result := executeCleanup(n, nodeParams)
			resultChan <- result
			
			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 暂停/清理成功\n", index+1, len(nodes), n.Name)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 暂停/清理失败: %s\n", index+1, len(nodes), n.Name, result.Message)
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
	
	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量暂停/清理结果:")
	fmt.Printf("   总节点数: %d\n", len(nodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", failCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(nodes))*100)
}

// executeCleanup 执行单个节点的暂停/清理
func executeCleanup(node NodeConfig, params CleanupRequest) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "",
	}
	
	// 确保URL包含协议前缀
	nodeIP := node.IP
	if !strings.HasPrefix(nodeIP, "http://") && !strings.HasPrefix(nodeIP, "https://") {
		// 检查是否已经包含端口
		if !strings.Contains(nodeIP, ":") {
			// 如果没有端口，添加默认端口8080
			nodeIP = "http://" + nodeIP + ":8080"
		} else {
			nodeIP = "http://" + nodeIP
		}
	}
	
	// 构建请求URL - 修改为全局暂停端点
	url := fmt.Sprintf("%s/pause-all", nodeIP)
	
	// 构建请求体 - 只包含必要的参数
	pauseParams := struct {
		PauseDuration  int    `json:"pause_duration"`
		PricePrecision int    `json:"price_precision,omitempty"`
		Csrftoken      string `json:"csrftoken"`
		Cookie         string `json:"cookie"`
	}{
		PauseDuration:  params.PauseDuration,
		PricePrecision: params.PricePrecision,
		Csrftoken:      params.Csrftoken,
		Cookie:         params.Cookie,
	}
	
	reqBody, err := json.Marshal(pauseParams)
	if err != nil {
		result.Message = fmt.Sprintf("序列化请求失败: %v", err)
		return result
	}
	
	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		result.Message = fmt.Sprintf("创建请求失败: %v", err)
		return result
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	if params.Csrftoken != "" {
		req.Header.Set("Csrftoken", params.Csrftoken)
	}
	if params.Cookie != "" {
		req.Header.Set("Cookie", params.Cookie)
	}
	
	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		result.Message = fmt.Sprintf("发送请求失败: %v", err)
		return result
	}
	defer resp.Body.Close()
	
	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		result.Message = fmt.Sprintf("读取响应失败: %v", err)
		return result
	}
	
	// 解析响应
	var cleanupResp CleanupResponse
	if err := json.Unmarshal(body, &cleanupResp); err != nil {
		result.Message = fmt.Sprintf("解析响应失败: %v", err)
		return result
	}
	
	if cleanupResp.Success {
		result.Success = true
		result.Message = cleanupResp.Message
	} else {
		result.Message = cleanupResp.Message
	}
	
	return result
}

// executeBatchResume 执行批量恢复
func executeBatchResume(nodes []NodeConfig) {
	fmt.Println("\n🚀 开始执行批量恢复...")
	fmt.Printf("📊 节点总数: %d\n", len(nodes))
	fmt.Println("========================================")
	
	// 创建结果通道和等待组
	resultChan := make(chan TaskResult, len(nodes))
	var wg sync.WaitGroup
	
	// 启动协程处理每个节点
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n NodeConfig) {
			defer wg.Done()
			
			fmt.Printf("🔄 [%d/%d] 开始处理节点: %s (%s)\n", index+1, len(nodes), n.Name, n.IP)
			
			// 执行恢复操作
			result := executeResume(n)
			resultChan <- result
			
			if result.Success {
				fmt.Printf("✅ [%d/%d] 节点 %s 恢复成功\n", index+1, len(nodes), n.Name)
			} else {
				fmt.Printf("❌ [%d/%d] 节点 %s 恢复失败: %s\n", index+1, len(nodes), n.Name, result.Message)
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
	
	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 批量恢复结果:")
	fmt.Printf("   总节点数: %d\n", len(nodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", failCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(nodes))*100)
}

// executeResume 执行单个节点的恢复
func executeResume(node NodeConfig) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "",
	}
	
	// 确保URL包含协议前缀
	nodeIP := node.IP
	if !strings.HasPrefix(nodeIP, "http://") && !strings.HasPrefix(nodeIP, "https://") {
		// 检查是否已经包含端口
		if !strings.Contains(nodeIP, ":") {
			// 如果没有端口，添加默认端口8080
			nodeIP = "http://" + nodeIP + ":8080"
		} else {
			nodeIP = "http://" + nodeIP
		}
	}
	
	// 构建请求URL - 修改为全局恢复端点
	url := fmt.Sprintf("%s/resume-all", nodeIP)
	
	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		result.Message = fmt.Sprintf("创建请求失败: %v", err)
		return result
	}
	
	// 添加认证信息
	if node.Csrftoken != "" {
		req.Header.Set("Csrftoken", node.Csrftoken)
	}
	if node.Cookie != "" {
		req.Header.Set("Cookie", node.Cookie)
	}
	
	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		result.Message = fmt.Sprintf("发送请求失败: %v", err)
		return result
	}
	defer resp.Body.Close()
	
	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		result.Message = fmt.Sprintf("读取响应失败: %v", err)
		return result
	}
	
	// 解析响应
	var response struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		result.Message = fmt.Sprintf("解析响应失败: %v", err)
		return result
	}
	
	if response.Success {
		result.Success = true
		result.Message = response.Message
	} else {
		result.Message = response.Message
	}
	
	return result
} 