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

// AccountConfig 账户配置结构体
type AccountConfig struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	NodeID   string `json:"node_id"`
	NodeName string `json:"node_name"`
}

// PauseRequest 暂停请求结构体
type PauseRequest struct {
	PauseDuration  int    `json:"pause_duration"`  // 暂停时长（秒），默认300秒
	PricePrecision int    `json:"price_precision"` // 价格精度，默认8位
	CleanupTokens  bool   `json:"cleanup_tokens"`  // 是否清理代币余额
	Csrftoken      string `json:"csrftoken"`
	Cookie         string `json:"cookie"`
}

// PauseResponse 暂停响应结构体
type PauseResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
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
	fmt.Println("🧹 Flash Trade 批量账户管理脚本 v2.0 (Mac版)")
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
		pauseParams, err := getPauseInput()
		if err != nil {
			fmt.Printf("❌ 获取参数失败: %v\n", err)
			return
		}

		// 确认执行
		if !confirmPauseExecute(pauseParams) {
			fmt.Println("❌ 用户取消执行")
			return
		}

		// 执行批量暂停/清理
		executeBatchPause(selectedNodes, pauseParams)

	case "resume":
		// 确认执行
		if !confirmResumeExecute() {
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

// getPauseInput 获取用户输入的暂停参数
func getPauseInput() (PauseRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	var pauseParams PauseRequest

	// 默认值
	pauseParams.PauseDuration = 300     // 默认暂停5分钟
	pauseParams.PricePrecision = 8      // 默认精度8位
	pauseParams.CleanupTokens = true    // 默认清理代币

	fmt.Println("📝 请输入暂停/清理参数 (直接回车使用默认值)")
	fmt.Println("----------------------------------------")

	// 获取暂停时长
	fmt.Print("⏱️ 暂停时长 (秒，默认300): ")
	pauseDurationStr, _ := reader.ReadString('\n')
	pauseDurationStr = strings.TrimSpace(pauseDurationStr)
	if pauseDurationStr != "" {
		pauseDuration, err := strconv.Atoi(pauseDurationStr)
		if err == nil && pauseDuration > 0 {
			pauseParams.PauseDuration = pauseDuration
		}
	}

	// 获取价格精度
	fmt.Print("🔢 价格精度 (默认8): ")
	precisionStr, _ := reader.ReadString('\n')
	precisionStr = strings.TrimSpace(precisionStr)
	if precisionStr != "" {
		precision, err := strconv.Atoi(precisionStr)
		if err == nil && precision > 0 {
			pauseParams.PricePrecision = precision
		}
	}

	// 是否清理代币
	fmt.Print("🧹 是否清理代币余额 (y/n，默认y): ")
	cleanupStr, _ := reader.ReadString('\n')
	cleanupStr = strings.TrimSpace(cleanupStr)
	if strings.ToLower(cleanupStr) == "n" {
		pauseParams.CleanupTokens = false
	}

	fmt.Println("----------------------------------------")
	return pauseParams, nil
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

// confirmPauseExecute 确认暂停执行
func confirmPauseExecute(params PauseRequest) bool {
	reader := bufio.NewReader(os.Stdin)
	
	fmt.Println("\n📋 操作确认:")
	fmt.Println("   模式: 暂停/清理账户")
	fmt.Printf("   暂停时长: %d秒\n", params.PauseDuration)
	fmt.Printf("   价格精度: %d\n", params.PricePrecision)
	fmt.Printf("   清理代币: %v\n", params.CleanupTokens)
	
	fmt.Print("\n⚠️ 确认执行以上操作? (y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	return strings.ToLower(confirm) == "y" || confirm == ""
}

// confirmResumeExecute 确认恢复执行
func confirmResumeExecute() bool {
	reader := bufio.NewReader(os.Stdin)
	
	fmt.Println("\n📋 操作确认:")
	fmt.Println("   模式: 恢复账户")
	fmt.Println("   操作: 恢复所有被暂停的账户")
	
	fmt.Print("\n⚠️ 确认执行以上操作? (y/n): ")
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	return strings.ToLower(confirm) == "y" || confirm == ""
}

// executeBatchPause 执行批量暂停
func executeBatchPause(nodes []NodeConfig, params PauseRequest) {
	fmt.Println("🚀 开始执行批量暂停...")
	fmt.Printf("📊 节点总数: %d\n", len(nodes))
	fmt.Println("========================================")
	
	results := make([]TaskResult, 0, len(nodes))
	
	for i, n := range nodes {
		fmt.Printf("🔄 [%d/%d] 开始处理节点: %s (%s)\n", i+1, len(nodes), n.Name, n.IP)
			
		// 获取节点上的账户
			accounts, err := getNodeAccounts(n)
			if err != nil {
			fmt.Printf("❌ [%d/%d] 节点 %s 获取账户列表失败: %v\n", i+1, len(nodes), n.Name, err)
			results = append(results, TaskResult{
					Node:    n,
					Success: false,
					Message: fmt.Sprintf("获取账户列表失败: %v", err),
			})
			continue
		}
		
		fmt.Printf("📋 [%d/%d] 节点 %s 发现 %d 个账户\n", i+1, len(nodes), n.Name, len(accounts))
		
		// 如果没有账户，跳过
			if len(accounts) == 0 {
			fmt.Printf("⚠️ [%d/%d] 节点 %s 没有账户，跳过\n", i+1, len(nodes), n.Name)
			results = append(results, TaskResult{
					Node:    n,
				Success: false,
				Message: "节点没有账户",
			})
			continue
		}
		
		// 创建节点级别的暂停参数
			nodePauseParams := params
			nodePauseParams.Csrftoken = n.Csrftoken
			nodePauseParams.Cookie = n.Cookie
			
		// 成功暂停的账户数量
		successCount := 0
		
		// 对每个账户单独发送暂停请求
		for _, account := range accounts {
			fmt.Printf("🔄 [%d/%d] 暂停账户: %s\n", i+1, len(nodes), account.ID)
			
			// 执行单个账户的暂停
			accountResult := executePauseAccount(n, account.ID, nodePauseParams)
			
			if accountResult.Success {
				successCount++
				fmt.Printf("✅ [%d/%d] 账户 %s 暂停成功\n", i+1, len(nodes), account.ID)
			} else {
				fmt.Printf("❌ [%d/%d] 账户 %s 暂停失败: %s\n", i+1, len(nodes), account.ID, accountResult.Message)
			}
	}
	
		// 添加节点结果
		results = append(results, TaskResult{
			Node:    n,
			Success: successCount > 0,
			Message: fmt.Sprintf("成功暂停 %d/%d 个账户", successCount, len(accounts)),
		})
		
		fmt.Printf("📊 [%d/%d] 节点 %s 暂停结果: %d/%d 成功\n", i+1, len(nodes), n.Name, successCount, len(accounts))
	}
	
	fmt.Println("========================================")
	fmt.Println("📊 批量暂停结果:")
	
	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}
	
	fmt.Printf("   总节点数: %d\n", len(nodes))
	fmt.Printf("   成功: %d\n", successCount)
	fmt.Printf("   失败: %d\n", len(nodes)-successCount)
	fmt.Printf("   成功率: %.1f%%\n", float64(successCount)/float64(len(nodes))*100)
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
			
			// 获取节点上的账户列表
			accounts, err := getNodeAccounts(n)
			if err != nil {
				result := TaskResult{
					Node:    n,
					Success: false,
					Message: fmt.Sprintf("获取账户列表失败: %v", err),
				}
				resultChan <- result
				fmt.Printf("❌ [%d/%d] 节点 %s 获取账户列表失败: %s\n", index+1, len(nodes), n.Name, err)
				return
			}
			
			if len(accounts) == 0 {
				result := TaskResult{
					Node:    n,
					Success: true,
					Message: "节点没有账户，跳过",
				}
				resultChan <- result
				fmt.Printf("ℹ️ [%d/%d] 节点 %s 没有账户，跳过\n", index+1, len(nodes), n.Name)
				return
			}
			
			fmt.Printf("📋 [%d/%d] 节点 %s 发现 %d 个账户\n", index+1, len(nodes), n.Name, len(accounts))
			
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

// getNodeAccounts 获取节点上的账户列表
func getNodeAccounts(node NodeConfig) ([]AccountConfig, error) {
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
	
	// 构建请求URL
	url := fmt.Sprintf("%s/accounts", nodeIP)
	
	// 创建HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
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
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()
	
	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}
	
	// 打印原始响应，用于调试
	fmt.Printf("🔍 节点 %s 原始响应: %s\n", node.Name, string(body))
	
	// 尝试解析响应
	var result struct {
		Success  bool                   `json:"success"`
		Accounts map[string]interface{} `json:"accounts"`
	}
	
	// 先尝试解析为map格式的accounts
	if err := json.Unmarshal(body, &result); err != nil || !result.Success {
		// 如果解析失败，尝试解析为数组格式
		var arrayResult struct {
		Success  bool            `json:"success"`
		Accounts []AccountConfig `json:"accounts"`
	}
		
		if err := json.Unmarshal(body, &arrayResult); err != nil || !arrayResult.Success {
			// 如果还是失败，尝试直接从body中提取accounts字段
			var rawData map[string]interface{}
			if err := json.Unmarshal(body, &rawData); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}
	
			// 检查是否存在accounts字段
			accountsRaw, exists := rawData["accounts"]
			if !exists {
				return nil, fmt.Errorf("响应中不存在accounts字段")
			}
			
			// 根据accounts字段的类型进行处理
			switch accounts := accountsRaw.(type) {
			case map[string]interface{}:
				// 如果是map格式，转换为AccountConfig数组
				var accountList []AccountConfig
				for _, value := range accounts {
					if accountMap, ok := value.(map[string]interface{}); ok {
						account := AccountConfig{
							ID:     accountMap["account_id"].(string),
							Name:   accountMap["account_key"].(string),
							Status: "active", // 默认状态
						}
						accountList = append(accountList, account)
					}
				}
				return accountList, nil
				
			case []interface{}:
				// 如果是数组格式，转换为AccountConfig数组
				var accountList []AccountConfig
				for _, item := range accounts {
					if accountMap, ok := item.(map[string]interface{}); ok {
						account := AccountConfig{
							ID:     accountMap["id"].(string),
							Name:   accountMap["name"].(string),
							Status: accountMap["status"].(string),
						}
						accountList = append(accountList, account)
					}
				}
				return accountList, nil
				
			default:
				return nil, fmt.Errorf("无法识别的accounts格式")
			}
		}
		
		// 如果成功解析为数组格式，直接返回
		return arrayResult.Accounts, nil
	}
	
	// 如果成功解析为map格式，转换为AccountConfig数组
	var accountList []AccountConfig
	for key, value := range result.Accounts {
		if accountMap, ok := value.(map[string]interface{}); ok {
			accountID, _ := accountMap["account_id"].(string)
			accountKey, _ := accountMap["account_key"].(string)
			
			if accountID == "" {
				// 尝试使用其他可能的字段名
				accountID, _ = accountMap["id"].(string)
	}
	
			if accountKey == "" {
				// 尝试使用其他可能的字段名
				accountKey, _ = accountMap["name"].(string)
			}
			
			if accountID == "" {
				// 如果仍然没有ID，使用map的key作为ID
				accountID = key // 这里使用了key变量
				// 如果key是类似"account_0"的格式，尝试提取真实ID
				if strings.HasPrefix(key, "account_") {
					if accountMap["account_id"] != nil {
						accountID = fmt.Sprintf("%v", accountMap["account_id"])
					}
				}
			}
			
			account := AccountConfig{
				ID:     accountID,
				Name:   accountKey,
				Status: "active", // 默认状态
			}
			accountList = append(accountList, account)
		}
	}
	
	return accountList, nil
}

// executePause 执行单个节点的暂停
func executePause(node NodeConfig, params PauseRequest) TaskResult {
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
	
	// 构建请求URL - 修改为正确的API端点
	url := fmt.Sprintf("%s/pause-status", nodeIP)
	
	// 构建请求体 - 修改为符合API预期的格式
	pauseRequest := struct {
		AccountID string `json:"account_id"`
		Duration  int    `json:"duration"`
		Reason    string `json:"reason"`
	}{
		AccountID: "", // 空字符串表示所有账号
		Duration:  params.PauseDuration,
		Reason:    "batch_pause", // 批量暂停原因
	}
	
	reqBody, err := json.Marshal(pauseRequest)
	if err != nil {
		result.Message = fmt.Sprintf("序列化请求失败: %v", err)
		return result
	}
	
	// 打印请求信息
	fmt.Printf("🔄 发送暂停请求到 %s\n", url)
	fmt.Printf("📝 请求体: %s\n", string(reqBody))
	
	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		result.Message = fmt.Sprintf("创建请求失败: %v", err)
		return result
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
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
	
	// 打印原始响应
	fmt.Printf("📄 原始响应: %s\n", string(body))
	
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		result.Message = fmt.Sprintf("服务器返回错误状态码: %d %s", resp.StatusCode, resp.Status)
		return result
	}
	
	// 尝试解析响应
	var pauseResp PauseResponse
	
	// 清理响应内容，移除可能的前缀和后缀干扰字符
	cleanBody := bytes.TrimSpace(body)
	
	// 查找第一个 '{' 和最后一个 '}'
	start := bytes.IndexByte(cleanBody, '{')
	end := bytes.LastIndexByte(cleanBody, '}')
	
	if start >= 0 && end > start {
		// 提取有效的JSON部分
		validJSON := cleanBody[start : end+1]
		fmt.Printf("🔍 提取的JSON: %s\n", string(validJSON))
		
		if err := json.Unmarshal(validJSON, &pauseResp); err != nil {
			// 如果解析失败，尝试更宽松的方式解析
			var respMap map[string]interface{}
			if err := json.Unmarshal(validJSON, &respMap); err != nil {
		result.Message = fmt.Sprintf("解析响应失败: %v", err)
				return result
			}
			
			// 从map中提取success和message
			if success, ok := respMap["success"].(bool); ok {
				pauseResp.Success = success
			}
			
			if message, ok := respMap["message"].(string); ok {
				pauseResp.Message = message
			} else {
				// 尝试将整个响应转换为字符串作为消息
				pauseResp.Message = fmt.Sprintf("%v", respMap)
			}
		}
	} else {
		result.Message = "响应格式无效，未找到有效的JSON"
		return result
	}
	
	if pauseResp.Success {
		result.Success = true
		result.Message = pauseResp.Message
	} else {
		result.Message = pauseResp.Message
	}
	
	return result
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
	
	// 构建请求URL - 使用正确的API端点
	url := fmt.Sprintf("%s/pause-status", nodeIP)
	
	// 构建请求体
	resumeRequest := struct {
		AccountID string `json:"account_id"`
	}{
		AccountID: "", // 空字符串表示所有账号
	}
	
	// 打印请求信息
	fmt.Printf("🔄 发送恢复请求到 %s\n", url)
	
	// 创建HTTP请求 - 使用DELETE方法恢复/取消暂停
	reqBody, _ := json.Marshal(resumeRequest)
	req, err := http.NewRequest("DELETE", url, bytes.NewBuffer(reqBody))
	if err != nil {
		result.Message = fmt.Sprintf("创建请求失败: %v", err)
		return result
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
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
	
	// 打印原始响应
	fmt.Printf("📄 原始响应: %s\n", string(body))
	
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		result.Message = fmt.Sprintf("服务器返回错误状态码: %d %s", resp.StatusCode, resp.Status)
		return result
	}
	
	// 尝试解析响应
	var resumeResp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	
	// 清理响应内容，移除可能的前缀和后缀干扰字符
	cleanBody := bytes.TrimSpace(body)
	
	// 查找第一个 '{' 和最后一个 '}'
	start := bytes.IndexByte(cleanBody, '{')
	end := bytes.LastIndexByte(cleanBody, '}')
	
	if start >= 0 && end > start {
		// 提取有效的JSON部分
		validJSON := cleanBody[start : end+1]
		fmt.Printf("🔍 提取的JSON: %s\n", string(validJSON))
		
		if err := json.Unmarshal(validJSON, &resumeResp); err != nil {
			// 如果解析失败，尝试更宽松的方式解析
			var respMap map[string]interface{}
			if err := json.Unmarshal(validJSON, &respMap); err != nil {
				result.Message = fmt.Sprintf("解析响应失败: %v", err)
				return result
			}
			
			// 从map中提取success和message
			if success, ok := respMap["success"].(bool); ok {
				resumeResp.Success = success
			}
			
			if message, ok := respMap["message"].(string); ok {
				resumeResp.Message = message
			} else {
				// 尝试将整个响应转换为字符串作为消息
				resumeResp.Message = fmt.Sprintf("%v", respMap)
			}
		}
	} else {
		result.Message = "响应格式无效，未找到有效的JSON"
		return result
	}
	
	if resumeResp.Success {
		result.Success = true
		result.Message = resumeResp.Message
	} else {
		result.Message = resumeResp.Message
	}
	
	return result
} 

// executePauseAccount 执行单个账户的暂停
func executePauseAccount(node NodeConfig, accountID string, params PauseRequest) TaskResult {
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
	
	// 构建请求URL - 修改为正确的API端点
	url := fmt.Sprintf("%s/pause-status", nodeIP)
	
	// 构建请求体 - 修改为符合API预期的格式
	pauseRequest := struct {
		AccountID string `json:"account_id"`
		Duration  int    `json:"duration"`
		Reason    string `json:"reason"`
	}{
		AccountID: accountID, // 使用具体的账户ID
		Duration:  params.PauseDuration,
		Reason:    "batch_pause", // 批量暂停原因
	}
	
	reqBody, err := json.Marshal(pauseRequest)
	if err != nil {
		result.Message = fmt.Sprintf("序列化请求失败: %v", err)
		return result
	}
	
	// 打印请求信息
	fmt.Printf("🔄 发送账户暂停请求到 %s\n", url)
	fmt.Printf("📝 请求体: %s\n", string(reqBody))
	
	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		result.Message = fmt.Sprintf("创建请求失败: %v", err)
		return result
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
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
	
	// 打印原始响应
	fmt.Printf("📄 账户 %s 暂停原始响应: %s\n", accountID, string(body))
	
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		result.Message = fmt.Sprintf("服务器返回错误状态码: %d %s", resp.StatusCode, resp.Status)
		return result
	}
	
	// 尝试解析响应
	var pauseResp PauseResponse
	
	// 清理响应内容，移除可能的前缀和后缀干扰字符
	cleanBody := bytes.TrimSpace(body)
	
	// 查找第一个 '{' 和最后一个 '}'
	start := bytes.IndexByte(cleanBody, '{')
	end := bytes.LastIndexByte(cleanBody, '}')
	
	if start >= 0 && end > start {
		// 提取有效的JSON部分
		validJSON := cleanBody[start : end+1]
		fmt.Printf("🔍 账户 %s 提取的JSON: %s\n", accountID, string(validJSON))
		
		if err := json.Unmarshal(validJSON, &pauseResp); err != nil {
			// 如果解析失败，尝试更宽松的方式解析
			var respMap map[string]interface{}
			if err := json.Unmarshal(validJSON, &respMap); err != nil {
		result.Message = fmt.Sprintf("解析响应失败: %v", err)
		return result
	}
	
			// 从map中提取success和message
			if success, ok := respMap["success"].(bool); ok {
				pauseResp.Success = success
			}
			
			if message, ok := respMap["message"].(string); ok {
				pauseResp.Message = message
			} else {
				// 尝试将整个响应转换为字符串作为消息
				pauseResp.Message = fmt.Sprintf("%v", respMap)
			}
		}
	} else {
		result.Message = "响应格式无效，未找到有效的JSON"
		return result
	}
	
	if pauseResp.Success {
		result.Success = true
		result.Message = pauseResp.Message
	} else {
		result.Message = pauseResp.Message
	}
	
	return result
} 