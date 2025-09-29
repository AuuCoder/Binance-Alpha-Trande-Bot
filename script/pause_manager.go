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
	fmt.Println("🧹 Flash Trade 账户管理脚本 v1.0")
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

	// 选择操作模式
	mode := selectMode()
	if mode == "" {
		fmt.Println("❌ 未选择操作模式，程序退出")
		return
	}

	// 选择要执行的节点
	selectedNodes := selectNodes(nodes)
	if len(selectedNodes) == 0 {
		fmt.Println("❌ 未选择任何节点，程序退出")
		return
	}

	var results []TaskResult

	if mode == "pause" {
		// 获取用户输入的清理参数
		cleanupParams, err := getUserInput()
		if err != nil {
			fmt.Printf("❌ 获取用户输入失败: %v\n", err)
			return
		}

		// 执行批量暂停与清理任务
		results = executeBatchCleanup(selectedNodes, cleanupParams)
	} else if mode == "resume" {
		// 执行批量恢复任务
		results = executeBatchResume(selectedNodes)
	}

	// 显示执行结果
	fmt.Println("\n========================================")
	fmt.Println("📊 执行结果统计")
	fmt.Println("========================================")

	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}

	fmt.Printf("✅ 成功: %d\n", successCount)
	fmt.Printf("❌ 失败: %d\n", len(results)-successCount)
	fmt.Println()

	// 显示详细结果
	fmt.Println("📋 详细结果:")
	for i, result := range results {
		statusIcon := "❌"
		if result.Success {
			statusIcon = "✅"
		}
		fmt.Printf("%s [%d] %s (%s): %s\n", statusIcon, i+1, result.Node.Name, result.Node.ID, result.Message)
	}

	fmt.Println("\n🏁 批量操作任务执行完毕")
	fmt.Println("按任意键退出...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

// selectMode 选择操作模式
func selectMode() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("🔍 请选择操作模式:")
	fmt.Println("   1. 暂停账户并清理 (Pause & Cleanup)")
	fmt.Println("   2. 恢复账户 (Resume)")
	fmt.Print("请输入选择 [1]: ")
	
	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)
	
	if choice == "" || choice == "1" {
		fmt.Println("✅ 已选择: 暂停账户并清理")
		return "pause"
	}
	
	if choice == "2" {
		fmt.Println("✅ 已选择: 恢复账户")
		return "resume"
	}
	
	fmt.Println("⚠️ 无效的选择，默认使用暂停模式")
	return "pause"
}

// loadConfig 从配置文件加载节点信息
func loadConfig(configFile string) ([]NodeConfig, error) {
	// 尝试在多个位置查找配置文件
	searchPaths := []string{
		configFile,
		filepath.Join(".", configFile),
		filepath.Join("..", configFile),
		filepath.Join(filepath.Dir(os.Args[0]), configFile),
	}

	var configData []byte
	var err error

	for _, path := range searchPaths {
		configData, err = ioutil.ReadFile(path)
		if err == nil {
			fmt.Printf("📄 使用配置文件: %s\n", path)
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("无法找到或读取配置文件: %v", err)
	}

	// 尝试直接解析为节点数组
	var nodes []NodeConfig
	if err := json.Unmarshal(configData, &nodes); err != nil {
		// 如果直接解析失败，尝试解析为包含nodes字段的对象
		var config struct {
			Nodes []NodeConfig `json:"nodes"`
		}
		
		if err := json.Unmarshal(configData, &config); err != nil {
			return nil, fmt.Errorf("解析配置文件失败: %v", err)
		}
		
		nodes = config.Nodes
	}

	return nodes, nil
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

// selectNodes 选择要执行的节点
func selectNodes(nodes []NodeConfig) []NodeConfig {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("🔍 请选择要执行的节点:")
	fmt.Println("   1. 全部节点")
	fmt.Println("   2. 指定节点 (输入序号，多个用逗号分隔)")
	fmt.Print("请输入选择 [1]: ")
	
	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)
	
	if choice == "" || choice == "1" {
		fmt.Printf("✅ 已选择全部 %d 个节点\n", len(nodes))
		return nodes
	}
	
	if choice == "2" {
		fmt.Print("请输入节点序号 (例如: 1,3,5): ")
		indexStr, _ := reader.ReadString('\n')
		indexStr = strings.TrimSpace(indexStr)
		
		indexStrs := strings.Split(indexStr, ",")
		selectedNodes := make([]NodeConfig, 0)
		
		for _, idxStr := range indexStrs {
			idxStr = strings.TrimSpace(idxStr)
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 1 || idx > len(nodes) {
				fmt.Printf("⚠️ 忽略无效的节点序号: %s\n", idxStr)
				continue
			}
			selectedNodes = append(selectedNodes, nodes[idx-1])
		}
		
		fmt.Printf("✅ 已选择 %d 个节点\n", len(selectedNodes))
		return selectedNodes
	}
	
	fmt.Println("⚠️ 无效的选择，默认使用全部节点")
	return nodes
}

// executeBatchCleanup 执行批量清理任务
func executeBatchCleanup(nodes []NodeConfig, params CleanupRequest) []TaskResult {
	fmt.Println("\n========================================")
	fmt.Println("🚀 开始执行批量暂停与清理任务")
	fmt.Println("========================================")

	var wg sync.WaitGroup
	results := make([]TaskResult, len(nodes))
	resultMutex := sync.Mutex{}

	// 显示进度条
	fmt.Printf("进度: [%s] 0/%d\n", strings.Repeat(" ", len(nodes)), len(nodes))

	for i, node := range nodes {
		wg.Add(1)
		go func(index int, node NodeConfig) {
			defer wg.Done()

			// 复制请求参数，并设置认证信息
			req := params
			req.AccountID = node.ID
			req.Csrftoken = node.Csrftoken
			req.Cookie = node.Cookie

			// 执行清理请求
			result := executeCleanup(node, req)
			
			// 保存结果
			resultMutex.Lock()
			results[index] = result
			
			// 更新进度条
			completed := 0
			for _, r := range results {
				if r.Node.ID != "" {
					completed++
				}
			}
			progressBar := strings.Repeat("█", completed) + strings.Repeat(" ", len(nodes)-completed)
			fmt.Printf("\r进度: [%s] %d/%d", progressBar, completed, len(nodes))
			
			resultMutex.Unlock()
		}(i, node)
	}

	wg.Wait()
	fmt.Println() // 进度条完成后换行
	return results
}

// executeBatchResume 执行批量恢复任务
func executeBatchResume(nodes []NodeConfig) []TaskResult {
	fmt.Println("\n========================================")
	fmt.Println("🚀 开始执行批量恢复任务")
	fmt.Println("========================================")

	var wg sync.WaitGroup
	results := make([]TaskResult, len(nodes))
	resultMutex := sync.Mutex{}

	// 显示进度条
	fmt.Printf("进度: [%s] 0/%d\n", strings.Repeat(" ", len(nodes)), len(nodes))

	for i, node := range nodes {
		wg.Add(1)
		go func(index int, node NodeConfig) {
			defer wg.Done()

			// 执行恢复请求
			result := executeResume(node)
			
			// 保存结果
			resultMutex.Lock()
			results[index] = result
			
			// 更新进度条
			completed := 0
			for _, r := range results {
				if r.Node.ID != "" {
					completed++
				}
			}
			progressBar := strings.Repeat("█", completed) + strings.Repeat(" ", len(nodes)-completed)
			fmt.Printf("\r进度: [%s] %d/%d", progressBar, completed, len(nodes))
			
			resultMutex.Unlock()
		}(i, node)
	}

	wg.Wait()
	fmt.Println() // 进度条完成后换行
	return results
}

// executeCleanup 执行单个清理任务
func executeCleanup(node NodeConfig, params CleanupRequest) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "未知错误",
		Retries: 0,
	}

	// 构建请求URL
	url := fmt.Sprintf("http://%s/complete-cleanup", node.IP)
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}

	// 序列化请求体
	reqBody, err := json.Marshal(params)
	if err != nil {
		result.Message = fmt.Sprintf("序列化请求失败: %v", err)
		return result
	}

	// 最多重试3次
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		result.Retries = attempt

		// 创建HTTP请求
		req, err := http.NewRequest("POST", url, strings.NewReader(string(reqBody)))
		if err != nil {
			result.Message = fmt.Sprintf("创建HTTP请求失败: %v", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")

		// 发送请求
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			result.Message = fmt.Sprintf("发送请求失败: %v", err)
			time.Sleep(1 * time.Second) // 重试前等待
			continue
		}

		// 读取响应
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			result.Message = fmt.Sprintf("读取响应失败: %v", err)
			continue
		}

		// 解析响应
		var cleanupResp CleanupResponse
		if err := json.Unmarshal(respBody, &cleanupResp); err != nil {
			result.Message = fmt.Sprintf("解析响应失败: %v", err)
			continue
		}

		// 检查响应状态
		if cleanupResp.Success {
			result.Success = true
			if params.TokenAddress != "" {
				result.Message = fmt.Sprintf("暂停成功，清理了%v个挂单，代币清理: %v", 
					cleanupResp.Results["orders_cleaned"], 
					cleanupResp.Results["token_clean"])
			} else {
				result.Message = fmt.Sprintf("暂停成功，清理了%v个挂单", 
					cleanupResp.Results["orders_cleaned"])
			}
			return result
		} else {
			result.Message = fmt.Sprintf("请求失败: %s", cleanupResp.Message)
		}
	}

	return result
}

// executeResume 执行单个恢复任务
func executeResume(node NodeConfig) TaskResult {
	result := TaskResult{
		Node:    node,
		Success: false,
		Message: "未知错误",
		Retries: 0,
	}

	// 构建请求URL
	url := fmt.Sprintf("http://%s/pause-status?account_id=%s", node.IP, node.ID)
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}

	// 最多重试3次
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		result.Retries = attempt

		// 创建HTTP请求
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			result.Message = fmt.Sprintf("创建HTTP请求失败: %v", err)
			continue
		}

		// 发送请求
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			result.Message = fmt.Sprintf("发送请求失败: %v", err)
			time.Sleep(1 * time.Second) // 重试前等待
			continue
		}

		// 读取响应
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			result.Message = fmt.Sprintf("读取响应失败: %v", err)
			continue
		}

		// 解析响应
		var resumeResp map[string]interface{}
		if err := json.Unmarshal(respBody, &resumeResp); err != nil {
			result.Message = fmt.Sprintf("解析响应失败: %v", err)
			continue
		}

		// 检查响应状态
		success, ok := resumeResp["success"].(bool)
		if ok && success {
			result.Success = true
			if message, ok := resumeResp["message"].(string); ok {
				result.Message = message
			} else {
				result.Message = "账户恢复成功"
			}
			return result
		} else {
			if message, ok := resumeResp["message"].(string); ok {
				result.Message = fmt.Sprintf("请求失败: %s", message)
			} else {
				result.Message = "请求失败: 未知错误"
			}
		}
	}

	return result
}
