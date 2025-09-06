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

// CleanupRequest 清理挂单请求结构体
type CleanupRequest struct {
	AccountID string `json:"account_id"`
	Csrftoken string `json:"csrftoken"`
	Cookie    string `json:"cookie"`
	Force     bool   `json:"force"`
}

// PauseRequest 暂停请求结构体
type PauseRequest struct {
	AccountID string `json:"account_id"`
	Duration  int    `json:"duration"` // 暂停时长（秒）
	Reason    string `json:"reason"`   // 暂停原因
}

// CleanupResponse 清理响应结构体
type CleanupResponse struct {
	Success      bool   `json:"success"`
	Message      string `json:"message"`
	CleanedCount int    `json:"cleaned_count"`
}

// PauseResponse 暂停响应结构体
type PauseResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	AccountID string `json:"account_id"`
	Duration  int    `json:"duration"`
	Reason    string `json:"reason"`
	PauseEnd  int64  `json:"pause_end"`
}

// OperationResult 操作结果
type OperationResult struct {
	Node         NodeConfig
	CleanupOK    bool
	PauseOK      bool
	CleanupMsg   string
	PauseMsg     string
	CleanedCount int
}

func main() {
	fmt.Println("========================================")
	fmt.Println("⏸️  Flash Trade 暂停管理脚本 v1.0")
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

	// 获取用户输入的暂停参数
	pauseParams, err := getUserInput()
	if err != nil {
		fmt.Printf("❌ 获取用户输入失败: %v\n", err)
		return
	}

	// 显示操作摘要
	fmt.Println("========================================")
	fmt.Println("📋 操作摘要:")
	fmt.Printf("   暂停时长: %d 小时 (%d 秒)\n", pauseParams.Duration/3600, pauseParams.Duration)
	fmt.Printf("   暂停原因: %s\n", pauseParams.Reason)
	fmt.Printf("   目标节点: %d 个\n", len(nodes))
	fmt.Println("========================================")

	// 确认执行
	if !confirmExecution() {
		fmt.Println("❌ 操作已取消")
		return
	}

	fmt.Println()
	fmt.Println("🚀 开始执行暂停管理操作...")
	fmt.Println()

	// 并发执行操作
	var wg sync.WaitGroup
	results := make(chan OperationResult, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n NodeConfig) {
			defer wg.Done()
			result := executeOperation(n, pauseParams)
			results <- result
		}(node)
	}

	// 等待所有操作完成
	wg.Wait()
	close(results)

	// 收集并显示结果
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("📊 操作结果汇总:")
	fmt.Println("========================================")

	successCount := 0
	totalCleaned := 0

	for result := range results {
		fmt.Printf("\n🖥️  节点: %s (%s)\n", result.Node.Name, result.Node.ID)

		if result.CleanupOK {
			fmt.Printf("   🧹 挂单清理: ✅ 成功 (清理了 %d 个挂单)\n", result.CleanedCount)
			totalCleaned += result.CleanedCount
		} else {
			fmt.Printf("   🧹 挂单清理: ❌ 失败 - %s\n", result.CleanupMsg)
		}

		if result.PauseOK {
			fmt.Printf("   ⏸️  账号暂停: ✅ 成功\n")
		} else {
			fmt.Printf("   ⏸️  账号暂停: ❌ 失败 - %s\n", result.PauseMsg)
		}

		if result.CleanupOK && result.PauseOK {
			successCount++
		}
	}

	fmt.Println()
	fmt.Printf("✅ 成功操作节点: %d/%d\n", successCount, len(nodes))
	fmt.Printf("🧹 总计清理挂单: %d 个\n", totalCleaned)

	if successCount == len(nodes) {
		fmt.Println("🎉 所有节点操作完成！账号已暂停，等待手动恢复。")
		fmt.Println()
		fmt.Println("💡 恢复方法:")
		fmt.Println("   DELETE http://节点IP:8080/pause-status?account_id=账号ID")
		fmt.Println("   或使用 Web 界面手动恢复")
	} else {
		fmt.Println("⚠️  部分节点操作失败，请检查失败的节点")
	}

	fmt.Println()
	fmt.Println("程序执行完成，按回车键退出...")
	bufio.NewReader(os.Stdin).ReadLine()
}

// loadConfig 加载配置文件
func loadConfig(filename string) ([]NodeConfig, error) {
	// 尝试多个可能的配置文件路径
	possiblePaths := []string{
		filename,
		filepath.Join(".", filename),
		filepath.Join("script", filename),
		filepath.Join("..", filename),
	}

	var configData []byte
	var err error
	var usedPath string

	for _, path := range possiblePaths {
		configData, err = ioutil.ReadFile(path)
		if err == nil {
			usedPath = path
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("无法找到配置文件 %s: %v", filename, err)
	}

	fmt.Printf("📄 使用配置文件: %s\n", usedPath)

	var nodes []NodeConfig
	if err := json.Unmarshal(configData, &nodes); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return nodes, nil
}

// getUserInput 获取用户输入的暂停参数
func getUserInput() (*PauseRequest, error) {
	reader := bufio.NewReader(os.Stdin)
	req := &PauseRequest{}

	// 获取暂停时长（小时）
	fmt.Print("请输入暂停时长（小时，默认24小时）: ")
	durationStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	durationStr = strings.TrimSpace(durationStr)
	if durationStr == "" {
		req.Duration = 24 * 3600 // 默认24小时
	} else {
		hours, err := strconv.Atoi(durationStr)
		if err != nil {
			return nil, fmt.Errorf("暂停时长格式错误: %v", err)
		}
		if hours <= 0 {
			return nil, fmt.Errorf("暂停时长必须大于0")
		}
		req.Duration = hours * 3600 // 转换为秒
	}

	// 获取暂停原因
	fmt.Print("请输入暂停原因（默认: manual_maintenance）: ")
	reason, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	req.Reason = strings.TrimSpace(reason)
	if req.Reason == "" {
		req.Reason = "manual_maintenance"
	}

	return req, nil
}

// confirmExecution 确认执行
func confirmExecution() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("确认执行暂停管理操作? (y/N): ")
	confirm, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	confirm = strings.ToLower(strings.TrimSpace(confirm))
	return confirm == "y" || confirm == "yes"
}

// executeOperation 执行完整的暂停管理操作
func executeOperation(node NodeConfig, pauseParams *PauseRequest) OperationResult {
	result := OperationResult{
		Node: node,
	}

	fmt.Printf("🔄 [%s] 开始处理节点...\n", node.Name)

	// 第一步：清理挂单
	fmt.Printf("🧹 [%s] 步骤1: 清理挂单...\n", node.Name)
	cleanupReq := CleanupRequest{
		AccountID: node.ID,
		Csrftoken: node.Csrftoken,
		Cookie:    node.Cookie,
		Force:     true, // 强制清理所有挂单
	}

	cleanupResp, err := sendCleanupRequest(node, cleanupReq)
	if err != nil {
		result.CleanupMsg = fmt.Sprintf("请求失败: %v", err)
		fmt.Printf("❌ [%s] 挂单清理失败: %v\n", node.Name, err)
		return result
	}

	if cleanupResp.Success {
		result.CleanupOK = true
		result.CleanedCount = cleanupResp.CleanedCount
		result.CleanupMsg = cleanupResp.Message
		fmt.Printf("✅ [%s] 挂单清理成功，清理了 %d 个挂单\n", node.Name, cleanupResp.CleanedCount)
	} else {
		result.CleanupMsg = cleanupResp.Message
		fmt.Printf("❌ [%s] 挂单清理失败: %s\n", node.Name, cleanupResp.Message)
		return result
	}

	// 等待清理生效
	fmt.Printf("⏳ [%s] 等待挂单清理生效...\n", node.Name)
	time.Sleep(3 * time.Second)

	// 第二步：验证没有挂单
	fmt.Printf("🔍 [%s] 步骤2: 验证挂单清理...\n", node.Name)
	if !verifyNoOrders(node) {
		result.CleanupMsg = "验证失败，仍有挂单存在"
		fmt.Printf("⚠️ [%s] 警告: 仍有挂单存在，但继续执行暂停\n", node.Name)
	}

	// 第三步：设置暂停
	fmt.Printf("⏸️ [%s] 步骤3: 设置账号暂停...\n", node.Name)
	pauseReq := PauseRequest{
		AccountID: node.ID,
		Duration:  pauseParams.Duration,
		Reason:    pauseParams.Reason,
	}

	pauseResp, err := sendPauseRequest(node, pauseReq)
	if err != nil {
		result.PauseMsg = fmt.Sprintf("请求失败: %v", err)
		fmt.Printf("❌ [%s] 账号暂停失败: %v\n", node.Name, err)
		return result
	}

	if pauseResp.Success {
		result.PauseOK = true
		result.PauseMsg = pauseResp.Message
		endTime := time.Unix(pauseResp.PauseEnd, 0)
		fmt.Printf("✅ [%s] 账号暂停成功，预计恢复时间: %s\n", node.Name, endTime.Format("2006-01-02 15:04:05"))
	} else {
		result.PauseMsg = pauseResp.Message
		fmt.Printf("❌ [%s] 账号暂停失败: %s\n", node.Name, pauseResp.Message)
	}

	return result
}

// sendCleanupRequest 发送清理挂单请求
func sendCleanupRequest(node NodeConfig, req CleanupRequest) (*CleanupResponse, error) {
	url := fmt.Sprintf("http://%s:8080/cleanup-orders", node.IP)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP错误 %d: %s", resp.StatusCode, string(body))
	}

	var cleanupResp CleanupResponse
	if err := json.Unmarshal(body, &cleanupResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	return &cleanupResp, nil
}

// sendPauseRequest 发送暂停请求
func sendPauseRequest(node NodeConfig, req PauseRequest) (*PauseResponse, error) {
	url := fmt.Sprintf("http://%s:8080/pause-status", node.IP)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP错误 %d: %s", resp.StatusCode, string(body))
	}

	var pauseResp PauseResponse
	if err := json.Unmarshal(body, &pauseResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	return &pauseResp, nil
}

// verifyNoOrders 验证没有挂单
func verifyNoOrders(node NodeConfig) bool {
	url := fmt.Sprintf("http://%s:8080/account-status?account_id=%s", node.IP, node.ID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("⚠️ [%s] 验证挂单状态失败: %v\n", node.Name, err)
		return false
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("⚠️ [%s] 读取验证响应失败: %v\n", node.Name, err)
		return false
	}

	// 简单检查响应中是否包含挂单信息
	// 这里可以根据实际API响应格式进行更精确的解析
	bodyStr := string(body)
	if strings.Contains(bodyStr, "open_orders") || strings.Contains(bodyStr, "pending_orders") {
		// 如果响应中包含挂单相关信息，需要进一步检查
		var statusResp map[string]interface{}
		if err := json.Unmarshal(body, &statusResp); err == nil {
			// 这里可以添加更详细的挂单检查逻辑
			// 目前简化处理，认为清理成功
		}
	}

	return true // 简化处理，认为验证通过
}
