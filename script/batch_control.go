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

// ServerConfig 服务器配置
type ServerConfig struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IP        string `json:"ip"`
	CSRFToken string `json:"csrftoken"`
	Cookie    string `json:"cookie"`
}

// ControlRequest 控制请求
type ControlRequest struct {
	Mode string `json:"mode"` // "pause" 或 "resume"
}

func main() {
	// 显示欢迎信息
	fmt.Println("===================================")
	fmt.Println("    Flash Trade 批量控制工具")
	fmt.Println("===================================")

	// 获取可执行文件所在目录
	execPath, err := os.Executable()
	if err != nil {
		fmt.Printf("错误: 无法获取可执行文件路径: %v\n", err)
		fmt.Println("按回车键退出...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	
	execDir := filepath.Dir(execPath)
	configFile := filepath.Join(execDir, "config.json")
	
	fmt.Printf("可执行文件目录: %s\n", execDir)
	fmt.Printf("正在读取配置文件: %s\n", configFile)
	
	// 检查文件是否存在
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Printf("错误: 配置文件 %s 不存在\n", configFile)
		fmt.Println("按回车键退出...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	
	// 读取文件内容
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Printf("错误: 无法读取配置文件: %v\n", err)
		fmt.Println("按回车键退出...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	
	fmt.Printf("配置文件内容: %s\n", string(configData))
	
	// 尝试解析为直接的服务器数组
	var servers []ServerConfig
	if err := json.Unmarshal(configData, &servers); err != nil {
		fmt.Printf("错误: 解析配置文件失败: %v\n", err)
		fmt.Println("按回车键退出...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}

	if len(servers) == 0 {
		fmt.Println("错误: 配置文件中未找到服务器")
		fmt.Println("按回车键退出...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}

	fmt.Printf("成功加载 %d 个服务器配置\n", len(servers))
	fmt.Println("===================================")

	// 显示主菜单
	for {
		fmt.Println("\n请选择操作:")
		fmt.Println("1. 批量暂停所有服务器")
		fmt.Println("2. 批量恢复所有服务器")
		fmt.Println("3. 查看服务器列表")
		fmt.Println("4. 单独控制服务器")
		fmt.Println("0. 退出程序")
		fmt.Print("\n请输入选项 [0-4]: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "1":
			batchControl(servers, "pause")
		case "2":
			batchControl(servers, "resume")
		case "3":
			listServers(servers)
		case "4":
			controlSingleServer(servers)
		case "0":
			fmt.Println("程序已退出")
			return
		default:
			fmt.Println("无效的选项，请重新输入")
		}
	}
}

// batchControl 批量控制所有服务器
func batchControl(servers []ServerConfig, mode string) {
	modeText := "暂停"
	if mode == "resume" {
		modeText = "恢复"
	}

	fmt.Printf("\n开始批量%s %d 个服务器...\n", modeText, len(servers))
	fmt.Println("===================================")

	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// 准备请求体
	reqBody := ControlRequest{
		Mode: mode,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		fmt.Printf("生成请求数据失败: %v\n", err)
		return
	}

	// 并发发送请求
	var wg sync.WaitGroup
	results := make(map[string]string)
	var resultsMutex sync.Mutex

	for _, server := range servers {
		wg.Add(1)
		go func(server ServerConfig) {
			defer wg.Done()

			// 构建URL
			url := fmt.Sprintf("http://%s:8080/account-control", server.IP)
			
			// 发送请求
			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
			if err != nil {
				resultsMutex.Lock()
				results[server.IP] = fmt.Sprintf("创建请求失败: %v", err)
				resultsMutex.Unlock()
				return
			}
			
			// 设置请求头
			req.Header.Set("Content-Type", "application/json")
			if server.CSRFToken != "" {
				req.Header.Set("csrftoken", server.CSRFToken)
			} else {
				fmt.Printf("警告: 服务器 %s (%s) 没有提供CSRFToken\n", getName(server), server.IP)
			}
			if server.Cookie != "" {
				req.Header.Set("cookie", server.Cookie)
			} else {
				fmt.Printf("警告: 服务器 %s (%s) 没有提供Cookie\n", getName(server), server.IP)
			}
			
			fmt.Printf("发送请求到 %s: %s\n", url, string(jsonData))
			
			// 发送请求
			resp, err := client.Do(req)
			
			// 记录结果
			resultsMutex.Lock()
			defer resultsMutex.Unlock()
			
			if err != nil {
				results[server.IP] = fmt.Sprintf("错误: %v", err)
				return
			}
			defer resp.Body.Close()
			
			if resp.StatusCode != http.StatusOK {
				results[server.IP] = fmt.Sprintf("HTTP错误: %d", resp.StatusCode)
				return
			}
			
			// 读取响应
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				results[server.IP] = fmt.Sprintf("读取响应失败: %v", err)
				return
			}
			
			fmt.Printf("收到响应: %s\n", string(body))
			
			// 解析响应
			var response map[string]interface{}
			if err := json.Unmarshal(body, &response); err != nil {
				results[server.IP] = fmt.Sprintf("解析响应失败: %v", err)
				return
			}
			
			// 检查操作是否成功
			if success, ok := response["success"].(bool); ok && success {
				results[server.IP] = "成功"
			} else {
				message := "未知错误"
				if msg, ok := response["message"].(string); ok {
					message = msg
				}
				results[server.IP] = fmt.Sprintf("失败: %s", message)
			}
		}(server)
	}

	// 等待所有请求完成
	wg.Wait()

	// 输出结果
	fmt.Println("\n批量操作结果:")
	fmt.Println("-----------------------------")
	successCount := 0
	failCount := 0
	
	for _, server := range servers {
		result := results[server.IP]
		if result == "成功" {
			successCount++
			fmt.Printf("✅ %s (%s): %s\n", getName(server), server.IP, result)
		} else {
			failCount++
			fmt.Printf("❌ %s (%s): %s\n", getName(server), server.IP, result)
		}
	}
	
	fmt.Println("-----------------------------")
	fmt.Printf("总计: %d 成功, %d 失败\n", successCount, failCount)
	
	fmt.Println("\n按回车键返回主菜单...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

// getName 获取服务器名称，如果为空则返回默认名称
func getName(server ServerConfig) string {
	if server.Name != "" {
		return server.Name
	}
	return "未命名服务器"
}

// listServers 显示服务器列表
func listServers(servers []ServerConfig) {
	fmt.Println("\n服务器列表:")
	fmt.Println("===================================")
	for i, server := range servers {
		fmt.Printf("%d. %s (%s)\n", i+1, getName(server), server.IP)
	}
	fmt.Println("===================================")
	
	fmt.Println("\n按回车键返回主菜单...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

// controlSingleServer 控制单个服务器
func controlSingleServer(servers []ServerConfig) {
	fmt.Println("\n选择要控制的服务器:")
	fmt.Println("===================================")
	for i, server := range servers {
		fmt.Printf("%d. %s (%s)\n", i+1, getName(server), server.IP)
	}
	fmt.Println("0. 返回主菜单")
	fmt.Println("===================================")
	
	fmt.Print("\n请输入服务器编号: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	
	if input == "0" {
		return
	}
	
	index, err := strconv.Atoi(input)
	if err != nil || index < 1 || index > len(servers) {
		fmt.Println("无效的选择，返回主菜单")
		return
	}
	
	server := servers[index-1]
	name := getName(server)
	
	fmt.Printf("\n已选择: %s (%s)\n", name, server.IP)
	fmt.Println("===================================")
	fmt.Println("1. 暂停")
	fmt.Println("2. 恢复")
	fmt.Println("0. 返回主菜单")
	fmt.Print("\n请选择操作 [0-2]: ")
	
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	
	var mode string
	switch input {
	case "1":
		mode = "pause"
	case "2":
		mode = "resume"
	default:
		return
	}
	
	// 执行单个服务器控制
	modeText := "暂停"
	if mode == "resume" {
		modeText = "恢复"
	}
	
	fmt.Printf("\n正在%s服务器 %s (%s)...\n", modeText, name, server.IP)
	
	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	
	// 准备请求体
	reqBody := ControlRequest{
		Mode: mode,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		fmt.Printf("生成请求数据失败: %v\n", err)
		return
	}
	
	// 构建URL
	url := fmt.Sprintf("http://%s:8080/account-control", server.IP)
	
	// 发送请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return
	}
	
	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	if server.CSRFToken != "" {
		req.Header.Set("csrftoken", server.CSRFToken)
	}
	if server.Cookie != "" {
		req.Header.Set("cookie", server.Cookie)
	}
	
	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("请求失败: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("HTTP错误: %d\n", resp.StatusCode)
		return
	}
	
	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("读取响应失败: %v\n", err)
		return
	}
	
	// 解析响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}
	
	// 检查操作是否成功
	if success, ok := response["success"].(bool); ok && success {
		fmt.Printf("✅ 操作成功: %s\n", name)
	} else {
		message := "未知错误"
		if msg, ok := response["message"].(string); ok {
			message = msg
		}
		fmt.Printf("❌ 操作失败: %s - %s\n", name, message)
	}
	
	fmt.Println("\n按回车键返回主菜单...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
} 