package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"alpha-autosell-bot/internal/common"
	"alpha-autosell-bot/internal/slave"
	"alpha-autosell-bot/pkg/utils"
	"encoding/json"
	"io/ioutil"
)

var (
	configPath = flag.String("config", "config.json", "配置文件路径")
	nodeID     = flag.String("node-id", "", "节点ID")
	masterAddr = flag.String("master", "", "主控端地址（覆盖配置文件）")
	port       = flag.Int("port", 0, "HTTP服务端口（覆盖配置文件）")
)

func main() {
	flag.Parse()

	// 加载配置
	config, err := common.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 验证配置文件是否为被控端配置
	if config.Server.Mode != "slave" {
		log.Fatalf("❌ 配置文件错误: 这是%s模式的配置文件，不是slave模式！请检查配置文件路径", config.Server.Mode)
	}

	// 设置节点ID
	if *nodeID != "" {
		config.Server.NodeID = *nodeID
	} else if config.Server.NodeID == "" {
		config.Server.NodeID = "" // 保持为空，让Client生成固定ID
	}

	// 配置文件优先，只有在命令行明确指定时才覆盖
	if *masterAddr != "" {
		config.Server.MasterAddr = *masterAddr
	} else if config.Server.MasterAddr == "" {
		config.Server.MasterAddr = "localhost:29090"
		
	}

	// 配置文件优先，只有在命令行明确指定时才覆盖
	if *port != 0 {
		config.Server.Port = *port
	} else if config.Server.Port == 0 {
		config.Server.Port = 28081
		
	}

	// 确保是被控端模式
	config.Server.Mode = "slave"

	// 确保数据库配置正确
	if config.MongoDB == nil {
		config.MongoDB = &common.MongoDBConfig{
			URI:      "mongodb+srv://yuucoder:yuucoder0208..@bn-alpha.vamde.mongodb.net/?retryWrites=true&w=majority&appName=bn-alpha",
			Database: "alpha",
			Enabled:  true,
			MaxPoolSize: 20,
			MinPoolSize: 5,
			MaxConnIdleTime: 300,
		}
	} else if !config.MongoDB.Enabled || config.MongoDB.URI == "" {
		config.MongoDB.URI = "mongodb+srv://yuucoder:yuucoder0208..@bn-alpha.vamde.mongodb.net/?retryWrites=true&w=majority&appName=bn-alpha"
		config.MongoDB.Database = "alpha"
		config.MongoDB.Enabled = true
		// 设置连接池参数（如果未设置）
		if config.MongoDB.MaxPoolSize == 0 {
			config.MongoDB.MaxPoolSize = 20
		}
		if config.MongoDB.MinPoolSize == 0 {
			config.MongoDB.MinPoolSize = 5
		}
		if config.MongoDB.MaxConnIdleTime == 0 {
			config.MongoDB.MaxConnIdleTime = 300
		}
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		log.Fatalf("❌ 配置验证失败: %v", err)
	}

	// 🔧 修改：默认启动flash_trade服务
	var flashTradeCmd *exec.Cmd
	disableFlashTrade := os.Getenv("DISABLE_FLASH_TRADE")

	if disableFlashTrade == "1" || disableFlashTrade == "true" {
		log.Printf("💡 独立模式运行")
	} else {
		// 尝试启动 flash_trade.go 服务
		flashTradeCmd = startFlashTradeService()
		if flashTradeCmd != nil {
			defer func() {
				if flashTradeCmd.Process != nil {
					flashTradeCmd.Process.Kill()
					flashTradeCmd.Wait()
				}
			}()
		}
	}

	// 创建客户端
	client, err := slave.NewClient(config)
	if err != nil {
		log.Fatalf("❌ 创建客户端失败: %v", err)
	}

	// 启动客户端
	if err := client.Start(); err != nil {
		log.Fatalf("❌ 启动客户端失败: %v", err)
	}

	// 启动简单的状态API服务器（仅用于监控）
	httpServer := setupSimpleHTTPServer(config, client)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ HTTP服务器错误: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("✅ 服务启动完成")

	<-sigChan
	log.Printf("🛑 正在关闭...")

	// 优雅关闭
	client.Stop()
	log.Printf("✅ 已停止")
}

// startFlashTradeService 启动 flash_trade.go 服务（可选）
func startFlashTradeService() *exec.Cmd {

	// 首先检查是否有可执行的flash_trade程序
	var cmd *exec.Cmd

	// 尝试Windows可执行文件
	if _, err := os.Stat("flash_trade.exe"); err == nil {
		cmd = exec.Command("./flash_trade.exe")
	} else if _, err := os.Stat("flash_trade"); err == nil {
		cmd = exec.Command("./flash_trade")
	} else if _, err := os.Stat("flash_trade.go"); err == nil {
		// 检查Go是否可用
		if _, err := exec.LookPath("go"); err != nil {
			log.Printf("⚠️ Go环境不可用，无法运行 flash_trade.go: %v", err)
			return nil
		}
		cmd = exec.Command("go", "run", "flash_trade.go")
	} else {
		log.Printf("⚠️ 未找到 flash_trade 相关文件，跳过启动")
		log.Printf("💡 提示: 服务可以独立运行，不依赖 flash_trade 服务")
		return nil
	}

	// 确保slave和flash_trade使用相同的节点ID
	// 读取配置文件中的节点ID
	configData, err := ioutil.ReadFile("config.json")
	if err == nil {
		var configTemp struct {
			Server struct {
				NodeID string `json:"node_id"`
			} `json:"server"`
		}
		if json.Unmarshal(configData, &configTemp) == nil {
			// 如果配置文件中有节点ID，则将其写入临时配置文件
			if configTemp.Server.NodeID != "" {
				log.Printf("🔄 确保flash_trade使用相同的节点ID: %s", configTemp.Server.NodeID)
				
				// 创建临时配置文件
				tempConfig := map[string]interface{}{
					"server": map[string]interface{}{
						"node_id": configTemp.Server.NodeID,
					},
				}
				
				tempConfigData, err := json.MarshalIndent(tempConfig, "", "  ")
				if err == nil {
					// 写入临时配置文件
					if err := ioutil.WriteFile("flash_trade_config.json", tempConfigData, 0644); err == nil {
						// 使用临时配置文件启动flash_trade
						cmd.Args = append(cmd.Args, "-config", "flash_trade_config.json")
					}
				}
			}
		}
	}

	// 设置输出
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// 启动服务
	if err := cmd.Start(); err != nil {
		log.Printf("❌ 启动 flash_trade 服务失败: %v", err)
		log.Printf("💡 服务将以独立模式运行")
		return nil
	}

	log.Printf("✅ flash_trade 服务已启动 (PID: %d)", cmd.Process.Pid)
	log.Printf("📡 Flash Trade 服务监听: http://localhost:8080")

	// 等待服务启动
	log.Printf("⏳ 等待 flash_trade 服务启动...")
	time.Sleep(3 * time.Second)

	// 检查服务是否正常启动
	if err := checkFlashTradeService(); err != nil {
		log.Printf("💡 服务可能仍在启动中，将继续运行")
	} else {
		log.Printf("✅ flash_trade 服务检查通过")
	}

	return cmd
}

// checkFlashTradeService 检查 flash_trade 服务是否正常
func checkFlashTradeService() error {
	// 创建带超时的HTTP客户端
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// 尝试多个端点
	endpoints := []string{
		"http://localhost:8080/health",
		"http://localhost:8080/stats",
		"http://localhost:8080/",
	}

	for _, endpoint := range endpoints {
		resp, err := client.Get(endpoint)
		if err != nil {
			continue // 尝试下一个端点
		}
		resp.Body.Close()

		if resp.StatusCode == 200 {
			return nil // 找到一个可用的端点
		}
	}

	return fmt.Errorf("所有端点都无法访问")
}

// setupSimpleHTTPServer 设置HTTP服务器（包含自动卖出功能）
func setupSimpleHTTPServer(config *common.Config, client *slave.Client) *http.Server {
	// 设置Gin模式
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// 创建WebServer并设置路由
	webServer := slave.NewWebServer(client.GetAccountManager(), client.GetNodeID())
	webServer.SetupRoutes(router)

	// 额外的兼容性路由（不与WebServer冲突的）
	api := router.Group("/api/v1")
	{
		// 节点状态（如果WebServer没有提供的话）
		api.GET("/node-status", handleGetStatus(client))

		// 统计信息（如果WebServer没有提供的话）
		api.GET("/node-stats", handleGetStats(client))
	}

	// 注意：根路径 "/" 已经在 webServer.SetupRoutes 中注册了，不需要重复注册

	return &http.Server{
		Addr:         config.GetServerAddr(),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}

// getMasterWebURL 获取主控端Web界面URL
func getMasterWebURL(masterAddr string) string {
	if masterAddr == "" {
		return "http://localhost:28080/accounts"
	}

	// 从gRPC地址转换为HTTP地址
	parts := strings.Split(masterAddr, ":")
	if len(parts) < 2 {
		return "http://localhost:28080/accounts"
	}

	host := parts[0]

	// 将gRPC端口29090转换为HTTP端口28080
	if strings.Contains(masterAddr, ":29090") {
		return fmt.Sprintf("http://%s:28080/accounts", host)
	}

	// 默认返回28080端口
	return fmt.Sprintf("http://%s:28080/accounts", host)
}

// handleHealth 健康检查
func handleHealth(config *common.Config, client *slave.Client) gin.HandlerFunc {
	startTime := time.Now()

	return func(c *gin.Context) {
		total, active := client.GetAccountManager().GetAccountCount()

		c.JSON(http.StatusOK, gin.H{
			"status":          "ok",
			"timestamp":       time.Now().Format(time.RFC3339),
			"uptime":          utils.FormatDuration(time.Since(startTime)),
			"node_id":         client.GetNodeID(),
			"node_status":     client.GetStatus(),
			"mode":            config.Server.Mode,
			"master":          config.Server.MasterAddr,
			"total_accounts":  total,
			"active_accounts": active,
		})
	}
}

// handleGetStatus 获取节点状态
func handleGetStatus(client *slave.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := client.GetStats()

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"node_id": client.GetNodeID(),
			"status":  client.GetStatus(),
			"stats":   stats,
		})
	}
}

// handleGetStats 获取统计信息
func handleGetStats(client *slave.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := client.GetStats()

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"node_id": client.GetNodeID(),
			"stats":   stats,
		})
	}
}

// handleGetTasks 获取任务列表
func handleGetTasks(client *slave.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Flash Trade 不使用传统的任务列表，返回空列表
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"count":   0,
			"tasks":   []interface{}{},
			"message": "Flash Trade 使用独立的任务管理",
		})
	}
}

// handleStopTask 停止任务
func handleStopTask(client *slave.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskId := c.Param("taskId")

		// 这里需要实现停止任务的逻辑
		// 由于任务是通过Redis命令控制的，这里只是一个示例

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"task_id": taskId,
			"message": "停止请求已发送",
		})
	}
}

// handleGetAccounts 获取账号列表（只读）
func handleGetAccounts(client *slave.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		accounts := client.GetAccountManager().GetAllAccounts()

		// 隐藏敏感信息
		safeAccounts := make(map[string]interface{})
		for id, account := range accounts {
			safeAccounts[id] = gin.H{
				"id":          account.ID,
				"name":        account.Name,
				"status":      account.Status,
				"created_at":  account.CreatedAt.Format(time.RFC3339),
				"updated_at":  account.UpdatedAt.Format(time.RFC3339),
				"last_used":   account.LastUsed.Format(time.RFC3339),
				"description": account.Description,
				"has_token":   account.Csrftoken != "",
				"has_cookie":  account.Cookie != "",
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success":  true,
			"accounts": safeAccounts,
			"count":    len(accounts),
		})
	}
}
