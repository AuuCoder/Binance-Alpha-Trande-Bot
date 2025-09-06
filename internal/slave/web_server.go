package slave

import (
	"net/http"
	"os"
	"time"

	"alpha-autosell-bot/pkg/utils"
	"github.com/gin-gonic/gin"
)

// WebServer 被控端Web服务器
type WebServer struct {
	accountManager *AccountManager
	nodeID         string
	startTime      time.Time
}

// NewWebServer 创建Web服务器
func NewWebServer(accountManager *AccountManager, nodeID string) *WebServer {
	return &WebServer{
		accountManager: accountManager,
		nodeID:         nodeID,
		startTime:      time.Now(),
	}
}

// SetupRoutes 设置路由
func (ws *WebServer) SetupRoutes(router *gin.Engine) {
	// 静态文件
	router.Static("/static", "./web/static")

	// 尝试加载模板文件，如果失败则跳过
	if _, err := os.Stat("web/templates"); err == nil {
		router.LoadHTMLGlob("web/templates/*")
	}

	// 主页
	router.GET("/", ws.handleIndex)

	// API路由
	api := router.Group("/api/v1")
	{
		// 系统信息
		api.GET("/info", ws.handleGetInfo)
		api.GET("/health", ws.handleHealth)

		// 账号管理
		api.GET("/accounts", ws.handleGetAccounts)
		api.POST("/accounts", ws.handleAddAccount)
		api.PUT("/accounts/:id", ws.handleUpdateAccount)
		api.DELETE("/accounts/:id", ws.handleDeleteAccount)
		api.GET("/accounts/:id", ws.handleGetAccount)
		api.POST("/accounts/:id/test", ws.handleTestAccount)
		api.POST("/accounts/:id/toggle", ws.handleToggleAccount)
	}
}

// handleIndex 主页
func (ws *WebServer) handleIndex(c *gin.Context) {
	total, active := ws.accountManager.GetAccountCount()

	// 检查模板是否存在
	if _, err := os.Stat("web/templates/slave_index.html"); err == nil {
		c.HTML(http.StatusOK, "slave_index.html", gin.H{
			"title":           "币安自动卖出机器人 - 被控端",
			"node_id":         ws.nodeID,
			"uptime":          utils.FormatDuration(time.Since(ws.startTime)),
			"total_accounts":  total,
			"active_accounts": active,
		})
	} else {
		// 如果模板不存在，返回JSON响应
		c.JSON(http.StatusOK, gin.H{
			"title":           "币安自动卖出机器人 - 被控端",
			"node_id":         ws.nodeID,
			"uptime":          utils.FormatDuration(time.Since(ws.startTime)),
			"total_accounts":  total,
			"active_accounts": active,
			"status":          "running",
			"timestamp":       time.Now().Format(time.RFC3339),
			"message":         "被控端运行正常，Web配置界面可用",
			"endpoints": gin.H{
				"accounts": "/api/v1/accounts",
				"health":   "/api/v1/health",
				"info":     "/api/v1/info",
			},
		})
	}
}

// handleGetInfo 获取系统信息
func (ws *WebServer) handleGetInfo(c *gin.Context) {
	total, active := ws.accountManager.GetAccountCount()

	c.JSON(http.StatusOK, gin.H{
		"success":         true,
		"node_id":         ws.nodeID,
		"uptime":          utils.FormatDuration(time.Since(ws.startTime)),
		"start_time":      ws.startTime.Format(time.RFC3339),
		"total_accounts":  total,
		"active_accounts": active,
		"status":          "running",
	})
}

// handleHealth 健康检查
func (ws *WebServer) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
		"node_id":   ws.nodeID,
		"uptime":    utils.FormatDuration(time.Since(ws.startTime)),
	})
}

// handleGetAccounts 获取所有账号
func (ws *WebServer) handleGetAccounts(c *gin.Context) {
	accounts := ws.accountManager.GetAllAccounts()

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

// handleAddAccount 添加账号
func (ws *WebServer) handleAddAccount(c *gin.Context) {
	var req struct {
		ID          string `json:"id" binding:"required"`
		Name        string `json:"name"`
		Csrftoken   string `json:"csrftoken" binding:"required"`
		Cookie      string `json:"cookie" binding:"required"`
		Description string `json:"description"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "请求参数错误: " + err.Error(),
		})
		return
	}

	account := &Account{
		ID:          req.ID,
		Name:        req.Name,
		Csrftoken:   req.Csrftoken,
		Cookie:      req.Cookie,
		Description: req.Description,
	}

	if err := ws.accountManager.AddAccount(account); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":    true,
		"message":    "账号添加成功",
		"account_id": req.ID,
	})
}

// handleUpdateAccount 更新账号
func (ws *WebServer) handleUpdateAccount(c *gin.Context) {
	accountID := c.Param("id")

	var req struct {
		Name        string `json:"name"`
		Csrftoken   string `json:"csrftoken"`
		Cookie      string `json:"cookie"`
		Description string `json:"description"`
		Status      string `json:"status"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "请求参数错误: " + err.Error(),
		})
		return
	}

	updates := &Account{
		Name:        req.Name,
		Csrftoken:   req.Csrftoken,
		Cookie:      req.Cookie,
		Description: req.Description,
		Status:      req.Status,
	}

	if err := ws.accountManager.UpdateAccount(accountID, updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "账号更新成功",
	})
}

// handleDeleteAccount 删除账号
func (ws *WebServer) handleDeleteAccount(c *gin.Context) {
	accountID := c.Param("id")

	if err := ws.accountManager.DeleteAccount(accountID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "账号删除成功",
	})
}

// handleGetAccount 获取单个账号
func (ws *WebServer) handleGetAccount(c *gin.Context) {
	accountID := c.Param("id")

	account, err := ws.accountManager.GetAccount(accountID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// 隐藏敏感信息
	safeAccount := gin.H{
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

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"account": safeAccount,
	})
}

// handleTestAccount 测试账号
func (ws *WebServer) handleTestAccount(c *gin.Context) {
	accountID := c.Param("id")

	_, err := ws.accountManager.GetAccount(accountID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// TODO: 实现账号测试逻辑
	// 这里可以调用币安API测试账号是否有效

	c.JSON(http.StatusOK, gin.H{
		"success":    true,
		"message":    "账号测试功能待实现",
		"account_id": accountID,
	})
}

// handleToggleAccount 切换账号状态
func (ws *WebServer) handleToggleAccount(c *gin.Context) {
	accountID := c.Param("id")

	account, err := ws.accountManager.GetAccount(accountID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	newStatus := "inactive"
	if account.Status == "inactive" {
		newStatus = "active"
	}

	if err := ws.accountManager.SetAccountStatus(accountID, newStatus); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":    true,
		"message":    "账号状态已更新",
		"new_status": newStatus,
	})
}
