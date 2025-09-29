package auth

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
)

// LoginUI 登录界面管理器
type LoginUI struct {
	authManager *AuthManager
	uid         string
	username    string
	code        string
	isLoggedIn  bool
}

// NewLoginUI 创建新的登录界面管理器
func NewLoginUI() (*LoginUI, error) {
	// 创建认证管理器
	authManager, err := NewAuthManager()
	if err != nil {
		return nil, fmt.Errorf("创建认证管理器失败: %v", err)
	}

	// 生成唯一设备ID
	uid := getOrCreateDeviceID()

	return &LoginUI{
		authManager: authManager,
		uid:         uid,
		isLoggedIn:  false,
	}, nil
}

// Close 关闭登录界面管理器
func (ui *LoginUI) Close() {
	if ui.authManager != nil {
		ui.authManager.Close()
	}
}

// ShowLoginPrompt 显示登录提示
func (ui *LoginUI) ShowLoginPrompt() bool {
	// 检查是否已有用户记录
	user, err := ui.authManager.GetUserByUID(ui.uid)
	if err != nil {
		log.Printf("❌ 检查用户记录时出错: %v", err)
	}

	// 如果已有用户记录，尝试自动登录
	if user != nil {
		log.Printf("👤 欢迎回来, %s", user.Username)
		ui.username = user.Username
		ui.code = user.Code
		ui.isLoggedIn = true
		
		// 更新最后在线时间
		err = ui.authManager.UpdateUserLastSeen(ui.uid)
		if err != nil {
			log.Printf("⚠️ 更新用户在线时间失败: %v", err)
		}
		
		return true
	}

	// 否则，提示用户输入卡密
	fmt.Println("\n============================")
	fmt.Println("🔑 请输入卡密进行验证")
	fmt.Println("============================")
	
	var code string
	fmt.Print("卡密: ")
	fmt.Scanln(&code)
	code = strings.TrimSpace(code)
	
	if code == "" {
		log.Println("❌ 卡密不能为空")
		return false
	}
	
	// 验证卡密
	valid, err := ui.authManager.VerifyLicense(code)
	if err != nil || !valid {
		log.Printf("❌ 卡密验证失败: %v", err)
		return false
	}
	
	// 提示用户输入用户名
	var username string
	fmt.Print("请输入您的用户名: ")
	fmt.Scanln(&username)
	username = strings.TrimSpace(username)
	
	if username == "" {
		log.Println("❌ 用户名不能为空")
		return false
	}
	
	// 激活卡密
	err = ui.authManager.ActivateLicense(code, ui.uid, username)
	if err != nil {
		log.Printf("❌ 激活卡密失败: %v", err)
		return false
	}
	
	ui.username = username
	ui.code = code
	ui.isLoggedIn = true
	
	log.Printf("✅ 欢迎, %s! 卡密验证成功", username)
	return true
}

// UpdateKYCInfo 更新KYC信息
func (ui *LoginUI) UpdateKYCInfo(kycInfo string) error {
	if !ui.isLoggedIn {
		return fmt.Errorf("用户未登录")
	}
	
	return ui.authManager.UpdateUserKYC(ui.uid, kycInfo)
}

// IsLoggedIn 检查是否已登录
func (ui *LoginUI) IsLoggedIn() bool {
	return ui.isLoggedIn
}

// GetUID 获取用户UID
func (ui *LoginUI) GetUID() string {
	return ui.uid
}

// GetUsername 获取用户名
func (ui *LoginUI) GetUsername() string {
	return ui.username
}

// 获取或创建设备ID
func getOrCreateDeviceID() string {
	// 设备ID文件路径
	uidFilePath := ".device_uid"
	
	// 尝试读取现有设备ID
	data, err := os.ReadFile(uidFilePath)
	if err == nil && len(data) > 0 {
		return string(data)
	}
	
	// 创建新的设备ID
	uid := uuid.New().String()
	
	// 保存设备ID
	err = os.WriteFile(uidFilePath, []byte(uid), 0644)
	if err != nil {
		log.Printf("⚠️ 保存设备ID失败: %v", err)
	}
	
	return uid
} 