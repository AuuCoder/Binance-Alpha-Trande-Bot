package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"crypto/md5"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// MongoDB连接URI
	mongoURI = "mongodb+srv://yuucoder:yuucoder0208..@bn-alpha.vamde.mongodb.net/?retryWrites=true&w=majority&appName=bn-alpha"
	// 数据库名称
	dbName = "alpha"
	// 节点集合名称
	nodeCollection = "nodes"
)

// NodeInfo 节点信息结构
type NodeInfo struct {
	ID            string    `bson:"_id,omitempty"`  // MongoDB文档ID
	NodeID        string    `bson:"node_id"`        // 节点ID，作为唯一索引
	BinanceID     string    `bson:"binance_id"`     // 币安ID
	User          string    `bson:"user"`           // 用户名（可能是中文或英文）
	Hostname      string    `bson:"hostname"`       // 主机名
	IPAddress     string    `bson:"ip_address"`     // IP地址
	MacAddress    string    `bson:"mac_address"`    // MAC地址
	OSInfo        string    `bson:"os_info"`        // 操作系统信息
	RegisterTime  time.Time `bson:"register_time"`  // 注册时间
	IsAuthorized  int       `bson:"is_authorized"`  // 授权状态：0=未授权，1=已授权
	AuthorizedAt  time.Time `bson:"authorized_at"`  // 授权时间
	LastHeartbeat time.Time `bson:"last_heartbeat"` // 最后心跳时间
}

func main() {
	log.Printf("🔍 节点注册程序启动...")

	// 获取节点ID
	nodeID := getNodeID()
	log.Printf("🆔 节点ID: %s", nodeID)

	// 连接MongoDB
	client, err := connectMongoDB()
	if err != nil {
		log.Printf("❌ 连接MongoDB失败: %v", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.Background())

	// 检查节点是否已注册，如果未注册则注册
	authorized, err := checkAndRegisterNode(client, nodeID)
	if err != nil {
		log.Printf("❌ 节点注册/检查失败: %v", err)
		os.Exit(1)
	}

	// 如果节点已授权，启动flash_trade
	if authorized {
		log.Printf("✅ 节点已授权，正在启动Flash Trade...")
		startFlashTrade()
		os.Exit(0)
	} else {
		log.Printf("⚠️ 节点未授权，请联系管理员授权后再使用")
		time.Sleep(3 * time.Second) // 等待一段时间让用户看到消息
		os.Exit(1)
	}
}

// connectMongoDB 连接MongoDB
func connectMongoDB() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 连接MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("连接MongoDB失败: %v", err)
	}

	// 检查连接
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("MongoDB连接测试失败: %v", err)
	}

	
	return client, nil
}

// checkAndRegisterNode 检查节点是否已注册，如果未注册则注册
func checkAndRegisterNode(client *mongo.Client, nodeID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 创建唯一索引
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "node_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return false, fmt.Errorf("创建唯一索引失败: %v", err)
	}

	// 获取节点信息
	hostname, _ := os.Hostname()
	ipAddress := getLocalIP()
	macAddress := getMacAddress()
	osInfo := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	
	// 获取用户名和币安ID
	userName := getUserName()
	binanceID := getBinanceID()
	log.Printf("👤 用户名: %s, 币安ID: %s", userName, binanceID)

	// 检查节点是否已存在
	var existingNode NodeInfo
	err = collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&existingNode)
	if err == nil {
		// 节点已存在，更新信息
		
		
		update := bson.M{
			"$set": bson.M{
				"hostname":       hostname,
				"ip_address":     ipAddress,
				"mac_address":    macAddress,
				"os_info":        osInfo,
				"user":           userName,
				"binance_id":     binanceID,
				"last_heartbeat": time.Now(),
			},
		}
		
		_, err := collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
		if err != nil {
			return false, fmt.Errorf("更新节点信息失败: %v", err)
		}
		
		// 返回授权状态
		return existingNode.IsAuthorized == 1, nil
	} else if err != mongo.ErrNoDocuments {
		// 发生其他错误
		return false, fmt.Errorf("查询节点失败: %v", err)
	}

	// 节点不存在，插入新节点
	nodeInfo := NodeInfo{
		NodeID:        nodeID,
		User:          userName,
		BinanceID:     binanceID,
		Hostname:      hostname,
		IPAddress:     ipAddress,
		MacAddress:    macAddress,
		OSInfo:        osInfo,
		RegisterTime:  time.Now(),
		IsAuthorized:  0, // 初始未授权
		LastHeartbeat: time.Now(),
	}

	_, err = collection.InsertOne(ctx, nodeInfo)
	if err != nil {
		return false, fmt.Errorf("插入节点信息失败: %v", err)
	}

	
	return false, nil // 新注册的节点默认未授权
}

// getNodeID 获取节点ID
func getNodeID() string {
	// 尝试从配置文件获取节点ID
	configData, err := ioutil.ReadFile("config.json")
	if err == nil {
		var configTemp struct {
			Server struct {
				NodeID string `json:"node_id"`
			} `json:"server"`
		}
		if json.Unmarshal(configData, &configTemp) == nil && configTemp.Server.NodeID != "" {
			return configTemp.Server.NodeID
		}
	}

	// 使用与现有系统一致的方式生成节点ID
	return generateNodeID("slave")
}

// generateNodeID 生成基于硬件信息的固定节点ID
func generateNodeID(nodeType string) string {
	// 获取硬件特征信息
	hardwareInfo := getHardwareFingerprint()

	// 使用MD5生成固定的8位ID
	hash := md5.Sum([]byte(hardwareInfo))
	nodeID := fmt.Sprintf("%x", hash)[:8]

	// 调试信息
	
	log.Printf("🆔 生成节点ID: %s-%s", nodeType, nodeID)

	return fmt.Sprintf("%s-%s", nodeType, nodeID)
}

// getHardwareFingerprint 获取硬件指纹
func getHardwareFingerprint() string {
	var fingerprint strings.Builder

	// 1. 操作系统信息
	fingerprint.WriteString(runtime.GOOS)
	fingerprint.WriteString("-")
	fingerprint.WriteString(runtime.GOARCH)
	fingerprint.WriteString("-")

	// 2. 主机名（最重要的标识符）
	if hostname, err := os.Hostname(); err == nil {
		fingerprint.WriteString(hostname)
	}
	fingerprint.WriteString("-")

	// 3. 计算机名（Windows特有）
	if computerName := os.Getenv("COMPUTERNAME"); computerName != "" {
		fingerprint.WriteString(computerName)
	} else if computerName := os.Getenv("HOSTNAME"); computerName != "" {
		fingerprint.WriteString(computerName)
	}
	fingerprint.WriteString("-")

	// 4. 获取CPU信息（Windows）
	if cpuInfo := getCPUInfo(); cpuInfo != "" {
		fingerprint.WriteString(cpuInfo)
	}
	fingerprint.WriteString("-")

	// 5. 获取主板序列号（Windows）
	if motherboardSerial := getMotherboardSerial(); motherboardSerial != "" {
		fingerprint.WriteString(motherboardSerial)
	}
	fingerprint.WriteString("-")

	// 6. 获取MAC地址
	if macAddr := getMacAddress(); macAddr != "" {
		fingerprint.WriteString(macAddr)
	}
	fingerprint.WriteString("-")

	// 7. 用户名（作为辅助标识）
	if username := os.Getenv("USERNAME"); username != "" {
		fingerprint.WriteString(username)
	} else if username := os.Getenv("USER"); username != "" {
		fingerprint.WriteString(username)
	}

	return fingerprint.String()
}

// getCPUInfo 获取CPU信息
func getCPUInfo() string {
	if runtime.GOOS == "windows" {
		// 使用wmic获取CPU信息
		cmd := exec.Command("wmic", "cpu", "get", "ProcessorId", "/value")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "ProcessorId=") {
					return strings.TrimSpace(strings.TrimPrefix(line, "ProcessorId="))
				}
			}
		}
	}
	return ""
}

// getMotherboardSerial 获取主板序列号
func getMotherboardSerial() string {
	if runtime.GOOS == "windows" {
		// 使用wmic获取主板序列号
		cmd := exec.Command("wmic", "baseboard", "get", "SerialNumber", "/value")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "SerialNumber=") {
					return strings.TrimSpace(strings.TrimPrefix(line, "SerialNumber="))
				}
			}
		}
	}
	return ""
}

// getLocalIP 获取本机IP地址
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	
	return "unknown"
}

// getMacAddress 获取MAC地址
func getMacAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						return iface.HardwareAddr.String()
					}
				}
			}
		}
	}

	return ""
}

// getUserName 获取当前用户名
func getUserName() string {
	// 默认返回空字符串，由后续操作添加
	return ""
}

// getBinanceID 获取币安ID
func getBinanceID() string {
	// 默认返回空字符串，由后续操作添加
	return ""
}

// startFlashTrade 启动Flash Trade程序
func startFlashTrade() {
	// 获取当前可执行文件的路径
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("❌ 获取可执行文件路径失败: %v", err)
		return
	}
	
	// 获取可执行文件所在目录
	exeDir := filepath.Dir(exePath)
	
	// 构建Flash Trade可执行文件路径
	var flashTradePath string
	if runtime.GOOS == "windows" {
		flashTradePath = filepath.Join(exeDir, "flash_trade.exe")
	} else {
		flashTradePath = filepath.Join(exeDir, "flash_trade")
	}
	
	// 检查Flash Trade可执行文件是否存在
	if _, err := os.Stat(flashTradePath); os.IsNotExist(err) {
		// 尝试在当前目录查找
		if runtime.GOOS == "windows" {
			flashTradePath = "flash_trade.exe"
		} else {
			flashTradePath = "./flash_trade"
		}
		
		if _, err := os.Stat(flashTradePath); os.IsNotExist(err) {
			log.Printf("❌ Flash Trade可执行文件不存在: %s", flashTradePath)
			return
		}
	}
	
	// 获取命令行参数
	args := os.Args[1:]
	
	// 启动Flash Trade程序
	cmd := exec.Command(flashTradePath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	
	log.Printf("🚀 启动Flash Trade程序: %s %v", flashTradePath, args)
	if err := cmd.Start(); err != nil {
		log.Printf("❌ 启动Flash Trade程序失败: %v", err)
		return
	}
	
	// 等待Flash Trade程序启动
	time.Sleep(1 * time.Second)
} 