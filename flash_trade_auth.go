package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"alpha-autosell-bot/internal/auth"   // 导入auth包
	"alpha-autosell-bot/internal/common" // 导入common包
	"alpha-autosell-bot/pkg/utils"       // 导入utils包

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

// 全局变量，保存节点ID和MongoDB客户端
var (
	// NodeID 导出节点ID，使其可以被其他包使用
	NodeID       string
	nodeID       string // 为了向后兼容，保留原来的nodeID变量
	mongoClient  *mongo.Client
	mongoMutex   sync.Mutex
	kycChecked   bool
	kycCheckOnce sync.Once
)

// 在main函数开始时检查授权
func init() {
	// 获取节点ID
	NodeID = getNodeID()
	nodeID = NodeID // 为了向后兼容，保留原来的nodeID变量

	// 检查授权
	authorized, err := checkNodeAuthorization()
	if err != nil {
		log.Printf("❌ 授权检查失败: %v", err)
		os.Exit(1)
	}

	if !authorized {
		log.Printf("⚠️ 节点未授权，请联系管理员授权后再使用")
		os.Exit(1)
	}

	log.Printf("✅ 节点已授权，可以运行")
}

// checkAndPrintNodeKYCInfo 检查并打印节点的KYC信息
func checkAndPrintNodeKYCInfo() {
	// 连接MongoDB
	client, err := connectMongoDB()
	if err != nil {
		return
	}
	defer client.Disconnect(context.Background())
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)
	
	// 查询节点信息
	var node NodeInfo
	err = collection.FindOne(ctx, bson.M{"node_id": NodeID}).Decode(&node)
	if err != nil {
		return
	}
}

// monitorKYCInfo 监控KYC信息并更新节点信息
func monitorKYCInfo() {
	// 等待5秒，让程序完全启动
	time.Sleep(5 * time.Second)

	// 使用sync.Once确保只执行一次KYC检查
	kycCheckOnce.Do(func() {
		

		// 无限循环，每30秒尝试获取一次KYC信息
		for !kycChecked {
			// 检查节点的BinanceID和User是否为空
			needsUpdate, err := checkNodeNeedsKYCUpdate()
			if err != nil {
				
				time.Sleep(30 * time.Second)
				continue
			}

			// 如果不需要更新，标记为已检查并退出
			if !needsUpdate {
				
				kycChecked = true
				break
			}

			// 尝试获取KYC信息
			kycInfo, err := fetchKYCInfo()
			if err != nil {
				log.Printf("⚠️ 获取KYC信息失败: %v", err)
				time.Sleep(30 * time.Second)
				continue
			}

			// 更新节点信息
			if err := updateNodeKYCInfo(kycInfo); err != nil {
				
				time.Sleep(30 * time.Second)
				continue
			}

			
			kycChecked = true
		}
	})
}

// checkNodeNeedsKYCUpdate 检查节点是否需要更新KYC信息
func checkNodeNeedsKYCUpdate() (bool, error) {
	mongoMutex.Lock()
	defer mongoMutex.Unlock()

	// 如果MongoDB客户端未初始化，则初始化
	if mongoClient == nil {
		var err error
		mongoClient, err = connectMongoDB()
		if err != nil {
			return false, fmt.Errorf("连接MongoDB失败: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := mongoClient.Database(dbName).Collection(nodeCollection)

	// 查询节点信息
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": NodeID}).Decode(&node)
	if err != nil {
		return false, fmt.Errorf("查询节点信息失败: %v", err)
	}

	// 如果BinanceID和User都为空，则需要更新
	return node.BinanceID == "" && node.User == "", nil
}

// fetchKYCInfo 获取KYC信息
func fetchKYCInfo() (*http.Response, error) {
	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// 发送请求获取KYC信息
	req, err := http.NewRequest("GET", "https://www.binance.com/bapi/accounts/v1/private/account/user-kyc/current-kyc-status", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 添加必要的头信息
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	// 从cookie中获取csrftoken和cookie
	// 注意：这里需要从实际的请求中获取cookie信息
	// 这里只是一个示例，实际实现需要根据实际情况获取cookie
	cookie := getCookieFromRequest()
	if cookie != "" {
		req.Header.Add("Cookie", cookie)
	}

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	return resp, nil
}

// getCookieFromRequest 从请求中获取cookie
func getCookieFromRequest() string {

	return ""
}

// updateNodeKYCInfo 更新节点KYC信息
func updateNodeKYCInfo(kycInfo *http.Response) error {
	mongoMutex.Lock()
	defer mongoMutex.Unlock()

	// 如果MongoDB客户端未初始化，则初始化
	if mongoClient == nil {
		var err error
		mongoClient, err = connectMongoDB()
		if err != nil {
			return fmt.Errorf("连接MongoDB失败: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := mongoClient.Database(dbName).Collection(nodeCollection)

	// 更新节点信息
	update := bson.M{
		"$set": bson.M{
			"binance_id": "N/A", // 从响应中提取
			"user":       "N/A", // 从响应中提取
		},
	}

	_, err := collection.UpdateOne(ctx, bson.M{"node_id": NodeID}, update)
	if err != nil {
		return fmt.Errorf("更新节点信息失败: %v", err)
	}

	
	return nil
}

// checkNodeAuthorization 检查节点是否已授权
func checkNodeAuthorization() (bool, error) {
	// 连接MongoDB
	client, err := connectMongoDB()
	if err != nil {
		return false, fmt.Errorf("连接MongoDB失败")
	}
	defer client.Disconnect(context.Background())

	// 检查节点是否已注册，如果未注册则注册
	return checkAndRegisterNode(client, NodeID)
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
	macAddress := utils.GetMACAddress()
	osInfo := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	
	// 获取用户名和币安ID
	userName := ""
	binanceID := ""

	// 检查节点是否已存在
	var existingNode NodeInfo
	err = collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&existingNode)
	if err == nil {

		
		update := bson.M{
			"$set": bson.M{
				"hostname":       hostname,
				"ip_address":     ipAddress,
				"mac_address":    macAddress,
				"os_info":        osInfo,
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

	log.Printf("✅ 节点信息已添加到数据库")
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

	// 使用utils包中的函数生成节点ID
	return utils.GenerateNodeID("slave")
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

// 使用MongoDB连接管理器获取连接
func getMongoClient() (*mongo.Client, func(), error) {
	// 创建配置
	config := &common.MongoDBConfig{
		URI:            mongoURI,
		Database:       "alpha",
		Enabled:        true,
		MaxPoolSize:    10, // 短期工具使用较小的连接池
		MinPoolSize:    2,
		MaxConnIdleTime: 60, // 短期工具使用较短的空闲时间
	}

	// 获取连接管理器
	connManager, err := auth.GetMongoDBConnManager(config)
	if err != nil {
		return nil, nil, err
	}

	// 获取客户端
	client := connManager.GetClient()
	if client == nil {
		return nil, nil, fmt.Errorf("无法获取MongoDB客户端")
	}

	// 创建清理函数
	cleanup := func() {
		// 短期工具不需要关闭连接，连接由连接管理器管理
		log.Println("MongoDB连接已释放")
	}

	return client, cleanup, nil
} 