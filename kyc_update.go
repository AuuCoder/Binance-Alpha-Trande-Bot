package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"alpha-autosell-bot/internal/auth"
	"alpha-autosell-bot/internal/common"

	"bytes"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDB连接URI
var mongoURI = "mongodb+srv://yuucoder:yuucoder0208..@bn-alpha.vamde.mongodb.net/?retryWrites=true&w=majority&appName=bn-alpha"

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

// KYCFullResponse 完整的KYC响应结构
type KYCFullResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		KycStatus int    `json:"kycStatus"`
		FillInfo  struct {
			FirstName string `json:"firstName"`
		} `json:"fillInfo"`
		UserId int `json:"userId"`
	} `json:"data"`
	Success bool `json:"success"`
}

// 在程序启动时执行
func init() {
	// 启动一个后台协程，定期检查是否有新的KYC信息需要更新
	go monitorKYCInfoSimple()
}

// monitorKYCInfoSimple 监控KYC信息
func monitorKYCInfoSimple() {
	// 等待5秒，确保程序完全启动
	time.Sleep(5 * time.Second)
	
	// 每隔10秒检查一次
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		// 检查是否有新的KYC信息需要更新
		checkAndUpdateKYCInfoSimple()
	}
}

// checkAndUpdateKYCInfoSimple 检查并更新KYC信息
func checkAndUpdateKYCInfoSimple() {
	// 检查节点是否需要更新KYC信息
	needsUpdate, err := checkNodeNeedsUpdateSimple()
	if err != nil {
		// 静默处理错误
		return
	}
	
	// 如果不需要更新，则直接返回
	if !needsUpdate {
		return
	}
}

// updateKYCInfoSimple 更新KYC信息
func updateKYCInfoSimple(firstName string) {
	// 检查参数
	if firstName == "" {
		return
	}
	
	// 检查NodeID是否已设置
	if NodeID == "" {
		return
	}
	
	// 连接MongoDB
	client, err := connectMongoDBSimple()
	if err != nil {
		return
	}
	defer client.Disconnect(context.Background())
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取节点集合
	collection := client.Database("alpha").Collection("nodes")
	
	// 更新节点信息
	update := bson.M{
		"$set": bson.M{
			"binance_id": firstName, // 使用firstName作为binance_id
			"user":       firstName, // 使用firstName作为user
		},
	}
	
	result, err := collection.UpdateOne(ctx, bson.M{"node_id": NodeID}, update)
	if err != nil {
		return
	}
	
	if result.MatchedCount == 0 {
		return
	}
}

// checkNodeNeedsUpdateSimple 检查节点是否需要更新KYC信息
func checkNodeNeedsUpdateSimple() (bool, error) {
	// 检查NodeID是否已设置
	if NodeID == "" {
		return false, fmt.Errorf("NodeID未设置")
	}
	
	// 连接MongoDB
	client, err := connectMongoDBSimple()
	if err != nil {
		return false, fmt.Errorf("连接MongoDB失败")
	}
	defer client.Disconnect(context.Background())
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 获取节点集合
	collection := client.Database("alpha").Collection("nodes")
	
	// 查询节点信息
	var node struct {
		NodeID    string `bson:"node_id"`
		BinanceID string `bson:"binance_id"`
		User      string `bson:"user"`
	}
	err = collection.FindOne(ctx, bson.M{"node_id": NodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, fmt.Errorf("未找到节点")
		}
		return false, fmt.Errorf("查询节点信息失败")
	}
	
	// 如果BinanceID和User都为空，则需要更新
	return node.BinanceID == "" && node.User == "", nil
}

// connectMongoDBSimple 连接MongoDB
func connectMongoDBSimple() (*mongo.Client, error) {
	client, cleanup, err := getMongoClient()
	if err != nil {
		return nil, err
	}
	
	// 注意：这里不使用cleanup，因为我们在调用处使用defer client.Disconnect
	// 这是为了保持与现有代码兼容
	_ = cleanup // 避免未使用变量警告
	
	return client, nil
}

// DirectUpdateKYC 直接更新KYC信息
// 这个函数可以在flash_trade.go中的handleTrade函数中调用
func DirectUpdateKYC(csrftoken, cookie, firstName string) {
	if csrftoken == "" || cookie == "" || firstName == "" {
		return
	}
	
		// 使用getUserFirstNameAndId函数同时获取firstName和userId
		userId, userName, err := getUserFirstNameAndId(csrftoken, cookie)
		if err != nil {
			userId = firstName
			userName = firstName
		}
		
	// 更新KYC信息 - 同步执行，不使用goroutine
		updateKYCInfoWithUserId(userId, userName)
}

// getUserFirstNameAndId 从KYC接口同时获取firstName和userId
func getUserFirstNameAndId(csrftoken, cookie string) (string, string, error) {
	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// 正确的API端点
	url := "https://www.binance.com/bapi/kyc/v2/private/certificate/user-kyc/current-kyc-status"

	// 创建POST请求，而不是GET请求
	reqBody := []byte("{}")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", "", fmt.Errorf("创建请求失败: %v", err)
	}

	// 添加所有必要的头信息
	req.Header.Add("accept", "*/*")
	req.Header.Add("accept-language", "zh,zh-CN;q=0.9,en;q=0.8")
	req.Header.Add("bnc-level", "0")
	req.Header.Add("bnc-location", "CN")
	req.Header.Add("bnc-time-zone", "Asia/Shanghai")
	req.Header.Add("bnc-uuid", "d14a8e22-e3f4-4d39-8594-8e67093b4cbb")
	req.Header.Add("cache-control", "no-cache")
	req.Header.Add("clienttype", "web")
	req.Header.Add("content-type", "application/json")
	req.Header.Add("csrftoken", csrftoken)
	req.Header.Add("device-info", "eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTA4MCIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6Ijk5NCwxOTIwIiwic3lzdGVtX3ZlcnNpb24iOiJtYWNPUyAxMC4xNS43IiwiYnJhbmRfbW9kZWwiOiJkZXNrdG9wIEFwcGxlIE1hY2ludG9zaCAiLCJzeXN0ZW1fbGFuZyI6InpoIiwidGltZXpvbmUiOiJHTVQrMDg6MDAiLCJ0aW1lem9uZU9mZnNldCI6LTQ4MCwidXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwXzE1XzcpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xNDAuMC4wLjAgU2FmYXJpLzUzNy4zNiIsImxpc3RfcGx1Z2luIjoiUERGIFZpZXdlcixDaHJvbWUgUERGIFZpZXdlcixDaHJvbWl1bSBQREYgVmlld2VyLE1pY3Jvc29mdCBFZGdlIFBERiBWaWV3ZXIsV2ViS2l0IGJ1aWx0LWluIFBERiIsImNhbnZhc19jb2RlIjoiNTVkNTEwOGMiLCJ3ZWJnbF92ZW5kb3IiOiJHb29nbGUgSW5jLiAoQXBwbGUpIiwid2ViZ2xfcmVuZGVyZXIiOiJBTkdMRSAoQXBwbGUsIEFOR0xFIE1ldGFsIFJlbmRlcmVyOiBBcHBsZSBNMiBQcm8sIFVuc3BlY2lmaWVkIFZlcnNpb24pIiwiYXVkaW8iOiIxMjQuMDQzNDgxNTU4NzY1MDUiLCJwbGF0Zm9ybSI6Ik1hY0ludGVsIiwid2ViX3RpbWV6b25lIjoiQXNpYS9TaGFuZ2hhaSIsImRldmljZV9uYW1lIjoiQ2hyb21lIFYxNDAuMC4wLjAgKG1hY09TKSIsImZpbmdlcnByaW50IjoiZmQ3NmUwM2EzNTQ2N2VmNjQxMGY4YWQ1ZTljOWI2NmIiLCJkZXZpY2VfaWQiOiIiLCJyZWxhdGVkX2RldmljZV9pZHMiOiIifQ==")
	req.Header.Add("fvideo-id", "3387fca56213a0d88dcdb38a7832825fdf8abcb3")
	req.Header.Add("fvideo-token", "GXmL3QwGfz7ulNmmIJe1hs6e2bCreVM4ZNQZFbsek+Ffl7lbKLYNl9n3Y84RhgRLtlvOF7vKSSxFscB39Su2+jHvMOCcAkY8uanYzSGw4yaf9sGUGRVtGd/UlSgvQ6tfvbBZvJAwkhJkbE9N91+06vxUvFe130ITn3Fty1a+MdTntWco5GaTf5IGbe+pP21Ro=12")
	req.Header.Add("lang", "zh-CN")
	req.Header.Add("pragma", "no-cache")
	req.Header.Add("priority", "u=1, i")
	req.Header.Add("sec-ch-ua", "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"")
	req.Header.Add("sec-ch-ua-mobile", "?0")
	req.Header.Add("sec-ch-ua-platform", "\"macOS\"")
	req.Header.Add("sec-fetch-dest", "empty")
	req.Header.Add("sec-fetch-mode", "cors")
	req.Header.Add("sec-fetch-site", "same-origin")
	req.Header.Add("x-trace-id", "435fc3a4-ddab-47f9-96d9-3867d7d2790b")
	req.Header.Add("x-ui-request-trace", "435fc3a4-ddab-47f9-96d9-3867d7d2790b")
	req.Header.Add("Cookie", cookie)
	req.Header.Add("Referer", "https://www.binance.com/zh-CN/my/settings/kyc")

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("读取响应失败: %v", err)
	}

	// 解析为通用JSON结构
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", "", fmt.Errorf("解析响应失败: %v", err)
	}

	// 检查响应是否成功
	success, ok := data["success"].(bool)
	if !ok || !success {
		return "", "", fmt.Errorf("KYC响应不成功")
	}

	// 获取userId和firstName
	var userId, firstName string

	// 获取data字段
	dataObj, ok := data["data"].(map[string]interface{})
	if !ok {
		return "", "", fmt.Errorf("data字段格式错误")
	}

	// 尝试获取userId
	if userIdVal, ok := dataObj["userId"]; ok {
		switch v := userIdVal.(type) {
		case float64:
			userId = fmt.Sprintf("%.0f", v)
		case int:
			userId = fmt.Sprintf("%d", v)
		case string:
			userId = v
		default:
			userId = fmt.Sprintf("%v", userIdVal)
		}
	}

	// 尝试获取firstName
	if fillInfo, ok := dataObj["fillInfo"].(map[string]interface{}); ok {
		if firstNameVal, ok := fillInfo["firstName"].(string); ok {
			firstName = firstNameVal
		}
	}

	// 检查是否成功获取了userId和firstName
	if userId == "" {
		return "", "", fmt.Errorf("未找到userId")
	}
	if firstName == "" {
		return "", "", fmt.Errorf("未找到firstName")
	}

	return userId, firstName, nil
}

// getUserIdFromKYC 从KYC接口获取userId
func getUserIdFromKYC(csrftoken, cookie string) (string, error) {
	// 使用新的函数获取userId和firstName
	userId, _, err := getUserFirstNameAndId(csrftoken, cookie)
	return userId, err
}

// updateKYCInfoWithUserId 使用userId更新KYC信息
func updateKYCInfoWithUserId(userId, userName string) bool {
	// 检查参数
	if userId == "" || userName == "" {
		return false
	}
	
	// 检查NodeID是否已设置
	if NodeID == "" {
		return false
	}

	// 检查节点是否需要更新KYC信息 - 只检查错误
	_, err := checkNodeNeedsUpdateSimple()
	if err != nil {
		return false
	}

	// 连接MongoDB
	client, err := connectMongoDBSimple()
	if err != nil {
		return false
	}
	defer client.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database("alpha").Collection("nodes")

	// 先查询当前节点信息
	var currentNode struct {
		NodeID    string `bson:"node_id"`
		BinanceID string `bson:"binance_id"`
		User      string `bson:"user"`
	}
	err = collection.FindOne(ctx, bson.M{"node_id": NodeID}).Decode(&currentNode)

	// 更新节点信息
	update := bson.M{
		"$set": bson.M{
			"binance_id": userId,
			"user":       userName,
		},
	}

	// 执行更新操作
	result, err := collection.UpdateOne(ctx, bson.M{"node_id": NodeID}, update)
	if err != nil {
		return false
	}

	if result.MatchedCount == 0 {
		// 尝试创建新节点
		newNode := bson.M{
			"node_id":        NodeID,
			"binance_id":     userId,
			"user":           userName,
			"register_time":  time.Now(),
			"is_authorized":  0,
			"last_heartbeat": time.Now(),
		}
		
		_, err = collection.InsertOne(ctx, newNode)
		if err != nil {
			return false
		}
		
		return true
	}

	return true
} 

// DirectUpdateKYCWithUserId 直接使用提供的userId和userName更新KYC信息
// 这个函数可以在flash_trade.go中的handleTrade函数中调用，传入已获取的userId和firstName
func DirectUpdateKYCWithUserId(csrftoken, cookie, userName, userId string) {
	if csrftoken == "" || cookie == "" || userName == "" || userId == "" {
		return
	}
	
	// 同步更新KYC信息，不使用goroutine
		updateKYCInfoWithUserId(userId, userName)
} 

// 使用MongoDB连接管理器获取连接 - 已移至函数开头 