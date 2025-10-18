package tools

import (
	"alpha-autosell-bot/internal/auth"
	"alpha-autosell-bot/internal/common"
	"bufio"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// 默认MongoDB连接URI
const defaultMongoURI = ""

// 节点信息结构
type NodeInfo struct {
	NodeID        string    `bson:"node_id"`
	User          string    `bson:"user"`
	BinanceID     string    `bson:"binance_id"`
	Hostname      string    `bson:"hostname"`
	IPAddress     string    `bson:"ip_address"`
	MACAddress    string    `bson:"mac_address"`
	OSInfo        string    `bson:"os_info"`
	RegisterTime  time.Time `bson:"register_time"`
	IsAuthorized  int       `bson:"is_authorized"`
	LastHeartbeat time.Time `bson:"last_heartbeat"`
}

// 使用连接管理器获取MongoDB连接
func getMongoClient(uri string) (*mongo.Client, func(), error) {
	// 创建配置
	config := &common.MongoDBConfig{
		URI:            uri,
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

// 授权节点
func AuthorizeNode(nodeID, mongoURI string) error {
	if mongoURI == "" {
		mongoURI = defaultMongoURI
	}

	client, cleanup, err := getMongoClient(mongoURI)
	if err != nil {
		return fmt.Errorf("连接MongoDB失败: %v", err)
	}
	defer cleanup()

	// 获取集合
	collection := client.Database("alpha").Collection("nodes")

	// 更新授权状态
	authValue := 0
	if authorize {
		authValue = 1
	}

	update := bson.M{
		"$set": bson.M{
			"is_authorized":  authValue,
			"last_heartbeat": time.Now(),
		},
	}

	result, err := collection.UpdateOne(context.Background(), bson.M{"node_id": nodeID}, update)
	if err != nil {
		fmt.Printf("更新节点授权状态失败: %v\n", err)
		return err
	}

	action := "授权"
	if !authorize {
		action = "取消授权"
	}

	if result.MatchedCount == 0 {
		fmt.Printf("节点 %s 不存在\n", nodeID)
	} else {
		fmt.Printf("成功%s节点 %s\n", action, nodeID)
	}
	return nil
}

// 批量授权或取消授权节点（交互式）
func batchAuthorizeInteractive(collection *mongo.Collection, reader *bufio.Reader, authorize bool) {
	// 获取并显示节点列表
	nodes := listNodes(collection)
	displayNodes(nodes)
	
	// 提示用户选择节点
	action := "授权"
	if !authorize {
		action = "取消授权"
	}
	
	fmt.Printf("\n请输入要%s的节点序号，用逗号分隔 (例如: 1,3,5): ", action)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	
	// 解析输入
	indexStrs := strings.Split(input, ",")
	var selectedIndices []int
	
	for _, indexStr := range indexStrs {
		indexStr = strings.TrimSpace(indexStr)
		index, err := strconv.Atoi(indexStr)
		if err != nil || index < 1 || index > len(nodes) {
			fmt.Printf("无效的节点序号 %s，将被跳过。\n", indexStr)
			continue
		}
		selectedIndices = append(selectedIndices, index)
	}
	
	if len(selectedIndices) == 0 {
		fmt.Println("未选择有效的节点，操作取消。")
		return
	}
	
	// 显示选中的节点
	fmt.Println("\n已选择以下节点:")
	for _, index := range selectedIndices {
		node := nodes[index-1]
		fmt.Printf("- %s (%s)\n", node.NodeID, node.Hostname)
	}
	
	// 确认操作
	fmt.Printf("\n确认%s以上 %d 个节点? (y/n): ", action, len(selectedIndices))
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(strings.ToLower(confirm))
	
	if confirm != "y" && confirm != "yes" {
		fmt.Println("操作已取消。")
		return
	}
	
	// 执行批量授权/取消授权
	successCount := 0
	failCount := 0
	
	for _, index := range selectedIndices {
		node := nodes[index-1]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		
		// 更新授权状态
		authValue := 0
		if authorize {
			authValue = 1
		}

		update := bson.M{
			"$set": bson.M{
				"is_authorized":  authValue,
				"last_heartbeat": time.Now(),
			},
		}

		result, err := collection.UpdateOne(ctx, bson.M{"node_id": node.NodeID}, update)
		cancel()
		
		if err != nil {
			fmt.Printf("更新节点 %s 失败: %v\n", node.NodeID, err)
			failCount++
			continue
		}

		if result.MatchedCount == 0 {
			fmt.Printf("节点 %s 不存在\n", node.NodeID)
			failCount++
		} else {
			fmt.Printf("成功%s节点 %s\n", action, node.NodeID)
			successCount++
		}
	}
	
	fmt.Printf("\n批量%s完成: 成功 %d 个, 失败 %d 个\n", action, successCount, failCount)
} 

// 授权或取消授权所有节点
func authorizeAllNodes(collection *mongo.Collection, reader *bufio.Reader, authorize bool) {
	// 获取所有节点
	nodes := listNodes(collection)
	if len(nodes) == 0 {
		fmt.Println("没有找到任何节点。")
		return
	}
	
	// 显示节点数量
	displayNodes(nodes)
	
	// 确认操作
	action := "授权"
	if !authorize {
		action = "取消授权"
	}
	
	fmt.Printf("\n确认%s所有 %d 个节点? (y/n): ", action, len(nodes))
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(strings.ToLower(confirm))
	
	if confirm != "y" && confirm != "yes" {
		fmt.Println("操作已取消。")
		return
	}
	
	// 执行批量授权/取消授权
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// 更新授权状态
	authValue := 0
	if authorize {
		authValue = 1
	}

	update := bson.M{
		"$set": bson.M{
			"is_authorized":  authValue,
			"last_heartbeat": time.Now(),
		},
	}

	result, err := collection.UpdateMany(ctx, bson.M{}, update)
	if err != nil {
		fmt.Printf("更新节点授权状态失败: %v\n", err)
		return
	}

	fmt.Printf("\n成功%s了 %d 个节点\n", action, result.ModifiedCount)
} 