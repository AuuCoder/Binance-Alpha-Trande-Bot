package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// MongoDB连接URI
	defaultMongoURI = "mongodb+srv://auucoder:muyuai0208..@alpha.mpn1slf.mongodb.net/?retryWrites=true&w=majority&appName=alpha"
	// 数据库名称
	dbName = "alpha"
	// 节点集合名称
	nodeCollection = "nodes"
)

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

func main() {
	// 命令行参数
	mongoURI := flag.String("uri", defaultMongoURI, "MongoDB连接URI")
	action := flag.String("action", "", "操作类型: list, authorize, unauthorize, batch-authorize, batch-unauthorize")
	nodeID := flag.String("node", "", "节点ID (用于单个节点操作)")
	inputFile := flag.String("file", "", "包含节点ID列表的文件路径 (用于批量操作)")
	outputFile := flag.String("output", "nodes_list.json", "节点列表输出文件 (用于list操作)")

	flag.Parse()

	// 验证参数
	if *action == "" {
		log.Fatal("必须指定操作类型: list, authorize, unauthorize, batch-authorize, batch-unauthorize")
	}

	// 连接MongoDB
	client, err := connectMongoDB(*mongoURI)
	if err != nil {
		log.Fatalf("连接MongoDB失败: %v", err)
	}
	defer client.Disconnect(context.Background())

	// 获取集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 执行操作
	switch *action {
	case "list":
		listNodes(collection, *outputFile)
	case "authorize":
		if *nodeID == "" {
			log.Fatal("必须指定节点ID")
		}
		authorizeNode(collection, *nodeID, true)
	case "unauthorize":
		if *nodeID == "" {
			log.Fatal("必须指定节点ID")
		}
		authorizeNode(collection, *nodeID, false)
	case "batch-authorize":
		if *inputFile == "" {
			log.Fatal("必须指定包含节点ID列表的文件路径")
		}
		batchAuthorizeNodes(collection, *inputFile, true)
	case "batch-unauthorize":
		if *inputFile == "" {
			log.Fatal("必须指定包含节点ID列表的文件路径")
		}
		batchAuthorizeNodes(collection, *inputFile, false)
	default:
		log.Fatalf("未知操作类型: %s", *action)
	}
}

// 连接MongoDB
func connectMongoDB(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// 检查连接
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	fmt.Println("成功连接到MongoDB")
	return client, nil
}

// 列出所有节点
func listNodes(collection *mongo.Collection, outputFile string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatalf("查询节点失败: %v", err)
	}
	defer cursor.Close(ctx)

	var nodes []NodeInfo
	if err = cursor.All(ctx, &nodes); err != nil {
		log.Fatalf("解析节点数据失败: %v", err)
	}

	// 输出到控制台
	fmt.Printf("共找到 %d 个节点:\n", len(nodes))
	for i, node := range nodes {
		authStatus := "未授权"
		if node.IsAuthorized == 1 {
			authStatus = "已授权"
		}
		fmt.Printf("%d. 节点ID: %s, 状态: %s, 用户: %s, 币安ID: %s, 主机名: %s\n",
			i+1, node.NodeID, authStatus, node.User, node.BinanceID, node.Hostname)
	}

	// 保存到文件
	nodesJSON, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		log.Fatalf("序列化节点数据失败: %v", err)
	}

	err = ioutil.WriteFile(outputFile, nodesJSON, 0644)
	if err != nil {
		log.Fatalf("保存节点数据到文件失败: %v", err)
	}

	fmt.Printf("节点列表已保存到 %s\n", outputFile)
}

// 授权或取消授权单个节点
func authorizeNode(collection *mongo.Collection, nodeID string, authorize bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 检查节点是否存在
	var existingNode NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&existingNode)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			log.Fatalf("节点 %s 不存在", nodeID)
		}
		log.Fatalf("查询节点失败: %v", err)
	}

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

	_, err = collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
	if err != nil {
		log.Fatalf("更新节点授权状态失败: %v", err)
	}

	action := "授权"
	if !authorize {
		action = "取消授权"
	}

	fmt.Printf("成功%s节点 %s\n", action, nodeID)
}

// 批量授权或取消授权节点
func batchAuthorizeNodes(collection *mongo.Collection, inputFile string, authorize bool) {
	// 读取节点ID列表
	content, err := ioutil.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("读取文件失败: %v", err)
	}

	// 解析节点ID
	lines := strings.Split(string(content), "\n")
	var nodeIDs []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			nodeIDs = append(nodeIDs, line)
		}
	}

	if len(nodeIDs) == 0 {
		log.Fatal("文件中没有找到节点ID")
	}

	// 更新授权状态
	authValue := 0
	if authorize {
		authValue = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	action := "授权"
	if !authorize {
		action = "取消授权"
	}

	// 逐个处理节点
	successCount := 0
	failCount := 0
	for _, nodeID := range nodeIDs {
		update := bson.M{
			"$set": bson.M{
				"is_authorized":  authValue,
				"last_heartbeat": time.Now(),
			},
		}

		result, err := collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
		if err != nil {
			fmt.Printf("更新节点 %s 失败: %v\n", nodeID, err)
			failCount++
			continue
		}

		if result.MatchedCount == 0 {
			fmt.Printf("节点 %s 不存在\n", nodeID)
			failCount++
		} else {
			fmt.Printf("成功%s节点 %s\n", action, nodeID)
			successCount++
		}
	}

	fmt.Printf("批量%s完成: 成功 %d 个, 失败 %d 个\n", action, successCount, failCount)
} 