package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

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
	// 连接MongoDB
	client, err := connectMongoDB()
	if err != nil {
		log.Fatalf("❌ 连接MongoDB失败: %v", err)
	}
	defer client.Disconnect(context.Background())

	reader := bufio.NewReader(os.Stdin)
	
	for {
		// 显示主菜单
		fmt.Println("\n========== MongoDB节点授权管理工具 ==========")
		fmt.Println("1. 列出所有节点")
		fmt.Println("2. 授权指定节点")
		fmt.Println("3. 撤销指定节点的授权")
		fmt.Println("4. 删除指定节点")
		fmt.Println("5. 设置节点的用户名")
		fmt.Println("6. 设置节点的币安ID")
		fmt.Println("7. 授权所有节点")
		fmt.Println("8. 撤销所有节点的授权")
		fmt.Println("0. 退出程序")
		fmt.Print("\n请输入选项 [0-8]: ")
		
		// 读取用户输入
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		
		// 处理用户选择
		switch input {
		case "1":
		listNodes(client)
		case "2":
			fmt.Print("请输入要授权的节点ID: ")
			nodeID, _ := reader.ReadString('\n')
			nodeID = strings.TrimSpace(nodeID)
			if nodeID != "" {
				authorizeNode(client, nodeID)
			} else {
				fmt.Println("❌ 节点ID不能为空")
			}
		case "3":
			fmt.Print("请输入要撤销授权的节点ID: ")
			nodeID, _ := reader.ReadString('\n')
			nodeID = strings.TrimSpace(nodeID)
			if nodeID != "" {
				revokeNode(client, nodeID)
			} else {
				fmt.Println("❌ 节点ID不能为空")
			}
		case "4":
			fmt.Print("请输入要删除的节点ID: ")
			nodeID, _ := reader.ReadString('\n')
			nodeID = strings.TrimSpace(nodeID)
			if nodeID != "" {
				// 二次确认
				fmt.Printf("⚠️ 确认删除节点 %s? (y/n): ", nodeID)
				confirm, _ := reader.ReadString('\n')
				confirm = strings.TrimSpace(confirm)
				if strings.ToLower(confirm) == "y" {
					deleteNode(client, nodeID)
				} else {
					fmt.Println("❌ 已取消删除操作")
				}
			} else {
				fmt.Println("❌ 节点ID不能为空")
			}
		case "5":
			fmt.Print("请输入节点ID: ")
			nodeID, _ := reader.ReadString('\n')
			nodeID = strings.TrimSpace(nodeID)
			
			if nodeID != "" {
				fmt.Print("请输入用户名: ")
				userName, _ := reader.ReadString('\n')
				userName = strings.TrimSpace(userName)
				
				if userName != "" {
					setUserName(client, nodeID, userName)
				} else {
					fmt.Println("❌ 用户名不能为空")
				}
			} else {
				fmt.Println("❌ 节点ID不能为空")
			}
		case "6":
			fmt.Print("请输入节点ID: ")
			nodeID, _ := reader.ReadString('\n')
			nodeID = strings.TrimSpace(nodeID)
			
			if nodeID != "" {
				fmt.Print("请输入币安ID: ")
				binanceID, _ := reader.ReadString('\n')
				binanceID = strings.TrimSpace(binanceID)
				
				if binanceID != "" {
					setBinanceID(client, nodeID, binanceID)
				} else {
					fmt.Println("❌ 币安ID不能为空")
				}
			} else {
				fmt.Println("❌ 节点ID不能为空")
			}
		case "7":
			// 二次确认
			fmt.Print("⚠️ 确认授权所有节点? (y/n): ")
			confirm, _ := reader.ReadString('\n')
			confirm = strings.TrimSpace(confirm)
			if strings.ToLower(confirm) == "y" {
				authorizeAllNodes(client)
			} else {
				fmt.Println("❌ 已取消授权所有节点操作")
			}
		case "8":
			// 二次确认
			fmt.Print("⚠️ 确认撤销所有节点的授权? (y/n): ")
			confirm, _ := reader.ReadString('\n')
			confirm = strings.TrimSpace(confirm)
			if strings.ToLower(confirm) == "y" {
				revokeAllNodes(client)
			} else {
				fmt.Println("❌ 已取消撤销所有节点授权操作")
			}
		case "0":
			fmt.Println("👋 感谢使用MongoDB节点授权管理工具")
			return
		default:
			fmt.Println("❌ 无效选项，请重新输入")
		}
	}
}

// printUsage 打印使用说明
func printUsage() {
	fmt.Println("MongoDB节点授权管理工具")
	fmt.Println("用法:")
	fmt.Println("  list                       - 列出所有节点")
	fmt.Println("  auth <node_id>             - 授权指定节点")
	fmt.Println("  revoke <node_id>           - 撤销指定节点的授权")
	fmt.Println("  delete <node_id>           - 删除指定节点")
	fmt.Println("  set-user <node_id> <user>  - 设置节点的用户名")
	fmt.Println("  set-binance <node_id> <id> - 设置节点的币安ID")
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

// listNodes 列出所有节点
func listNodes(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 查询所有节点
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		fmt.Printf("❌ 查询节点失败: %v\n", err)
		return
	}
	defer cursor.Close(ctx)

	// 解码节点数据
	var nodes []NodeInfo
	if err := cursor.All(ctx, &nodes); err != nil {
		fmt.Printf("❌ 解码节点数据失败: %v\n", err)
		return
	}

	// 打印节点信息
	fmt.Printf("共找到 %d 个节点:\n", len(nodes))
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("%-20s %-12s %-12s %-10s %-10s %-15s\n", "节点ID", "用户名", "币安ID", "主机名", "授权状态", "注册时间")
	fmt.Println("------------------------------------------------------------")
	for _, node := range nodes {
		authStatus := "❌ 未授权"
		if node.IsAuthorized == 1 {
			authStatus = "✅ 已授权"
		}
		fmt.Printf("%-20s %-12s %-12s %-10s %-10s %-15s\n",
			truncateString(node.NodeID, 20),
			truncateString(node.User, 12),
			truncateString(node.BinanceID, 12),
			truncateString(node.Hostname, 10),
			authStatus,
			node.RegisterTime.Format("2006-01-02 15:04"),
		)
	}
	fmt.Println("------------------------------------------------------------")
}

// authorizeNode 授权指定节点
func authorizeNode(client *mongo.Client, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 检查节点是否存在
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("❌ 节点 %s 不存在\n", nodeID)
		} else {
			fmt.Printf("❌ 查询节点失败: %v\n", err)
		}
		return
	}

	// 更新节点授权状态
	update := bson.M{
		"$set": bson.M{
			"is_authorized": 1,
			"authorized_at": time.Now(),
		},
	}

	_, err = collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
	if err != nil {
		fmt.Printf("❌ 更新节点授权状态失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 节点 %s 已授权\n", nodeID)
}

// revokeNode 撤销指定节点的授权
func revokeNode(client *mongo.Client, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 检查节点是否存在
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("❌ 节点 %s 不存在\n", nodeID)
		} else {
			fmt.Printf("❌ 查询节点失败: %v\n", err)
		}
		return
	}

	// 更新节点授权状态
	update := bson.M{
		"$set": bson.M{
			"is_authorized": 0,
		},
	}

	_, err = collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
	if err != nil {
		fmt.Printf("❌ 更新节点授权状态失败: %v\n", err)
		return
	}

	fmt.Printf("⚠️ 节点 %s 的授权已撤销\n", nodeID)
}

// deleteNode 删除指定节点
func deleteNode(client *mongo.Client, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 检查节点是否存在
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("❌ 节点 %s 不存在\n", nodeID)
		} else {
			fmt.Printf("❌ 查询节点失败: %v\n", err)
		}
		return
	}

	// 确认删除
	fmt.Printf("确定要删除节点 %s (%s) 吗? [y/N]: ", nodeID, node.Hostname)
	var confirm string
	fmt.Scanln(&confirm)
	if strings.ToLower(confirm) != "y" {
		fmt.Println("❌ 操作已取消")
		return
	}

	// 删除节点
	_, err = collection.DeleteOne(ctx, bson.M{"node_id": nodeID})
	if err != nil {
		fmt.Printf("❌ 删除节点失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 节点 %s 已删除\n", nodeID)
}

// setUserName 设置节点的用户名
func setUserName(client *mongo.Client, nodeID string, userName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 检查节点是否存在
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("❌ 节点 %s 不存在\n", nodeID)
		} else {
			fmt.Printf("❌ 查询节点失败: %v\n", err)
		}
		return
	}

	// 更新用户名
	update := bson.M{
		"$set": bson.M{
			"user": userName,
		},
	}

	_, err = collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
	if err != nil {
		fmt.Printf("❌ 设置用户名失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 节点 %s 的用户名已更新为: %s\n", nodeID, userName)
}

// setBinanceID 设置节点的币安ID
func setBinanceID(client *mongo.Client, nodeID string, binanceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 检查节点是否存在
	var node NodeInfo
	err := collection.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&node)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("❌ 节点 %s 不存在\n", nodeID)
		} else {
			fmt.Printf("❌ 查询节点失败: %v\n", err)
		}
		return
	}

	// 更新币安ID
	update := bson.M{
		"$set": bson.M{
			"binance_id": binanceID,
		},
	}

	_, err = collection.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
	if err != nil {
		fmt.Printf("❌ 设置币安ID失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 节点 %s 的币安ID已更新为: %s\n", nodeID, binanceID)
}

// authorizeAllNodes 授权所有节点
func authorizeAllNodes(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 更新所有节点的授权状态
	update := bson.M{
		"$set": bson.M{
			"is_authorized": 1,
			"authorized_at": time.Now(),
		},
	}

	result, err := collection.UpdateMany(ctx, bson.M{}, update)
	if err != nil {
		fmt.Printf("❌ 授权所有节点失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 成功授权 %d 个节点\n", result.ModifiedCount)
}

// revokeAllNodes 撤销所有节点的授权
func revokeAllNodes(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取节点集合
	collection := client.Database(dbName).Collection(nodeCollection)

	// 更新所有节点的授权状态
	update := bson.M{
		"$set": bson.M{
			"is_authorized": 0,
		},
	}

	result, err := collection.UpdateMany(ctx, bson.M{}, update)
	if err != nil {
		fmt.Printf("❌ 撤销所有节点授权失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 成功撤销 %d 个节点的授权\n", result.ModifiedCount)
}

// truncateString 截断字符串
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
} 