package auth

import (
	"alpha-autosell-bot/internal/common"
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// 节点授权集合名称
	nodeAuthCollection = "node_auth"
	// 账户集合名称
	accountCollection = "accounts"
)

// NodeAuth 节点授权信息
type NodeAuth struct {
	NodeID      string    `bson:"node_id"`
	Status      string    `bson:"status"` // authorized, unauthorized
	AuthorizedAt time.Time `bson:"authorized_at,omitempty"`
	RevokedAt   time.Time `bson:"revoked_at,omitempty"`
	LastSeen    time.Time `bson:"last_seen"`
	Metadata    map[string]interface{} `bson:"metadata,omitempty"`
}

// AccountInfo 账户信息
type AccountInfo struct {
	AccountID    string                 `bson:"account_id"`
	Name         string                 `bson:"name"`
	Csrftoken    string                 `bson:"csrftoken"`
	Cookie       string                 `bson:"cookie"`
	Status       string                 `bson:"status"`
	Description  string                 `bson:"description"`
	AssignedNode string                 `bson:"assigned_node"`
	KYCInfo      string                 `bson:"kyc_info,omitempty"`
	CreateAt     time.Time              `bson:"create_at"`
	UpdateAt     time.Time              `bson:"update_at"`
	LastSeen     time.Time              `bson:"last_seen"`
	Metadata     map[string]interface{} `bson:"metadata,omitempty"`
}

// MongoAuthManager MongoDB授权管理器
type MongoAuthManager struct {
	connManager  *MongoDBConnManager
	nodeAuth     *mongo.Collection
	accounts     *mongo.Collection
	users        *mongo.Collection
	licenses     *mongo.Collection
	ctx          context.Context
	cancelFunc   context.CancelFunc
	config       *common.MongoDBConfig
}

// NewMongoAuthManager 创建新的MongoDB授权管理器
func NewMongoAuthManager() (*MongoAuthManager, error) {
	return NewMongoAuthManagerWithConfig(nil)
}

// NewMongoAuthManagerWithConfig 使用指定配置创建新的MongoDB授权管理器
func NewMongoAuthManagerWithConfig(config *common.MongoDBConfig) (*MongoAuthManager, error) {
	// 如果没有提供配置，使用默认配置
	if config == nil {
		config = &common.MongoDBConfig{
			URI:            mongoURI,
			Database:       dbName,
			Enabled:        true,
			MaxPoolSize:    20,
			MinPoolSize:    5,
			MaxConnIdleTime: 300,
		}
	}

	// 获取连接管理器
	connManager, err := GetMongoDBConnManager(config)
	if err != nil {
		return nil, fmt.Errorf("获取MongoDB连接管理器失败: %v", err)
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 获取集合
	db := connManager.GetDatabase()
	if db == nil {
		cancel()
		return nil, fmt.Errorf("获取MongoDB数据库失败")
	}

	manager := &MongoAuthManager{
		connManager: connManager,
		nodeAuth:    db.Collection(nodeAuthCollection),
		accounts:    db.Collection(accountCollection),
		users:       db.Collection(userCollection),
		licenses:    db.Collection(licenseCollection),
		ctx:         ctx,
		cancelFunc:  cancel,
		config:      config,
	}

	log.Println("🔌 MongoDB授权管理器初始化成功")
	return manager, nil
}

// Close 关闭MongoDB连接
func (m *MongoAuthManager) Close() {
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	// 不需要关闭连接，连接由连接管理器管理
	log.Println("🔌 MongoDB授权管理器已关闭")
}

// IsNodeAuthorized 检查节点是否已授权
func (m *MongoAuthManager) IsNodeAuthorized(nodeID string) bool {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	var nodeAuth NodeAuth
	err := m.nodeAuth.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&nodeAuth)
	if err != nil {
		return false
	}

	// 更新最后在线时间
	_, _ = m.nodeAuth.UpdateOne(
		ctx,
		bson.M{"node_id": nodeID},
		bson.M{"$set": bson.M{"last_seen": time.Now()}},
	)

	return nodeAuth.Status == "authorized"
}

// AuthorizeNode 授权节点
func (m *MongoAuthManager) AuthorizeNode(nodeID string, metadata map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	now := time.Now()
	
	// 查找节点
	var nodeAuth NodeAuth
	err := m.nodeAuth.FindOne(ctx, bson.M{"node_id": nodeID}).Decode(&nodeAuth)
	
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// 创建新节点授权记录
			nodeAuth = NodeAuth{
				NodeID:       nodeID,
				Status:       "authorized",
				AuthorizedAt: now,
				LastSeen:     now,
				Metadata:     metadata,
			}
			
			_, err := m.nodeAuth.InsertOne(ctx, nodeAuth)
			if err != nil {
				return fmt.Errorf("创建节点授权记录失败: %v", err)
			}
		} else {
			return fmt.Errorf("查询节点授权状态失败: %v", err)
		}
	} else {
		// 更新现有节点授权记录
		update := bson.M{
			"$set": bson.M{
				"status":        "authorized",
				"authorized_at": now,
				"last_seen":     now,
			},
			"$unset": bson.M{"revoked_at": ""},
		}
		
		// 如果提供了元数据，则更新
		if metadata != nil {
			update["$set"].(bson.M)["metadata"] = metadata
		}
		
		_, err := m.nodeAuth.UpdateOne(ctx, bson.M{"node_id": nodeID}, update)
		if err != nil {
			return fmt.Errorf("更新节点授权状态失败: %v", err)
		}
	}

	log.Printf("✅ 节点 %s 已授权", nodeID)
	return nil
}

// RevokeNodeAuthorization 撤销节点授权
func (m *MongoAuthManager) RevokeNodeAuthorization(nodeID string) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	now := time.Now()
	
	// 更新节点授权状态
	_, err := m.nodeAuth.UpdateOne(
		ctx,
		bson.M{"node_id": nodeID},
		bson.M{
			"$set": bson.M{
				"status":     "unauthorized",
				"revoked_at": now,
				"last_seen":  now,
			},
		},
	)
	
	if err != nil {
		return fmt.Errorf("撤销节点授权失败: %v", err)
	}

	log.Printf("⚠️ 节点 %s 授权已撤销", nodeID)
	return nil
}

// GetAuthorizedNodes 获取所有已授权的节点
func (m *MongoAuthManager) GetAuthorizedNodes() ([]NodeAuth, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	cursor, err := m.nodeAuth.Find(ctx, bson.M{"status": "authorized"})
	if err != nil {
		return nil, fmt.Errorf("查询已授权节点失败: %v", err)
	}
	defer cursor.Close(ctx)

	var nodes []NodeAuth
	if err := cursor.All(ctx, &nodes); err != nil {
		return nil, fmt.Errorf("解析节点数据失败: %v", err)
	}

	return nodes, nil
}

// GetAccountAuth 获取账号认证信息
func (m *MongoAuthManager) GetAccountAuth(accountID string) (*AccountInfo, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	var account AccountInfo
	err := m.accounts.FindOne(ctx, bson.M{"account_id": accountID}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("账号不存在: %s", accountID)
		}
		return nil, fmt.Errorf("查询账号失败: %v", err)
	}

	// 更新最后在线时间
	_, _ = m.accounts.UpdateOne(
		ctx,
		bson.M{"account_id": accountID},
		bson.M{"$set": bson.M{"last_seen": time.Now()}},
	)

	return &account, nil
}

// AddAccount 添加账号
func (m *MongoAuthManager) AddAccount(account *AccountInfo) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	// 检查账号是否已存在
	count, err := m.accounts.CountDocuments(ctx, bson.M{"account_id": account.AccountID})
	if err != nil {
		return fmt.Errorf("检查账号是否存在失败: %v", err)
	}

	now := time.Now()
	
	if count > 0 {
		return fmt.Errorf("账号已存在: %s", account.AccountID)
	}

	// 设置创建时间和更新时间
	account.CreateAt = now
	account.UpdateAt = now
	account.LastSeen = now

	// 插入账号
	_, err = m.accounts.InsertOne(ctx, account)
	if err != nil {
		return fmt.Errorf("添加账号失败: %v", err)
	}

	log.Printf("✅ 账号 %s 已添加", account.AccountID)
	return nil
}

// UpdateAccount 更新账号
func (m *MongoAuthManager) UpdateAccount(accountID string, updates *AccountInfo) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	// 构建更新文档
	updateDoc := bson.M{
		"$set": bson.M{
			"update_at": time.Now(),
			"last_seen": time.Now(),
		},
	}

	// 添加非空字段到更新文档
	if updates.Name != "" {
		updateDoc["$set"].(bson.M)["name"] = updates.Name
	}
	if updates.Csrftoken != "" {
		updateDoc["$set"].(bson.M)["csrftoken"] = updates.Csrftoken
	}
	if updates.Cookie != "" {
		updateDoc["$set"].(bson.M)["cookie"] = updates.Cookie
	}
	if updates.Status != "" {
		updateDoc["$set"].(bson.M)["status"] = updates.Status
	}
	if updates.Description != "" {
		updateDoc["$set"].(bson.M)["description"] = updates.Description
	}
	if updates.AssignedNode != "" {
		updateDoc["$set"].(bson.M)["assigned_node"] = updates.AssignedNode
	}
	if updates.KYCInfo != "" {
		updateDoc["$set"].(bson.M)["kyc_info"] = updates.KYCInfo
	}
	if updates.Metadata != nil {
		updateDoc["$set"].(bson.M)["metadata"] = updates.Metadata
	}

	// 执行更新
	result, err := m.accounts.UpdateOne(ctx, bson.M{"account_id": accountID}, updateDoc)
	if err != nil {
		return fmt.Errorf("更新账号失败: %v", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	log.Printf("✅ 账号 %s 已更新", accountID)
	return nil
}

// RemoveAccount 删除账号
func (m *MongoAuthManager) RemoveAccount(accountID string) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	result, err := m.accounts.DeleteOne(ctx, bson.M{"account_id": accountID})
	if err != nil {
		return fmt.Errorf("删除账号失败: %v", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	log.Printf("✅ 账号 %s 已删除", accountID)
	return nil
}

// GetAllAccounts 获取所有账号
func (m *MongoAuthManager) GetAllAccounts() ([]*AccountInfo, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	cursor, err := m.accounts.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("查询所有账号失败: %v", err)
	}
	defer cursor.Close(ctx)

	var accounts []*AccountInfo
	if err := cursor.All(ctx, &accounts); err != nil {
		return nil, fmt.Errorf("解析账号数据失败: %v", err)
	}

	return accounts, nil
}

// GetNodeAccounts 获取分配给指定节点的所有账号
func (m *MongoAuthManager) GetNodeAccounts(nodeID string) ([]*AccountInfo, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	cursor, err := m.accounts.Find(ctx, bson.M{"assigned_node": nodeID})
	if err != nil {
		return nil, fmt.Errorf("查询节点账号失败: %v", err)
	}
	defer cursor.Close(ctx)

	var accounts []*AccountInfo
	if err := cursor.All(ctx, &accounts); err != nil {
		return nil, fmt.Errorf("解析账号数据失败: %v", err)
	}

	return accounts, nil
}

// UpdateAccountKYC 更新账号KYC信息
func (m *MongoAuthManager) UpdateAccountKYC(accountID string, kycInfo string) error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	// 更新账号KYC信息
	result, err := m.accounts.UpdateOne(
		ctx,
		bson.M{"account_id": accountID},
		bson.M{
			"$set": bson.M{
				"kyc_info":  kycInfo,
				"update_at": time.Now(),
				"last_seen": time.Now(),
			},
		},
	)
	
	if err != nil {
		return fmt.Errorf("更新账号KYC信息失败: %v", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("账号不存在: %s", accountID)
	}

	log.Printf("✅ 账号 %s 的KYC信息已更新", accountID)
	return nil
} 

// GetClient 获取MongoDB客户端
func (m *MongoAuthManager) GetClient() *mongo.Client {
	return m.connManager.GetClient()
} 