package auth

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	// 集合名称
	userCollection = "users"
	licenseCollection = "licenses"
)

// User 用户信息结构
type User struct {
	UID      string    `bson:"uid"`
	Username string    `bson:"username"`
	Code     string    `bson:"code"`
	KYCInfo  string    `bson:"kyc_info"`
	CreateAt time.Time `bson:"create_at"`
	LastSeen time.Time `bson:"last_seen"`
}

// License 卡密信息结构
type License struct {
	Code      string    `bson:"code"`
	IsUsed    bool      `bson:"is_used"`
	UsedBy    string    `bson:"used_by,omitempty"`
	CreateAt  time.Time `bson:"create_at"`
	UsedAt    time.Time `bson:"used_at,omitempty"`
	ExpiresAt time.Time `bson:"expires_at"`
}

// AuthManager 认证管理器
type AuthManager struct {
	client     *mongo.Client
	db         *mongo.Database
	users      *mongo.Collection
	licenses   *mongo.Collection
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewAuthManager 创建新的认证管理器
func NewAuthManager() (*AuthManager, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	
	// 连接MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("连接MongoDB失败: %v", err)
	}

	// 检查连接
	err = client.Ping(ctx, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("MongoDB连接测试失败: %v", err)
	}

	

	// 创建认证管理器
	db := client.Database(dbName)
	return &AuthManager{
		client:     client,
		db:         db,
		users:      db.Collection(userCollection),
		licenses:   db.Collection(licenseCollection),
		ctx:        context.Background(),
		cancelFunc: cancel,
	}, nil
}

// Close 关闭MongoDB连接
func (am *AuthManager) Close() {
	if am.cancelFunc != nil {
		am.cancelFunc()
	}
	if am.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		am.client.Disconnect(ctx)
		log.Println("🔌 MongoDB连接已关闭")
	}
}

// VerifyLicense 验证卡密是否有效
func (am *AuthManager) VerifyLicense(code string) (bool, error) {
	ctx, cancel := context.WithTimeout(am.ctx, 5*time.Second)
	defer cancel()

	// 查找卡密
	var license License
	err := am.licenses.FindOne(ctx, bson.M{"code": code, "is_used": false, "expires_at": bson.M{"$gt": time.Now()}}).Decode(&license)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, errors.New("卡密无效或已被使用")
		}
		return false, fmt.Errorf("验证卡密时出错: %v", err)
	}

	return true, nil
}

// ActivateLicense 激活卡密并绑定到用户
func (am *AuthManager) ActivateLicense(code, uid, username string) error {
	ctx, cancel := context.WithTimeout(am.ctx, 5*time.Second)
	defer cancel()

	// 开始事务
	session, err := am.client.StartSession()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer session.EndSession(ctx)

	// 在事务中执行操作
	_, err = session.WithTransaction(ctx, func(sessionCtx mongo.SessionContext) (interface{}, error) {
		// 1. 查找并更新卡密状态
		result := am.licenses.FindOneAndUpdate(
			sessionCtx,
			bson.M{"code": code, "is_used": false, "expires_at": bson.M{"$gt": time.Now()}},
			bson.M{"$set": bson.M{
				"is_used": true,
				"used_by": uid,
				"used_at": time.Now(),
			}},
		)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				return nil, errors.New("卡密无效或已被使用")
			}
			return nil, fmt.Errorf("更新卡密状态失败: %v", result.Err())
		}

		// 2. 创建用户记录
		now := time.Now()
		user := User{
			UID:      uid,
			Username: username,
			Code:     code,
			CreateAt: now,
			LastSeen: now,
		}

		_, err := am.users.InsertOne(sessionCtx, user)
		if err != nil {
			return nil, fmt.Errorf("创建用户记录失败: %v", err)
		}

		return nil, nil
	})

	if err != nil {
		return err
	}

	log.Printf("✅ 用户 %s 已成功激活卡密 %s", username, code)
	return nil
}

// UpdateUserKYC 更新用户KYC信息
func (am *AuthManager) UpdateUserKYC(uid, kycInfo string) error {
	ctx, cancel := context.WithTimeout(am.ctx, 5*time.Second)
	defer cancel()

	// 查找用户
	var user User
	err := am.users.FindOne(ctx, bson.M{"uid": uid}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.New("用户不存在")
		}
		return fmt.Errorf("查询用户失败: %v", err)
	}

	// 更新KYC信息
	_, err = am.users.UpdateOne(
		ctx,
		bson.M{"uid": uid},
		bson.M{"$set": bson.M{
			"kyc_info": kycInfo,
			"last_seen": time.Now(),
		}},
	)
	if err != nil {
		return fmt.Errorf("更新KYC信息失败: %v", err)
	}

	log.Printf("✅ 用户 %s 的KYC信息已更新", uid)
	return nil
}

// GetUserByUID 通过UID获取用户信息
func (am *AuthManager) GetUserByUID(uid string) (*User, error) {
	ctx, cancel := context.WithTimeout(am.ctx, 5*time.Second)
	defer cancel()

	var user User
	err := am.users.FindOne(ctx, bson.M{"uid": uid}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil // 用户不存在
		}
		return nil, fmt.Errorf("查询用户失败: %v", err)
	}

	return &user, nil
}

// UpdateUserLastSeen 更新用户最后在线时间
func (am *AuthManager) UpdateUserLastSeen(uid string) error {
	ctx, cancel := context.WithTimeout(am.ctx, 5*time.Second)
	defer cancel()

	_, err := am.users.UpdateOne(
		ctx,
		bson.M{"uid": uid},
		bson.M{"$set": bson.M{"last_seen": time.Now()}},
	)
	if err != nil {
		return fmt.Errorf("更新用户在线时间失败: %v", err)
	}

	return nil
} 