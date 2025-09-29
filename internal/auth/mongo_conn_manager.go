package auth

import (
	"alpha-autosell-bot/internal/common"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// 默认连接池配置
const (
	defaultMaxPoolSize     = 20
	defaultMinPoolSize     = 5
	defaultMaxConnIdleTime = 300 // 5分钟
)

// MongoDBConnManager MongoDB连接管理器
type MongoDBConnManager struct {
	client       *mongo.Client
	db           *mongo.Database
	ctx          context.Context
	cancelFunc   context.CancelFunc
	isConnected  bool
	connMutex    sync.RWMutex
	lastPingTime time.Time
	config       *common.MongoDBConfig
}

var (
	// 全局单例实例
	instance *MongoDBConnManager
	once     sync.Once
	mutex    sync.RWMutex
)

// GetMongoDBConnManager 获取MongoDB连接管理器单例
func GetMongoDBConnManager(config *common.MongoDBConfig) (*MongoDBConnManager, error) {
	mutex.RLock()
	if instance != nil && instance.isConnected {
		mutex.RUnlock()
		return instance, nil
	}
	mutex.RUnlock()

	mutex.Lock()
	defer mutex.Unlock()

	// 双重检查锁定
	if instance != nil && instance.isConnected {
		return instance, nil
	}

	// 创建新实例
	var err error
	once.Do(func() {
		instance = &MongoDBConnManager{
			config: config,
		}
		err = instance.Connect()
	})

	if err != nil {
		return nil, err
	}

	return instance, nil
}

// Connect 连接到MongoDB
func (m *MongoDBConnManager) Connect() error {
	m.connMutex.Lock()
	defer m.connMutex.Unlock()

	// 如果已经连接，先关闭
	if m.isConnected && m.client != nil {
		m.disconnect()
	}

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	m.ctx = context.Background()
	m.cancelFunc = cancel

	// 设置连接池选项
	maxPoolSize := defaultMaxPoolSize
	minPoolSize := defaultMinPoolSize
	maxConnIdleTime := defaultMaxConnIdleTime

	if m.config != nil {
		if m.config.MaxPoolSize > 0 {
			maxPoolSize = int(m.config.MaxPoolSize)
		}
		if m.config.MinPoolSize > 0 {
			minPoolSize = int(m.config.MinPoolSize)
		}
		if m.config.MaxConnIdleTime > 0 {
			maxConnIdleTime = m.config.MaxConnIdleTime
		}
	}

	// 连接MongoDB
	clientOptions := options.Client().
		ApplyURI(mongoURI).
		SetMaxPoolSize(uint64(maxPoolSize)).
		SetMinPoolSize(uint64(minPoolSize)).
		SetMaxConnIdleTime(time.Duration(maxConnIdleTime) * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		cancel()
		return fmt.Errorf("连接MongoDB失败: %v", err)
	}

	// 检查连接
	err = client.Ping(ctx, nil)
	if err != nil {
		cancel()
		return fmt.Errorf("MongoDB连接测试失败: %v", err)
	}

	m.client = client
	m.db = client.Database(dbName)
	m.isConnected = true
	m.lastPingTime = time.Now()

	log.Println("🔌 MongoDB连接管理器连接成功")
	log.Printf("📊 MongoDB连接池配置: 最大连接数=%d, 最小连接数=%d, 最大空闲时间=%d秒",
		maxPoolSize, minPoolSize, maxConnIdleTime)

	// 启动健康检查
	go m.startHealthCheck()

	return nil
}

// Close 关闭MongoDB连接
func (m *MongoDBConnManager) Close() {
	m.connMutex.Lock()
	defer m.connMutex.Unlock()

	m.disconnect()
}

// 内部方法，不加锁
func (m *MongoDBConnManager) disconnect() {
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.client.Disconnect(ctx)
		log.Println("🔌 MongoDB连接已关闭")
	}
	m.isConnected = false
}

// GetClient 获取MongoDB客户端
func (m *MongoDBConnManager) GetClient() *mongo.Client {
	m.connMutex.RLock()
	defer m.connMutex.RUnlock()

	return m.client
}

// GetDatabase 获取MongoDB数据库
func (m *MongoDBConnManager) GetDatabase() *mongo.Database {
	m.connMutex.RLock()
	defer m.connMutex.RUnlock()

	return m.db
}

// GetCollection 获取MongoDB集合
func (m *MongoDBConnManager) GetCollection(name string) *mongo.Collection {
	m.connMutex.RLock()
	defer m.connMutex.RUnlock()

	if m.db == nil {
		return nil
	}
	return m.db.Collection(name)
}

// IsConnected 检查是否已连接
func (m *MongoDBConnManager) IsConnected() bool {
	m.connMutex.RLock()
	defer m.connMutex.RUnlock()

	return m.isConnected
}

// Ping 测试连接
func (m *MongoDBConnManager) Ping() error {
	m.connMutex.RLock()
	client := m.client
	m.connMutex.RUnlock()

	if client == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		return fmt.Errorf("MongoDB ping失败: %v", err)
	}

	m.connMutex.Lock()
	m.lastPingTime = time.Now()
	m.connMutex.Unlock()

	return nil
}

// 启动健康检查
func (m *MongoDBConnManager) startHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Ping(); err != nil {
				log.Printf("⚠️ MongoDB健康检查失败: %v, 尝试重新连接", err)
				if err := m.Connect(); err != nil {
					log.Printf("❌ MongoDB重新连接失败: %v", err)
				} else {
					log.Println("✅ MongoDB重新连接成功")
				}
			}
		case <-m.ctx.Done():
			return
		}
	}
} 