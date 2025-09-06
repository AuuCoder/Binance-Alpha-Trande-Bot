package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// Client Redis客户端封装
type Client struct {
	rdb *redis.Client
	ctx context.Context
}

// Config Redis配置
type Config struct {
	Addr     string
	Password string
	DB       int
}

// NewClient 创建Redis客户端
func NewClient(config Config) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	ctx := context.Background()

	// 测试连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("❌ Redis连接失败: %v", err)
		return nil
	}

	log.Printf("✅ Redis连接成功: %s", config.Addr)
	return &Client{
		rdb: rdb,
		ctx: ctx,
	}
}

// GetNativeClient 获取原生 Redis 客户端
func (c *Client) GetNativeClient() *redis.Client {
	return c.rdb
}

// PublishCommand 发布命令到消息队列
func (c *Client) PublishCommand(channel string, command interface{}) error {
	data, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("序列化命令失败: %v", err)
	}

	err = c.rdb.Publish(c.ctx, channel, data).Err()
	if err != nil {
		return fmt.Errorf("发布命令失败: %v", err)
	}

	log.Printf("📤 命令已发布到频道 %s", channel)
	return nil
}

// SubscribeCommands 订阅命令频道
func (c *Client) SubscribeCommands(channels ...string) *redis.PubSub {
	pubsub := c.rdb.Subscribe(c.ctx, channels...)
	log.Printf("📥 已订阅频道: %v", channels)
	return pubsub
}

// SetNodeStatus 设置节点状态
func (c *Client) SetNodeStatus(nodeID string, status interface{}) error {
	key := fmt.Sprintf("node:status:%s", nodeID)
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}

	return c.rdb.Set(c.ctx, key, data, 30*time.Second).Err()
}

// GetNodeStatus 获取节点状态
func (c *Client) GetNodeStatus(nodeID string) ([]byte, error) {
	key := fmt.Sprintf("node:status:%s", nodeID)
	return c.rdb.Get(c.ctx, key).Bytes()
}

// GetAllNodeStatuses 获取所有节点状态
func (c *Client) GetAllNodeStatuses() (map[string][]byte, error) {
	keys, err := c.rdb.Keys(c.ctx, "node:status:*").Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)
	for _, key := range keys {
		data, err := c.rdb.Get(c.ctx, key).Bytes()
		if err != nil {
			continue
		}
		// 提取节点ID
		nodeID := key[len("node:status:"):]
		result[nodeID] = data
	}

	return result, nil
}

// SetCache 设置缓存
func (c *Client) SetCache(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.rdb.Set(c.ctx, key, data, expiration).Err()
}

// GetCache 获取缓存
func (c *Client) GetCache(key string) ([]byte, error) {
	return c.rdb.Get(c.ctx, key).Bytes()
}

// DeleteCache 删除缓存
func (c *Client) DeleteCache(key string) error {
	return c.rdb.Del(c.ctx, key).Err()
}

// IncrementCounter 递增计数器
func (c *Client) IncrementCounter(key string) (int64, error) {
	return c.rdb.Incr(c.ctx, key).Result()
}

// SetExpire 设置过期时间
func (c *Client) SetExpire(key string, expiration time.Duration) error {
	return c.rdb.Expire(c.ctx, key, expiration).Err()
}

// AddToSet 添加到集合
func (c *Client) AddToSet(key string, members ...interface{}) error {
	return c.rdb.SAdd(c.ctx, key, members...).Err()
}

// RemoveFromSet 从集合移除
func (c *Client) RemoveFromSet(key string, members ...interface{}) error {
	return c.rdb.SRem(c.ctx, key, members...).Err()
}

// GetSetMembers 获取集合成员
func (c *Client) GetSetMembers(key string) ([]string, error) {
	return c.rdb.SMembers(c.ctx, key).Result()
}

// IsSetMember 检查是否为集合成员
func (c *Client) IsSetMember(key string, member interface{}) (bool, error) {
	return c.rdb.SIsMember(c.ctx, key, member).Result()
}

// PushToList 推送到列表
func (c *Client) PushToList(key string, values ...interface{}) error {
	return c.rdb.LPush(c.ctx, key, values...).Err()
}

// PopFromList 从列表弹出
func (c *Client) PopFromList(key string) (string, error) {
	return c.rdb.RPop(c.ctx, key).Result()
}

// GetListLength 获取列表长度
func (c *Client) GetListLength(key string) (int64, error) {
	return c.rdb.LLen(c.ctx, key).Result()
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Health 健康检查
func (c *Client) Health() error {
	_, err := c.rdb.Ping(c.ctx).Result()
	return err
}
