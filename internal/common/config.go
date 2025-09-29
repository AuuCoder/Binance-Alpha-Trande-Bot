package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Config 应用配置
type Config struct {
	Server  ServerConfig  `json:"server"`
	Redis   RedisConfig   `json:"redis"`
	MongoDB *MongoDBConfig `json:"mongodb,omitempty"`
	GRPC    GRPCConfig    `json:"grpc"`
	Binance BinanceConfig `json:"binance"`
	Log     LogConfig     `json:"log"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port       int    `json:"port"`
	Host       string `json:"host"`
	Mode       string `json:"mode"` // master, slave
	NodeID     string `json:"node_id"`
	MasterAddr string `json:"master_addr"` // 主控端地址（仅slave模式需要）
}

// RedisConfig Redis配置
type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// GRPCConfig gRPC配置
type GRPCConfig struct {
	Port              int `json:"port"`
	MaxRecvMsgSize    int `json:"max_recv_msg_size"`
	MaxSendMsgSize    int `json:"max_send_msg_size"`
	ConnectionTimeout int `json:"connection_timeout"`
	KeepAliveTime     int `json:"keepalive_time"`
	KeepAliveTimeout  int `json:"keepalive_timeout"`
}

// BinanceConfig 币安配置
type BinanceConfig struct {
	BaseURL        string `json:"base_url"`
	WSUrl          string `json:"ws_url"`
	RequestTimeout int    `json:"request_timeout"`
	MaxRetries     int    `json:"max_retries"`
	RetryDelay     int    `json:"retry_delay"`
	PriceCacheTime int    `json:"price_cache_time"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level      string `json:"level"`
	Format     string `json:"format"`
	Output     string `json:"output"`
	MaxSize    int    `json:"max_size"`
	MaxBackups int    `json:"max_backups"`
	MaxAge     int    `json:"max_age"`
}

// MongoDBConfig MongoDB配置
type MongoDBConfig struct {
	URI      string `json:"uri"`      // MongoDB连接URI
	Database string `json:"database"` // 数据库名称
	Enabled  bool   `json:"enabled"`  // 是否启用
	// 连接池配置
	MaxPoolSize     uint64 `json:"max_pool_size"`     // 最大连接池大小
	MinPoolSize     uint64 `json:"min_pool_size"`     // 最小连接池大小
	MaxConnIdleTime int    `json:"max_conn_idle_time"` // 最大连接空闲时间(秒)
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:       28080,
			Host:       "0.0.0.0",
			Mode:       "master",
			NodeID:     "",
			MasterAddr: "",
		},
		Redis: RedisConfig{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
		MongoDB: &MongoDBConfig{
			URI:      "",
			Database: "alpha",
			Enabled:  false,
		},
		GRPC: GRPCConfig{
			Port:              29090,
			MaxRecvMsgSize:    4 * 1024 * 1024, // 4MB
			MaxSendMsgSize:    4 * 1024 * 1024, // 4MB
			ConnectionTimeout: 10,
			KeepAliveTime:     30,
			KeepAliveTimeout:  5,
		},
		Binance: BinanceConfig{
			BaseURL:        "https://www.binance.com",
			WSUrl:          "wss://nbstream.binance.com/w3w/wsa/stream",
			RequestTimeout: 10,
			MaxRetries:     3,
			RetryDelay:     1000,
			PriceCacheTime: 5,
		},
		Log: LogConfig{
			Level:      "info",
			Format:     "text",
			Output:     "stdout",
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     7,
		},
	}
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	// 如果配置文件不存在，创建默认配置
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Printf("配置文件不存在，创建默认配置: %s", configPath)
		config := DefaultConfig()
		if err := config.Save(configPath); err != nil {
			return nil, fmt.Errorf("保存默认配置失败: %v", err)
		}
		return config, nil
	}

	// 读取配置文件
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	// 处理BOM问题（Windows下常见）
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:] // 移除UTF-8 BOM
	}

	// 解析配置
	config := DefaultConfig()
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return config, nil
}

// Save 保存配置到文件
func (c *Config) Save(configPath string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %v", err)
	}

	if err := ioutil.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %v", err)
	}

	return nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("无效的服务器端口: %d", c.Server.Port)
	}

	if c.GRPC.Port <= 0 || c.GRPC.Port > 65535 {
		return fmt.Errorf("无效的gRPC端口: %d", c.GRPC.Port)
	}

	if c.Server.Mode != "master" && c.Server.Mode != "slave" {
		return fmt.Errorf("无效的服务器模式: %s", c.Server.Mode)
	}

	if c.Server.Mode == "slave" && c.Server.MasterAddr == "" {
		return fmt.Errorf("slave模式需要指定master地址")
	}

	if c.Redis.Addr == "" {
		return fmt.Errorf("Redis地址不能为空")
	}

	return nil
}

// IsMaster 是否为主控端
func (c *Config) IsMaster() bool {
	return c.Server.Mode == "master"
}

// IsSlave 是否为被控端
func (c *Config) IsSlave() bool {
	return c.Server.Mode == "slave"
}

// GetGRPCAddr 获取gRPC地址
func (c *Config) GetGRPCAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.GRPC.Port)
}

// GetServerAddr 获取HTTP服务器地址
func (c *Config) GetServerAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}
