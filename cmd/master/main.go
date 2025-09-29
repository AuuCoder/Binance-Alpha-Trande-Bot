package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"alpha-autosell-bot/internal/common"
	"alpha-autosell-bot/internal/master"
)

func main() {
	// 解析命令行参数
	configFile := flag.String("config", "configs/master.json", "配置文件路径")
	flag.Parse()

	// 加载配置
	log.Printf("📋 使用配置文件: %s", *configFile)
	config, err := common.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 确保MongoDB配置包含连接池设置
	if config.MongoDB != nil {
		if config.MongoDB.MaxPoolSize == 0 {
			config.MongoDB.MaxPoolSize = 20
		}
		if config.MongoDB.MinPoolSize == 0 {
			config.MongoDB.MinPoolSize = 5
		}
		if config.MongoDB.MaxConnIdleTime == 0 {
			config.MongoDB.MaxConnIdleTime = 300
		}
	}

	// 创建服务器实例
	server, err := master.NewServer(config)
	if err != nil {
		log.Fatalf("创建服务器失败: %v", err)
	}

	// 启动服务器
	go func() {
	if err := server.Start(); err != nil {
			log.Fatalf("服务器启动失败: %v", err)
		}
	}()

	log.Printf("主控端已启动，gRPC端口: %s, HTTP端口: %s", config.GetGRPCAddr(), config.GetServerAddr())

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n收到中断信号，正在关闭服务器...")
	server.Stop()
	fmt.Println("服务器已关闭")
}
