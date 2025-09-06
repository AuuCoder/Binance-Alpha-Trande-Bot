# Alpha AutoSell Bot

一个高效、智能的自动化加密货币交易机器人，专为快速交易和风险控制而设计。

## 🚀 核心特性

### 🔥 Flash Trade 核心引擎
- **超快速交易**: 毫秒级下单响应，抢占市场先机
- **智能递减策略**: 万1 → 万6 → 万11 → ... → 百10 的渐进式降价
- **多层风险控制**: 智能止损、磨损限制、异步监控
- **防卡单优化**: 超时时间统一、订单状态同步、竞态条件优化

### 🎯 交易策略
- **市场价策略**: 实时价格获取，精准定价
- **递减重试**: 21步精细化递减，最大化成交概率
- **挂单模式**: 达到磨损限制时智能挂单等待成交
- **异步处理**: 挂单异步监控，不阻塞新交易

### 🛡️ 风险管理
- **磨损控制**: 最大磨损限制 10% (百10)
- **智能止损**: 市场异常时快速止损
- **资金管理**: 动态余额检查，防止过度交易
- **网络容错**: 断网重连、API限频处理

### 📊 监控统计
- **实时统计**: 交易量、盈亏、成功率实时监控
- **账户管理**: 多账户支持，独立统计
- **交易日志**: 详细交易记录，便于分析优化

## 🏗️ 系统架构

### 🎛️ 三端分离架构

本系统采用主控端、被控端、网页端分离的架构设计，实现灵活部署和统一管理。

```
┌─────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│     🖥️ 主控端        │    │     🤖 被控端         │    │     🌐 网页端        │
│   Flash Trade       │◄──►│  Alpha AutoSell      │◄──►│   Web Dashboard      │
│   (端口 8080)        │    │   (端口 8081)        │    │   (静态网页)          │
│                     │    │                     │    │                     │
│ • 核心交易引擎        │    │ • 账户管理           │    │ • 监控面板           │
│ • 递减策略执行        │    │ • 自动循环           │    │ • 配置管理           │
│ • 风险控制           │    │ • 统计上报           │    │ • 数据可视化         │
│ • API 接口           │    │ • 远程控制           │    │ • 用户界面           │
└─────────────────────┘    └──────────────────────┘    └──────────────────────┘
           │                           │                           │
           ▼                           ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          🗄️ Redis 统一数据存储                              │
│             (账户信息、交易统计、配置管理、状态同步)                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 🎛️ 主控端 (Flash Trade)
**核心交易引擎 - 端口 8080**

#### 主要功能
- 🔥 **Flash Trade 引擎**: 毫秒级交易执行
- 🎯 **智能递减策略**: 21步精细化递减
- 🛡️ **风险控制系统**: 磨损限制、智能止损
- 📊 **交易统计**: 实时数据分析
- 🔌 **API 服务**: RESTful 接口提供

#### 核心特性
- 超快速下单响应 (< 100ms)
- 防卡单优化机制
- 异步挂单监控
- 网络容错处理
- 多账户并发支持

### 🤖 被控端 (Alpha AutoSell)
**账户管理系统 - 端口 8081**

#### 主要功能
- 👥 **多账户管理**: 统一管理多个交易账户
- 🔄 **自动循环交易**: 持续自动化交易
- 📈 **数据统计上报**: 向主控端同步数据
- 🎛️ **远程配置**: 接收主控端控制指令
- 🔍 **状态监控**: 账户状态实时监控

#### 核心特性
- 独立账户隔离
- 自动故障恢复
- 配置热更新
- 状态实时同步
- 远程控制响应

### 🌐 网页端 (Web Dashboard)
**可视化管理界面**

#### 主要功能
- 📊 **监控面板**: 实时交易数据展示
- 🎛️ **配置管理**: 参数设置和调整
- 📈 **数据可视化**: 图表分析交易表现
- 👥 **账户管理**: 账户状态和权限管理
- 🔧 **系统控制**: 启停服务和故障处理

#### 界面特性
- 响应式设计
- 实时数据更新
- 直观操作界面
- 移动端适配
- 主题切换支持

## 📦 部署方案

### 🚀 方案一：单机部署 (推荐新手)
```bash
# 同时运行主控端和被控端
./flash-trade &          # 主控端 (端口 8080)
./alpha-autosell &       # 被控端 (端口 8081)
```

### 🌐 方案二：分布式部署 (推荐生产)
```bash
# 服务器A - 主控端
./flash-trade

# 服务器B - 被控端1
./alpha-autosell --config=account1.json

# 服务器C - 被控端2  
./alpha-autosell --config=account2.json
```

### ☁️ 方案三：云端部署
```bash
# Docker 容器化部署
docker-compose up -d
```

## 🔨 编译打包

### Mac 平台打包
```bash
# 主控端编译
go build -o flash-trade-mac flash_trade.go

# 被控端编译  
go build -o alpha-autosell-mac alpha_autosell.go

# 网页端打包 (如果有前端构建)
cd web && npm run build
```

### Windows 平台打包
```bash
# 主控端编译
GOOS=windows GOARCH=amd64 go build -o flash-trade.exe flash_trade.go

# 被控端编译
GOOS=windows GOARCH=amd64 go build -o alpha-autosell.exe alpha_autosell.go
```

### Linux 平台打包
```bash
# 主控端编译
GOOS=linux GOARCH=amd64 go build -o flash-trade-linux flash_trade.go

# 被控端编译
GOOS=linux GOARCH=amd64 go build -o alpha-autosell-linux alpha_autosell.go
```

### 🎁 一键打包脚本
```bash
# 创建打包脚本
cat > build.sh << 'EOF'
#!/bin/bash

echo "🔨 开始编译 Alpha AutoSell Bot..."

# 清理旧文件
rm -rf dist/
mkdir -p dist/{mac,windows,linux}

# Mac 版本
echo "📱 编译 Mac 版本..."
go build -o dist/mac/flash-trade flash_trade.go
go build -o dist/mac/alpha-autosell alpha_autosell.go

# Windows 版本
echo "🪟 编译 Windows 版本..."
GOOS=windows GOARCH=amd64 go build -o dist/windows/flash-trade.exe flash_trade.go
GOOS=windows GOARCH=amd64 go build -o dist/windows/alpha-autosell.exe alpha_autosell.go

# Linux 版本
echo "🐧 编译 Linux 版本..."
GOOS=linux GOARCH=amd64 go build -o dist/linux/flash-trade flash_trade.go
GOOS=linux GOARCH=amd64 go build -o dist/linux/alpha-autosell alpha_autosell.go

# 复制配置文件和网页文件
echo "📋 复制配置文件..."
for platform in mac windows linux; do
    cp config.json dist/$platform/
    cp -r web dist/$platform/ 2>/dev/null || true
done

echo "✅ 编译完成！文件位于 dist/ 目录"
EOF

chmod +x build.sh
./build.sh
```

### 📦 Docker 部署
```dockerfile
# Dockerfile
FROM golang:1.19-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o flash-trade flash_trade.go
RUN go build -o alpha-autosell alpha_autosell.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/flash-trade .
COPY --from=builder /app/alpha-autosell .
COPY --from=builder /app/config.json .
COPY --from=builder /app/web ./web/

EXPOSE 8080 8081
CMD ["./flash-trade"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    
  flash-trade:
    build: .
    ports:
      - "8080:8080"
    depends_on:
      - redis
    command: ["./flash-trade"]
    
  alpha-autosell:
    build: .
    ports:
      - "8081:8081"  
    depends_on:
      - redis
    command: ["./alpha-autosell"]
```

## 🚀 快速启动指南

### 前置要求
- Go 1.19+
- Redis 服务器
- 稳定的网络连接

### 🎯 新手推荐启动方式

1. **克隆项目**
```bash
git clone https://github.com/yourusername/alpha-autosell-bot.git
cd alpha-autosell-bot
```

2. **配置 Redis**
```bash
# 安装 Redis (Mac)
brew install redis
redis-server

# 或使用 Docker
docker run -d -p 6379:6379 redis:alpine
```

3. **配置文件**
```bash
cp config.json.example config.json
# 编辑配置文件，填入Redis连接信息
```

4. **编译运行**
```bash
# 编译
go build -o flash-trade flash_trade.go
go build -o alpha-autosell alpha_autosell.go

# 运行主控端
./flash-trade &

# 运行被控端  
./alpha-autosell &
```

5. **访问控制台**
```
主控端: http://localhost:8080
被控端: http://localhost:8081
```

### ⚡ 一键启动脚本
```bash
# 创建启动脚本
cat > start.sh << 'EOF'
#!/bin/bash

echo "🚀 启动 Alpha AutoSell Bot..."

# 检查 Redis
if ! pgrep -x "redis-server" > /dev/null; then
    echo "📡 启动 Redis..."
    redis-server --daemonize yes
fi

# 编译程序
echo "🔨 编译程序..."
go build -o flash-trade flash_trade.go
go build -o alpha-autosell alpha_autosell.go

# 启动服务
echo "🎛️ 启动主控端 (端口 8080)..."
./flash-trade &
MAIN_PID=$!

echo "🤖 启动被控端 (端口 8081)..."
./alpha-autosell &  
AGENT_PID=$!

echo "✅ 启动完成！"
echo "主控端: http://localhost:8080"
echo "被控端: http://localhost:8081"
echo ""
echo "按 Ctrl+C 停止服务..."

# 等待中断信号
trap 'kill $MAIN_PID $AGENT_PID; exit' INT
wait
EOF

chmod +x start.sh
./start.sh
```

## ⚙️ 配置说明

### config.json 配置示例
```json
{
  "redis": {
    "host": "localhost",
    "port": 6379,
    "password": "",
    "db": 0
  },
  "server": {
    "port": 8080,
    "timeout": 30
  },
  "trading": {
    "max_loss_rate": 0.1,
    "quick_sell_threshold": 0.006
  }
}
```

### 关键配置参数
- `max_loss_rate`: 最大磨损率 (0.1 = 10%)
- `quick_sell_threshold`: 快速卖出阈值
- `redis`: Redis 连接配置
- `timeout`: 请求超时时间

## 🔧 API 接口

### 交易接口
```bash
# 执行交易
POST /trade
Content-Type: application/json

{
  "account_id": "user123",
  "token_address": "0x...",
  "base_asset": "TOKEN",
  "usdt_amount": 100,
  "csrftoken": "...",
  "cookie": "...",
  "auto_loop": true
}
```

### 监控接口
```bash
# 获取统计信息
GET /stats?account_id=user123

# 获取网络状态
GET /network-status

# 获取限频状态
GET /rate-limit-status
```

### 管理接口
```bash
# 清理挂单
POST /cleanup-orders
{
  "account_id": "user123",
  "force": true
}

# 暂停/恢复交易
POST /pause-trading
POST /resume-trading
```

## 🎯 交易流程详解

### 1. 买入阶段
```
价格获取 → 资金检查 → 下单 → 成交确认 → 启动卖出监控
```

### 2. 卖出策略
```
市场价尝试 → 递减重试 → 挂单等待 → 异步监控 → 强制处理
```

### 3. 递减步骤
```
万1(0.01%) → 万6(0.06%) → 万11(0.11%) → ... → 百10(10%)
```

### 4. 风控机制
```
智能止损 → 磨损控制 → 异步处理 → 兜底清理
```

## 📈 性能特性

- **响应速度**: 平均响应时间 < 100ms
- **并发处理**: 支持多账户并发交易
- **成交率**: 递减策略下成交率 > 95%
- **稳定性**: 7x24小时稳定运行
- **容错性**: 网络断线自动重连

## 🛠️ 高级功能

### 智能监控
- 异步挂单监控，1分钟超时自动处理
- 订单状态实时同步，避免卡单
- 网络异常自动重试机制

### 数据统计
- 实时交易量统计
- 盈亏分析报告
- 账户表现监控
- 风险指标追踪

### 系统管理
- 热重载配置更新
- 动态参数调整
- 日志级别控制
- 健康状态检查

## 🔒 安全特性

- 账户ID安全验证
- Cookie和Token加密存储
- API访问频率限制
- 异常交易自动拦截

## 📊 监控面板

Web控制台提供：
- 实时交易数据
- 账户统计信息
- 系统运行状态
- 风险控制面板
- 交易日志查看

## 🚨 风险提示

**⚠️ 投资有风险，使用需谨慎**

本系统为自动化交易工具，使用前请：
1. 充分了解加密货币交易风险
2. 合理设置资金规模
3. 定期监控交易情况
4. 做好风险控制措施

## 📞 技术支持

- **GitHub Issues**: 提交Bug反馈和功能建议
- **Wiki文档**: 查看详细使用说明
- **更新日志**: 关注版本更新信息

## 📄 开源协议

本项目采用 MIT 开源协议。

---

**⭐ 如果这个项目对你有帮助，请给个 Star ⭐**

---
*最后更新: 2025年9月*