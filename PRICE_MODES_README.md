# 币安价格模式兼容功能

## 📋 功能概述

本次更新为项目添加了对币安3种价格模式的完整支持，允许在获取价格时指定不同的数据源模式。

### 🎯 支持的价格模式

| 模式 | 代码 | WebSocket订阅格式 | 说明 |
|------|------|------------------|------|
| **限价模式** | `limit` | `came@{address}@{chainID}@limit@kline_15m` | 仅获取限价订单价格 |
| **链上模式** | `market` | `came@{address}@{chainID}@market@kline_15m` | 仅获取链上交易价格 |
| **链上+限价模式** | `combined` | `came@{address}@{chainID}@kline_15m` | 获取综合价格信息 |
| **自动模式** | `auto` | `came@{address}@{chainID}@kline_15m` | 系统自动选择（默认combined） |

## 🚀 快速开始

### 1. Flash Trade 接口使用

在 TradeRequest 中添加 `price_mode` 字段：

```json
{
  "token_address": "0xa2be3e48170a60119b5f0400c65f65f3158fbeee",
  "usdt_amount": 100,
  "base_asset": "USDT",
  "csrftoken": "your_csrf_token",
  "cookie": "your_cookie",
  "target_volume": 1000,
  "auto_loop": true,
  "price_precision": 8,
  "chain_id": "56",
  "price_mode": "combined"
}
```

### 2. 直接API调用

```go
import "./price"

// 获取指定模式的价格
price, err := price.GetTokenPriceWithMode(
    "0xa2be3e48170a60119b5f0400c65f65f3158fbeee", 
    "56", 
    price.PriceModeCombined
)

// 获取指定精度和模式的价格
price, err := price.GetTokenPriceWithPrecisionAndMode(
    "0xa2be3e48170a60119b5f0400c65f65f3158fbeee", 
    "56", 
    8, 
    price.PriceModeMarket
)

// 订阅指定模式的价格
priceChan, err := price.SubscribeTokenPriceWithMode(
    "0xa2be3e48170a60119b5f0400c65f65f3158fbeee", 
    "56", 
    price.PriceModeLimit
)
```

## 🔧 技术实现

### 核心组件修改

#### 1. **价格模式枚举** (`price/client.go`)
```go
type PriceMode string

const (
    PriceModeLimit      PriceMode = "limit"       // 限价模式
    PriceModeMarket     PriceMode = "market"      // 链上模式
    PriceModeCombined   PriceMode = "combined"    // 链上+限价模式
    PriceModeAuto       PriceMode = "auto"        // 自动选择模式
)
```

#### 2. **订阅参数构建** (`price/client.go`)
```go
func buildSubscriptionParam(contractAddress, chainID string, mode PriceMode) string {
    interval := getKlineInterval(chainID)
    
    switch mode {
    case PriceModeLimit:
        return fmt.Sprintf("came@%s@%s@limit@kline_%s", contractAddress, chainID, interval)
    case PriceModeMarket:
        return fmt.Sprintf("came@%s@%s@market@kline_%s", contractAddress, chainID, interval)
    case PriceModeCombined:
        return fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)
    default:
        return fmt.Sprintf("came@%s@%s@kline_%s", contractAddress, chainID, interval)
    }
}
```

#### 3. **TradeRequest 扩展** (`flash_trade.go`)
```go
type TradeRequest struct {
    // ... 现有字段 ...
    PriceMode      string  `json:"price_mode,omitempty" json_cn:"价格模式,omitempty"`
}
```

### 新增API函数

#### 价格获取
- `GetTokenPriceWithMode(address, chainID, mode)`
- `GetTokenPriceWithPrecisionAndMode(address, chainID, precision, mode)`
- `GetTokenPriceEmergencyWithMode(address, chainID, precision, mode)`

#### 价格订阅
- `SubscribeTokenPriceWithMode(address, chainID, mode)`
- `SubscribePriceWithMode(address, chainID, mode)`

#### 缓存管理
- `GetCachedPriceWithMode(address, chainID, mode)`

## 📊 使用场景

### 1. **限价模式 (limit)**
- **适用场景**: 需要精确限价交易
- **数据源**: 币安限价订单簿
- **优势**: 价格稳定，适合大额交易
- **示例**: 机构交易、大额套利

### 2. **链上模式 (market)**
- **适用场景**: 需要真实市场价格
- **数据源**: 链上实际交易记录
- **优势**: 反映真实成交价格
- **示例**: 价格监控、市场分析

### 3. **链上+限价模式 (combined)**
- **适用场景**: 需要全面价格信息
- **数据源**: 综合限价和链上数据
- **优势**: 信息最全面，推荐使用
- **示例**: 自动交易、风险控制

### 4. **自动模式 (auto)**
- **适用场景**: 大多数通用场景
- **数据源**: 系统自动选择（默认combined）
- **优势**: 无需手动选择，智能适配
- **示例**: 默认配置、快速集成

## 🔄 向后兼容性

### ✅ **完全兼容**
- 所有现有代码无需修改
- 现有API调用自动使用 `auto` 模式
- 现有配置文件继续有效
- WebSocket连接自动适配

### 🆕 **新功能**
- 可选择指定价格模式
- 支持模式切换和缓存
- 提供详细的模式日志
- 支持紧急模式切换

## 🧪 测试验证

### 构建和测试
```bash
# 给脚本执行权限
chmod +x build_and_test_price_modes.sh

# 运行构建和测试
./build_and_test_price_modes.sh
```

### 手动测试
```bash
# 构建测试程序
go build -o test_price_modes test_price_modes.go

# 运行测试
./test_price_modes
```

### 测试内容
- ✅ 所有4种价格模式的价格获取
- ✅ WebSocket订阅参数正确性
- ✅ 缓存机制有效性
- ✅ 错误处理和重试机制
- ✅ 性能和延迟测试

## 📈 性能优化

### 缓存策略
- **缓存时间**: 5秒内有效
- **缓存键**: 包含价格模式信息
- **缓存清理**: 30秒后自动清理
- **缓存命中**: 不同模式独立缓存

### 连接管理
- **连接复用**: 同一模式共享连接
- **自动重连**: 支持模式切换重连
- **负载均衡**: 智能分配连接资源

## ⚠️ 注意事项

### 1. **网络要求**
- 确保能访问币安WebSocket服务
- 不同模式可能有不同的延迟
- 建议使用稳定的网络连接

### 2. **模式选择**
- `combined` 模式信息最全面，推荐使用
- `market` 模式适合需要真实价格的场景
- `limit` 模式适合大额交易场景
- `auto` 模式适合大多数通用场景

### 3. **错误处理**
- 模式切换可能导致短暂的价格获取失败
- 建议实现重试机制
- 监控不同模式的成功率

## 🔗 相关文件

- `price/client.go` - 核心价格客户端
- `price/api.go` - 价格API接口
- `flash_trade.go` - Flash Trade主程序
- `price_mode_example.json` - 使用示例
- `test_price_modes.go` - 测试程序
- `build_and_test_price_modes.sh` - 构建脚本

## 📝 更新日志

### v1.0 (2025-08-13)
- ✅ 添加币安3种价格模式支持
- ✅ 扩展TradeRequest支持price_mode字段
- ✅ 新增模式相关API函数
- ✅ 完整的向后兼容性
- ✅ 详细的测试和文档
