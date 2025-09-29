# 买单价格优化策略

## 🎯 优化目标

通过智能选择价格模式和动态调整加价策略，确保买单的**高成功率**，同时控制成本。

## 📊 核心策略

### **1. 买单专用价格模式选择**

#### **优先级策略**
```
用户指定模式 → 买单优化模式 → 降级策略
```

#### **模式映射表**
| 用户选择 | 买单实际使用 | 原因 | 加价策略 |
|----------|-------------|------|----------|
| `limit` | `market` | 限价价格可能滞后，用真实成交价确保成功 | +0.02% |
| `market` | `market` | 直接使用真实成交价，最准确 | +0.02% |
| `combined` | `market` | 避免限价订单影响，用纯市场价 | +0.02% |
| `auto` | `market` | 默认使用最可靠的市场价 | +0.02% |

### **2. 智能加价策略**

#### **首次买单加价**
```go
switch priceMode {
case PriceModeMarket:
    buyPrice = currentPrice * 1.0002  // +0.02% (万二)
case PriceModeLimit:
    buyPrice = currentPrice * 1.0001  // +0.01% (万一)
default:
    buyPrice = currentPrice * 1.0015  // +0.015% (万一点五)
}
```

#### **重试买单加价**
```go
// 正常模式
switch priceMode {
case PriceModeMarket:
    retryPrice = newPrice * (1.0003 + retryAttempt*0.0001)  // 万三起步，递增万一
case PriceModeLimit:
    retryPrice = newPrice * (1.0002 + retryAttempt*0.00005) // 万二起步，递增万0.5
default:
    retryPrice = newPrice * (1.0002 + retryAttempt*0.0001)  // 万二起步，递增万一
}

// 刷量模式
retryPrice = newPrice * (1.0003 + retryAttempt*0.0001)  // 更激进
```

### **3. 多层降级策略**

#### **价格获取降级**
```
1. 首选: Market模式 (真实成交价)
   ↓ 失败
2. 降级1: Combined模式 (综合信息)
   ↓ 失败  
3. 降级2: Limit模式 (限价订单)
   ↓ 失败
4. 报错: 所有模式都失败
```

#### **重试时降级**
```
1. 使用买单专用模式
   ↓ 失败
2. 降级到Combined模式
   ↓ 失败
3. 快速重试或报错
```

## 🔧 技术实现

### **核心函数**

#### **1. getBuyPriceMode() - 买单专用模式选择**
```go
func getBuyPriceMode(req *TradeRequest) price.PriceMode {
    switch strings.ToLower(req.PriceMode) {
    case "limit":
        // 限价模式切换到market确保成功率
        return price.PriceModeMarket
    case "market":
        return price.PriceModeMarket
    case "combined":
        // 综合模式切换到market避免限价影响
        return price.PriceModeMarket
    default:
        return price.PriceModeMarket  // 默认使用market
    }
}
```

#### **2. 智能价格计算**
```go
// 根据价格模式调整加价幅度
switch buyPriceMode {
case price.PriceModeMarket:
    buyPrice = currentPrice * 1.0002  // 链上模式加万二
case price.PriceModeLimit:
    buyPrice = currentPrice * 1.0001  // 限价模式加万一
default:
    buyPrice = currentPrice * 1.0015  // 其他模式加万一点五
}
```

#### **3. 降级策略实现**
```go
// 首选模式失败时的降级逻辑
if err != nil {
    // 降级策略1: 尝试combined模式
    if buyPriceMode != price.PriceModeCombined {
        currentPrice, err = price.GetTokenPriceWithPrecisionAndMode(
            req.TokenAddress, getChainID(req), req.PricePrecision, 
            price.PriceModeCombined)
        if err == nil {
            goto priceObtained
        }
    }
    
    // 降级策略2: 尝试limit模式
    currentPrice, err = price.GetTokenPriceWithPrecisionAndMode(
        req.TokenAddress, getChainID(req), req.PricePrecision, 
        price.PriceModeLimit)
}
```

## 📈 优化效果

### **成功率提升**
- **Market模式**: 使用真实成交价，避免价格滞后
- **智能加价**: 根据模式特点调整加价幅度
- **多层降级**: 确保在网络波动时仍能获取价格
- **重试优化**: 递增加价策略提高重试成功率

### **成本控制**
- **精确加价**: 避免过度加价造成损失
- **模式优化**: 选择最适合的价格源
- **动态调整**: 根据重试次数智能调整策略

## 🎯 使用建议

### **推荐配置**
```json
{
  "price_mode": "market",     // 推荐使用market模式
  "price_precision": 8,       // 8位精度平衡准确性和性能
  "auto_loop": true,          // 启用自动循环
  "target_volume": 1000       // 根据需求设置目标交易额
}
```

### **不同场景的最佳实践**

#### **高频交易场景**
- 使用 `market` 模式
- 较低的加价幅度 (+0.01%)
- 快速重试策略

#### **大额交易场景**
- 使用 `limit` 模式
- 适中的加价幅度 (+0.02%)
- 更多重试次数

#### **网络不稳定场景**
- 使用 `auto` 模式
- 启用所有降级策略
- 延长超时时间

## 📊 监控指标

### **关键指标**
- **买单成功率**: 目标 >95%
- **价格获取延迟**: 目标 <2秒
- **加价成本**: 目标 <0.05%
- **重试次数**: 目标 <3次

### **告警阈值**
- 买单成功率 <90%
- 价格获取失败率 >10%
- 平均重试次数 >5次
- 价格获取延迟 >5秒

## 🔍 故障排查

### **常见问题**

#### **1. 买单成功率低**
- 检查价格模式选择
- 调整加价策略
- 验证网络连接

#### **2. 价格获取失败**
- 检查WebSocket连接
- 验证降级策略
- 检查API限制

#### **3. 成本过高**
- 优化加价幅度
- 检查重试策略
- 分析价格波动

## 🚀 未来优化

### **计划改进**
1. **机器学习价格预测**: 基于历史数据预测最佳买入时机
2. **动态加价算法**: 根据市场波动性动态调整加价幅度
3. **多交易所价格对比**: 集成多个数据源提高准确性
4. **实时成功率监控**: 自动调整策略参数

### **A/B测试计划**
- 不同加价策略的成功率对比
- 各种价格模式的性能测试
- 降级策略的有效性验证
