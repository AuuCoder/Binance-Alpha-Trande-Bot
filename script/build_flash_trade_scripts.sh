#!/bin/bash

# Flash Trade 脚本构建脚本
# 支持多链和价格模式的分布式任务下发

echo "========================================="
echo "🔨 Flash Trade 脚本构建工具 v1.0"
echo "========================================="
echo "📦 构建支持多链和价格模式的任务下发脚本"
echo ""

# 设置构建目录
BUILD_DIR="script/build_flash_trade"
SCRIPT_DIR="script"

# 创建构建目录
echo "📁 创建构建目录..."
mkdir -p $BUILD_DIR

# 进入脚本目录
cd $SCRIPT_DIR

echo "🔧 开始构建脚本..."

# 1. 构建更新后的alpha.go脚本 (支持price_mode)
echo "📦 构建 alpha.go (多节点直连脚本)..."
GOOS=linux GOARCH=amd64 go build -o ../build_flash_trade/alpha_linux_amd64 alpha.go
GOOS=windows GOARCH=amd64 go build -o ../build_flash_trade/alpha_windows_amd64.exe alpha.go
GOOS=darwin GOARCH=amd64 go build -o ../build_flash_trade/alpha_mac_amd64 alpha.go
GOOS=darwin GOARCH=arm64 go build -o ../build_flash_trade/alpha_mac_arm64 alpha.go

# 2. 构建分布式Flash Trade脚本
echo "📦 构建 distributed_flash_trade.go (分布式任务下发脚本)..."
GOOS=linux GOARCH=amd64 go build -o ../build_flash_trade/distributed_flash_trade_linux_amd64 distributed_flash_trade.go
GOOS=windows GOARCH=amd64 go build -o ../build_flash_trade/distributed_flash_trade_windows_amd64.exe distributed_flash_trade.go
GOOS=darwin GOARCH=amd64 go build -o ../build_flash_trade/distributed_flash_trade_mac_amd64 distributed_flash_trade.go
GOOS=darwin GOARCH=arm64 go build -o ../build_flash_trade/distributed_flash_trade_mac_arm64 distributed_flash_trade.go

# 3. 构建快速Flash Trade脚本
echo "📦 构建 quick_flash_trade.go (快速任务下发脚本)..."
GOOS=linux GOARCH=amd64 go build -o ../build_flash_trade/quick_flash_trade_linux_amd64 quick_flash_trade.go
GOOS=windows GOARCH=amd64 go build -o ../build_flash_trade/quick_flash_trade_windows_amd64.exe quick_flash_trade.go
GOOS=darwin GOARCH=amd64 go build -o ../build_flash_trade/quick_flash_trade_mac_amd64 quick_flash_trade.go
GOOS=darwin GOARCH=arm64 go build -o ../build_flash_trade/quick_flash_trade_mac_arm64 quick_flash_trade.go

# 返回根目录
cd ..

# 复制配置文件
echo "📄 复制配置文件..."
cp script/config.json $BUILD_DIR/config.json

# 创建使用说明文档
echo "📝 创建使用说明文档..."
cat > $BUILD_DIR/README.md << 'EOF'
# Flash Trade 任务下发脚本集合

## 📋 脚本说明

### 1. alpha_* - 多节点直连脚本
- **功能**: 直接连接多个Flash Trade节点执行任务
- **特点**: 支持多链(BSC+Solana)和价格模式
- **配置**: 需要config.json配置文件
- **使用**: `./alpha_平台名称`

### 2. distributed_flash_trade_* - 分布式任务下发脚本
- **功能**: 通过Master节点下发分布式Flash Trade任务
- **特点**: 完整的分布式架构支持
- **配置**: 交互式输入所有参数
- **使用**: `./distributed_flash_trade_平台名称`

### 3. quick_flash_trade_* - 快速任务下发脚本
- **功能**: 使用预设配置快速下发任务
- **特点**: 预设多种常用配置，一键下发
- **配置**: 命令行参数指定
- **使用**: `./quick_flash_trade_平台名称 <代币地址> [预设配置]`

## 🔧 支持的链和价格模式

### 支持的区块链
- **BSC**: chain_id = "56"
- **Solana**: chain_id = "CT_501"

### 支持的价格模式
- **limit**: 限价模式 - 获取限价订单价格
- **market**: 市价模式 - 获取真实成交价格 (推荐买单使用)
- **combined**: 综合模式 - 获取综合价格信息
- **auto**: 自动模式 - 智能选择最佳模式

## 📝 使用示例

### 快速下发BSC代币任务
```bash
./quick_flash_trade_linux_amd64 0xa2be3e48170a60119b5f0400c65f65f3158fbeee BSC_MARKET_NORMAL
```

### 快速下发Solana代币任务
```bash
./quick_flash_trade_linux_amd64 SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL SOLANA_MARKET_NORMAL
```

### 分布式任务下发
```bash
./distributed_flash_trade_linux_amd64
# 然后按提示输入参数
```

### 多节点直连
```bash
# 先配置config.json文件
./alpha_linux_amd64
# 然后按提示输入参数
```

## ⚠️ 注意事项

1. **网络连接**: 确保能够访问Master节点或Flash Trade服务
2. **配置文件**: alpha脚本需要正确的config.json配置
3. **权限设置**: Linux/Mac需要执行权限 `chmod +x script_name`
4. **参数验证**: 确保代币地址、链ID、价格模式等参数正确

## 🚀 预设配置说明

### BSC链预设
- **BSC_MARKET_SMALL**: 小额测试 (10 USDT, 50目标量)
- **BSC_MARKET_NORMAL**: 正常交易 (100 USDT, 1000目标量)
- **BSC_LIMIT_NORMAL**: 限价模式正常交易

### Solana链预设
- **SOLANA_COMBINED_NORMAL**: 综合模式正常交易
- **SOLANA_MARKET_NORMAL**: 市价模式正常交易

## 📞 技术支持

如有问题请检查：
1. Master节点是否正常运行 (端口28080)
2. Flash Trade服务是否正常运行 (端口8080)
3. 网络连接是否正常
4. 参数格式是否正确
EOF

# 创建快速启动脚本
echo "🚀 创建快速启动脚本..."

# Linux快速启动脚本
cat > $BUILD_DIR/quick_start_linux.sh << 'EOF'
#!/bin/bash
echo "🚀 Flash Trade 快速启动脚本"
echo "选择要使用的脚本:"
echo "1. 快速任务下发 (推荐)"
echo "2. 分布式任务下发"
echo "3. 多节点直连"
read -p "请选择 (1-3): " choice

case $choice in
    1)
        echo "请输入代币地址:"
        read token_address
        echo "选择预设配置:"
        echo "1. BSC_MARKET_NORMAL (BSC链-市价模式)"
        echo "2. SOLANA_MARKET_NORMAL (Solana链-市价模式)"
        echo "3. BSC_MARKET_SMALL (BSC链-小额测试)"
        read -p "请选择 (1-3): " preset_choice
        
        case $preset_choice in
            1) ./quick_flash_trade_linux_amd64 $token_address BSC_MARKET_NORMAL ;;
            2) ./quick_flash_trade_linux_amd64 $token_address SOLANA_MARKET_NORMAL ;;
            3) ./quick_flash_trade_linux_amd64 $token_address BSC_MARKET_SMALL ;;
            *) echo "无效选择" ;;
        esac
        ;;
    2)
        ./distributed_flash_trade_linux_amd64
        ;;
    3)
        ./alpha_linux_amd64
        ;;
    *)
        echo "无效选择"
        ;;
esac
EOF

# Windows快速启动脚本
cat > $BUILD_DIR/quick_start_windows.bat << 'EOF'
@echo off
echo 🚀 Flash Trade 快速启动脚本
echo 选择要使用的脚本:
echo 1. 快速任务下发 (推荐)
echo 2. 分布式任务下发
echo 3. 多节点直连
set /p choice=请选择 (1-3): 

if "%choice%"=="1" (
    set /p token_address=请输入代币地址: 
    echo 选择预设配置:
    echo 1. BSC_MARKET_NORMAL (BSC链-市价模式)
    echo 2. SOLANA_MARKET_NORMAL (Solana链-市价模式)
    echo 3. BSC_MARKET_SMALL (BSC链-小额测试)
    set /p preset_choice=请选择 (1-3): 
    
    if "%preset_choice%"=="1" quick_flash_trade_windows_amd64.exe %token_address% BSC_MARKET_NORMAL
    if "%preset_choice%"=="2" quick_flash_trade_windows_amd64.exe %token_address% SOLANA_MARKET_NORMAL
    if "%preset_choice%"=="3" quick_flash_trade_windows_amd64.exe %token_address% BSC_MARKET_SMALL
) else if "%choice%"=="2" (
    distributed_flash_trade_windows_amd64.exe
) else if "%choice%"=="3" (
    alpha_windows_amd64.exe
) else (
    echo 无效选择
)
pause
EOF

# 设置执行权限
chmod +x $BUILD_DIR/quick_start_linux.sh
chmod +x $BUILD_DIR/alpha_linux_amd64
chmod +x $BUILD_DIR/distributed_flash_trade_linux_amd64
chmod +x $BUILD_DIR/quick_flash_trade_linux_amd64

echo ""
echo "========================================="
echo "🎉 构建完成！"
echo "========================================="
echo "📁 构建文件位置: $BUILD_DIR/"
echo ""
echo "📦 生成的文件:"
echo "   - alpha_* (多节点直连脚本)"
echo "   - distributed_flash_trade_* (分布式任务下发脚本)"
echo "   - quick_flash_trade_* (快速任务下发脚本)"
echo "   - quick_start_* (快速启动脚本)"
echo "   - config.json (配置文件)"
echo "   - README.md (使用说明)"
echo ""
echo "🚀 快速开始:"
echo "   Linux: cd $BUILD_DIR && ./quick_start_linux.sh"
echo "   Windows: cd $BUILD_DIR && quick_start_windows.bat"
echo ""
echo "💡 新功能:"
echo "   ✅ 支持多链 (BSC + Solana)"
echo "   ✅ 支持多价格模式 (limit/market/combined/auto)"
echo "   ✅ 分布式任务下发"
echo "   ✅ 预设配置快速下发"
echo "   ✅ 交互式参数输入"
echo ""
