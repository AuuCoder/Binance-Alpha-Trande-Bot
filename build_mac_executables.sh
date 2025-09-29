#!/bin/bash

# Mac 可执行文件构建脚本
# 构建 Flash Trade 和相关工具的 Mac 版本

echo "========================================="
echo "🍎 Mac 可执行文件构建工具 v1.0"
echo "========================================="
echo "📦 构建 Flash Trade 和相关工具的 Mac 版本"
echo ""

# 设置构建目录
BUILD_DIR="build_mac"
mkdir -p $BUILD_DIR

echo "🔧 开始构建 Mac 可执行文件..."

# 1. 构建 Flash Trade 主服务
echo "📦 构建 Flash Trade 主服务..."
echo "   - Intel Mac (x86_64)..."
GOOS=darwin GOARCH=amd64 go build -o $BUILD_DIR/flash_trade_mac_intel flash_trade.go

echo "   - Apple Silicon Mac (arm64)..."
GOOS=darwin GOARCH=arm64 go build -o $BUILD_DIR/flash_trade_mac_arm64 flash_trade.go

echo "   - Universal Binary (支持所有Mac)..."
lipo -create -output $BUILD_DIR/flash_trade_mac_universal $BUILD_DIR/flash_trade_mac_intel $BUILD_DIR/flash_trade_mac_arm64

# 2. 构建 Master 节点
echo "📦 构建 Master 节点..."
echo "   - Intel Mac..."
GOOS=darwin GOARCH=amd64 go build -o $BUILD_DIR/master_mac_intel cmd/master/main.go

echo "   - Apple Silicon Mac..."
GOOS=darwin GOARCH=arm64 go build -o $BUILD_DIR/master_mac_arm64 cmd/master/main.go

echo "   - Universal Binary..."
lipo -create -output $BUILD_DIR/master_mac_universal $BUILD_DIR/master_mac_intel $BUILD_DIR/master_mac_arm64

# 3. 构建 Slave 节点
echo "📦 构建 Slave 节点..."
echo "   - Intel Mac..."
GOOS=darwin GOARCH=amd64 go build -o $BUILD_DIR/slave_mac_intel cmd/slave/main.go

echo "   - Apple Silicon Mac..."
GOOS=darwin GOARCH=arm64 go build -o $BUILD_DIR/slave_mac_arm64 cmd/slave/main.go

echo "   - Universal Binary..."
lipo -create -output $BUILD_DIR/slave_mac_universal $BUILD_DIR/slave_mac_intel $BUILD_DIR/slave_mac_arm64

# 4. 构建任务下发脚本
echo "📦 构建任务下发脚本..."
cd script

echo "   - Alpha 脚本..."
GOOS=darwin GOARCH=amd64 go build -o ../$BUILD_DIR/alpha_mac_intel alpha.go
GOOS=darwin GOARCH=arm64 go build -o ../$BUILD_DIR/alpha_mac_arm64 alpha.go
lipo -create -output ../$BUILD_DIR/alpha_mac_universal ../$BUILD_DIR/alpha_mac_intel ../$BUILD_DIR/alpha_mac_arm64

echo "   - 分布式任务下发脚本..."
GOOS=darwin GOARCH=amd64 go build -o ../$BUILD_DIR/distributed_flash_trade_mac_intel distributed_flash_trade.go
GOOS=darwin GOARCH=arm64 go build -o ../$BUILD_DIR/distributed_flash_trade_mac_arm64 distributed_flash_trade.go
lipo -create -output ../$BUILD_DIR/distributed_flash_trade_mac_universal ../$BUILD_DIR/distributed_flash_trade_mac_intel ../$BUILD_DIR/distributed_flash_trade_mac_arm64

echo "   - 快速任务下发脚本..."
GOOS=darwin GOARCH=amd64 go build -o ../$BUILD_DIR/quick_flash_trade_mac_intel quick_flash_trade.go
GOOS=darwin GOARCH=arm64 go build -o ../$BUILD_DIR/quick_flash_trade_mac_arm64 quick_flash_trade.go
lipo -create -output ../$BUILD_DIR/quick_flash_trade_mac_universal ../$BUILD_DIR/quick_flash_trade_mac_intel ../$BUILD_DIR/quick_flash_trade_mac_arm64

cd ..

# 5. 复制配置文件
echo "📄 复制配置文件..."
cp config.json $BUILD_DIR/config.json
cp script/config.json $BUILD_DIR/script_config.json
cp -r web $BUILD_DIR/web

# 6. 设置执行权限
echo "🔐 设置执行权限..."
chmod +x $BUILD_DIR/*_mac_*

# 7. 创建启动脚本
echo "🚀 创建启动脚本..."

# Flash Trade 启动脚本
cat > $BUILD_DIR/start_flash_trade.sh << 'EOF'
#!/bin/bash
echo "🚀 启动 Flash Trade 服务"
echo "选择版本:"
echo "1. Universal Binary (推荐 - 支持所有Mac)"
echo "2. Intel Mac 专用"
echo "3. Apple Silicon Mac 专用"
read -p "请选择 (1-3, 默认1): " choice

case ${choice:-1} in
    1)
        echo "启动 Universal Binary 版本..."
        ./flash_trade_mac_universal
        ;;
    2)
        echo "启动 Intel Mac 版本..."
        ./flash_trade_mac_intel
        ;;
    3)
        echo "启动 Apple Silicon Mac 版本..."
        ./flash_trade_mac_arm64
        ;;
    *)
        echo "启动 Universal Binary 版本..."
        ./flash_trade_mac_universal
        ;;
esac
EOF

# Master 节点启动脚本
cat > $BUILD_DIR/start_master.sh << 'EOF'
#!/bin/bash
echo "🚀 启动 Master 节点"
echo "选择版本:"
echo "1. Universal Binary (推荐)"
echo "2. Intel Mac 专用"
echo "3. Apple Silicon Mac 专用"
read -p "请选择 (1-3, 默认1): " choice

case ${choice:-1} in
    1)
        echo "启动 Universal Binary 版本..."
        ./master_mac_universal
        ;;
    2)
        echo "启动 Intel Mac 版本..."
        ./master_mac_intel
        ;;
    3)
        echo "启动 Apple Silicon Mac 版本..."
        ./master_mac_arm64
        ;;
    *)
        echo "启动 Universal Binary 版本..."
        ./master_mac_universal
        ;;
esac
EOF

# Slave 节点启动脚本
cat > $BUILD_DIR/start_slave.sh << 'EOF'
#!/bin/bash
echo "🚀 启动 Slave 节点"
echo "选择版本:"
echo "1. Universal Binary (推荐)"
echo "2. Intel Mac 专用"
echo "3. Apple Silicon Mac 专用"
read -p "请选择 (1-3, 默认1): " choice

case ${choice:-1} in
    1)
        echo "启动 Universal Binary 版本..."
        ./slave_mac_universal
        ;;
    2)
        echo "启动 Intel Mac 版本..."
        ./slave_mac_intel
        ;;
    3)
        echo "启动 Apple Silicon Mac 版本..."
        ./slave_mac_arm64
        ;;
    *)
        echo "启动 Universal Binary 版本..."
        ./slave_mac_universal
        ;;
esac
EOF

# 快速任务下发脚本
cat > $BUILD_DIR/quick_task.sh << 'EOF'
#!/bin/bash
echo "⚡ 快速任务下发"
if [ $# -eq 0 ]; then
    echo "使用方法: $0 <代币地址> [预设配置]"
    echo "示例:"
    echo "  $0 0xa2be3e48170a60119b5f0400c65f65f3158fbeee"
    echo "  $0 0xa2be3e48170a60119b5f0400c65f65f3158fbeee BSC_MARKET_NORMAL"
    echo "  $0 SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL SOLANA_MARKET_NORMAL"
    exit 1
fi

./quick_flash_trade_mac_universal "$@"
EOF

# 设置启动脚本执行权限
chmod +x $BUILD_DIR/start_*.sh
chmod +x $BUILD_DIR/quick_task.sh

# 8. 创建使用说明
cat > $BUILD_DIR/README_MAC.md << 'EOF'
# Flash Trade Mac 版本使用说明

## 📦 包含文件

### 主要服务
- `flash_trade_mac_universal` - Flash Trade 主服务 (Universal Binary)
- `master_mac_universal` - Master 节点 (Universal Binary)
- `slave_mac_universal` - Slave 节点 (Universal Binary)

### 任务下发工具
- `alpha_mac_universal` - 多节点直连脚本
- `distributed_flash_trade_mac_universal` - 分布式任务下发脚本
- `quick_flash_trade_mac_universal` - 快速任务下发脚本

### 启动脚本
- `start_flash_trade.sh` - Flash Trade 服务启动脚本
- `start_master.sh` - Master 节点启动脚本
- `start_slave.sh` - Slave 节点启动脚本
- `quick_task.sh` - 快速任务下发脚本

### 配置文件
- `config.json` - 主配置文件
- `script_config.json` - 脚本配置文件
- `web/` - Web 界面文件

## 🚀 快速开始

### 1. 启动 Flash Trade 服务
```bash
./start_flash_trade.sh
```

### 2. 启动分布式系统
```bash
# 启动 Master 节点
./start_master.sh

# 启动 Slave 节点 (在其他机器上)
./start_slave.sh
```

### 3. 快速下发任务
```bash
# BSC 代币
./quick_task.sh 0xa2be3e48170a60119b5f0400c65f65f3158fbeee BSC_MARKET_NORMAL

# Solana 代币
./quick_task.sh SarosY6Vscao718M4A778z4CGtvcwcGef5M9MEH1LGL SOLANA_MARKET_NORMAL
```

## 💡 版本说明

- **Universal Binary**: 同时支持 Intel Mac 和 Apple Silicon Mac
- **Intel 版本**: 专为 Intel Mac 优化
- **ARM64 版本**: 专为 Apple Silicon Mac 优化

推荐使用 Universal Binary 版本，兼容性最好。

## ⚠️ 注意事项

1. 首次运行可能需要在"系统偏好设置 > 安全性与隐私"中允许运行
2. 确保网络连接正常
3. 根据需要修改配置文件
4. 建议在终端中运行以查看详细日志

## 📞 技术支持

如有问题请检查：
1. 文件执行权限是否正确
2. 配置文件是否正确
3. 网络连接是否正常
4. 系统安全设置是否允许运行
EOF

# 9. 显示文件信息
echo ""
echo "📊 构建结果:"
echo "========================================="
ls -lah $BUILD_DIR/*_mac_universal | while read line; do
    file_info=$(echo $line | awk '{print $9, $5}')
    file_name=$(echo $line | awk '{print $9}')
    arch_info=$(file "$file_name" 2>/dev/null | grep -o "universal binary\|x86_64\|arm64" | head -1)
    echo "   $file_info ($arch_info)"
done

echo ""
echo "========================================="
echo "🎉 Mac 可执行文件构建完成！"
echo "========================================="
echo "📁 构建文件位置: $BUILD_DIR/"
echo ""
echo "🚀 快速开始:"
echo "   cd $BUILD_DIR"
echo "   ./start_flash_trade.sh"
echo ""
echo "💡 特性:"
echo "   ✅ Universal Binary - 支持所有 Mac"
echo "   ✅ 多链支持 (BSC + Solana)"
echo "   ✅ 多价格模式支持"
echo "   ✅ 分布式架构支持"
echo "   ✅ 一键启动脚本"
echo ""
echo "📋 主要文件:"
echo "   - flash_trade_mac_universal (Flash Trade 主服务)"
echo "   - master_mac_universal (Master 节点)"
echo "   - slave_mac_universal (Slave 节点)"
echo "   - quick_flash_trade_mac_universal (快速任务下发)"
echo ""
