#!/bin/bash

echo "========================================="
echo "🔨 构建 Flash Trade 暂停管理脚本"
echo "========================================="

# 设置构建目录
BUILD_DIR="build"
SCRIPT_NAME="pause_manager"

# 创建构建目录
mkdir -p $BUILD_DIR

echo "📦 开始构建多平台版本..."

# 构建 Windows 版本
echo "🪟 构建 Windows 版本..."
GOOS=windows GOARCH=amd64 go build -o $BUILD_DIR/${SCRIPT_NAME}_windows_amd64.exe pause_manager.go
if [ $? -eq 0 ]; then
    echo "✅ Windows 版本构建成功: ${SCRIPT_NAME}_windows_amd64.exe"
else
    echo "❌ Windows 版本构建失败"
fi

# 构建 Linux 版本
echo "🐧 构建 Linux 版本..."
GOOS=linux GOARCH=amd64 go build -o $BUILD_DIR/${SCRIPT_NAME}_linux_amd64 pause_manager.go
if [ $? -eq 0 ]; then
    echo "✅ Linux 版本构建成功: ${SCRIPT_NAME}_linux_amd64"
else
    echo "❌ Linux 版本构建失败"
fi

# 构建 macOS Intel 版本
echo "🍎 构建 macOS Intel 版本..."
GOOS=darwin GOARCH=amd64 go build -o $BUILD_DIR/${SCRIPT_NAME}_mac_amd64 pause_manager.go
if [ $? -eq 0 ]; then
    echo "✅ macOS Intel 版本构建成功: ${SCRIPT_NAME}_mac_amd64"
else
    echo "❌ macOS Intel 版本构建失败"
fi

# 构建 macOS ARM 版本
echo "🍎 构建 macOS ARM 版本..."
GOOS=darwin GOARCH=arm64 go build -o $BUILD_DIR/${SCRIPT_NAME}_mac_arm64 pause_manager.go
if [ $? -eq 0 ]; then
    echo "✅ macOS ARM 版本构建成功: ${SCRIPT_NAME}_mac_arm64"
else
    echo "❌ macOS ARM 版本构建失败"
fi

# 复制配置文件示例
echo "📄 复制配置文件..."
cp config_pause_example.json $BUILD_DIR/config.json

echo ""
echo "========================================="
echo "🎉 构建完成！"
echo "========================================="
echo "📁 构建文件位置: $BUILD_DIR/"
echo ""
echo "📋 使用说明:"
echo "1. 将对应平台的可执行文件复制到目标服务器"
echo "2. 修改 config.json 配置文件，填入正确的节点信息"
echo "3. 运行程序: ./${SCRIPT_NAME}_平台名称"
echo ""
echo "💡 配置文件格式:"
echo "   - id: 账号ID"
echo "   - name: 节点名称"
echo "   - ip: 节点IP地址"
echo "   - csrftoken: CSRF令牌"
echo "   - cookie: Cookie信息"
echo ""
echo "⚠️  注意事项:"
echo "   - 确保目标服务器的 Flash Trade 服务正在运行"
echo "   - 确保网络连接正常"
echo "   - 建议在维护时间窗口执行"
echo ""
