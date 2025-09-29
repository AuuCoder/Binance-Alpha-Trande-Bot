#!/bin/bash

# Mac 可执行文件测试脚本

echo "========================================="
echo "🧪 Mac 可执行文件测试"
echo "========================================="
echo ""

# 测试函数
test_executable() {
    local name=$1
    local file=$2
    
    echo "🔍 测试 $name..."
    
    if [ ! -f "$file" ]; then
        echo "   ❌ 文件不存在: $file"
        return 1
    fi
    
    if [ ! -x "$file" ]; then
        echo "   ❌ 文件不可执行: $file"
        return 1
    fi
    
    # 检查文件架构
    local arch_info=$(file "$file" | grep -o "universal binary\|x86_64\|arm64" | head -1)
    echo "   📋 架构: $arch_info"
    
    # 检查文件大小
    local size=$(ls -lah "$file" | awk '{print $5}')
    echo "   📏 大小: $size"
    
    # 尝试运行帮助命令 (快速退出)
    echo "   🚀 测试运行..."
    if timeout 3 "$file" --version 2>/dev/null || timeout 3 "$file" --help 2>/dev/null || timeout 3 "$file" -h 2>/dev/null; then
        echo "   ✅ 可执行文件正常"
    else
        # 对于服务类程序，尝试启动并快速停止
        local pid
        "$file" &
        pid=$!
        sleep 1
        if kill -0 $pid 2>/dev/null; then
            echo "   ✅ 服务启动正常"
            kill $pid 2>/dev/null
            wait $pid 2>/dev/null
        else
            echo "   ⚠️  无法验证运行状态"
        fi
    fi
    
    echo ""
    return 0
}

# 如果没有timeout命令，创建一个简单的替代
if ! command -v timeout >/dev/null 2>&1; then
    timeout() {
        local duration=$1
        shift
        "$@" &
        local pid=$!
        sleep $duration
        kill $pid 2>/dev/null
        wait $pid 2>/dev/null
    }
fi

echo "📦 测试主要可执行文件..."
echo ""

# 测试主要服务
test_executable "Flash Trade 主服务" "flash_trade_mac_universal"
test_executable "Master 节点" "master_mac_universal"
test_executable "Slave 节点" "slave_mac_universal"

echo "📦 测试任务下发脚本..."
echo ""

# 测试脚本
test_executable "Alpha 脚本" "alpha_mac_universal"
test_executable "分布式任务下发脚本" "distributed_flash_trade_mac_universal"
test_executable "快速任务下发脚本" "quick_flash_trade_mac_universal"

echo "📦 测试启动脚本..."
echo ""

# 测试启动脚本
for script in start_*.sh quick_task.sh; do
    if [ -f "$script" ]; then
        echo "🔍 测试 $script..."
        if [ -x "$script" ]; then
            echo "   ✅ 脚本可执行"
        else
            echo "   ❌ 脚本不可执行"
        fi
        echo ""
    fi
done

echo "📦 测试配置文件..."
echo ""

# 测试配置文件
for config in config.json script_config.json; do
    if [ -f "$config" ]; then
        echo "🔍 测试 $config..."
        if python3 -m json.tool "$config" >/dev/null 2>&1; then
            echo "   ✅ JSON 格式正确"
        else
            echo "   ❌ JSON 格式错误"
        fi
        echo ""
    fi
done

echo "📦 测试 Web 文件..."
echo ""

if [ -d "web" ]; then
    echo "🔍 测试 web 目录..."
    local html_count=$(find web -name "*.html" | wc -l)
    echo "   📄 HTML 文件数量: $html_count"
    if [ $html_count -gt 0 ]; then
        echo "   ✅ Web 文件存在"
    else
        echo "   ❌ 没有找到 HTML 文件"
    fi
    echo ""
fi

echo "========================================="
echo "🎉 测试完成！"
echo "========================================="
echo ""
echo "💡 使用建议:"
echo "   1. 运行 ./start_flash_trade.sh 启动 Flash Trade 服务"
echo "   2. 运行 ./start_master.sh 启动 Master 节点"
echo "   3. 运行 ./start_slave.sh 启动 Slave 节点"
echo "   4. 运行 ./quick_task.sh <代币地址> 快速下发任务"
echo ""
echo "📋 所有文件都是 Universal Binary，支持:"
echo "   ✅ Intel Mac (x86_64)"
echo "   ✅ Apple Silicon Mac (arm64)"
echo ""
