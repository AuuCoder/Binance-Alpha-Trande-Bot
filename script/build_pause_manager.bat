@echo off
chcp 65001 >nul

echo =========================================
echo 🔨 构建 Flash Trade 暂停管理脚本
echo =========================================

:: 设置构建目录
set BUILD_DIR=build
set SCRIPT_NAME=pause_manager

:: 创建构建目录
if not exist %BUILD_DIR% mkdir %BUILD_DIR%

echo 📦 开始构建多平台版本...

:: 构建 Windows 版本
echo 🪟 构建 Windows 版本...
set GOOS=windows
set GOARCH=amd64
go build -o %BUILD_DIR%\%SCRIPT_NAME%_windows_amd64.exe pause_manager.go
if %errorlevel% equ 0 (
    echo ✅ Windows 版本构建成功: %SCRIPT_NAME%_windows_amd64.exe
) else (
    echo ❌ Windows 版本构建失败
)

:: 构建 Linux 版本
echo 🐧 构建 Linux 版本...
set GOOS=linux
set GOARCH=amd64
go build -o %BUILD_DIR%\%SCRIPT_NAME%_linux_amd64 pause_manager.go
if %errorlevel% equ 0 (
    echo ✅ Linux 版本构建成功: %SCRIPT_NAME%_linux_amd64
) else (
    echo ❌ Linux 版本构建失败
)

:: 构建 macOS Intel 版本
echo 🍎 构建 macOS Intel 版本...
set GOOS=darwin
set GOARCH=amd64
go build -o %BUILD_DIR%\%SCRIPT_NAME%_mac_amd64 pause_manager.go
if %errorlevel% equ 0 (
    echo ✅ macOS Intel 版本构建成功: %SCRIPT_NAME%_mac_amd64
) else (
    echo ❌ macOS Intel 版本构建失败
)

:: 构建 macOS ARM 版本
echo 🍎 构建 macOS ARM 版本...
set GOOS=darwin
set GOARCH=arm64
go build -o %BUILD_DIR%\%SCRIPT_NAME%_mac_arm64 pause_manager.go
if %errorlevel% equ 0 (
    echo ✅ macOS ARM 版本构建成功: %SCRIPT_NAME%_mac_arm64
) else (
    echo ❌ macOS ARM 版本构建失败
)

:: 复制配置文件示例
echo 📄 复制配置文件...
copy config_pause_example.json %BUILD_DIR%\config.json >nul

echo.
echo =========================================
echo 🎉 构建完成！
echo =========================================
echo 📁 构建文件位置: %BUILD_DIR%\
echo.
echo 📋 使用说明:
echo 1. 将对应平台的可执行文件复制到目标服务器
echo 2. 修改 config.json 配置文件，填入正确的节点信息
echo 3. 运行程序
echo.
echo 💡 配置文件格式:
echo    - id: 账号ID
echo    - name: 节点名称  
echo    - ip: 节点IP地址
echo    - csrftoken: CSRF令牌
echo    - cookie: Cookie信息
echo.
echo ⚠️  注意事项:
echo    - 确保目标服务器的 Flash Trade 服务正在运行
echo    - 确保网络连接正常
echo    - 建议在维护时间窗口执行
echo.

pause
