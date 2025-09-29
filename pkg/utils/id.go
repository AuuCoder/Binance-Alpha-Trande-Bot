package utils

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GenerateNodeID 生成基于硬件信息的固定节点ID
func GenerateNodeID(nodeType string) string {
	// 获取硬件特征信息
	hardwareInfo := getHardwareFingerprint()

	// 使用MD5生成固定的8位ID
	hash := md5.Sum([]byte(hardwareInfo))
	nodeID := fmt.Sprintf("%x", hash)[:8]

	// 调试信息（可以通过环境变量控制）
	if os.Getenv("DEBUG_NODE_ID") == "1" {
		fmt.Printf("🔍 硬件指纹: %s\n", hardwareInfo)
		fmt.Printf("🆔 生成节点ID: %s-%s\n", nodeType, nodeID)
	}

	return fmt.Sprintf("%s-%s", nodeType, nodeID)
}

// getHardwareFingerprint 获取硬件指纹
func getHardwareFingerprint() string {
	var fingerprint strings.Builder

	// 1. 操作系统信息
	fingerprint.WriteString(runtime.GOOS)
	fingerprint.WriteString("-")
	fingerprint.WriteString(runtime.GOARCH)
	fingerprint.WriteString("-")

	// 2. 主机名（最重要的标识符）
	if hostname, err := os.Hostname(); err == nil {
		fingerprint.WriteString(hostname)
	}
	fingerprint.WriteString("-")

	// 3. 计算机名（Windows特有）
	if computerName := os.Getenv("COMPUTERNAME"); computerName != "" {
		fingerprint.WriteString(computerName)
	} else if computerName := os.Getenv("HOSTNAME"); computerName != "" {
		fingerprint.WriteString(computerName)
	}
	fingerprint.WriteString("-")

	// 4. 获取CPU信息（Windows）
	if cpuInfo := getCPUInfo(); cpuInfo != "" {
		fingerprint.WriteString(cpuInfo)
	}
	fingerprint.WriteString("-")

	// 5. 获取主板序列号（Windows）
	if motherboardSerial := getMotherboardSerial(); motherboardSerial != "" {
		fingerprint.WriteString(motherboardSerial)
	}
	fingerprint.WriteString("-")

	// 6. 获取MAC地址
	if macAddr := getMACAddress(); macAddr != "" {
		fingerprint.WriteString(macAddr)
	}
	fingerprint.WriteString("-")

	// 7. 用户名（作为辅助标识）
	if username := os.Getenv("USERNAME"); username != "" {
		fingerprint.WriteString(username)
	} else if username := os.Getenv("USER"); username != "" {
		fingerprint.WriteString(username)
	}

	return fingerprint.String()
}

// getCPUInfo 获取CPU信息
func getCPUInfo() string {
	if runtime.GOOS == "windows" {
		// 使用wmic获取CPU信息
		cmd := exec.Command("wmic", "cpu", "get", "ProcessorId", "/value")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "ProcessorId=") {
					return strings.TrimSpace(strings.TrimPrefix(line, "ProcessorId="))
				}
			}
		}
	}
	return ""
}

// getMotherboardSerial 获取主板序列号
func getMotherboardSerial() string {
	if runtime.GOOS == "windows" {
		// 使用wmic获取主板序列号
		cmd := exec.Command("wmic", "baseboard", "get", "SerialNumber", "/value")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "SerialNumber=") {
					serial := strings.TrimSpace(strings.TrimPrefix(line, "SerialNumber="))
					if serial != "" && serial != "To be filled by O.E.M." {
						return serial
					}
				}
			}
		}
	}
	return ""
}

// GetMACAddress 获取MAC地址（公开函数）
func GetMACAddress() string {
	if runtime.GOOS == "windows" {
		// 使用getmac命令获取MAC地址
		cmd := exec.Command("getmac", "/fo", "csv", "/nh")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.Contains(line, ",") {
					parts := strings.Split(line, ",")
					if len(parts) > 0 {
						mac := strings.Trim(parts[0], "\"")
						if mac != "" && mac != "N/A" {
							return mac
						}
					}
				}
			}
		}
	}
	return ""
}

// getMACAddress 获取MAC地址（内部使用，保持兼容性）
func getMACAddress() string {
	return GetMACAddress()
}

// GenerateTradeID 生成交易ID
func GenerateTradeID() string {
	return fmt.Sprintf("trade-%d-%s", time.Now().UnixMilli(), uuid.New().String()[:8])
}

// GenerateTaskID 生成任务ID
func GenerateTaskID() string {
	return fmt.Sprintf("task-%d-%s", time.Now().UnixMilli(), uuid.New().String()[:8])
}

// GenerateCommandID 生成命令ID
func GenerateCommandID() string {
	return fmt.Sprintf("cmd-%d-%s", time.Now().UnixMilli(), uuid.New().String()[:8])
}

// RandomDelay 生成随机延迟
func RandomDelay(minSeconds, maxSeconds int) time.Duration {
	if minSeconds >= maxSeconds {
		return time.Duration(minSeconds) * time.Second
	}

	delay := rand.Intn(maxSeconds-minSeconds+1) + minSeconds
	return time.Duration(delay) * time.Second
}

// FormatDuration 格式化持续时间
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// GetCurrentTimestamp 获取当前时间戳（毫秒）
func GetCurrentTimestamp() int64 {
	return time.Now().UnixMilli()
}

// TimestampToTime 时间戳转时间
func TimestampToTime(timestamp int64) time.Time {
	return time.UnixMilli(timestamp)
}

// TimeToTimestamp 时间转时间戳
func TimeToTimestamp(t time.Time) int64 {
	return t.UnixMilli()
}
