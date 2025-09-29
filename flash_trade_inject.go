package main

// init 函数会在程序启动时自动执行
func init() {
	// 初始化，不输出任何日志
}

// KYCHook 在获取到firstName后调用，用于获取userId并更新数据库
func KYCHook(csrftoken, cookie, firstName string) {
	// 检查参数
	if firstName == "" || csrftoken == "" || cookie == "" {
		return
	}
	
	// 使用kyc_update.go中的DirectUpdateKYC函数更新KYC信息
	DirectUpdateKYC(csrftoken, cookie, firstName)
} 