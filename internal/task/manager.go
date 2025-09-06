package task

import (
	"fmt"
	"log"
	"sync"
	"time"

	"alpha-autosell-bot/internal/account"
)

// TaskType 任务类型
type TaskType string

const (
	TaskTypeFlashTrade TaskType = "flash_trade"
	TaskTypeAutoSell   TaskType = "auto_sell"
	TaskTypeBulkTrade  TaskType = "bulk_trade"
	TaskTypeCustom     TaskType = "custom"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
	TaskStatusPaused    TaskStatus = "paused"
)

// UniversalTask 通用任务结构
type UniversalTask struct {
	// 基本信息
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Type        TaskType   `json:"type"`
	Status      TaskStatus `json:"status"`
	Description string     `json:"description"`

	// 时间管理
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`

	// 执行配置
	TargetNodes    []string `json:"target_nodes"`    // 目标节点列表，空表示所有节点
	TargetAccounts []string `json:"target_accounts"` // 目标账号列表，空表示所有账号
	Priority       int      `json:"priority"`        // 优先级 1-10，数字越大优先级越高

	// 任务参数 - 使用通用的参数结构
	Parameters map[string]interface{} `json:"parameters"`

	// 执行结果
	Results map[string]interface{} `json:"results"`

	// 进度跟踪
	Progress TaskProgress `json:"progress"`

	// 错误信息
	ErrorMessage string `json:"error_message,omitempty"`

	// 创建者信息
	CreatedBy string `json:"created_by"`

	// 扩展字段
	Metadata map[string]interface{} `json:"metadata"`
}

// TaskProgress 任务进度
type TaskProgress struct {
	TotalSteps     int     `json:"total_steps"`
	CompletedSteps int     `json:"completed_steps"`
	Percentage     float64 `json:"percentage"`
	CurrentStep    string  `json:"current_step"`

	// 账号级别的进度
	AccountProgress map[string]AccountProgress `json:"account_progress"`
}

// AccountProgress 账号进度
type AccountProgress struct {
	AccountID     string    `json:"account_id"`
	Status        string    `json:"status"`
	Progress      float64   `json:"progress"`
	CurrentAction string    `json:"current_action"`
	StartTime     time.Time `json:"start_time"`
	LastUpdate    time.Time `json:"last_update"`
	ErrorMessage  string    `json:"error_message,omitempty"`

	// 任务特定的统计数据
	Stats map[string]interface{} `json:"stats"`
}

// UniversalTaskManager 通用任务管理器
type UniversalTaskManager struct {
	tasks         map[string]*UniversalTask
	tasksByType   map[TaskType][]*UniversalTask
	tasksByStatus map[TaskStatus][]*UniversalTask
	mutex         sync.RWMutex

	// 账号管理器引用
	accountManager *account.UniversalAccountManager

	// 任务执行器注册
	executors map[TaskType]TaskExecutor
}

// TaskExecutor 任务执行器接口
type TaskExecutor interface {
	Execute(task *UniversalTask, accountManager *account.UniversalAccountManager) error
	Validate(parameters map[string]interface{}) error
	GetDefaultParameters() map[string]interface{}
}

// NewUniversalTaskManager 创建通用任务管理器
func NewUniversalTaskManager(accountManager *account.UniversalAccountManager) *UniversalTaskManager {
	return &UniversalTaskManager{
		tasks:          make(map[string]*UniversalTask),
		tasksByType:    make(map[TaskType][]*UniversalTask),
		tasksByStatus:  make(map[TaskStatus][]*UniversalTask),
		accountManager: accountManager,
		executors:      make(map[TaskType]TaskExecutor),
	}
}

// RegisterExecutor 注册任务执行器
func (m *UniversalTaskManager) RegisterExecutor(taskType TaskType, executor TaskExecutor) {
	m.executors[taskType] = executor
	log.Printf("✅ 注册任务执行器: %s", taskType)
}

// CreateTask 创建任务
func (m *UniversalTaskManager) CreateTask(task *UniversalTask) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if task.ID == "" {
		task.ID = generateTaskID()
	}

	if _, exists := m.tasks[task.ID]; exists {
		return fmt.Errorf("任务ID已存在: %s", task.ID)
	}

	// 验证任务执行器
	executor, exists := m.executors[task.Type]
	if !exists {
		return fmt.Errorf("未找到任务类型 %s 的执行器", task.Type)
	}

	// 验证参数
	if err := executor.Validate(task.Parameters); err != nil {
		return fmt.Errorf("任务参数验证失败: %v", err)
	}

	// 设置默认值
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now
	task.Status = TaskStatusPending

	if task.Parameters == nil {
		task.Parameters = make(map[string]interface{})
	}
	if task.Results == nil {
		task.Results = make(map[string]interface{})
	}
	if task.Metadata == nil {
		task.Metadata = make(map[string]interface{})
	}

	// 初始化进度
	task.Progress = TaskProgress{
		AccountProgress: make(map[string]AccountProgress),
	}

	// 验证目标账号
	if err := m.validateTargetAccounts(task); err != nil {
		return err
	}

	m.tasks[task.ID] = task
	m.addToIndex(task)

	log.Printf("✅ 创建任务: %s (%s)", task.ID, task.Type)
	return nil
}

// StartTask 启动任务
func (m *UniversalTaskManager) StartTask(taskID string) error {
	m.mutex.Lock()
	task, exists := m.tasks[taskID]
	if !exists {
		m.mutex.Unlock()
		return fmt.Errorf("任务不存在: %s", taskID)
	}

	if task.Status != TaskStatusPending {
		m.mutex.Unlock()
		return fmt.Errorf("任务状态不允许启动: %s", task.Status)
	}

	task.Status = TaskStatusRunning
	now := time.Now()
	task.StartedAt = &now
	task.UpdatedAt = now

	m.updateIndex(task)
	m.mutex.Unlock()

	// 异步执行任务
	go m.executeTask(task)

	log.Printf("🚀 启动任务: %s", taskID)
	return nil
}

// executeTask 执行任务
func (m *UniversalTaskManager) executeTask(task *UniversalTask) {
	log.Printf("🔧 [调试] 开始执行任务: %s, 类型: %s", task.ID, task.Type)

	executor, exists := m.executors[task.Type]
	if !exists {
		log.Printf("❌ [调试] 未找到任务执行器: %s, 可用执行器: %v", task.Type, m.getExecutorTypes())
		m.failTask(task, fmt.Sprintf("未找到任务执行器: %s", task.Type))
		return
	}

	log.Printf("✅ [调试] 找到任务执行器: %s", task.Type)

	// 记录任务使用到账号管理器
	for _, accountID := range task.TargetAccounts {
		usage := account.TaskUsage{
			TaskID:    task.ID,
			TaskType:  string(task.Type),
			StartTime: *task.StartedAt,
			Status:    "running",
			NodeID:    "", // 将在实际执行时填充
		}
		m.accountManager.RecordTaskUsage(accountID, usage)
	}

	// 执行任务
	if err := executor.Execute(task, m.accountManager); err != nil {
		m.failTask(task, err.Error())
		return
	}

	m.completeTask(task)
}

// getExecutorTypes 获取已注册的执行器类型（用于调试）
func (m *UniversalTaskManager) getExecutorTypes() []string {
	var types []string
	for taskType := range m.executors {
		types = append(types, string(taskType))
	}
	return types
}

// completeTask 完成任务
func (m *UniversalTaskManager) completeTask(task *UniversalTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task.Status = TaskStatusCompleted
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now
	task.Progress.Percentage = 100

	m.updateIndex(task)

	log.Printf("✅ 任务完成: %s", task.ID)
}

// failTask 任务失败
func (m *UniversalTaskManager) failTask(task *UniversalTask, errorMsg string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task.Status = TaskStatusFailed
	task.ErrorMessage = errorMsg
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now

	m.updateIndex(task)

	log.Printf("❌ 任务失败: %s - %s", task.ID, errorMsg)
}

// GetTask 获取任务
func (m *UniversalTaskManager) GetTask(taskID string) (*UniversalTask, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("任务不存在: %s", taskID)
	}

	return task, nil
}

// GetTasksByType 按类型获取任务
func (m *UniversalTaskManager) GetTasksByType(taskType TaskType) []*UniversalTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.tasksByType[taskType]
}

// GetTasksByStatus 按状态获取任务
func (m *UniversalTaskManager) GetTasksByStatus(status TaskStatus) []*UniversalTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.tasksByStatus[status]
}

// GetAllTasks 获取所有任务
func (m *UniversalTaskManager) GetAllTasks() map[string]*UniversalTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]*UniversalTask)
	for id, task := range m.tasks {
		result[id] = task
	}

	return result
}

// UpdateTaskProgress 更新任务进度
func (m *UniversalTaskManager) UpdateTaskProgress(taskID string, progress TaskProgress) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return fmt.Errorf("任务不存在: %s", taskID)
	}

	task.Progress = progress
	task.UpdatedAt = time.Now()

	return nil
}

// validateTargetAccounts 验证目标账号
func (m *UniversalTaskManager) validateTargetAccounts(task *UniversalTask) error {
	if len(task.TargetAccounts) == 0 {
		// 如果没有指定账号，使用所有活跃账号
		allAccounts := m.accountManager.GetAllAccounts()
		for id, account := range allAccounts {
			if account.Status == "active" {
				task.TargetAccounts = append(task.TargetAccounts, id)
			}
		}
	} else {
		// 验证指定的账号是否存在且可用
		for _, accountID := range task.TargetAccounts {
			account, err := m.accountManager.GetAccount(accountID)
			if err != nil {
				return fmt.Errorf("账号不存在: %s", accountID)
			}
			if account.Status != "active" {
				return fmt.Errorf("账号不可用: %s (状态: %s)", accountID, account.Status)
			}
		}
	}

	return nil
}

// addToIndex 添加到索引
func (m *UniversalTaskManager) addToIndex(task *UniversalTask) {
	// 按类型索引
	m.tasksByType[task.Type] = append(m.tasksByType[task.Type], task)

	// 按状态索引
	m.tasksByStatus[task.Status] = append(m.tasksByStatus[task.Status], task)
}

// updateIndex 更新索引
func (m *UniversalTaskManager) updateIndex(task *UniversalTask) {
	// 重建索引（简化实现）
	m.tasksByType = make(map[TaskType][]*UniversalTask)
	m.tasksByStatus = make(map[TaskStatus][]*UniversalTask)

	for _, t := range m.tasks {
		m.tasksByType[t.Type] = append(m.tasksByType[t.Type], t)
		m.tasksByStatus[t.Status] = append(m.tasksByStatus[t.Status], t)
	}
}

// StopTask 停止任务
func (m *UniversalTaskManager) StopTask(taskID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return fmt.Errorf("任务不存在: %s", taskID)
	}

	if task.Status != TaskStatusRunning {
		return fmt.Errorf("任务状态不允许停止: %s", task.Status)
	}

	// 设置任务状态为已取消
	task.Status = TaskStatusCancelled
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now
	task.ErrorMessage = "任务被手动停止"

	// 更新索引
	m.updateIndex(task)

	// 记录账号使用结束
	for _, accountID := range task.TargetAccounts {
		usage := account.TaskUsage{
			TaskID:    task.ID,
			TaskType:  string(task.Type),
			StartTime: *task.StartedAt,
			EndTime:   now,
			Status:    "canceled",
			NodeID:    "", // 将在实际执行时填充
		}
		m.accountManager.RecordTaskUsage(accountID, usage)
	}

	log.Printf("🛑 任务已停止: %s", taskID)
	return nil
}

// CancelTask 取消任务（StopTask的别名）
func (m *UniversalTaskManager) CancelTask(taskID string) error {
	return m.StopTask(taskID)
}

// generateTaskID 生成任务ID
func generateTaskID() string {
	return fmt.Sprintf("task-%d", time.Now().UnixNano()/1000000)
}
