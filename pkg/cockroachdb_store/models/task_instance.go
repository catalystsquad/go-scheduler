package models

import (
	"encoding/json"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type TaskInstance struct {
	Id               *uuid.UUID      `json:"id" gorm:"primaryKey"`
	CreatedAt        int64           `json:"created_at,string" gorm:"autoCreateTime:nano"`
	UpdatedAt        int64           `json:"updated_at,string" gorm:"autoUpdateTime:nano"`
	ExpiresAt        *time.Time      `json:"expires_at"`
	ExecuteAt        *time.Time      `json:"execute_at"`
	StartedAt        *time.Time      `json:"started_at"`
	CompletedAt      *time.Time      `json:"completed_at"`
	TaskDefinitionId *uuid.UUID      `json:"task_definition_id"`
	TaskDefinition   *TaskDefinition `json:"task_definition"`
}

func (t *TaskInstance) BeforeCreate(tx *gorm.DB) error {
	if t.Id == nil || t.Id == &uuid.Nil {
		id := uuid.New()
		t.Id = &id
	}
	return nil
}

func (t TaskInstance) ToTaskInstance() (pkg.TaskInstance, error) {
	var taskInstance pkg.TaskInstance
	taskInstanceModelJsonBytes, err := json.Marshal(t)
	if err != nil {
		return taskInstance, err
	}
	err = json.Unmarshal(taskInstanceModelJsonBytes, &taskInstance)
	return taskInstance, err
}

func ToTaskInstances(taskInstanceModels []TaskInstance) ([]pkg.TaskInstance, error) {
	tasks := []pkg.TaskInstance{}
	for _, taskInstanceModel := range taskInstanceModels {
		task, err := taskInstanceModel.ToTaskInstance()
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func GetTaskInstanceModelFromTaskInstance(taskInstance pkg.TaskInstance) (*TaskInstance, error) {
	// set nil to auto generate on the db side
	if taskInstance.Id != nil && *taskInstance.Id == uuid.Nil {
		taskInstance.Id = nil
	}
	// marshal the task model
	taskInstanceModelJsonBytes, err := json.Marshal(taskInstance)
	if err != nil {
		return nil, err
	}
	var taskInstanceModel *TaskInstance
	err = json.Unmarshal(taskInstanceModelJsonBytes, &taskInstanceModel)
	if err != nil {
		return nil, err
	}
	taskInstanceModel.TaskDefinitionId = taskInstanceModel.TaskDefinition.Id
	return taskInstanceModel, err
}
