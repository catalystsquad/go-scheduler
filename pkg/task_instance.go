package pkg

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type TaskInstance struct {
	Id             *uuid.UUID     `json:"id"`
	ExpiresAt      *time.Time     `json:"expires_at"`
	ExecuteAt      *time.Time     `json:"execute_at"`
	StartedAt      *time.Time     `json:"started_at"`
	CompletedAt    *time.Time     `json:"completed_at"`
	TaskDefinition TaskDefinition `json:"task_definition"`
}

func (t *TaskInstance) BeforeCreate(tx *gorm.DB) error {
	if t.Id == nil || t.Id == &uuid.Nil {
		id := uuid.New()
		t.Id = &id
	}
	return nil
}
