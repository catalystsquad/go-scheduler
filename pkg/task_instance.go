package pkg

import (
	"github.com/google/uuid"
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
