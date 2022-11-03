package pkg

import (
	"github.com/google/uuid"
	"time"
)

type StoreInterface interface {
	Initialize() error
	CreateTaskDefinition(taskDefinition TaskDefinition) error
	ListTaskDefinitions(offset, limit int) ([]TaskDefinition, error)
	GetTaskDefinition(id *uuid.UUID) (TaskDefinition, error)
	UpdateTaskDefinition(taskDefinition TaskDefinition) error
	DeleteTaskDefinition(id *uuid.UUID) error
	CreateTaskInstance(taskInstance TaskInstance) error
	GetTaskInstance(id *uuid.UUID) (TaskInstance, error)
	ListTaskInstances(offset, limit int) ([]TaskInstance, error)
	UpdateTaskInstance(taskInstance TaskInstance) error
	DeleteTaskInstance(id *uuid.UUID) error
	GetTaskInstancesToRun(limit time.Time) ([]TaskInstance, error)
}
