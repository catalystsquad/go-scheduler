package pkg

import (
	"github.com/google/uuid"
	"time"
)

type StoreInterface interface {
	Initialize() error
	UpsertTaskDefinition(definition TaskDefinition) error
	ListTaskDefinitions(offset, limit int, metadataQuery interface{}) ([]TaskDefinition, error)
	GetTaskDefinition(id *uuid.UUID) (TaskDefinition, error)
	GetTaskDefinitions(ids []*uuid.UUID) ([]TaskDefinition, error)
	DeleteTaskDefinition(id *uuid.UUID) error
	DeleteTaskDefinitions(ids []*uuid.UUID) error
	DeleteTaskDefinitionsByMetadata(query interface{}, args ...interface{}) error
	UpsertTaskInstance(taskInstance TaskInstance) error
	GetTaskInstance(id *uuid.UUID) (TaskInstance, error)
	ListTaskInstances(offset, limit int) ([]TaskInstance, error)
	DeleteTaskInstance(id *uuid.UUID) error
	GetTaskDefinitionsToSchedule(limit time.Time) ([]TaskDefinition, error)
	GetTaskInstancesToRun(limit time.Time) ([]TaskInstance, error)
	// markTaskInstanceComplete() should also mark the task definition complete, if the definition is non-recurring
	MarkTaskInstanceComplete(instance TaskInstance) error
	DeleteCompletedTaskInstances() error
	DeleteCompletedTaskDefinitions() error
}
