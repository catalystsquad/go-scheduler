package pkg

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type TaskDefinition struct {
	Id                 *uuid.UUID
	Metadata           interface{}         `json:"metadata"`
	RetryOnError       bool                `json:"retry_on_error"`
	ExpireAfter        *time.Duration      `json:"expire_after"`
	NextFireTime       *time.Time          `json:"next_fire_time"`
	TaskInstances      []TaskInstance      `json:"task_instances"`
	ExecuteOnceTrigger *ExecuteOnceTrigger `json:"execute_once_trigger"`
	CronTrigger        *CronTrigger        `json:"cron_trigger"`
}

func (t TaskDefinition) GetIdBytes() []byte {
	return []byte(t.Id.String())
}

func (t TaskDefinition) AsBytes() ([]byte, error) {
	return json.Marshal(t)
}

func TaskFromBytes(bytes []byte) (TaskDefinition, error) {
	task := TaskDefinition{}
	err := json.Unmarshal(bytes, &task)
	return task, err
}

func (t TaskDefinition) GetTrigger() TriggerInterface {
	if t.CronTrigger != nil {
		return t.CronTrigger
	} else if t.ExecuteOnceTrigger != nil {
		return t.ExecuteOnceTrigger
	}
	return nil
}

func (t TaskDefinition) IdString() string {
	return t.Id.String()
}
