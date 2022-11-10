package pkg

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type TaskDefinition struct {
	Id                 *uuid.UUID
	Metadata           interface{}         `json:"metadata"`
	ExpireAfter        time.Duration       `json:"expire_after"`
	NextFireTime       *time.Time          `json:"next_fire_time"`
	ExecuteOnceTrigger *ExecuteOnceTrigger `json:"execute_once_trigger"`
	CronTrigger        *CronTrigger        `json:"cron_trigger"`
	CompletedAt        *time.Time          `json:"completed_at"`
	Recurring          bool                `json:"recurring"`
	TaskInstances      []TaskInstance      `json:"task_instances" gorm:"foreignKey:Id"`
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

func (t TaskDefinition) GetFireTimeFrom(from time.Time) *time.Time {
	return t.GetTrigger().GetFireTime(from)
}

func (t TaskDefinition) GetNextFireTime() *time.Time {
	return t.GetTrigger().GetFireTime(time.Now())
}
