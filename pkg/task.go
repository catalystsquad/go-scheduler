package pkg

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type Task struct {
	Id                 *uuid.UUID
	Metadata           interface{}         `json:"metadata"`
	RetryOnError       bool                `json:"retry_on_error"`
	ExpireAfter        *time.Duration      `json:"expire_after"`
	InProgress         bool                `json:"in_progress_at"`
	LastFireTime       *time.Time          `json:"last_fire_time"`
	NextFireTime       *time.Time          `json:"next_fire_time"`
	ExecuteOnceTrigger *ExecuteOnceTrigger `json:"execute_once_trigger"`
	CronTrigger        *CronTrigger        `json:"cron_trigger"`
}

func (t Task) GetIdBytes() []byte {
	return []byte(t.Id.String())
}

func (t Task) AsBytes() ([]byte, error) {
	return json.Marshal(t)
}

func TaskFromBytes(bytes []byte) (Task, error) {
	task := Task{}
	err := json.Unmarshal(bytes, &task)
	return task, err
}

func (t Task) GetTrigger() TriggerInterface {
	if t.CronTrigger != nil {
		return t.CronTrigger
	} else if t.ExecuteOnceTrigger != nil {
		return t.ExecuteOnceTrigger
	}
	return nil
}

func (t Task) IdString() string {
	return t.Id.String()
}
