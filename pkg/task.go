package pkg

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type Task struct {
	Id                 *uuid.UUID
	Metadata           interface{}
	RetryOnError       bool
	ExpireAfter        *time.Duration
	InProgressAt       *time.Time
	ExecuteOnceTrigger *ExecuteOnceTrigger
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
	return t.ExecuteOnceTrigger
}
