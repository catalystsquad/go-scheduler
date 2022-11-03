package pkg

import (
	"time"
)

type ExecuteOnceTrigger struct {
	FireAt time.Time `json:"fire_at"`
}

func (t ExecuteOnceTrigger) GetNextFireTime(task TaskDefinition) *time.Time {
	return &t.FireAt
}

func (t ExecuteOnceTrigger) IsRecurring() bool {
	return false
}

func NewExecuteOnceTrigger(fireAt time.Time) *ExecuteOnceTrigger {
	return &ExecuteOnceTrigger{FireAt: fireAt}
}
