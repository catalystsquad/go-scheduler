package pkg

import "time"

type TriggerInterface interface {
	GetNextFireTime() *time.Time
}
type ExecuteOnceTrigger struct {
	FireAt time.Time
}

func (t ExecuteOnceTrigger) GetNextFireTime() *time.Time {
	return &t.FireAt
}

func NewExecuteOnceTrigger(fireAt time.Time) *ExecuteOnceTrigger {
	return &ExecuteOnceTrigger{FireAt: fireAt}
}
