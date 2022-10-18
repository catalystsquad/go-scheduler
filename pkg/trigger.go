package pkg

import "time"

type TriggerInterface interface {
	GetNextFireTime() (time.Time, error)
}
type ExecuteOnceTrigger struct {
	FireAt time.Time
}

func (t ExecuteOnceTrigger) GetNextFireTime() (time.Time, error) {
	return t.FireAt, nil
}

func NewExecuteOnceTrigger(fireAt time.Time) *ExecuteOnceTrigger {
	return &ExecuteOnceTrigger{FireAt: fireAt}
}
