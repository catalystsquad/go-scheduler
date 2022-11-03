package pkg

import (
	"time"
)

type TriggerInterface interface {
	GetNextFireTime(task TaskDefinition) *time.Time
	IsRecurring() bool
}
