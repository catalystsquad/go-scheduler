package pkg

import (
	"time"
)

type TriggerInterface interface {
	GetNextFireTime(task Task) *time.Time
	IsRecurring() bool
}
