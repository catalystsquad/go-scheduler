package pkg

import (
	"time"
)

type TriggerInterface interface {
	GetFireTime(from time.Time) *time.Time
	IsRecurring() bool
}
