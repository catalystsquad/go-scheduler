package pkg

import (
	"github.com/gorhill/cronexpr"
	"time"
)

const oncePerSecondCron = "* * * * * * *"

type CronTrigger struct {
	Expression string               `json:"expression"`
	cronexpr   *cronexpr.Expression `json:"cronexpr"`
}

func (t CronTrigger) GetFireTime(from time.Time) *time.Time {
	nextFireTime := t.cronexpr.Next(from)
	return &nextFireTime
}

func (t CronTrigger) IsRecurring() bool {
	return true
}

func NewCronTrigger(cronExpression string) (*CronTrigger, error) {
	// default to once per second
	if cronExpression == "" {
		cronExpression = oncePerSecondCron
	}
	cronexpr, err := cronexpr.Parse(cronExpression)
	if err != nil {
		return nil, err
	}
	return &CronTrigger{
		Expression: cronExpression,
		cronexpr:   cronexpr,
	}, nil
}
