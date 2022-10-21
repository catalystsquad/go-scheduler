package pkg

import (
	"github.com/gorhill/cronexpr"
	"time"
)

type CronTrigger struct {
	expression *cronexpr.Expression
}

func (t CronTrigger) GetNextFireTime(task Task) *time.Time {
	var nextFireTime time.Time
	nextFireTimes := t.expression.NextN(time.Now(), 2)
	if task.LastFireTime != nil && task.LastFireTime.Equal(nextFireTimes[0]) {
		// this can happen if the task executes faster than the tick rate, in which case, schedule the task for the next scheduled time
		nextFireTime = nextFireTimes[1]
	} else {
		nextFireTime = nextFireTimes[0]
	}
	return &nextFireTime
}

func (t CronTrigger) IsRecurring() bool {
	return true
}

func NewCronTrigger(cronExpression string) (*CronTrigger, error) {
	expression, err := cronexpr.Parse(cronExpression)
	if err != nil {
		return nil, err
	}
	return &CronTrigger{expression: expression}, nil
}
