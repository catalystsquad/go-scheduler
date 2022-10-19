package test

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/google/uuid"
	"time"
)

func GenerateExecuteOnceTask(fireAt *time.Time) pkg.Task {
	id := uuid.New()
	if fireAt == nil {
		generatedTime := time.Now().Add(time.Duration(gofakeit.IntRange(1, 5)) * time.Second).UTC()
		fireAt = &generatedTime
	}
	trigger := &pkg.ExecuteOnceTrigger{FireAt: *fireAt}
	return pkg.Task{
		Id:                 &id,
		ExecuteOnceTrigger: trigger,
	}
}
