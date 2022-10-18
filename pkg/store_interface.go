package pkg

import (
	"github.com/google/uuid"
	"time"
)

type StoreInterface interface {
	Initialize() error
	ScheduleTask(task Task) error
	UpdateTask(task Task) error
	DeleteTask(id *uuid.UUID) error
	GetUpcomingTasks(limit time.Time) ([]Task, error)
}
