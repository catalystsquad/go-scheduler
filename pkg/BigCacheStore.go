package pkg

import (
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

type BigCacheStore struct {
	config        bigcache.Config
	scheduleCache *bigcache.BigCache
	tasksCache    *bigcache.BigCache
}

func NewBigCacheStore(config *bigcache.Config) StoreInterface {
	if config == nil {
		defaultConfig := bigcache.DefaultConfig(1 * time.Hour)
		config = &defaultConfig
	}
	return &BigCacheStore{
		config: *config,
	}
}

func (s *BigCacheStore) UpdateTask(task Task) error {
	return s.setTask(task)
}

func (s *BigCacheStore) setTask(task Task) error {
	taskBytes, err := task.AsBytes()
	if err != nil {
		return err
	}
	return s.tasksCache.Set(task.Id.String(), taskBytes)
}

func (s *BigCacheStore) DeleteTask(id *uuid.UUID) error {
	return s.tasksCache.Delete(id.String())
}

func (s *BigCacheStore) GetUpcomingTasks(limit time.Time) ([]Task, error) {
	iterator := s.scheduleCache.Iterator()
	tasks := []Task{}
	for iterator.SetNext() == true {
		entry, err := iterator.Value()
		if err != nil {
			return nil, err
		}
		id := string(entry.Value())
		taskEntry, err := s.tasksCache.Get(id)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				// task no longer exists, delete the schedule entry
				err = s.scheduleCache.Delete(entry.Key())
				if err != nil {
					logging.Log.WithError(err).WithFields(logrus.Fields{"schedule_entry_key": entry.Key(), "task_id": id}).Error("error deleting schedule entry for task that no longer exists")
				}
				continue
			}
			return nil, err
		}
		task, err := TaskFromBytes(taskEntry)
		if err != nil {
			return nil, err
		}
		nextFireTime, err := task.GetTrigger().GetNextFireTime()
		if err != nil {
			return nil, err
		}
		if nextFireTime.Before(limit) {
			// task is within this execution window, add it to the tasks
			tasks = append(tasks, task)
		} else {
			// task is outside the execution window, which means all the rest will be as well, so break
			break
		}
	}
	return tasks, nil
}

func (s *BigCacheStore) MarkTaskInProgress(task Task) error {
	//TODO implement me
	panic("implement me")
}

func (s *BigCacheStore) Initialize() error {
	var err error
	s.scheduleCache, err = bigcache.NewBigCache(s.config)
	if err != nil {
		return err
	}
	s.tasksCache, err = bigcache.NewBigCache(s.config)
	return err
}

func (s *BigCacheStore) ScheduleTask(task Task) error {
	scheduleKey, err := s.generateScheduleKey(task)
	if err != nil {
		return err
	}
	err = s.scheduleCache.Set(scheduleKey, task.GetIdBytes())
	if err != nil {
		return err
	}
	// store the task
	return s.setTask(task)
}

func (s *BigCacheStore) generateScheduleKey(task Task) (string, error) {
	nextFireTime, err := task.GetTrigger().GetNextFireTime()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%s", TimeToString(nextFireTime), task.Id.String()), nil
}
