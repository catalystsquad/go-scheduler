package pkg

import (
	"fmt"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/emirpasic/gods/trees/btree"
	"github.com/google/uuid"
	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

type Scheduler struct {
	Window       *time.Duration
	Handler      func(task Task) error
	store        StoreInterface
	scheduleTree *btree.Tree
	lock         *sync.Mutex
}

func NewScheduler(window time.Duration, handler func(task Task) error, store StoreInterface) (*Scheduler, error) {
	scheduler := &Scheduler{
		Window:       &window,
		Handler:      handler,
		store:        store,
		scheduleTree: btree.NewWith(3, ScheduleTreeComparator),
		lock:         new(sync.Mutex),
	}
	err := scheduler.initializeStore()
	return scheduler, err
}

func (s *Scheduler) ScheduleTask(task Task) error {
	err := validateTask(task)
	if err != nil {
		return err
	}
	if task.ExpireAfter == nil {
		task.ExpireAfter = s.Window
	}
	return s.store.ScheduleTask(task)
}

func (s *Scheduler) UpdateTask(task Task) error {
	err := validateTask(task)
	if err != nil {
		return err
	}
	return s.store.UpdateTask(task)
}

func (s *Scheduler) DeleteTask(id *uuid.UUID) error {
	if id == nil {
		return errorx.IllegalArgument.New("an id must be provided")
	}
	return s.store.DeleteTask(id)
}

func (s *Scheduler) Run() error {
	if s.Handler == nil {
		return errorx.IllegalArgument.New("handler cannot be nil")
	}
	ticker := time.NewTicker(*s.Window)
	for c := range ticker.C {
		s.scheduleJobs(c)
		s.consumeTasks()
	}
	return nil
}

func validateTask(task Task) error {
	if task.Id == nil {
		return errorx.IllegalArgument.New("tasks must have an id")
	}
	if task.Metadata == nil {
		return errorx.IllegalArgument.New("tasks must have metadata")
	}
	return nil
}

func (s *Scheduler) scheduleJobs(firedAt time.Time) {
	limit := firedAt.Add(*s.Window)
	upcomingTasks, err := s.store.GetUpcomingTasks(limit)
	logging.Log.WithFields(logrus.Fields{"count": len(upcomingTasks)}).Debug("got upcoming tasks")
	if err != nil {
		logging.Log.WithError(err).Error("error fetching upcoming tasks")
		return
	}
	for _, task := range upcomingTasks {
		s.addTaskToTree(task)
	}
}

func (s *Scheduler) updateTaskInProgressTimestamp(task Task) error {
	inProgressAt := time.Now().UTC()
	task.InProgressAt = &inProgressAt
	return s.store.UpdateTask(task)
}

func (s *Scheduler) shouldHandleTask(task Task) bool {
	if task.InProgressAt == nil {
		// not in progress, so we should handle it
		return true
	}
	// check the expiration time against, if it's expired then that means it's run before but wasn't completed for some reason
	// so we should run it again
	expirationTime := task.InProgressAt.Add(*task.ExpireAfter)
	return time.Now().After(expirationTime)
}

func (s *Scheduler) handleTask(task Task) {
	// execute the handler
	err := s.Handler(task)
	if err == nil {
		// no error, update the task's next fire time if it's a cron, or delete it if it's a single task
		s.updateNextFireTime(task)
	} else {
		// log the error
		logging.Log.WithError(err).WithFields(logrus.Fields{"id": task.Id}).Error("error handling task")
		if !task.RetryOnError {
			// task shouldn't be retried, so update the next fire time. Only do this for tasks that shouldn't be retried because
			// if the next fire time isn't updated, then it will get picked up in the next window
			s.updateNextFireTime(task)
		}
	}
}

func (s *Scheduler) updateNextFireTime(task Task) {
	// only current trigger is execute once, so we just delete the task
	err := s.store.DeleteTask(task.Id)
	if err != nil {
		logging.Log.WithError(err).WithFields(logrus.Fields{"task_id": task.Id}).Error("error updating next fire time")
	}
}

func (s *Scheduler) initializeStore() error {
	return s.store.Initialize()
}

func (s *Scheduler) addTaskToTree(task Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.scheduleTree.Put(GetScheduleTreeKey(task), &task)
}

func (s *Scheduler) popTaskFromTree() *Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := s.scheduleTree.LeftKey()
	value := s.scheduleTree.LeftValue()
	if value != nil {
		s.scheduleTree.Remove(key)
		task := value.(*Task)
		return task
	} else {
		return nil
	}
}

func (s *Scheduler) consumeTasks() {
	run := true
	for run {
		task := s.popTaskFromTree()
		if task == nil {
			run = false // no more tasks left to handle
		} else {
			if s.shouldHandleTask(*task) {
				inProgressAt := time.Now().UTC()
				task.InProgressAt = &inProgressAt
				err := s.store.UpdateTask(*task)
				if err == nil {
					go s.handleTask(*task)
				} else {
					if err != nil {
						logging.Log.WithError(err).WithFields(logrus.Fields{"task_id": task.Id}).Error("error marking task in progress")
					}
				}
			}
		}
	}
}

func ScheduleTreeComparator(a, b interface{}) int {
	aTime, err := getTimestampFromScheduleKey(a.(string))
	if err != nil {
		logging.Log.WithError(err).WithFields(logrus.Fields{"key_value": a}).Error("error comparing keys")
	}
	bTime, err := getTimestampFromScheduleKey(b.(string))
	if err != nil {
		logging.Log.WithError(err).WithFields(logrus.Fields{"key_value": b}).Error("error comparing keys")
	}
	switch {
	case aTime.Before(bTime):
		return -1
	case aTime.After(bTime):
		return 1
	default:
		return 0
	}
}

func GetScheduleTreeKey(task Task) string {
	return fmt.Sprintf("%s_%s", task.GetTrigger().GetNextFireTime().Format(time.RFC3339), task.Id.String())
}

func getTimestampFromScheduleKey(key string) (time.Time, error) {
	timestampString := strings.Split(key, "_")[0]
	return time.Parse(time.RFC3339, timestampString)
}
