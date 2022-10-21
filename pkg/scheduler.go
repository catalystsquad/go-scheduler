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
	task.NextFireTime = task.GetTrigger().GetNextFireTime(task)
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

func (s *Scheduler) shouldHandleTask(task Task) bool {
	if !task.InProgress {
		// not in progress, so we should handle it
		return true
	}
	// check the expiration time, if it's expired then that means it's run before but wasn't completed for some reason
	// so we should run it again
	expirationTime := task.LastFireTime.Add(*task.ExpireAfter)
	return time.Now().After(expirationTime)
}

func (s *Scheduler) handleTask(task Task) {
	// execute the handler
	err := s.Handler(task)
	if err == nil || !task.RetryOnError {
		// either no error, or there is an error but task shouldn't be retried, so update the next fire time so it doesn't get
		// picked up again in the next window
		s.updateNextFireTime(task)
	} else {
		// just log the error, task will get picked up again in the next execution window since the fire time wasn't updated
		logging.Log.WithError(err).WithFields(logrus.Fields{"id": task.Id}).Error("error handling task")
	}
}

func (s *Scheduler) markTaskInProgress(task *Task) error {
	task.InProgress = true
	task.LastFireTime = task.NextFireTime
	logging.Log.Debug("setting task in progress")
	return s.store.UpdateTask(*task)
}

func (s *Scheduler) updateNextFireTime(task Task) {
	var err error
	nextFireTime := time.Time{}
	trigger := task.GetTrigger()
	if trigger.IsRecurring() {
		task.InProgress = false
		nextFireTime = *task.GetTrigger().GetNextFireTime(task)
		task.NextFireTime = &nextFireTime
		logging.Log.WithFields(logrus.Fields{"current_time": time.Now().UTC().Format(time.RFC3339Nano), "next_fire_time": nextFireTime.Format(time.RFC3339Nano)}).Debug("scheduler setting next fire time for recurring trigger")
		err = s.store.UpdateTask(task)
	} else {
		logging.Log.Debug("scheduler deleting task because trigger is non-recurring")
		err = s.store.DeleteTask(task.Id)
	}
	if err != nil {
		logging.Log.WithError(err).WithFields(logrus.Fields{"task_id": task.Id.String()}).Error("error updating or deleting task when updating next fire time")
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
				// mark task in progress
				err := s.markTaskInProgress(task)
				if err == nil {
					// task set as in progress handle the task
					go s.handleTask(*task)
				} else {
					// failed to mark the task in progress, don't call the handler, will try again in the next window execution
					logging.Log.WithError(err).WithFields(logrus.Fields{"task_id": task.Id}).Error("error marking task in progress")
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
	return fmt.Sprintf("%s_%s", task.GetTrigger().GetNextFireTime(task).Format(time.RFC3339Nano), task.Id.String())
}

func getTimestampFromScheduleKey(key string) (time.Time, error) {
	timestampString := strings.Split(key, "_")[0]
	return time.Parse(time.RFC3339Nano, timestampString)
}
