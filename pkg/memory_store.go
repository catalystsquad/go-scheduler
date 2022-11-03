package pkg

//
//import (
//	"github.com/emirpasic/gods/trees/btree"
//	"github.com/google/uuid"
//	"sync"
//	"time"
//)
//
//type MemoryStore struct {
//	lock         *sync.Mutex
//	scheduleTree *btree.Tree
//	taskTree     *btree.Tree
//}
//
//func (m *MemoryStore) Initialize() error {
//	m.scheduleTree = btree.NewWith(3, ScheduleTreeComparator)
//	m.taskTree = btree.NewWithStringComparator(3)
//	m.lock = new(sync.Mutex)
//	return nil
//}
//
//func (m *MemoryStore) ScheduleTask(task TaskDefinition) error {
//	m.lock.Lock()
//	defer m.lock.Unlock()
//	m.ScheduleTaskInScheduleTree(task)
//	m.taskTree.Put(task.IdString(), task)
//	return nil
//}
//
//func (m *MemoryStore) ScheduleTaskInScheduleTree(task TaskDefinition) {
//	scheduleKey := GetScheduleTreeKey(task)
//	taskId := task.IdString()
//	m.scheduleTree.Put(scheduleKey, taskId) // only store the ID for less memory usage
//}
//
//func (m *MemoryStore) UpdateTask(task TaskDefinition) error {
//	m.lock.Lock()
//	defer m.lock.Unlock()
//	m.taskTree.Put(task.IdString(), task)
//	return nil
//}
//
//func (m *MemoryStore) DeleteTask(id *uuid.UUID) error {
//	m.lock.Lock()
//	defer m.lock.Unlock()
//	m.taskTree.Remove(id.String())
//	return nil
//}
//
//func (m *MemoryStore) GetUpcomingTasks(limit time.Time) ([]TaskDefinition, error) {
//	m.lock.Lock()
//	defer m.lock.Unlock()
//	iterator := m.scheduleTree.Iterator()
//	tasks := []TaskDefinition{}
//	for iterator.Next() {
//		key, value := iterator.Key(), iterator.Value()
//		timestamp, err := getTimestampFromScheduleKey(key.(string))
//		if err != nil {
//			return nil, err
//		}
//		if timestamp.After(limit) {
//			break // hit the limit, exit the loop
//		}
//		id := value.(string)
//		taskValue, found := m.taskTree.Get(id)
//		if shouldReturnTask(found, timestamp, taskValue) {
//			tasks = append(tasks, taskValue.(TaskDefinition))
//		} else {
//			// task doesn't exist or the next fire time has been updated by the scheduler, remove it from the schedule tree
//			m.scheduleTree.Remove(key)
//			if found {
//				// task still exists but has a different fire time, add the task to the schedule tree with the new fire time
//				m.ScheduleTaskInScheduleTree(taskValue.(TaskDefinition))
//			}
//		}
//	}
//	return tasks, nil
//}
//
//func shouldReturnTask(found bool, keyTimestamp time.Time, taskValue interface{}) bool {
//	if !found {
//		return false
//	}
//	task := taskValue.(TaskDefinition)
//	taskNextFireTime := task.NextFireTime
//	return keyTimestamp.Format(time.RFC3339Nano) == taskNextFireTime.Format(time.RFC3339Nano)
//}
//
//func NewMemoryStore() StoreInterface {
//	store := &MemoryStore{}
//	return store
//}
