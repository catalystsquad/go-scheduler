package pkg

import (
	"github.com/emirpasic/gods/trees/btree"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryStore struct {
	lock         *sync.Mutex
	scheduleTree *btree.Tree
	taskTree     *btree.Tree
}

func (m *MemoryStore) Initialize() error {
	m.scheduleTree = btree.NewWith(3, ScheduleTreeComparator)
	m.taskTree = btree.NewWithStringComparator(3)
	m.lock = new(sync.Mutex)
	return nil
}

func (m *MemoryStore) ScheduleTask(task Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	scheduleKey := GetScheduleTreeKey(task)
	taskId := task.IdString()
	m.scheduleTree.Put(scheduleKey, taskId) // only store the ID for less memory usage
	m.taskTree.Put(taskId, task)
	return nil
}

func (m *MemoryStore) UpdateTask(task Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.taskTree.Put(task.IdString(), task)
	return nil
}

func (m *MemoryStore) DeleteTask(id *uuid.UUID) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.taskTree.Remove(id.String())
	return nil
}

func (m *MemoryStore) GetUpcomingTasks(limit time.Time) ([]Task, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	iterator := m.scheduleTree.Iterator()
	tasks := []Task{}
	for iterator.Next() {
		key, value := iterator.Key(), iterator.Value()
		timestamp, err := getTimestampFromScheduleKey(key.(string))
		if err != nil {
			return nil, err
		}
		if timestamp.After(limit) {
			break // hit the limit, exit the loop
		}
		id := value.(string)
		taskValue, found := m.taskTree.Get(id)
		if found {
			tasks = append(tasks, taskValue.(Task))
		} else {
			// task doesn't exist, remove it from the schedule tree
			m.scheduleTree.Remove(key)
		}
	}
	return tasks, nil
}

func NewMemoryStore() StoreInterface {
	store := &MemoryStore{}
	return store
}
