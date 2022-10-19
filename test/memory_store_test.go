package test

import (
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type MemoryStoreSuite struct {
	suite.Suite
}

func (s *MemoryStoreSuite) SetupSuite() {
}

func (s *MemoryStoreSuite) TearDownSuite() {
}

func (s *MemoryStoreSuite) SetupTest() {
}

func TestMemoryStoreSuite(t *testing.T) {
	suite.Run(t, new(MemoryStoreSuite))
}

func (s *MemoryStoreSuite) TestMemoryStoreHappyPath() {
	store := pkg.NewMemoryStore()
	TestSchedulerHappyPath(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreTasksRunInOrder() {
	store := pkg.NewMemoryStore()
	TestSchedulerTasksRunInOrder(s.T(), store)
}

func (s *MemoryStoreSuite) TestBigCacheStoreLongRunningTaskExpired() {
	store := pkg.NewMemoryStore()
	TestSchedulerLongRunningTaskExpired(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreLongRunningTaskNotExpired() {
	store := pkg.NewMemoryStore()
	TestSchedulerLongRunningTaskNotExpired(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreRetry() {
	store := pkg.NewMemoryStore()
	TestSchedulerRetry(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreNoRetry() {
	store := pkg.NewMemoryStore()
	TestSchedulerNoRetry(s.T(), store)
}

func (s *MemoryStoreSuite) TestGetUpcomingTasksAllUnderLimit() {
	store := pkg.NewMemoryStore()
	err := store.Initialize()
	require.NoError(s.T(), err)
	numTasks := 10
	tasks := make([]pkg.Task, numTasks)
	for i := numTasks; i > 0; i-- {
		fireAt := time.Now().Add(time.Duration(i) * time.Second)
		task := GenerateExecuteOnceTask(&fireAt)
		err = store.ScheduleTask(task)
		require.NoError(s.T(), err)
		tasks[i-1] = task
	}
	upcomingTasks, err := store.GetUpcomingTasks(time.Now().Add(1 * time.Minute))
	require.NoError(s.T(), err)
	require.Len(s.T(), upcomingTasks, numTasks)
	for i, task := range upcomingTasks {
		require.Equal(s.T(), tasks[i].Id, task.Id)
	}
}

func (s *MemoryStoreSuite) TestGetUpcomingTasksSomeOverLimit() {
	store := pkg.NewMemoryStore()
	err := store.Initialize()
	require.NoError(s.T(), err)
	numTasks := 20
	numLimit := 13
	tasks := make([]pkg.Task, numTasks)
	for i := numTasks; i > 0; i-- {
		fireAt := time.Now().Add(time.Duration(i) * time.Second)
		task := GenerateExecuteOnceTask(&fireAt)
		err = store.ScheduleTask(task)
		require.NoError(s.T(), err)
		tasks[i-1] = task
	}
	limit := tasks[numLimit].GetTrigger().GetNextFireTime()
	upcomingTasks, err := store.GetUpcomingTasks(*limit)
	require.NoError(s.T(), err)
	require.Len(s.T(), upcomingTasks, numLimit+1) // + 1 because the limit is inclusive, so we'll get the task at index numLimit back as well
	for i, task := range upcomingTasks {
		require.Equal(s.T(), tasks[i].Id, task.Id)
	}
}
