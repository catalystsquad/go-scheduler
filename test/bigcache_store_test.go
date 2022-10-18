package test

import (
	"errors"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type TestMetaData struct {
	Message string
}

type BigcacheStoreSuite struct {
	suite.Suite
}

func (s *BigcacheStoreSuite) SetupSuite() {
}

func (s *BigcacheStoreSuite) TearDownSuite() {
}

func (s *BigcacheStoreSuite) SetupTest() {
}

func TestBigcacheStoreSuite(t *testing.T) {
	suite.Run(t, new(BigcacheStoreSuite))
}

func (s *BigcacheStoreSuite) TestBigCacheStoreHappyPath() {
	store := pkg.NewBigCacheStore(nil)
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(s.T(), err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(5 * time.Second)
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(s.T(), err)
	go scheduler.Run()
	require.NoError(s.T(), err)
	time.Sleep(20 * time.Second)
	require.Equal(s.T(), 1, executionCount)
}

func (s *BigcacheStoreSuite) TestBigCacheStoreLongRunningTaskExpired() {
	// first task sleeps longer than the window and expiration, simulating a long running task that eventually completes successfully
	// this should result in the task expiring and being run twice.
	store := pkg.NewBigCacheStore(nil)
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		time.Sleep(3 * time.Second)
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(s.T(), err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(2 * time.Second)
	expireAfter := 2 * time.Second
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
		ExpireAfter:        &expireAfter,
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(s.T(), err)
	go scheduler.Run()
	require.NoError(s.T(), err)
	time.Sleep(10 * time.Second)
	require.Equal(s.T(), 2, executionCount)
}

func (s *BigcacheStoreSuite) TestBigCacheStoreLongRunningTaskNotExpired() {
	// first task sleeps longer than the window but less than the expiration, simulating a long running task that eventually completes successfully before the expiration time
	// this should result in the task being run once.
	store := pkg.NewBigCacheStore(nil)
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		time.Sleep(3 * time.Second)
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(s.T(), err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(2 * time.Second)
	expireAfter := 4 * time.Second
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
		ExpireAfter:        &expireAfter,
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(s.T(), err)
	go scheduler.Run()
	require.NoError(s.T(), err)
	time.Sleep(10 * time.Second)
	require.Equal(s.T(), 1, executionCount)
}

func (s *BigcacheStoreSuite) TestBigCacheStoreRetry() {
	store := pkg.NewBigCacheStore(nil)
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return errors.New("fayl")
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(s.T(), err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(2 * time.Second)
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
		RetryOnError:       true,
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(s.T(), err)
	go scheduler.Run()
	require.NoError(s.T(), err)
	time.Sleep(4 * time.Second)
	require.Equal(s.T(), 3, executionCount)
}

func (s *BigcacheStoreSuite) TestBigCacheStoreNoRetry() {
	store := pkg.NewBigCacheStore(nil)
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return errors.New("fayl")
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(s.T(), err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(2 * time.Second)
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
		RetryOnError:       false,
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(s.T(), err)
	go scheduler.Run()
	require.NoError(s.T(), err)
	time.Sleep(4 * time.Second)
	require.Equal(s.T(), 1, executionCount)
}
