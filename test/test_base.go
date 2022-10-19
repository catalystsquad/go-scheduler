package test

import (
	"errors"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type TestMetaData struct {
	Message string
}

func TestSchedulerHappyPath(t *testing.T, store pkg.StoreInterface) {
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	executeAt := time.Now().Add(5 * time.Second)
	task := pkg.Task{
		Id:                 &id,
		Metadata:           metaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
	}
	err = scheduler.ScheduleTask(task)
	require.NoError(t, err)
	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(20 * time.Second)
	require.Equal(t, 1, executionCount)
}

func TestSchedulerTasksRunInOrder(t *testing.T, store pkg.StoreInterface) {
	// this tests that tasks are executed in the right order. 3 tasks are scheduled with execution times nearer to now than the last
	// resulting in scheduling the last running task first, and the first running task last. The first running task should
	// be executed first even though it was scheduled last
	executedTasks := []pkg.Task{}
	handler := func(task pkg.Task) error {
		executedTasks = append(executedTasks, task)
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
	// create three tasks that should run in reverse order of when they're scheduled
	// task1
	task1Id := uuid.New()
	task1MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	task1ExecuteAt := time.Now().Add(7 * time.Second)
	task1 := pkg.Task{
		Id:                 &task1Id,
		Metadata:           task1MetaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task1ExecuteAt),
	}
	err = scheduler.ScheduleTask(task1)
	require.NoError(t, err)

	// task2
	task2Id := uuid.New()
	task2MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	task2ExecuteAt := time.Now().Add(5 * time.Second)
	task2 := pkg.Task{
		Id:                 &task2Id,
		Metadata:           task2MetaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task2ExecuteAt),
	}
	err = scheduler.ScheduleTask(task2)
	require.NoError(t, err)

	// task3
	task3Id := uuid.New()
	task3MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	task3ExecuteAt := time.Now().Add(3 * time.Second)
	task3 := pkg.Task{
		Id:                 &task3Id,
		Metadata:           task3MetaData,
		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task3ExecuteAt),
	}
	err = scheduler.ScheduleTask(task3)
	require.NoError(t, err)

	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	require.Len(t, executedTasks, 3)
	require.Equal(t, executedTasks[0].Id, task3.Id)
	require.Equal(t, executedTasks[1].Id, task2.Id)
	require.Equal(t, executedTasks[2].Id, task1.Id)
}

func TestSchedulerLongRunningTaskExpired(t *testing.T, store pkg.StoreInterface) {
	// first task sleeps longer than the window and expiration, simulating a long running task that eventually completes successfully
	// this should result in the task expiring and being run twice.
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		time.Sleep(3 * time.Second)
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
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
	require.NoError(t, err)
	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	require.Equal(t, 2, executionCount)
}

func TestSchedulerLongRunningTaskNotExpired(t *testing.T, store pkg.StoreInterface) {
	// first task sleeps longer than the window but less than the expiration, simulating a long running task that eventually completes successfully before the expiration time
	// this should result in the task being run once.
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		time.Sleep(3 * time.Second)
		return nil
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
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
	require.NoError(t, err)
	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	require.Equal(t, 1, executionCount)
}

func TestSchedulerRetry(t *testing.T, store pkg.StoreInterface) {
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return errors.New("fayl")
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
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
	require.NoError(t, err)
	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	require.GreaterOrEqual(t, executionCount, 5) // there is a timing issue here, we need to make sure that the task was retried but this will
	// have different timing based on the system running it and resources etc. I tried to test it with exactly 3 retries but it's difficult because
	// sometimes it will run longer and have executed 4 times, sometimes 2. So I settled on waiting 10 seconds which is much longer
	// than should be required, and verifying it's retried 5 times
}

func TestSchedulerNoRetry(t *testing.T, store pkg.StoreInterface) {
	executionCount := 0
	handler := func(task pkg.Task) error {
		executionCount++
		return errors.New("fayl")
	}
	// tick once per second
	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
	require.NoError(t, err)
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
	require.NoError(t, err)
	go scheduler.Run()
	require.NoError(t, err)
	time.Sleep(4 * time.Second)
	require.Equal(t, 1, executionCount)
}
