package test

import (
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const oncePerSecondCron = "* * * * * * *"
const oncePerMinuteCronFormat = "%d * * * * * *"
const everyNSecondsCronFormat = "0/%d * * * * * *"

type TestMetaData struct {
	Message string
}

func TestTaskDefinitionCrud(t *testing.T, store pkg.StoreInterface) {
	// create one task of each trigger type
	expectedExecuteOnceTask := generateRandomTaskWithExecuteOnceTrigger(time.Now().Add(5 * time.Second))
	expectedCronTask, err := generateRandomTaskWithCronTrigger("@hourly")
	require.NoError(t, err)
	err = store.CreateTaskDefinition(expectedExecuteOnceTask)
	require.NoError(t, err)
	err = store.CreateTaskDefinition(expectedCronTask)
	require.NoError(t, err)
	tasks, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assertTaskEquality(t, expectedExecuteOnceTask, tasks[0])
	assertTaskEquality(t, expectedCronTask, tasks[1])
	// update execute once task
	updatedExecuteOnceTask := tasks[0]
	expectedExpireAfter := 10 * time.Second
	expectedMetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	updatedExecuteOnceTask.ExpireAfter = &expectedExpireAfter
	updatedExecuteOnceTask.Metadata = expectedMetaData
	updatedExecuteOnceTask.ExecuteOnceTrigger = pkg.NewExecuteOnceTrigger(time.Now().Add(20 * time.Second))
	err = store.UpdateTaskDefinition(updatedExecuteOnceTask)
	require.NoError(t, err)
	// verify update
	fetchedExecuteOnceTask, err := store.GetTaskDefinition(updatedExecuteOnceTask.Id)
	require.NoError(t, err)
	assertTaskEquality(t, updatedExecuteOnceTask, fetchedExecuteOnceTask)
	// update cron task
	updatedCronTask := tasks[1]
	updatedCronExpression := "@daily"
	updatedCronTask.CronTrigger, err = pkg.NewCronTrigger(updatedCronExpression)
	require.NoError(t, err)
	updatedCronTask.ExpireAfter = &expectedExpireAfter
	updatedCronTask.Metadata = expectedMetaData
	err = store.UpdateTaskDefinition(updatedCronTask)
	require.NoError(t, err)
	// verify update
	fetchedCronTask, err := store.GetTaskDefinition(updatedCronTask.Id)
	require.NoError(t, err)
	assertTaskEquality(t, updatedCronTask, fetchedCronTask)
	// test list offset/limit
	tasks, err = store.ListTaskDefinitions(0, 1)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, updatedExecuteOnceTask.Id, tasks[0].Id)
	tasks, err = store.ListTaskDefinitions(1, 1)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, updatedCronTask.Id, tasks[0].Id)
	tasks, err = store.ListTaskDefinitions(2, 10)
	require.NoError(t, err)
	require.Len(t, tasks, 0)
	// delete task definitions
	err = store.DeleteTaskDefinition(updatedExecuteOnceTask.Id)
	require.NoError(t, err)
	fetchedExecuteOnceTask, err = store.GetTaskDefinition(updatedExecuteOnceTask.Id)
	require.ErrorContains(t, err, "record not found")
	err = store.DeleteTaskDefinition(updatedCronTask.Id)
	require.NoError(t, err)
	fetchedCronTask, err = store.GetTaskDefinition(updatedCronTask.Id)
	require.ErrorContains(t, err, "record not found")
}

func TestTaskInstanceCrud(t *testing.T, store pkg.StoreInterface) {
	// create one task of each trigger type
	expectedExecuteOnceTask := generateRandomTaskWithExecuteOnceTrigger(time.Now().Add(5 * time.Second))
	expireAfter := 2 * time.Second
	expectedExecuteOnceTask.ExpireAfter = &expireAfter
	expectedCronTask, err := generateRandomTaskWithCronTrigger("@hourly")
	expectedCronTask.ExpireAfter = &expireAfter
	require.NoError(t, err)
	err = store.CreateTaskDefinition(expectedExecuteOnceTask)
	require.NoError(t, err)
	err = store.CreateTaskDefinition(expectedCronTask)
	require.NoError(t, err)
	tasks, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	fetchedExecuteOnceTask := tasks[0]
	fetchedCronTask := tasks[1]
	// create a task instance for each task
	executeOnceTaskInstance := createTaskInstanceFromTaskDefinition(fetchedExecuteOnceTask)
	err = store.CreateTaskInstance(executeOnceTaskInstance)
	require.NoError(t, err)
	cronTaskInstance := createTaskInstanceFromTaskDefinition(fetchedCronTask)
	err = store.CreateTaskInstance(cronTaskInstance)
	require.NoError(t, err)
	// list task instances
	listedTaskInstances, err := store.ListTaskInstances(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 2)
	listedExecuteOnceTaskInstance := listedTaskInstances[0]
	listedCronTaskInstance := listedTaskInstances[1]
	// update task instances
	completedAt := time.Now().UTC()
	listedExecuteOnceTaskInstance.CompletedAt = &completedAt
	listedCronTaskInstance.CompletedAt = &completedAt
	err = store.UpdateTaskInstance(listedExecuteOnceTaskInstance)
	require.NoError(t, err)
	err = store.UpdateTaskInstance(listedCronTaskInstance)
	require.NoError(t, err)
	// fetch by id and verify the update
	fetchedExecuteOnceTaskInstance, err := store.GetTaskInstance(listedExecuteOnceTaskInstance.Id)
	require.NoError(t, err)
	assertTaskInstanceEquality(t, listedExecuteOnceTaskInstance, fetchedExecuteOnceTaskInstance)
	fetchedCronTaskInstance, err := store.GetTaskInstance(listedCronTaskInstance.Id)
	require.NoError(t, err)
	assertTaskInstanceEquality(t, listedCronTaskInstance, fetchedCronTaskInstance)
	// verify list with offest/limit
	listedTaskInstances, err = store.ListTaskInstances(0, 1)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	require.Equal(t, listedTaskInstances[0].Id, listedExecuteOnceTaskInstance.Id)
	listedTaskInstances, err = store.ListTaskInstances(1, 1)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	require.Equal(t, listedTaskInstances[0].Id, listedCronTaskInstance.Id)
	// delete
	err = store.DeleteTaskInstance(listedExecuteOnceTaskInstance.Id)
	require.NoError(t, err)
	err = store.DeleteTaskInstance(listedCronTaskInstance.Id)
	require.NoError(t, err)
	// verify delete
	_, err = store.GetTaskInstance(listedExecuteOnceTaskInstance.Id)
	require.ErrorContains(t, err, "record not found")
	_, err = store.GetTaskInstance(listedCronTaskInstance.Id)
	require.ErrorContains(t, err, "record not found")
}

func TestGetTaskInstancesToRunNotInProgressNotExpired(t *testing.T, store pkg.StoreInterface) {
	// create task definition
	executeAt := time.Now().Add(5 * time.Minute)
	taskDefinition := generateRandomTaskWithExecuteOnceTrigger(executeAt)
	expireAfter := 1 * time.Minute
	taskDefinition.ExpireAfter = &expireAfter
	err := store.CreateTaskDefinition(taskDefinition)
	require.NoError(t, err)
	listedTaskDefinitions, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskDefinitions, 1)
	listedTaskDefinition := listedTaskDefinitions[0]
	// create task instance
	taskInstance := createTaskInstanceFromTaskDefinition(listedTaskDefinition)
	err = store.CreateTaskInstance(taskInstance)
	require.NoError(t, err)
	// list task instances so we have a reference
	listedTaskInstances, err := store.ListTaskInstances(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	listedTaskInstance := listedTaskInstances[0]
	// query for tasks to run with a limit of now(), shouldn't come back
	taskInstancesToRun, err := store.GetTaskInstancesToRun(time.Now())
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 0)
	// query for tasks to run in the next 5 minutes, should get the task instance back
	taskInstancesToRun, err = store.GetTaskInstancesToRun(time.Now().Add(5 * time.Minute))
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 1)
	taskInstanceToRun := taskInstancesToRun[0]
	require.Equal(t, listedTaskInstance.Id, taskInstanceToRun.Id)
}

func TestGetTaskInstancesToRunInProgressNotExpired(t *testing.T, store pkg.StoreInterface) {
	// create task definition
	executeAt := time.Now().Add(5 * time.Minute)
	taskDefinition := generateRandomTaskWithExecuteOnceTrigger(executeAt)
	expireAfter := 1 * time.Minute
	taskDefinition.ExpireAfter = &expireAfter
	err := store.CreateTaskDefinition(taskDefinition)
	require.NoError(t, err)
	listedTaskDefinitions, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskDefinitions, 1)
	listedTaskDefinition := listedTaskDefinitions[0]
	// create task instance that is in progress
	taskInstance := createTaskInstanceFromTaskDefinition(listedTaskDefinition)
	taskInstance.StartedAt = taskInstance.ExecuteAt
	err = store.CreateTaskInstance(taskInstance)
	require.NoError(t, err)
	// list task instances so we have a reference
	listedTaskInstances, err := store.ListTaskInstances(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	// query for tasks to run with a limit of now(), shouldn't come back
	taskInstancesToRun, err := store.GetTaskInstancesToRun(time.Now())
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 0)
	// query for tasks to run in the next 5 minutes, shouldn't get the task instance back because it's already in progress
	taskInstancesToRun, err = store.GetTaskInstancesToRun(time.Now().Add(5 * time.Minute))
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 0)
}

func TestGetTaskInstancesToRunInProgressAndExpired(t *testing.T, store pkg.StoreInterface) {
	// create task definition
	executeAt := time.Now()
	taskDefinition := generateRandomTaskWithExecuteOnceTrigger(executeAt)
	expireAfter := 5 * time.Second
	taskDefinition.ExpireAfter = &expireAfter
	err := store.CreateTaskDefinition(taskDefinition)
	require.NoError(t, err)
	listedTaskDefinitions, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskDefinitions, 1)
	listedTaskDefinition := listedTaskDefinitions[0]
	// create task instance that is in progress
	taskInstance := createTaskInstanceFromTaskDefinition(listedTaskDefinition)
	taskInstance.StartedAt = taskInstance.ExecuteAt
	err = store.CreateTaskInstance(taskInstance)
	require.NoError(t, err)
	// list task instances so we have a reference
	listedTaskInstances, err := store.ListTaskInstances(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	listedTaskInstance := listedTaskInstances[0]
	// query for tasks to run with a limit of now(), shouldn't come back
	taskInstancesToRun, err := store.GetTaskInstancesToRun(time.Now())
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 0)
	// sleep until the expiration, then query for tasks to run in the next 5 minutes, should get the instance back because it's expired
	time.Sleep(time.Until(*listedTaskInstance.ExpiresAt))
	taskInstancesToRun, err = store.GetTaskInstancesToRun(time.Now().Add(5 * time.Minute))
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 1)
	require.Equal(t, listedTaskInstance.Id, taskInstancesToRun[0].Id)
}

func TestGetTaskInstancesToRun(t *testing.T, store pkg.StoreInterface) {
	// create task definition to run in 5 minutes
	executeAt := time.Now().Add(5 * time.Minute)
	expectedExecuteOnceTask := generateRandomTaskWithExecuteOnceTrigger(executeAt)
	expireAfter := 2 * time.Second
	expectedExecuteOnceTask.ExpireAfter = &expireAfter
	err := store.CreateTaskDefinition(expectedExecuteOnceTask)
	require.NoError(t, err)
	listedTaskDefinitions, err := store.ListTaskDefinitions(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskDefinitions, 1)
	listedTaskDefinition := listedTaskDefinitions[0]
	// create task instance
	executeOnceTaskInstance := createTaskInstanceFromTaskDefinition(listedTaskDefinition)
	err = store.CreateTaskInstance(executeOnceTaskInstance)
	require.NoError(t, err)
	// list task instances so we have a reference
	listedTaskInstances, err := store.ListTaskInstances(0, 1000)
	require.NoError(t, err)
	require.Len(t, listedTaskInstances, 1)
	listedTaskInstance := listedTaskInstances[0]
	// query for tasks to run, shouldn't come back yet
	taskInstancesToRun, err := store.GetTaskInstancesToRun(time.Now())
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 0)
	// query for tasks to run in the next 5 minutes, should get the task instance
	taskInstancesToRun, err = store.GetTaskInstancesToRun(time.Now().Add(5 * time.Minute))
	require.NoError(t, err)
	require.Len(t, taskInstancesToRun, 1)
	taskInstanceToRun := taskInstancesToRun[0]
	require.Equal(t, listedTaskInstance.Id, taskInstanceToRun.Id)
	// test cron trigger
}

//func TestExecuteOnceTriggerHappyPath(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		return nil
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	executeAt := time.Now().Add(5 * time.Second)
//	task := pkg.TaskDefinition{
//		Id:                 &id,
//		Metadata:           metaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(20 * time.Second)
//	require.Equal(t, 1, executionCount)
//}
//
//func TestExecuteOnceTriggerTasksRunInOrder(t *testing.T, store pkg.StoreInterface) {
//	// this tests that tasks are executed in the right order. 3 tasks are scheduled with execution times nearer to now than the last
//	// resulting in scheduling the last running task first, and the first running task last. The first running task should
//	// be executed first even though it was scheduled last
//	executedTasks := []pkg.TaskDefinition{}
//	handler := func(task pkg.TaskDefinition) error {
//		executedTasks = append(executedTasks, task)
//		return nil
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	// create three tasks that should run in reverse order of when they're scheduled
//	// task1
//	task1Id := uuid.New()
//	task1MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	task1ExecuteAt := time.Now().Add(7 * time.Second)
//	task1 := pkg.TaskDefinition{
//		Id:                 &task1Id,
//		Metadata:           task1MetaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task1ExecuteAt),
//	}
//	err = scheduler.ScheduleTask(task1)
//	require.NoError(t, err)
//
//	// task2
//	task2Id := uuid.New()
//	task2MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	task2ExecuteAt := time.Now().Add(5 * time.Second)
//	task2 := pkg.TaskDefinition{
//		Id:                 &task2Id,
//		Metadata:           task2MetaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task2ExecuteAt),
//	}
//	err = scheduler.ScheduleTask(task2)
//	require.NoError(t, err)
//
//	// task3
//	task3Id := uuid.New()
//	task3MetaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	task3ExecuteAt := time.Now().Add(3 * time.Second)
//	task3 := pkg.TaskDefinition{
//		Id:                 &task3Id,
//		Metadata:           task3MetaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(task3ExecuteAt),
//	}
//	err = scheduler.ScheduleTask(task3)
//	require.NoError(t, err)
//
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.Len(t, executedTasks, 3)
//	require.Equal(t, executedTasks[0].Id, task3.Id)
//	require.Equal(t, executedTasks[1].Id, task2.Id)
//	require.Equal(t, executedTasks[2].Id, task1.Id)
//}
//
//func TestExecuteOnceTriggerLongRunningTaskExpired(t *testing.T, store pkg.StoreInterface) {
//	// first task sleeps longer than the window and expiration, simulating a long running task that eventually completes successfully
//	// this should result in the task expiring and being run twice.
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		time.Sleep(3 * time.Second)
//		return nil
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	executeAt := time.Now().Add(2 * time.Second)
//	expireAfter := 2 * time.Second
//	task := pkg.TaskDefinition{
//		Id:                 &id,
//		Metadata:           metaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
//		ExpireAfter:        &expireAfter,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.Equal(t, 2, executionCount)
//}
//
//func TestExecuteOnceTriggerLongRunningTaskNotExpired(t *testing.T, store pkg.StoreInterface) {
//	// first task sleeps longer than the window but less than the expiration, simulating a long running task that eventually completes successfully before the expiration time
//	// this should result in the task being run once.
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		time.Sleep(3 * time.Second)
//		return nil
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	executeAt := time.Now().Add(2 * time.Second)
//	expireAfter := 4 * time.Second
//	task := pkg.TaskDefinition{
//		Id:                 &id,
//		Metadata:           metaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
//		ExpireAfter:        &expireAfter,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.Equal(t, 1, executionCount)
//}
//
//func TestExecuteOnceTriggerRetry(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		return errors.New("fayl")
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	executeAt := time.Now().Add(2 * time.Second)
//	task := pkg.TaskDefinition{
//		Id:                 &id,
//		Metadata:           metaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
//		RetryOnError:       true,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.GreaterOrEqual(t, executionCount, 5) // there is a timing issue here, we need to make sure that the task was retried but this will
//	// have different timing based on the system running it and resources etc. I tried to test it with exactly 3 retries but it's difficult because
//	// sometimes it will run longer and have executed 4 times, sometimes 2. So I settled on waiting 10 seconds which is much longer
//	// than should be required, and verifying it's retried 5 times
//}
//
//func TestExecuteOnceTriggerNoRetry(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		return errors.New("fayl")
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	executeAt := time.Now().Add(2 * time.Second)
//	task := pkg.TaskDefinition{
//		Id:                 &id,
//		Metadata:           metaData,
//		ExecuteOnceTrigger: pkg.NewExecuteOnceTrigger(executeAt),
//		RetryOnError:       false,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(4 * time.Second)
//	require.Equal(t, 1, executionCount)
//}
//
//func TestCronTriggerHappyPath(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		return nil
//	}
//	// tick every 500ms
//	scheduler, err := pkg.NewScheduler(500*time.Millisecond, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	cronTrigger, err := pkg.NewCronTrigger("")
//	require.NoError(t, err)
//	task := pkg.TaskDefinition{
//		Id:          &id,
//		Metadata:    metaData,
//		CronTrigger: cronTrigger,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.Greater(t, executionCount, 6)
//}
//
//func TestCronTriggerRetry(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	succeedAfter := 3
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		if executionCount == succeedAfter {
//			return nil
//		}
//		return errors.New("fayl")
//	}
//	// tick once per second
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	// run once per minute starting 1 second from now
//	cronTrigger, err := pkg.NewCronTrigger(fmt.Sprintf(oncePerMinuteCronFormat, time.Now().Second()+1))
//	require.NoError(t, err)
//	task := pkg.TaskDefinition{
//		Id:           &id,
//		Metadata:     metaData,
//		RetryOnError: true,
//		CronTrigger:  cronTrigger,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(10 * time.Second)
//	require.Equal(t, executionCount, 3) // trigger should only fire once, and it should get retried twice
//}
//
//func TestCronTriggerNoRetry(t *testing.T, store pkg.StoreInterface) {
//	executionCount := 0
//	handler := func(task pkg.TaskDefinition) error {
//		executionCount++
//		logging.Log.WithField("time", time.Now().UTC().Format(time.RFC3339)).Info("handler called")
//		return errors.New("fayl")
//	}
//	scheduler, err := pkg.NewScheduler(1*time.Second, handler, store)
//	require.NoError(t, err)
//	id := uuid.New()
//	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
//	// every 2 seconds starting 1 second from now
//	cronTrigger, err := pkg.NewCronTrigger(fmt.Sprintf(everyNSecondsCronFormat, 2))
//	require.NoError(t, err)
//	task := pkg.TaskDefinition{
//		Id:           &id,
//		Metadata:     metaData,
//		RetryOnError: false,
//		CronTrigger:  cronTrigger,
//	}
//	err = scheduler.ScheduleTask(task)
//	require.NoError(t, err)
//	go scheduler.Run()
//	require.NoError(t, err)
//	time.Sleep(7 * time.Second)
//	// between 3 and 4 executions depending on resources
//	require.GreaterOrEqual(t, executionCount, 3)
//	require.LessOrEqual(t, executionCount, 4)
//}

func assertTaskEquality(t *testing.T, expected, actual pkg.TaskDefinition) {
	require.Equal(t, expected.Id, actual.Id)
	require.Equal(t, expected.RetryOnError, actual.RetryOnError)
	require.Equal(t, expected.ExpireAfter, actual.ExpireAfter)
	require.Equal(t, expected.NextFireTime, actual.NextFireTime)
	expectedMetaJson, err := json.Marshal(expected.Metadata)
	require.NoError(t, err)
	actualMetaJson, err := json.Marshal(actual.Metadata)
	require.NoError(t, err)
	require.Equal(t, expectedMetaJson, actualMetaJson)
	if expected.ExecuteOnceTrigger != nil {
		expectedFireTime := expected.ExecuteOnceTrigger.GetNextFireTime(expected).UTC().Format(time.RFC3339)
		actualFireTime := actual.ExecuteOnceTrigger.GetNextFireTime(actual).UTC().Format(time.RFC3339)
		require.Equal(t, expectedFireTime, actualFireTime)
	}
	if expected.CronTrigger != nil {
		require.Equal(t, expected.CronTrigger.Expression, actual.CronTrigger.Expression)
	}
}

func assertTaskInstanceEquality(t *testing.T, expected, actual pkg.TaskInstance) {

}
func generateRandomTaskWithExecuteOnceTrigger(executeAt time.Time) pkg.TaskDefinition {
	task := generateRandomTaskWithoutTrigger()
	task.ExecuteOnceTrigger = pkg.NewExecuteOnceTrigger(executeAt)
	return task
}

func generateRandomTaskWithCronTrigger(expression string) (pkg.TaskDefinition, error) {
	var err error
	task := generateRandomTaskWithoutTrigger()
	task.CronTrigger, err = pkg.NewCronTrigger(expression)
	return task, err
}

func generateRandomTaskWithoutTrigger() pkg.TaskDefinition {
	id := uuid.New()
	metaData := TestMetaData{Message: gofakeit.HackerPhrase()}
	return pkg.TaskDefinition{
		Id:       &id,
		Metadata: metaData,
	}
}

func createTaskInstanceFromTaskDefinition(taskDefinition pkg.TaskDefinition) pkg.TaskInstance {
	executeAt := taskDefinition.GetTrigger().GetNextFireTime(taskDefinition)
	expiresAt := executeAt.Add(*taskDefinition.ExpireAfter).UTC()
	return pkg.TaskInstance{
		ExpiresAt:        &expiresAt,
		ExecuteAt:        executeAt,
		TaskDefinitionId: taskDefinition.Id,
	}
}
