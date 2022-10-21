package test

import (
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/stretchr/testify/suite"
	"testing"
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
	TestExecuteOnceTriggerHappyPath(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreTasksRunInOrder() {
	store := pkg.NewMemoryStore()
	TestExecuteOnceTriggerTasksRunInOrder(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreLongRunningTaskExpired() {
	store := pkg.NewMemoryStore()
	TestExecuteOnceTriggerLongRunningTaskExpired(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreLongRunningTaskNotExpired() {
	store := pkg.NewMemoryStore()
	TestExecuteOnceTriggerLongRunningTaskNotExpired(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreRetry() {
	store := pkg.NewMemoryStore()
	TestExecuteOnceTriggerRetry(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreNoRetry() {
	store := pkg.NewMemoryStore()
	TestExecuteOnceTriggerNoRetry(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreCronTriggerHappyPath() {
	store := pkg.NewMemoryStore()
	TestCronTriggerHappyPath(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreCronTriggerRetry() {
	store := pkg.NewMemoryStore()
	TestCronTriggerRetry(s.T(), store)
}

func (s *MemoryStoreSuite) TestMemoryStoreCronTriggerNoRetry() {
	store := pkg.NewMemoryStore()
	TestCronTriggerNoRetry(s.T(), store)
}

//func (s *MemoryStoreSuite) TestCronStuff() {
//	// every 5 seconds
//	cronTrigger, err := pkg.NewCronTrigger(fmt.Sprintf(everyNSecondsCronFormat, 5))
//	require.NoError(s.T(), err)
//	for i := 0; i < 16; i++ {
//		logging.Log.WithFields(logrus.Fields{"next_Time": cronTrigger.GetNextFireTime(task)}).Info("cron")
//		time.Sleep(1 * time.Second)
//	}
//}
