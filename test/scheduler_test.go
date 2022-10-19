package test

import (
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/emirpasic/gods/trees/btree"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type SchedulerSuite struct {
	suite.Suite
}

func (s *SchedulerSuite) SetupSuite() {
}

func (s *SchedulerSuite) TearDownSuite() {
}

func (s *SchedulerSuite) SetupTest() {
}

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

func (s *SchedulerSuite) TestTreeComparator() {
	tree := btree.NewWith(3, pkg.ScheduleTreeComparator)
	numTasks := 10
	tasks := make([]*pkg.Task, numTasks)
	for i := numTasks; i > 0; i-- {
		id := uuid.New()
		trigger := pkg.ExecuteOnceTrigger{FireAt: time.Now().Add(time.Duration(i) * time.Second)}
		task := &pkg.Task{
			Id:                 &id,
			ExecuteOnceTrigger: &trigger,
		}
		tree.Put(pkg.GetScheduleTreeKey(*task), task)
		tasks[i-1] = task
	}
	iterator := tree.Iterator()
	k := 0
	for iterator.Next() {
		value := iterator.Value()
		task := value.(*pkg.Task)
		require.Equal(s.T(), tasks[k].Id, task.Id)
		k++
	}
}
