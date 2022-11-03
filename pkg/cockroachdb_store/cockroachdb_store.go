package cockroachdb_store

import (
	"database/sql"
	"embed"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/catalystsquad/go-scheduler/pkg"
	"github.com/catalystsquad/go-scheduler/pkg/cockroachdb_store/models"
	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

const gooseTableName = "goose_catalyst_scheduler"

//go:embed migrations/*.sql
var migrations embed.FS

type CockroachdbStore struct {
	uri    string
	db     *gorm.DB
	config *gorm.Config
}

func (c *CockroachdbStore) GetTaskInstance(id *uuid.UUID) (pkg.TaskInstance, error) {
	taskInstanceModel := models.TaskInstance{Id: id}
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.First(&taskInstanceModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error getting task definition")
		return pkg.TaskInstance{}, err
	}
	return taskInstanceModel.ToTaskInstance()
}

func (c *CockroachdbStore) ListTaskInstances(offset, limit int) ([]pkg.TaskInstance, error) {
	taskInstanceModels := []models.TaskInstance{}
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Preload(clause.Associations).Order("created_at").Offset(offset).Limit(limit).Find(&taskInstanceModels).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error listing task instances")
	}
	return models.ToTaskInstances(taskInstanceModels)
}

func (c *CockroachdbStore) ListTaskDefinitions(offset, limit int) ([]pkg.TaskDefinition, error) {
	taskDefinitionModels := []models.TaskDefinition{}
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Preload(clause.Associations).Order("created_at").Offset(offset).Limit(limit).Find(&taskDefinitionModels).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error scheduling task with cockroachdb store")
	}
	return models.ToTasks(taskDefinitionModels)
}

func (c *CockroachdbStore) GetTaskDefinition(id *uuid.UUID) (pkg.TaskDefinition, error) {
	taskDefinitionModel := models.TaskDefinition{Id: id.String()}
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Preload(clause.Associations).First(&taskDefinitionModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error getting task definition with cockroachdb store")
		return pkg.TaskDefinition{}, err
	}
	return taskDefinitionModel.ToTask()
}

func (c *CockroachdbStore) UpdateTaskDefinition(taskDefinition pkg.TaskDefinition) error {
	taskDefinitionModel, err := models.GetTaskDefinitionModelFromTaskDefinition(taskDefinition)
	if err != nil {
		return err
	}
	err = c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Session(&gorm.Session{FullSaveAssociations: true}).Updates(&taskDefinitionModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error scheduling task with cockroachdb store")
	}
	return err
}

func (c *CockroachdbStore) DeleteTaskDefinition(id *uuid.UUID) error {
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Delete(models.TaskDefinition{Id: id.String()}).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error deleting task with cockroachdb store")
	}
	return err
}

func (c *CockroachdbStore) GetTaskInstancesToRun(limit time.Time) ([]pkg.TaskInstance, error) {
	limit = limit.UTC()
	taskInstanceModels := []models.TaskInstance{}
	err := c.db.Transaction(func(tx *gorm.DB) error {
		// query for task instances that aren't completed, and either aren't in progress, or are in progress but have expired
		return tx.Where("completed_at is null and ((started_at is null and execute_at <= ?) or (started_at is not null and expires_at <= now()))", limit).Find(&taskInstanceModels).Error
	})
	if err != nil {
		return nil, err
	}
	taskInstances := []pkg.TaskInstance{}
	for _, taskInstanceModel := range taskInstanceModels {
		taskInstance, err := taskInstanceModel.ToTaskInstance()
		if err != nil {
			return nil, err
		}
		taskInstances = append(taskInstances, taskInstance)
	}
	return taskInstances, nil
}

func (c *CockroachdbStore) CreateTaskInstance(taskInstance pkg.TaskInstance) error {
	taskInstanceModel, err := models.GetTaskInstanceModelFromTaskInstance(taskInstance)
	if taskInstanceModel.Id == nil {
		id := uuid.New()
		taskInstanceModel.Id = &id
	}
	if err != nil {
		return err
	}
	err = c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&taskInstanceModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error creating task instance")
	}
	return err
}

func (c *CockroachdbStore) UpdateTaskInstance(taskInstance pkg.TaskInstance) error {
	taskInstanceModel, err := models.GetTaskInstanceModelFromTaskInstance(taskInstance)
	if err != nil {
		return err
	}
	err = c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Updates(&taskInstanceModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error scheduling task with cockroachdb store")
	}
	return err
}

func (c *CockroachdbStore) DeleteTaskInstance(id *uuid.UUID) error {
	err := c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Delete(models.TaskInstance{Id: id}).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error deleting task instance")
	}
	return err
}

func NewCockroachdbStore(uri string, config *gorm.Config) pkg.StoreInterface {
	if config == nil {
		config = &gorm.Config{}
	}
	return &CockroachdbStore{
		uri:    uri,
		config: config,
	}
}

func (c *CockroachdbStore) Initialize() (err error) {
	// connect to db
	c.db, err = gorm.Open(postgres.Open(c.uri), c.config)
	if err != nil {
		logging.Log.WithError(err).Error("error connecting to cockroachdb")
		return err
	}
	// run migrations
	var sqldb *sql.DB
	sqldb, err = c.db.DB()
	if err != nil {
		return err
	}
	// set goose file system to use the embedded migrations
	goose.SetBaseFS(migrations)
	// set goose table name so it doesn't conflict with any other goose tables that the user may be using
	goose.SetTableName(gooseTableName)
	err = goose.Up(sqldb, "migrations")
	if err != nil {
		logging.Log.WithError(err).Error("error running scheduler migrations")
		return err
	}
	return nil
}

func (c *CockroachdbStore) CreateTaskDefinition(taskDefinition pkg.TaskDefinition) error {
	taskDefinitionModel, err := models.GetTaskDefinitionModelFromTaskDefinition(taskDefinition)
	if err != nil {
		return err
	}
	err = c.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&taskDefinitionModel).Error
	})
	if err != nil {
		logging.Log.WithError(err).Error("error scheduling task with cockroachdb store")
	}
	return err
}

//func (c *CockroachdbStore) GetUpcomingTasks(limit time.Time) ([]pkg.TaskDefinition, error) {
//	models := []models.TaskDefinition{}
//	err := c.db.Transaction(func(tx *gorm.DB) error {
//		return tx.Preload(clause.Associations).Where("next_fire_time <= ? and (in_progress = false or age(last_fire_time) >= expire_after_interval)", limit.Format(time.RFC3339)).Order("next_fire_time").Find(&models).Error
//	})
//	if err != nil {
//		logging.Log.WithError(err).Error("error scheduling task with cockroachdb store")
//	}
//	tasks := []pkg.TaskDefinition{}
//	for _, model := range models {
//		task, err := model.ToTask()
//		if err != nil {
//			return nil, err
//		}
//		tasks = append(tasks, task)
//	}
//	return tasks, err
//}