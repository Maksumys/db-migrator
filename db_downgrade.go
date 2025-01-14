package db_migrator

import (
	"fmt"
	"github.com/Maksumys/db-migrator/internal/models"
	"github.com/Maksumys/db-migrator/internal/repository"
	"gorm.io/gorm"
	"sort"
)

// Downgrade осуществляет отмену успешно выполненных или пропущенных миграций в обратном порядке.
// Миграции типа TypeRepeatable и TypeBaseline не отменяются.
// Новые миграции при вызове Downgrade не сохраняются.
//
// Паникует в случае, если какая-либо из миграций не была найдена.
func (m *MigrationManager) Downgrade(serviceName string) (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	service.Db = service.ConnectFunc()
	defer func() {
		service.DisconnectFunc(service.Db)
	}()

	m.logger.Info("preparing downgrade execution")

	if !repository.HasVersionTable(service.Db) || !repository.HasVersionTable(service.Db) {
		return fmt.Errorf("no migration table or Version table found, cannot perform downgrade")
	}

	savedMigrations, err := repository.GetMigrationsSorted(service.Db, repository.OrderDESC)
	if err != nil {
		return err
	}

	plan, err := m.planDowngrade(serviceName)
	if err != nil {
		return err
	}

	for !plan.IsEmpty() {
		migrationModel := plan.PopFirst()

		migration, ok, err := m.findMigration(serviceName, migrationModel)

		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf(
				"migration (type: %s, Version: %s) not found",
				migrationModel.Type, migrationModel.Version,
			)
		}

		err = m.executeDowngrade(serviceName, migrationModel, migration)
		if err != nil {
			return err
		}

		err = m.saveStateAfterDowngrading(serviceName, savedMigrations, migrationModel, migration)
		if err != nil {
			return err
		}
	}

	m.logger.Info("Downgrade completed")

	return
}

func (m *MigrationManager) planDowngrade(serviceName string) (migrationsPlan, error) {
	savedMigrations, err := m.saveNewMigrations(serviceName)
	if err != nil {
		return migrationsPlan{}, err
	}

	planner := downgradePlanner{
		manager:         m,
		savedMigrations: savedMigrations,
	}

	return planner.MakePlan(serviceName)
}

func (m *MigrationManager) executeDowngrade(serviceName string, migrationModel models.MigrationModel, migration *Migration) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Info(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	m.logger.Info(
		fmt.Sprintf(
			"downgrading %s migration: Version %s. State: %s",
			migrationModel.Type, migrationModel.Version, migrationModel.State,
		),
	)

	if migration.MigrationType != TypeVersioned {
		return fmt.Errorf("versioned migration must satisfy VersionedMigrator interface")
	}
	if len(migration.Down) == 0 && migration.DownF == nil {
		return fmt.Errorf("fail to downgrade, because Down and DownF is empty")
	}

	if migration.IsTransactional {
		err := service.Db.Transaction(func(tx *gorm.DB) error {
			if len(migration.Down) > 0 {
				return tx.Exec(migration.Down).Error
			} else {
				return migration.DownF(tx, nil)
			}
		})

		if err != nil {
			m.logger.Error(fmt.Sprintf("error occurred on migrate: %v", err))
			return err
		}
	} else {
		db, err := service.Db.DB()
		if err != nil {
			return err
		}

		if len(migration.Down) > 0 {
			_, err = db.Exec(migration.Down)
			if err != nil {
				return err
			}
		} else {
			return migration.DownF(service.Db, nil)
		}
	}

	m.logger.Info("downgrade complete")
	return nil
}

func (m *MigrationManager) saveStateAfterDowngrading(serviceName string, savedMigrations []models.MigrationModel, migrationModel models.MigrationModel, migration *Migration) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	if migration.CheckSum == nil {
		migration.CheckSum = func(db *gorm.DB) string {
			return ""
		}
	}

	err := repository.UpdateMigrationStateExecuted(service.Db, &migrationModel, models.StateUndone, migration.CheckSum(service.Db))
	if err != nil {
		return err
	}

	return m.saveVersionDowngrade(serviceName, migrationModel, savedMigrations)
}

func (m *MigrationManager) saveVersionDowngrade(
	serviceName string,
	migrationModel models.MigrationModel,
	savedMigrations []models.MigrationModel,
) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	// фильтруем миграции типа TypeRepeatable
	filteredMigrations := make([]models.MigrationModel, 0, len(savedMigrations))
	for i := range savedMigrations {
		if savedMigrations[i].Type == string(TypeRepeatable) {
			continue
		}
		filteredMigrations = append(filteredMigrations, savedMigrations[i])
	}

	sort.SliceStable(filteredMigrations, func(i, j int) bool {
		return filteredMigrations[i].Version.LessThan(filteredMigrations[j].Version)
	})

	undoneMigrationVersion := migrationModel.Version
	var versionToSave models.Version
	// находим предыдущую версию
	for i := range filteredMigrations {
		if filteredMigrations[i].Type != string(TypeVersioned) {
			continue
		}

		if filteredMigrations[i].Version.Equals(undoneMigrationVersion) {
			if i != 0 {
				versionToSave = filteredMigrations[i-1].Version
			}
			break
		}
	}

	return repository.SaveVersion(service.Db, versionToSave)
}
