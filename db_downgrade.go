package db_migrator

import (
	"database/sql"
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
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	service.Db = service.ConnectFunc()
	defer func() {
		service.DisconnectFunc(service.Db)
	}()

	m.logger.Println("Preparing downgrade execution")

	if !repository.HasVersionTable(service.Db) || !repository.HasVersionTable(service.Db) {
		panic("No migration table or Version table found. Cannot perform downgrade")
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
			panic(fmt.Sprintf(
				"migration (type: %s, Version: %s) not found\n",
				migrationModel.Type, migrationModel.Version,
			))
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

	m.logger.Println("Downgrade completed")
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

	return planner.MakePlan(serviceName), nil
}

func (m *MigrationManager) executeDowngrade(serviceName string, migrationModel models.MigrationModel, migration *Migration) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	m.logger.Printf(
		"Downgrading %s migration: Version %s. State: %s\n",
		migrationModel.Type, migrationModel.Version, migrationModel.State,
	)

	if migration.MigrationType != TypeVersioned {
		panic("versioned migration must satisfy VersionedMigrator interface")
	}
	if len(migration.Down) == 0 && migration.DownF == nil {
		panic("fail to downgrade, because Down and DownF is empty")
	}

	if migration.IsTransactional {
		err := service.Db.Transaction(func(tx *gorm.DB) error {
			if len(migration.Down) > 0 {
				return tx.Exec(migration.Down).Error
			} else {
				return migration.DownF(m)
			}
		})

		if err != nil {
			m.logger.Println("Error occurred on migrate:", err)
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
			return migration.DownF(m)
		}
	}

	m.logger.Println("Downgrade complete")
	return nil
}

func (m *MigrationManager) saveStateAfterDowngrading(serviceName string, savedMigrations []models.MigrationModel, migrationModel models.MigrationModel, migration *Migration) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	if migration.CheckSum == nil {
		migration.CheckSum = func(db *sql.DB) string {
			return ""
		}
	}

	db, err := service.Db.DB()
	if err != nil {
		return err
	}

	err = repository.UpdateMigrationStateExecuted(service.Db, &migrationModel, models.StateUndone, migration.CheckSum(db))
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
		m.logger.Printf("service %s not found", serviceName)
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
		leftVersioned := mustParseVersion(filteredMigrations[i].Version)
		rightVersioned := mustParseVersion(filteredMigrations[j].Version)

		return leftVersioned.LessThan(rightVersioned)
	})

	undoneMigrationVersion := mustParseVersion(migrationModel.Version)
	versionToSave := Version{Major: 0, Minor: 0, Patch: 0, PreRelease: 0}
	// находим предыдущую версию
	for i := range filteredMigrations {
		if filteredMigrations[i].Type != string(TypeVersioned) {
			continue
		}

		migrationVersion := mustParseVersion(filteredMigrations[i].Version)
		if migrationVersion == undoneMigrationVersion {
			if i != 0 {
				versionToSave = mustParseVersion(filteredMigrations[i-1].Version)
			}
			break
		}
	}

	return repository.SaveVersion(service.Db, versionToSave.String())
}
