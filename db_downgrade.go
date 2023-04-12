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
func (m *MigrationManager) Downgrade() (err error) {
	m.logger.Println("Preparing downgrade execution")

	if !repository.HasVersionTable(m.db) || !repository.HasVersionTable(m.db) {
		panic("No migration table or Version table found. Cannot perform downgrade")
	}

	savedMigrations, err := repository.GetMigrationsSorted(m.db, repository.OrderDESC)
	if err != nil {
		return err
	}

	plan, err := m.planDowngrade()
	if err != nil {
		return err
	}

	for !plan.IsEmpty() {
		migrationModel := plan.PopFirst()

		migration, ok := m.findMigration(migrationModel)
		if !ok {
			panic(fmt.Sprintf(
				"migration (type: %s, Version: %s) not found\n",
				migrationModel.Type, migrationModel.Version,
			))
		}

		err = m.executeDowngrade(migrationModel, migration)
		if err != nil {
			return err
		}

		err = m.saveStateAfterDowngrading(savedMigrations, migrationModel, migration)
		if err != nil {
			return err
		}
	}

	m.logger.Println("Downgrade completed")
	return
}

func (m *MigrationManager) planDowngrade() (migrationsPlan, error) {
	savedMigrations, err := m.saveNewMigrations()
	if err != nil {
		return migrationsPlan{}, err
	}

	planner := downgradePlanner{
		manager:         m,
		savedMigrations: savedMigrations,
	}

	return planner.MakePlan(), nil
}

func (m *MigrationManager) executeDowngrade(migrationModel models.MigrationModel, migration *MigrationLite) error {
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
		err := m.db.Transaction(func(tx *gorm.DB) error {
			if len(migration.Down) > 0 {
				return tx.Exec(migration.Down).Error
			} else {
				db, err := tx.DB()
				if err != nil {
					return err
				}
				return migration.DownF(db)
			}
		})

		if err != nil {
			m.logger.Println("Error occurred on migrate:", err)
			return err
		}
	} else {
		db, err := m.db.DB()
		if err != nil {
			return err
		}

		if len(migration.Down) > 0 {
			_, err = db.Exec(migration.Down)
			if err != nil {
				return err
			}
		} else {
			return migration.DownF(db)
		}
	}

	m.logger.Println("Downgrade complete")
	return nil
}

func (m *MigrationManager) saveStateAfterDowngrading(savedMigrations []models.MigrationModel, migrationModel models.MigrationModel, migration *MigrationLite) error {
	if migration.CheckSum == nil {
		migration.CheckSum = func(db *sql.DB) string {
			return ""
		}
	}

	db, err := m.db.DB()
	if err != nil {
		return err
	}

	err = repository.UpdateMigrationStateExecuted(m.db, &migrationModel, models.StateUndone, migration.CheckSum(db))
	if err != nil {
		return err
	}

	return m.saveVersionDowngrade(migrationModel, savedMigrations)
}

func (m *MigrationManager) saveVersionDowngrade(
	migrationModel models.MigrationModel,
	savedMigrations []models.MigrationModel,
) error {
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

	return repository.SaveVersion(m.db, versionToSave.String())
}
