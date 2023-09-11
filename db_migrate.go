package db_migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/Maksumys/db-migrator/internal/models"
	"github.com/Maksumys/db-migrator/internal/repository"
	"gorm.io/gorm"
	"sort"
)

// Migrate сохраняет и выполняет миграции в нужном порядке. Для этого на первом шаге создаются системные таблицы Version
// и migrations, затем определяется необходимость проведения миграции типа TypeBaseline, после чего выполняются миграции
// типов TypeVersioned. Миграции типа TypeRepeatable выполняются в последнюю очередь.
// Все зарегистрированные миграции сохраняются в таблицу migrations. Миграции считаются новыми по инедтификатору
// f(версия, тип миграции).
//
// Паникует при попытке сохранить миграцию с версией меньшей, чем уже сохраненные.
// Паникует в случае, если какая-либо из необходимых в рамках выполнения операции миграций не была найдена.
func (m *MigrationManager) Migrate(serviceName string) error {
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

	m.logger.Println("Preparing migrations execution")

	err := m.initSystemTables(serviceName)
	if err != nil {
		return err
	}

	savedMigrations, err := m.saveNewMigrations(serviceName)
	if err != nil {
		return err
	}

	plan := m.planMigrate(serviceName, savedMigrations)

	for !plan.IsEmpty() {
		migrationModel := plan.PopFirst()

		migration, ok, err := m.findMigration(serviceName, migrationModel)

		if err != nil {
			return err
		}

		if !ok {
			if !m.allowBypassNotFound(migrationModel) {
				panic(fmt.Sprintf(
					"migration (type: %s, Version: %s) not found\n",
					migrationModel.Type, migrationModel.Version,
				))
			}

			m.logger.Printf(
				"migration (type: %s, Version: %s) not found, skipping",
				migrationModel.Type, migrationModel.Version,
			)
			err = repository.UpdateMigrationState(service.Db, &migrationModel, models.StateNotFound)
			if err != nil {
				return err
			}

			continue
		}

		err = m.executeMigration(serviceName, migrationModel, migration)
		if err != nil && !migration.IsAllowFailure {
			return errors.Join(err, repository.UpdateMigrationState(service.Db, &migrationModel, models.StateFailure))
		}

		err = m.saveStateOnSuccessfulMigration(serviceName, savedMigrations, migrationModel, migration)
		if err != nil {
			return err
		}
	}

	m.logger.Printf("Migrations completed for service: %s, current repository Version is Up to date", serviceName)
	return nil
}

func (m *MigrationManager) planMigrate(serviceName string, savedMigrations []models.MigrationModel) migrationsPlan {
	planner := migratePlanner{
		manager:         m,
		savedMigrations: savedMigrations,
	}
	return planner.MakePlan(serviceName)
}

func (m *MigrationManager) initSystemTables(serviceName string) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	hasVersionTable := repository.HasVersionTable(service.Db)
	hasMigrationsTable := repository.HasMigrationsTable(service.Db)

	if !hasVersionTable {
		m.logger.Println("Table versions not found, creating")
		err := repository.CreateVersionTable(service.Db)
		if err != nil {
			return err
		}
	}

	if !hasMigrationsTable {
		m.logger.Println("Table migrations not found, creating")
		err := repository.CreateMigrationsTable(service.Db)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MigrationManager) saveNewMigrations(serviceName string) ([]models.MigrationModel, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return nil, fmt.Errorf("service %s not found", serviceName)
	}

	savedMigrations, err := repository.GetMigrationsSorted(service.Db, repository.OrderASC)
	if err != nil {
		return nil, err
	}

	maxRank := 0
	for i := range savedMigrations {
		if rank := savedMigrations[i].Rank; rank > maxRank {
			maxRank = rank
		}
	}

	newMigrations := make([]*Migration, 0, len(service.registeredMigrations))
	for i := range service.registeredMigrations {
		if migrationIsNew(service.registeredMigrations[i], savedMigrations) {
			newMigrations = append(newMigrations, service.registeredMigrations[i])
		}
	}

	// запрет на сохранение миграций с версией, которая ниже максимальной версии из уже загерисрированных миграций
	for i := range newMigrations {
		versionIncorrect := false
		for j := range savedMigrations {
			versionSaved := mustParseVersion(savedMigrations[j].Version)
			versionToSave := mustParseVersion(newMigrations[i].Version)

			if versionSaved.MoreThan(versionToSave) {
				versionIncorrect = true
			}
		}
		if versionIncorrect {
			panic(fmt.Sprintf(
				"Attempting to register migration with lower Version than existing one. Type: %s. Identifier: %d",
				newMigrations[i].MigrationType, newMigrations[i].Identifier,
			))
		}
	}

	sort.SliceStable(newMigrations, func(i, j int) bool {
		leftVersioned, err := parseVersion(service.registeredMigrations[i].Version)
		if err != nil {
			panic(err)
		}

		rightVersioned, err := parseVersion(service.registeredMigrations[j].Version)
		if err != nil {
			panic(err)
		}

		return leftVersioned.LessThan(rightVersioned)
	})

	err = service.Db.Transaction(func(tx *gorm.DB) error {
		for i := range newMigrations {
			migration, err := repository.SaveMigration(tx, repository.SaveMigrationRequest{
				Rank:        maxRank + (i + 1),
				Type:        string(newMigrations[i].MigrationType),
				Version:     newMigrations[i].Version,
				Description: newMigrations[i].Description,
				State:       models.StateRegistered,
			})
			if err != nil {
				return err
			}

			savedMigrations = append(savedMigrations, migration)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return savedMigrations, nil
}

func (m *MigrationManager) executeMigration(serviceName string, migrationModel models.MigrationModel, migration *Migration) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	m.logger.Printf(
		"Executing %s migration: Version %s. State: %s. Service %s.\n",
		migrationModel.Type, migrationModel.Version, migrationModel.State, serviceName,
	)

	if len(migration.Up) == 0 && migration.UpF == nil || len(migration.Up) > 0 && migration.UpF != nil {
		m.logger.Printf("Migration fail, because Up and upf is empty or both is not nil, service: %s\n", serviceName)
		return errors.New("fail to migrate, because Up and upf is empty or both is not nil")
	}

	if migration.Dependency != nil && len(migration.Dependency) > 0 {
		for _, dependency := range migration.Dependency {
			err := func() error {
				depsService, ok := m.services[dependency.Name]

				if !ok {
					m.logger.Printf("Migration fail, dependency is not valid, service: %s\n", serviceName)
					return errors.New("dependency is not valid")
				}

				depsService.Db = depsService.ConnectFunc()
				defer func() {
					depsService.DisconnectFunc(depsService.Db)
				}()

				if repository.HasVersionTable(depsService.Db) {
					version, err := repository.GetVersion(depsService.Db)
					if err != nil {
						return err
					}
					if dependency.Version != version {
						return errors.New("dependency version is not valid")
					}
				}

				return nil
			}()
			if err != nil {
				return err
			}
		}
	}

	if migration.IsTransactional {
		err := service.Db.Transaction(func(tx *gorm.DB) error {
			if len(migration.Up) > 0 {
				return tx.Exec(migration.Up).Error
			} else {
				return migration.UpF(m)
			}
		})

		if err != nil {
			m.logger.Printf("Migration fail, service: %s, err: %s\n", serviceName, err)
			return err
		}
	} else {
		db, err := service.Db.DB()
		if err != nil {
			m.logger.Printf("Migration fail, service: %s, err: %s\n", serviceName, err)
			return err
		}

		if len(migration.Up) > 0 {
			_, err = db.Exec(migration.Up)
			if err != nil {
				m.logger.Printf("Migration fail, service: %s, err: %s\n", serviceName, err)
				return err
			}
		} else {
			err = migration.UpF(m)
			if err != nil {
				m.logger.Printf("Migration fail, service: %s, err: %s\n", serviceName, err)
				return err
			}
		}
	}

	m.logger.Printf("Migration Complete, service: %s\n", serviceName)
	return nil
}

func (m *MigrationManager) saveStateOnSuccessfulMigration(
	serviceName string,
	savedMigrations []models.MigrationModel,
	migrationModel models.MigrationModel,
	migration *Migration,
) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Printf("service %s not found", serviceName)
		return fmt.Errorf("service %s not found", serviceName)
	}

	switch migration.MigrationType {
	case TypeVersioned:
		err := repository.SaveVersion(service.Db, migration.Version)
		if err != nil {
			return err
		}

	case TypeBaseline:
		err := repository.SaveVersion(service.Db, migration.Version)
		if err != nil {
			return err
		}

		// все миграции до текущей TypeBaseline помечаем как пропущенные
		for i := range savedMigrations {
			if migrationModel.Id == savedMigrations[i].Id {
				break
			}

			err = repository.UpdateMigrationState(service.Db, &savedMigrations[i], models.StateSkipped)
			if err != nil {
				return err
			}
		}
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

	err = repository.UpdateMigrationStateExecuted(service.Db, &migrationModel, models.StateSuccess, migration.CheckSum(db))
	if err != nil {
		return err
	}

	return nil
}

func (m *MigrationManager) allowBypassNotFound(migrationModel models.MigrationModel) bool {
	return migrationModel.Type == string(TypeRepeatable)
}
