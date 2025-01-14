package db_migrator

import (
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
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	service.Db = service.ConnectFunc()
	defer func() {
		service.DisconnectFunc(service.Db)
	}()

	m.logger.Info("preparing migrations execution")

	err := m.initSystemTables(serviceName)
	if err != nil {
		return err
	}

	savedMigrations, err := m.saveNewMigrations(serviceName)
	if err != nil {
		return err
	}

	plan, err := m.planMigrate(serviceName, savedMigrations)

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
			if !m.allowBypassNotFound(migrationModel) {
				return fmt.Errorf(
					"migration (type: %s, Version: %s) not found\n",
					migrationModel.Type, migrationModel.Version,
				)
			}

			m.logger.Info(
				fmt.Sprintf(
					"migration (type: %s, Version: %s) not found, skipping",
					migrationModel.Type, migrationModel.Version,
				),
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

	m.logger.Info(fmt.Sprintf("migrations completed for service: %s, current repository Version is Up to date", serviceName))
	return nil
}

func (m *MigrationManager) planMigrate(serviceName string, savedMigrations []models.MigrationModel) (migrationsPlan, error) {
	planner := migratePlanner{
		manager:         m,
		savedMigrations: savedMigrations,
	}
	return planner.MakePlan(serviceName)
}

func (m *MigrationManager) initSystemTables(serviceName string) error {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	hasVersionTable := repository.HasVersionTable(service.Db)
	hasMigrationsTable := repository.HasMigrationsTable(service.Db)

	if !hasVersionTable {
		m.logger.Warn("table versions not found, creating")
		err := repository.CreateVersionTable(service.Db)
		if err != nil {
			return err
		}
	}

	if !hasMigrationsTable {
		m.logger.Warn("table migrations not found, creating")
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
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
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

	newMigrations := make([]repository.SaveMigrationRequest, 0, len(service.registeredMigrations))
	for i := range service.registeredMigrations {
		if migrationIsNew(service.registeredMigrations[i], savedMigrations) {
			pv, err := models.ParseVersion(service.registeredMigrations[i].Version)
			if err != nil {
				return nil, err
			}

			newMigrations = append(newMigrations,
				repository.SaveMigrationRequest{
					Type:        string(service.registeredMigrations[i].MigrationType),
					Version:     pv,
					Description: service.registeredMigrations[i].Description,
					State:       models.StateRegistered,
				},
			)
		}
	}

	// запрет на сохранение миграций с версией, которая ниже максимальной версии из уже зарегистрированных миграций
	for i := range newMigrations {
		for j := range savedMigrations {
			if savedMigrations[j].Version.MoreThan(newMigrations[i].Version) {
				return nil, errors.New(fmt.Sprintf(
					"attempting to register migration with lower Version than existing one, type: %s, version: %s",
					newMigrations[i].Type,
					newMigrations[i].Version,
				))
			}
		}
	}

	sort.SliceStable(newMigrations, func(i, j int) bool {
		return newMigrations[i].Version.LessThan(newMigrations[j].Version)
	})

	err = service.Db.Transaction(func(tx *gorm.DB) error {
		for i := range newMigrations {
			newMigrations[i].Rank = maxRank + (i + 1)
			migration, err := repository.SaveMigration(tx, newMigrations[i])

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
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	m.logger.Info(
		fmt.Sprintf(
			"executing %s migration: Version %s. State: %s. Service %s.",
			migrationModel.Type, migrationModel.Version, migrationModel.State, serviceName,
		),
	)

	if len(migration.Up) == 0 && migration.UpF == nil || len(migration.Up) > 0 && migration.UpF != nil {
		m.logger.Error(fmt.Sprintf("migration fail, because Up and upf is empty or both is not nil, service: %s", serviceName))
		return errors.New("fail to migrate, because Up and upf is empty or both is not nil")
	}

	depsServices := make(map[string]*ServiceInfo)

	defer func() {
		for _, v := range depsServices {
			v.DisconnectFunc(v.Db)
		}
	}()

	if migration.Dependency != nil && len(migration.Dependency) > 0 {
		for _, dependency := range migration.Dependency {
			depsService, ok := m.services[dependency.Name]

			if !ok {
				m.logger.Error(fmt.Sprintf("migration fail, dependency is not valid, service: %s", serviceName))
				return errors.New("dependency is not valid")
			}

			if depsService.ConnectFunc == nil {
				m.logger.Error(fmt.Sprintf("migration fail, dependency is not registered, service: %s", serviceName))
				return errors.New("dependency is not valid")
			}

			depsService.Db = depsService.ConnectFunc()
			depsServices[dependency.Name] = depsService

			if !repository.HasVersionTable(depsService.Db) {
				return errors.New("dependency is not valid")
			}

			version, err := repository.GetVersion(depsService.Db)
			if err != nil {
				return err
			}

			if version.Equals(models.Version{}) {
				return errors.New("dependency is not valid")
			}

			dependencyVersion, err := models.ParseVersion(dependency.Version)

			if err != nil {
				return err
			}

			if dependency.Strict && !version.Equals(dependencyVersion) {
				return errors.New("dependency version is not valid")
			} else if version.LessThan(dependencyVersion) {
				return errors.New("dependency version is not valid")
			}
		}
	}

	depsServicesDb := make(map[string]*gorm.DB)

	for s, info := range depsServices {
		depsServicesDb[s] = info.Db
	}

	if migration.IsTransactional {
		err := service.Db.Transaction(func(tx *gorm.DB) error {
			if len(migration.Up) > 0 {
				return tx.Exec(migration.Up).Error
			} else {
				return migration.UpF(tx, depsServicesDb)
			}
		})

		if err != nil {
			m.logger.Error(fmt.Sprintf("migration fail, service: %s, err: %s", serviceName, err))
			return err
		}
	} else {
		db, err := service.Db.DB()
		if err != nil {
			m.logger.Error(fmt.Sprintf("migration fail, service: %s, err: %s", serviceName, err))
			return err
		}

		if len(migration.Up) > 0 {
			_, err = db.Exec(migration.Up)
			if err != nil {
				m.logger.Error(fmt.Sprintf("migration fail, service: %s, err: %s", serviceName, err))
				return err
			}
		} else {
			err = migration.UpF(service.Db, depsServicesDb)
			if err != nil {
				m.logger.Error(fmt.Sprintf("migration fail, service: %s, err: %s", serviceName, err))
				return err
			}
		}
	}

	m.logger.Info(fmt.Sprintf("migration Complete, service: %s", serviceName))
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
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return fmt.Errorf("service %s not found", serviceName)
	}

	migrationVersion, err := models.ParseVersion(migration.Version)

	if err != nil {
		return err
	}

	switch migration.MigrationType {
	case TypeVersioned:
		err := repository.SaveVersion(service.Db, migrationVersion)
		if err != nil {
			return err
		}

	case TypeBaseline:
		err := repository.SaveVersion(service.Db, migrationVersion)
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
		migration.CheckSum = func(db *gorm.DB) string {
			return ""
		}
	}

	err = repository.UpdateMigrationStateExecuted(
		service.Db,
		&migrationModel,
		models.StateSuccess,
		migration.CheckSum(service.Db),
	)

	if err != nil {
		return err
	}

	return nil
}

func (m *MigrationManager) allowBypassNotFound(migrationModel models.MigrationModel) bool {
	return migrationModel.Type == string(TypeRepeatable)
}
