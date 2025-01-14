package db_migrator

import (
	"container/list"
	"fmt"
	"github.com/Maksumys/db-migrator/internal/models"
	"gorm.io/gorm"
	"sort"
)

type migrationsPlan struct {
	migrationsToRun *list.List
}

func newMigrationsPlan() migrationsPlan {
	return migrationsPlan{
		migrationsToRun: list.New(),
	}
}

func (p migrationsPlan) IsEmpty() bool {
	return p.migrationsToRun.Len() == 0
}

func (p migrationsPlan) PopFirst() models.MigrationModel {
	first := p.migrationsToRun.Front()
	p.migrationsToRun.Remove(first)
	return first.Value.(models.MigrationModel)
}

type migratePlanner struct {
	manager         *MigrationManager
	savedMigrations []models.MigrationModel

	plannedBaseline   models.MigrationModel
	baselineIsPlanned bool
}

func (p *migratePlanner) MakePlan(serviceName string) (migrationsPlan, error) {
	plan := newMigrationsPlan()
	p.planMigrationsBaseline(serviceName, &plan)

	err := p.planMigrationsVersioned(serviceName, &plan)

	if err != nil {
		return plan, err
	}

	err = p.planMigrationsRepeatable(serviceName, &plan)

	if err != nil {
		return plan, err
	}

	return plan, nil
}

func (p *migratePlanner) planMigrationsBaseline(serviceName string, plan *migrationsPlan) {
	if !p.baselineRequired() {
		return
	}
	p.manager.logger.Warn("no successful baseline migrations found, planning to execute latest available")

	relevantBaseline, ok, err := p.findRelevantBaseline(serviceName)

	if err != nil {
		p.manager.logger.Error(err.Error())
		return
	}

	if !ok {
		p.manager.logger.Error("no relevant baseline migrations for current target Version found")
		return
	}

	plan.migrationsToRun.PushFront(relevantBaseline)

	p.baselineIsPlanned = true
	p.plannedBaseline = relevantBaseline
}

func (p *migratePlanner) planMigrationsVersioned(serviceName string, plan *migrationsPlan) error {
	service, ok := p.manager.services[serviceName]

	if !ok {
		return fmt.Errorf("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		return p.savedMigrations[j].Version.MoreThan(p.savedMigrations[i].Version)
	})

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeVersioned) {
			continue
		}
		if migrationModel.State == models.StateSuccess {
			continue
		}
		if migrationModel.State == models.StateSkipped {
			continue
		}

		if migrationModel.Version.MoreThan(service.TargetVersion) {
			continue
		}

		version, _ := p.manager.getSavedAppVersion(serviceName)

		if migrationModel.Version.LessOrEqual(version) {
			continue
		}

		if p.baselineIsPlanned {
			if p.plannedBaseline.Version.MoreThan(migrationModel.Version) {
				continue
			}
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}

	return nil
}

func (p *migratePlanner) planMigrationsRepeatable(serviceName string, plan *migrationsPlan) error {
	service, ok := p.manager.services[serviceName]

	if !ok {
		return fmt.Errorf("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		return p.savedMigrations[j].Version.MoreThan(p.savedMigrations[i].Version)
	})

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeRepeatable) {
			continue
		}

		migration, ok, err := p.manager.findMigration(serviceName, migrationModel)

		if err != nil {
			return err
		}

		if !ok {
			// добавляем в очередь, чтобы при выполнении проставить необходимые статусы
			plan.migrationsToRun.PushBack(migrationModel)
			continue
		}

		if migration.CheckSum == nil {
			migration.CheckSum = func(db *gorm.DB) string {
				return ""
			}
		}

		if !migration.RepeatUnconditional && migrationModel.Checksum == migration.CheckSum(service.Db) {
			p.manager.logger.Info(
				fmt.Sprintf(
					"migration (type: %s, Version: %s, checksum: %s) checksum not changed, skipping",
					migrationModel.Type, migrationModel.Version, migrationModel.Checksum,
				),
			)
			continue
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}

	return nil
}

func (p *migratePlanner) baselineRequired() bool {
	for _, migration := range p.savedMigrations {
		if migration.Type == string(TypeBaseline) && migration.State == models.StateSuccess {
			return false
		}
	}
	return true
}

func (p *migratePlanner) findRelevantBaseline(serviceName string) (models.MigrationModel, bool, error) {
	service, ok := p.manager.services[serviceName]

	if !ok {
		return models.MigrationModel{}, false, fmt.Errorf("fail to get service")
	}

	var latestBaselineMigration models.MigrationModel
	var latestBaselineMigrationFound bool

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeBaseline) {
			continue
		}

		if migrationModel.Version.LessOrEqual(service.TargetVersion) {
			latestBaselineMigration = migrationModel
			latestBaselineMigrationFound = true
		}
	}

	return latestBaselineMigration, latestBaselineMigrationFound, nil
}

type downgradePlanner struct {
	manager         *MigrationManager
	savedMigrations []models.MigrationModel
}

func (p *downgradePlanner) MakePlan(serviceName string) (migrationsPlan, error) {
	plan := newMigrationsPlan()

	service, ok := p.manager.services[serviceName]

	if !ok {
		return migrationsPlan{}, fmt.Errorf("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		return p.savedMigrations[i].Version.MoreThan(p.savedMigrations[j].Version)
	})

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeVersioned) {
			continue
		}

		version, _ := p.manager.getSavedAppVersion(serviceName)

		if migrationModel.Version.MoreThan(version) {
			continue
		}
		if migrationModel.Version.LessOrEqual(service.TargetVersion) {
			continue
		}
		if migrationModel.State == models.StateUndone {
			continue
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}

	return plan, nil
}
