package db_migrator

import (
	"container/list"
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

func (p *migratePlanner) MakePlan(serviceName string) migrationsPlan {
	plan := newMigrationsPlan()
	p.planMigrationsBaseline(serviceName, &plan)
	p.planMigrationsVersioned(serviceName, &plan)
	p.planMigrationsRepeatable(serviceName, &plan)

	return plan
}

func (p *migratePlanner) planMigrationsBaseline(serviceName string, plan *migrationsPlan) {
	if !p.baselineRequired() {
		return
	}
	p.manager.logger.Println("No successful baseline migrations found, planning to execute latest available")

	relevantBaseline, ok := p.findRelevantBaseline(serviceName)
	if !ok {
		p.manager.logger.Println("No relevant baseline migrations for current target Version found")
		return
	}

	plan.migrationsToRun.PushFront(relevantBaseline)

	p.baselineIsPlanned = true
	p.plannedBaseline = relevantBaseline
}

func (p *migratePlanner) planMigrationsVersioned(serviceName string, plan *migrationsPlan) {
	service, ok := p.manager.services[serviceName]

	if !ok {
		panic("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		leftVersioned := mustParseVersion(p.savedMigrations[i].Version)
		rightVersioned := mustParseVersion(p.savedMigrations[j].Version)

		return rightVersioned.MoreThan(leftVersioned)
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

		migrationVersion := mustParseVersion(migrationModel.Version)

		if migrationVersion.MoreThan(service.TargetVersion) {
			continue
		}

		version, _ := p.manager.getSavedAppVersion(serviceName)

		if migrationVersion.LessOrEqual(version) {
			continue
		}

		if p.baselineIsPlanned {
			baselineVersion := mustParseVersion(p.plannedBaseline.Version)
			if baselineVersion.MoreThan(migrationVersion) {
				continue
			}
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}
}

func (p *migratePlanner) planMigrationsRepeatable(serviceName string, plan *migrationsPlan) {
	service, ok := p.manager.services[serviceName]

	if !ok {
		panic("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		leftVersioned := mustParseVersion(p.savedMigrations[i].Version)
		rightVersioned := mustParseVersion(p.savedMigrations[j].Version)

		return rightVersioned.MoreThan(leftVersioned)
	})

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeRepeatable) {
			continue
		}

		migration, ok, err := p.manager.findMigration(serviceName, migrationModel)

		if err != nil {
			panic(err)
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
			p.manager.logger.Printf(
				"migration (type: %s, Version: %s, checksum: %s) checksum not changed, skipping\n",
				migrationModel.Type, migrationModel.Version, migrationModel.Checksum,
			)
			continue
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}
}

func (p *migratePlanner) baselineRequired() bool {
	for _, migration := range p.savedMigrations {
		if migration.Type == string(TypeBaseline) && migration.State == models.StateSuccess {
			return false
		}
	}
	return true
}

func (p *migratePlanner) findRelevantBaseline(serviceName string) (models.MigrationModel, bool) {
	service, ok := p.manager.services[serviceName]

	if !ok {
		panic("fail to get service")
	}

	var latestBaselineMigration models.MigrationModel
	var latestBaselineMigrationFound bool

	for _, migrationModel := range p.savedMigrations {
		if migrationModel.Type != string(TypeBaseline) {
			continue
		}

		version := mustParseVersion(migrationModel.Version)
		if version.LessOrEqual(service.TargetVersion) {
			latestBaselineMigration = migrationModel
			latestBaselineMigrationFound = true
		}
	}

	return latestBaselineMigration, latestBaselineMigrationFound
}

type downgradePlanner struct {
	manager         *MigrationManager
	savedMigrations []models.MigrationModel
}

func (p *downgradePlanner) MakePlan(serviceName string) migrationsPlan {
	plan := newMigrationsPlan()

	service, ok := p.manager.services[serviceName]

	if !ok {
		panic("fail to get service")
	}

	sort.SliceStable(p.savedMigrations, func(i, j int) bool {
		leftVersioned := mustParseVersion(p.savedMigrations[i].Version)
		rightVersioned := mustParseVersion(p.savedMigrations[j].Version)

		return leftVersioned.MoreThan(rightVersioned)
	})

	for _, migrationModel := range p.savedMigrations {
		migrationVersion := mustParseVersion(migrationModel.Version)

		if migrationModel.Type != string(TypeVersioned) {
			continue
		}

		version, _ := p.manager.getSavedAppVersion(serviceName)

		if migrationVersion.MoreThan(version) {
			continue
		}
		if migrationVersion.LessOrEqual(service.TargetVersion) {
			continue
		}
		if migrationModel.State == models.StateUndone {
			continue
		}

		plan.migrationsToRun.PushBack(migrationModel)
	}

	return plan
}
