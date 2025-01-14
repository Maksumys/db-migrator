package db_migrator

import (
	"errors"
	"fmt"
	"github.com/Maksumys/db-migrator/internal/models"
	"github.com/Maksumys/db-migrator/internal/repository"
	"gorm.io/gorm"
	"hash/fnv"
	"log/slog"
	"os"
	"sync"
)

var (
	ErrHasForthcomingMigrations = errors.New("found not completed forthcoming migrations, consider migrating")
	ErrHasFailedMigrations      = errors.New("found failed migrations, consider fixing your Db")
	ErrTargetVersionNotLatest   = errors.New("target Version falls behind migrations, consider raising target Version")
)

// NewMigrationsManager создает экземпляр управляющего миграциями (выступает в качестве фасада).
// TargetVersion - версия, до которой необходимо выполнить миграцию или до необходимо осуществить откат.
func NewMigrationsManager(opts ...ManagerOption) (*MigrationManager, error) {
	manager := MigrationManager{
		// log.New(os.Stderr, "", log.LstdFlags)
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		services: make(map[string]*ServiceInfo),
	}

	for _, opt := range opts {
		opt(&manager)
	}

	return &manager, nil
}

type ServiceInfo struct {
	Db                      *gorm.DB
	ConnectFunc             func() *gorm.DB
	DisconnectFunc          func(db *gorm.DB)
	TargetVersion           models.Version
	registeredMigrations    []*Migration
	registeredMigrationsSet map[uint32]*Migration
}

type MigrationManager struct {
	logger   *slog.Logger
	services map[string]*ServiceInfo

	mutex sync.Mutex
}

func (m *MigrationManager) RegisterService(name string, connectFunc func() *gorm.DB, disconnectFunc func(db *gorm.DB), targetVersion string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	parsedTargetVersion, err := models.ParseVersion(targetVersion)
	if err != nil {
		return err
	}

	service, ok := m.services[name]

	if !ok {
		service = &ServiceInfo{
			ConnectFunc:             connectFunc,
			DisconnectFunc:          disconnectFunc,
			TargetVersion:           parsedTargetVersion,
			registeredMigrations:    make([]*Migration, 0),
			registeredMigrationsSet: make(map[uint32]*Migration),
		}
		m.services[name] = service
	} else {
		service.ConnectFunc = connectFunc
		service.DisconnectFunc = disconnectFunc
		service.TargetVersion = parsedTargetVersion
		m.services[name] = service
	}

	return nil
}

func (m *MigrationManager) GetServiceInfoUnsafe(name string) (*ServiceInfo, bool) {
	serviceInfo, ok := m.services[name]
	return serviceInfo, ok
}

// Register сохраняет миграции в память.
// По умолчанию миграции осуществляются внутри транзакции.
//
// Паникует при регистрации миграций с одинаковымм версией и типом.
func (m *MigrationManager) Register(serviceName string, migrationsStruct ...Migration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	service, ok := m.services[serviceName]

	if !ok {
		service = &ServiceInfo{
			registeredMigrations:    make([]*Migration, 0),
			registeredMigrationsSet: make(map[uint32]*Migration),
		}

		m.services[serviceName] = service
	}

	for i := 0; i < len(migrationsStruct); i++ {
		migrationVersion, err := models.ParseVersion(migrationsStruct[i].Version)
		if err != nil {
			return err
		}

		identifier := getMigrationIdentifier(migrationVersion, string(migrationsStruct[i].MigrationType))
		if _, ok = service.registeredMigrationsSet[identifier]; ok {
			continue
		}

		migrationsStruct[i].Identifier = identifier
		service.registeredMigrationsSet[identifier] = &migrationsStruct[i]
		service.registeredMigrations = append(service.registeredMigrations, &migrationsStruct[i])
	}

	return nil
}

// CheckFulfillment проверяет корректность установки всех миграций. Проверяется, что нет миграций со статусом
// models.StateFailure, затем проверяется, что все зарегистрированные миграции выше послденей сохраненной версии сохранены и
// выполнены успешно, затем проверяется, что target версия установлена выше или равной последней найденной миграции.
func (m *MigrationManager) CheckFulfillment(serviceName string) (reasonErr error, ok bool, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return errors.New("service not found"), false, fmt.Errorf("service %s not found", serviceName)
	}

	service.Db = service.ConnectFunc()
	defer func() {
		service.DisconnectFunc(service.Db)
	}()

	hasForthcoming, err := m.hasForthcomingMigrations(serviceName)
	if err != nil {
		return nil, false, err
	}
	if hasForthcoming {
		return ErrHasForthcomingMigrations, false, nil
	}

	hasFailedMigrations, err := m.hasFailedMigrations(serviceName)
	if err != nil {
		return nil, false, err
	}
	if hasFailedMigrations {
		return ErrHasFailedMigrations, false, err
	}

	targetVersionNotLatest, err := m.targetVersionNotLatest(serviceName)
	if err != nil {
		return nil, false, err
	}
	if targetVersionNotLatest {
		return ErrTargetVersionNotLatest, false, nil
	}

	return nil, true, nil
}

// hasFailedMigrations определяет есть ли миграции, не выполненные из-за ошибки.
func (m *MigrationManager) hasFailedMigrations(serviceName string) (bool, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return false, fmt.Errorf("service %s not found", serviceName)
	}

	// не было выполнено ни одной, следовательно, пока ошибок не было
	if !repository.HasVersionTable(service.Db) || !repository.HasMigrationsTable(service.Db) {
		return false, nil
	}

	savedMigrations, err := repository.GetMigrationsSorted(service.Db, repository.OrderASC)
	if err != nil {
		return false, err
	}

	for i := range savedMigrations {
		if savedMigrations[i].State == models.StateFailure {
			return true, nil
		}
	}
	return false, nil
}

// hasForthcomingMigrations проверяет, есть ли зарегистрированные или сохраненные невыполненные миграции, выше текущей
// сохраненной версии.
func (m *MigrationManager) hasForthcomingMigrations(serviceName string) (bool, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return false, fmt.Errorf("service %s not found", serviceName)
	}

	// не было выполнено ни одной
	if !repository.HasVersionTable(service.Db) || !repository.HasMigrationsTable(service.Db) {
		return true, nil
	}

	savedVersion, err := m.getSavedAppVersion(serviceName)

	if err != nil {
		return false, err
	}

	savedMigrations, err := repository.GetMigrationsSorted(service.Db, repository.OrderASC)
	if err != nil {
		return false, err
	}

	for i := range savedMigrations {
		if savedMigrations[i].Version.MoreOrEqual(savedVersion) && savedMigrations[i].State != models.StateSuccess {
			return true, nil
		}
	}

	for i := range service.registeredMigrations {
		// достаточно проверить, что миграция еще не сохранена, т.к. создание новых миграций разрешено только для версий
		// выше текущей максимальной версии сохраненных миграций
		if migrationIsNew(service.registeredMigrations[i], savedMigrations) {
			return true, nil
		}
	}

	return false, nil
}

// targetVersionNotLatest проверяет, является ли target версия выше или равной максимальной версии зарегистрированной
// или сохраненной миграции.
func (m *MigrationManager) targetVersionNotLatest(serviceName string) (bool, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return false, fmt.Errorf("service %s not found", serviceName)
	}

	// не было выполнено ни одной, следовательно, пока ошибок не было
	if !repository.HasVersionTable(service.Db) || !repository.HasMigrationsTable(service.Db) {
		return false, nil
	}

	savedMigrations, err := repository.GetMigrationsSorted(service.Db, repository.OrderASC)
	if err != nil {
		return false, err
	}

	for i := range savedMigrations {
		if !service.TargetVersion.MoreOrEqual(savedMigrations[i].Version) {
			return true, nil
		}
	}

	for i := range service.registeredMigrations {
		migrationVersion, err := models.ParseVersion(service.registeredMigrations[i].Version)

		if err != nil {
			return false, err
		}

		if !service.TargetVersion.MoreOrEqual(migrationVersion) {
			return true, nil
		}
	}

	return false, nil
}

func (m *MigrationManager) findMigration(serviceName string, migrationModel models.MigrationModel) (*Migration, bool, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return nil, false, fmt.Errorf("service %s not found", serviceName)
	}

	migrationModelIdentifier := getMigrationIdentifier(migrationModel.Version, migrationModel.Type)

	for _, migration := range service.registeredMigrations {
		migrationVersion, err := models.ParseVersion(migration.Version)

		if err != nil {
			return nil, false, err
		}

		registeredMigrationIdentifier := getMigrationIdentifier(migrationVersion, string(migration.MigrationType))
		if registeredMigrationIdentifier == migrationModelIdentifier {
			return migration, true, nil
		}
	}

	return nil, false, nil
}

func (m *MigrationManager) getSavedAppVersion(serviceName string) (models.Version, error) {
	service, ok := m.services[serviceName]

	if !ok {
		m.logger.Error(fmt.Sprintf("service %s not found", serviceName))
		return models.Version{}, fmt.Errorf("service %s not found", serviceName)
	}

	savedAppVersion, err := repository.GetVersion(service.Db)
	// если текущая версия миграции не найдена, возвращаем версию 0.0.0, как минимально возможную
	if err != nil {
		return models.Version{}, err
	}

	return savedAppVersion, nil
}

func migrationIsNew(migration *Migration, savedMigrations []models.MigrationModel) bool {
	for j := range savedMigrations {
		savedMigrationIdentifier := getMigrationIdentifier(savedMigrations[j].Version, savedMigrations[j].Type)
		if migration.Identifier == savedMigrationIdentifier {
			return false
		}
	}
	return true
}

func getMigrationIdentifier(version models.Version, migrationType string) uint32 {
	h := fnv.New32a()
	// fmv.sum64a always writes with no error
	_, _ = h.Write([]byte(version.String() + migrationType))
	return h.Sum32()
}
