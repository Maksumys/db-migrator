package db_migrator

import (
	"database/sql"
)

type MigrationType string

const (
	TypeBaseline   MigrationType = "baseline"
	TypeVersioned  MigrationType = "versioned"
	TypeRepeatable MigrationType = "repeatable"
)

type DbDependency struct {
	Name    string
	Version string
}

type Migration struct {
	MigrationType MigrationType
	Version       string
	Description   string

	IsTransactional bool
	IsAllowFailure  bool

	Up   string
	Down string

	UpF   func(migrator *MigrationManager) error
	DownF func(migrator *MigrationManager) error

	CheckSum            func(db *sql.DB) string
	Identifier          uint32
	RepeatUnconditional bool

	Dependency []DbDependency
}
