package db_migrator

import (
	"gorm.io/gorm"
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
	Strict  bool
}

type Migration struct {
	MigrationType MigrationType
	Version       string
	Description   string

	IsTransactional bool
	IsAllowFailure  bool

	Up   string
	Down string

	UpF   func(selfDb *gorm.DB, depsDb map[string]*gorm.DB) error
	DownF func(selfDb *gorm.DB, depsDb map[string]*gorm.DB) error

	CheckSum            func(selfDb *gorm.DB) string
	Identifier          uint32
	RepeatUnconditional bool

	Dependency []DbDependency
}
