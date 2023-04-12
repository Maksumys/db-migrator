package db_migrator

import "database/sql"

type MigrationLite struct {
	MigrationType MigrationType
	Version       string
	Description   string

	IsTransactional bool
	IsAllowFailure  bool

	Up   string
	Down string

	UpF   func(db *sql.DB) error
	DownF func(db *sql.DB) error

	CheckSum            func(db *sql.DB) string
	Identifier          uint32
	RepeatUnconditional bool
}
