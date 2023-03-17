package db_migrator

import (
	"database/sql"
	"log"
	"testing"
)

const dsn = "postgres://admin:admin@127.0.0.1:5432/test"

func TestMigrate(t *testing.T) {
	{
		migrator, err := NewMigrationsManager(dsn, "1.1.0.0")
		if err != nil {
			log.Fatalln(err)
		}

		migrator.RegisterLite(
			MigrationLite{
				MigrationType:   TypeBaseline,
				Version:         "1.0.0.0",
				Description:     "initial migration with connections",
				IsTransactional: true,
				Up:              "",
				Down:            "",
				UpF: func(db *sql.DB) error {
					_, err := db.Exec("create table connections( id bigserial, one text, two numeric )")
					return err
				},
			},
			MigrationLite{
				MigrationType: TypeVersioned,
				Version:       "1.0.0.1",
				Description:   "Up connections",
				Up:            "",
				Down:          "",
				UpF: func(db *sql.DB) error {
					_, err := db.Exec("drop table connections")
					return err
				},
			},
		)

		err = migrator.Migrate()
		if err != nil {
			log.Fatalln(err)
		}
	}
}
