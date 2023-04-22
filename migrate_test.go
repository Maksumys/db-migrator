package db_migrator

import (
	"database/sql"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"testing"
)

const dsn = "postgres://admin:admin@127.0.0.1:5432/test"

func TestMigrate(t *testing.T) {
	logrus.SetLevel(logrus.Level(5))

	{
		db, err := gorm.Open(postgres.New(postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true,
		}))
		if err != nil {
			log.Fatalln(err)
		}

		migrator, err := NewMigrationsManager(db, "1.2.0.1", WithLogWriter(logrus.StandardLogger().Writer()))
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
				CheckSum: func(db *sql.DB) string {
					return "1"
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
			MigrationLite{
				MigrationType: TypeVersioned,
				Version:       "1.1.0.0",
				Description:   "Up connections",
				Up:            "",
				Down:          "",
				UpF: func(db *sql.DB) error {
					_, err := db.Exec("create table connections( id bigserial, one text, two numeric )")
					return err
				},
			},
			MigrationLite{
				MigrationType: TypeVersioned,
				Version:       "1.2.0.0",
				Description:   "Up connections",
				Up:            "",
				Down:          "",
				UpF: func(db *sql.DB) error {
					_, err := db.Exec("create table qwerty( id bigserial, one text, two numeric )")
					return err
				},
			},
			MigrationLite{
				MigrationType: TypeVersioned,
				Version:       "1.2.0.1",
				Description:   "Up connections 2",
				Up:            "alter table qwerty add column qwe123 numeric",
				Down:          "",
			},
			MigrationLite{
				MigrationType: TypeRepeatable,
				Version:       "1.2.0.1",
				Description:   "Up connections 2",
				Up:            `insert into qwerty (id, one, two, qwe123) values (1, '1', 1, 1)`,
				CheckSum: func(db *sql.DB) string {
					rows, err := db.Query("SELECT * from qwerty where one = '1';")
					if err != nil {
						return ""
					}

					defer rows.Close()

					for rows.Next() {
						return "1"
					}

					return "0"
				},
			},
		)

		err = migrator.Migrate()
		if err != nil {
			log.Fatalln(err)
		}
	}
}
