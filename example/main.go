package example

import (
	"database/sql"
	"embed"
	"github.com/Maksumys/db-migrator"
	"log"
)

const dsn = "postgres://admin:admin@127.0.0.1:5432/test"

//go:embed migrations
var migrations embed.FS

func readFile(file string) string {
	bytes, err := migrations.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func main() {
	migrator, err := db_migrator.NewMigrationsManager(dsn, "1.1.0.0")
	if err != nil {
		log.Fatalln(err)
	}

	migrator.RegisterLite(
		db_migrator.MigrationLite{
			MigrationType:   db_migrator.TypeBaseline,
			Version:         "1.0.0.0",
			Description:     "initial migration with connections",
			IsAllowFailure:  false,
			IsTransactional: true,
			Up:              "",
			Down:            "",
			UpF: func(db *sql.DB) error {
				_, err := db.Exec(readFile("v1.0.0.0_up.sql"))
				return err
			},
		},
		db_migrator.MigrationLite{
			MigrationType: db_migrator.TypeVersioned,
			Version:       "1.0.0.1",
			Description:   "up connections",
			Up:            "",
			Down:          "",
			UpF: func(db *sql.DB) error {
				_, err := db.Exec(readFile("v1.0.0.1_up.sql"))
				return err
			},
		},
	)

	err = migrator.Migrate()
	if err != nil {
		log.Fatalln(err)
	}
}
