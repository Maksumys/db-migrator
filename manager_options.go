package db_migrator

import (
	"log/slog"
)

type ManagerOption func(*MigrationManager)

func WithLogger(logger *slog.Logger) ManagerOption {
	return func(m *MigrationManager) {
		m.logger = logger
	}
}
