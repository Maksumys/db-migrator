package models

type MigrationState string

const (
	StateSuccess    MigrationState = "success"
	StateFailure    MigrationState = "failure"
	StateUndone     MigrationState = "undone"
	StateRegistered MigrationState = "registered"
	StateSkipped    MigrationState = "skipped"
	StateNotFound   MigrationState = "not found"
)

type MigrationModel struct {
	Id           uint32 `gorm:"primaryKey"`
	Rank         int
	Type         string
	Version      Version
	Description  string
	RegisteredOn CustomTime  `gorm:"type:datetime"`
	ExecutedOn   *CustomTime `gorm:"type:datetime"`
	Checksum     string
	State        MigrationState
}

func (v MigrationModel) TableName() string {
	return "migrations"
}
