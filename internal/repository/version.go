package repository

import (
	"errors"
	"github.com/Maksumys/db-migrator/internal/models"
	"gorm.io/gorm"
)

func GetVersion(db *gorm.DB) (string, error) {
	var row models.VersionModel
	res := db.Find(&row)

	if res.Error != nil {
		switch {
		case errors.Is(res.Error, gorm.ErrRecordNotFound):
			return "", ErrNotFound
		default:
			return "", res.Error
		}
	}

	if res.RowsAffected == 0 {
		return "", ErrNotFound
	}

	return row.Version, nil
}

func SaveVersion(db *gorm.DB, version string) error {
	var row models.VersionModel
	count := db.Find(&row).RowsAffected

	if count == 0 {
		_ = db.Create(&models.VersionModel{Version: version}).Error
		return nil
	}

	return db.Model(&models.VersionModel{}).Where("version = ?", row.Version).Update("version", version).Error
}

func HasVersionTable(db *gorm.DB) bool {
	return db.Migrator().HasTable(models.VersionModel{}.TableName())
}

func CreateVersionTable(db *gorm.DB) error {
	return db.Exec(`
		CREATE TABLE IF NOT EXISTS version (
			version TEXT
		)
	`).Error
}
