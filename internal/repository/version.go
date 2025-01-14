package repository

import (
	"errors"
	"github.com/Maksumys/db-migrator/internal/models"
	"gorm.io/gorm"
)

func GetVersion(db *gorm.DB) (models.Version, error) {
	var row models.VersionModel
	res := db.First(&row)

	if res.Error != nil {
		switch {
		case errors.Is(res.Error, gorm.ErrRecordNotFound):
			return models.Version{}, ErrNotFound
		default:
			return models.Version{}, res.Error
		}
	}

	if res.RowsAffected == 0 {
		return models.Version{}, ErrNotFound
	}

	return row.Version, nil
}

func SaveVersion(db *gorm.DB, version models.Version) error {
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
