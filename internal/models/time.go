package models

import (
	"database/sql/driver"
	"time"
)

type CustomTime struct {
	time.Time
}

func (c CustomTime) MarshalJson() ([]byte, error) {
	return c.MarshalJSON()
}

func (c CustomTime) Value() (driver.Value, error) {
	return c.Time, nil
}

func (c *CustomTime) Scan(value interface{}) error {
	switch value.(type) {
	case time.Time:
		*c = CustomTime{Time: value.(time.Time)}
	case int64:
		*c = CustomTime{Time: time.Unix(value.(int64), 0)}
	}

	return nil
}
