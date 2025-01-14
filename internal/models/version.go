package models

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type VersionModel struct {
	Version Version
}

func (v VersionModel) TableName() string {
	return "version"
}

type Version struct {
	Major      int
	Minor      int
	Patch      int
	PreRelease int
}

func (v Version) Value() (driver.Value, error) {
	return v.String(), nil
}

func (v *Version) Scan(value interface{}) error {
	var err error

	switch value.(type) {
	case string:
		*v, err = ParseVersion(value.(string))
		if err != nil {
			return err
		}
	case []byte:
		*v, err = ParseVersion(string(value.([]byte)))
		if err != nil {
			return err
		}
	default:
		err = errors.New("invalid type")
	}
	return nil
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.Major, v.Minor, v.Patch, v.PreRelease)
}

func (v Version) Equals(version Version) bool {
	return v == version
}

func (v Version) MoreThan(version Version) bool {
	if v.Major > version.Major {
		return true
	} else if v.Major < version.Major {
		return false
	}

	if v.Minor > version.Minor {
		return true
	} else if v.Minor < version.Minor {
		return false
	}

	if v.Patch > version.Patch {
		return true
	} else if v.Patch < version.Patch {
		return false
	}

	if v.PreRelease > version.PreRelease {
		return true
	} else if v.PreRelease < version.PreRelease {
		return false
	}

	return false
}

func (v Version) MoreOrEqual(version Version) bool {
	return v.MoreThan(version) || v.Equals(version)
}

func (v Version) LessThan(version Version) bool {
	return !v.MoreOrEqual(version)
}

func (v Version) LessOrEqual(version Version) bool {
	return !v.MoreThan(version)
}

func ParseVersion(versionString string) (Version, error) {
	versions := strings.Split(versionString, ".")

	if len(versions) != 4 {
		return Version{}, errors.New(fmt.Sprintf("invalid Version format: %s", versionString))
	}

	major, _ := strconv.Atoi(versions[0])
	minor, _ := strconv.Atoi(versions[1])
	patch, _ := strconv.Atoi(versions[2])
	preRelease, _ := strconv.Atoi(versions[3])

	return Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: preRelease,
	}, nil
}
