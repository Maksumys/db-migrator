### Описание
Библиотека осуществляет управление миграциями базы данных.  
В качестве базы данных используется Postgresql. В качестве ORM используется GORM.  
Интерфейс миграций подразумевает возможность отката текущей миграции.  
Управление текущей версией бд и статусом миграций осуществляется по таблицам version и migrations.

### Сущности
**Migrator** - интерфейс, описывающий миграцию  
**MigrationManager** - фасад для управления миграциями  



### Пример использования
см. [migrate_test.go](migrate_test.go)