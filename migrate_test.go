package db_migrator

//const dsn = "postgres://admin:admin@127.0.0.1:5432/test"
//const dsn2 = "postgres://admin:admin@127.0.0.1:5432/test2"
//
//func TestMigrations(t *testing.T) {
//	var postgresContainer *postgres.PostgresContainer
//	var err error
//
//	postgresContainer, err = postgres.Run(
//		context.Background(),
//		"postgres:16-alpine",
//		postgres.WithDatabase("postgres"),
//		postgres.WithUsername("postgres"),
//		postgres.WithPassword("postgres"),
//		postgres.BasicWaitStrategies(),
//		postgres.WithSQLDriver("pgx"),
//	)
//
//	require.NoError(t, err)
//
//	migrator, errNew := NewMigrationsManager()
//	if errNew != nil {
//		log.Fatalln(errNew)
//	}
//
//	err = migrator.RegisterService("service1", func() *gorm.DB {
//		return createDb(postgresContainer.MustConnectionString(context.Background()))
//	}, func(db *gorm.DB) {
//		d, _ := db.DB()
//		d.Close()
//	},
//		"1.0.1.0")
//
//	require.NoError(t, err)
//
//	err = migrator.Register(
//		"service1",
//		Migration{
//			MigrationType:   TypeBaseline,
//			Version:         "1.0.0.0",
//			Description:     "initial migration with connections",
//			IsAllowFailure:  false,
//			IsTransactional: true,
//			Down:            "",
//			Up:              "create table connections( id bigserial, one text, two numeric );",
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.0.1",
//			Description:   "up connections",
//			Up:            "alter table connections add column three text;",
//			Down:          "",
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.1.0",
//			Description:   "up connections",
//			Up:            "alter table connections add column four text;",
//			Down:          "",
//		},
//	)
//
//	require.NoError(t, err)
//
//	err = migrator.Migrate("service1")
//	require.NoError(t, err)
//}
//
//func createDb(dsn string) *gorm.DB {
//	dbConfig := &gorm.Config{
//		NamingStrategy: schema.NamingStrategy{
//			SingularTable: true,
//		},
//		NowFunc: func() time.Time {
//			return time.Now().UTC()
//		},
//		Logger: nil,
//	}
//
//	db, err := gorm.Open(gormPg.New(gormPg.Config{
//		DSN:                  dsn,
//		PreferSimpleProtocol: true,
//	}), dbConfig)
//
//	if err != nil {
//		panic(err)
//	}
//
//	return db
//}

//func TestMigrate2(t *testing.T) {
//	migrator, errNew := NewMigrationsManager()
//	if errNew != nil {
//		log.Fatalln(errNew)
//	}
//
//	err := migrator.RegisterService(
//		"service2", func() *gorm.DB {
//			return nil
//		},
//		func(db *gorm.DB) {
//
//		}, "1.0.0.0")
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	err = DB1(migrator)
//	if err != nil {
//		log.Fatalln(err)
//	}
//}
//
//func TestMigrate(t *testing.T) {
//	migrator, errNew := NewMigrationsManager()
//	if errNew != nil {
//		log.Fatalln(errNew)
//	}
//
//	wait := sync.WaitGroup{}
//	wait.Add(2)
//
//	go func() {
//		for {
//			time.Sleep(5 * time.Second)
//
//			err := DB1(migrator)
//			if err == nil {
//				break
//			}
//		}
//
//		wait.Done()
//	}()
//
//	go func() {
//		for {
//			err := DB2(migrator)
//			if err == nil {
//				break
//			}
//			time.Sleep(2 * time.Second)
//		}
//
//		wait.Done()
//	}()
//
//	wait.Wait()
//}
//
//func DB1(migrator *MigrationManager) error {
//	err := migrator.RegisterService("service1", func() *gorm.DB {
//		dbConfig := &gorm.Config{
//			NamingStrategy: schema.NamingStrategy{
//				SingularTable: true,
//			},
//			NowFunc: func() time.Time {
//				return time.Now().UTC()
//			},
//		}
//
//		db, err := gorm.Open(postgres.New(postgres.Config{
//			DSN:                  dsn,
//			PreferSimpleProtocol: true,
//		}), dbConfig)
//
//		if err != nil {
//			panic(err)
//		}
//
//		return db
//	}, func(db *gorm.DB) {
//		d, _ := db.DB()
//		d.Close()
//	},
//		"1.0.1.0")
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	err = migrator.Register(
//		"service1",
//		Migration{
//			MigrationType:   TypeBaseline,
//			Version:         "1.0.0.0",
//			Description:     "initial migration with connections",
//			IsAllowFailure:  false,
//			IsTransactional: true,
//			Down:            "",
//			Up:              "create table connections( id bigserial, one text, two numeric );",
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.0.1",
//			Description:   "up connections",
//			Up:            "alter table connections add column three text;",
//			Down:          "",
//			Dependency: []DbDependency{
//				{
//					Name:    "service2",
//					Version: "1.0.0.1",
//				},
//			},
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.1.0",
//			Description:   "up connections",
//			Up:            "alter table connections add column four text;",
//			Down:          "",
//		},
//	)
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	return migrator.Migrate("service1")
//}
//
//func DB2(migrator *MigrationManager) error {
//	err := migrator.RegisterService("service2", func() *gorm.DB {
//		dbConfig := &gorm.Config{
//			NamingStrategy: schema.NamingStrategy{
//				SingularTable: true,
//			},
//			NowFunc: func() time.Time {
//				return time.Now().UTC()
//			},
//		}
//
//		db, err := gorm.Open(postgres.New(postgres.Config{
//			DSN:                  dsn2,
//			PreferSimpleProtocol: true,
//		}), dbConfig)
//
//		if err != nil {
//			panic(err)
//		}
//
//		return db
//	}, func(db *gorm.DB) {
//		d, _ := db.DB()
//		d.Close()
//	}, "1.0.1.0")
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	err = migrator.Register(
//		"service2",
//		Migration{
//			MigrationType:   TypeBaseline,
//			Version:         "1.0.0.0",
//			Description:     "initial migration with connections",
//			IsAllowFailure:  false,
//			IsTransactional: true,
//			Down:            "",
//			Up:              "create table connections( id bigserial, one text, two numeric );",
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.0.1",
//			Description:   "up connections",
//			Up:            "alter table connections add column three text;",
//			Down:          "",
//		},
//		Migration{
//			MigrationType: TypeVersioned,
//			Version:       "1.0.1.0",
//			Description:   "up connections2",
//			Up:            "alter table connections add column four text;",
//			Down:          "",
//			Dependency: []DbDependency{
//				{
//					Name:    "service1",
//					Version: "1.0.1.0",
//				},
//			},
//		},
//	)
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	return migrator.Migrate("service2")
//}
