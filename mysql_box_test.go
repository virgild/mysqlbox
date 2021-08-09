package mysqlbox_test

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/virgild/mysqlbox"
)

func ExampleStart() {
	// Start the MySQL server container
	b, err := mysqlbox.Start(&mysqlbox.Config{})
	if err != nil {
		log.Printf("MySQLBox failed to start: %s\n", err.Error())
		return
	}

	// Query the database
	db, err := b.DB()
	if err != nil {
		log.Printf("db error: %s\n", err.Error())
		return
	}
	_, err = db.Query("SELECT * FROM users LIMIT 5")
	if err != nil {
		log.Printf("select failed: %s\n", err.Error())
		return
	}

	// Stop the container
	err = b.Stop()
	if err != nil {
		log.Printf("stop container failed: %s\n", err.Error())
	}
}

func TestMySQLBoxNilError(t *testing.T) {
	t.Parallel()
	var b *mysqlbox.MySQLBox

	t.Run("dsn", func(t *testing.T) {
		_, err := b.DSN()
		require.Error(t, err)
	})

	t.Run("must_dsn", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustDSN()
		})
	})

	t.Run("db", func(t *testing.T) {
		_, err := b.DB()
		require.Error(t, err)
	})

	t.Run("must_db", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustDB()
		})
	})

	t.Run("stop", func(t *testing.T) {
		err := b.Stop()
		require.Error(t, err)
	})

	t.Run("must_stop", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustStop()
		})
	})

	t.Run("container_name", func(t *testing.T) {
		_, err := b.ContainerName()
		require.Error(t, err)
	})

	t.Run("must_container_name", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustContainerName()
		})
	})

	t.Run("clean_tables", func(t *testing.T) {
		err := b.CleanTables("testing")
		require.Error(t, err)
	})

	t.Run("must_clean_tables", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustCleanTables("testing")
		})
	})

	t.Run("clean_all_tables", func(t *testing.T) {
		err := b.CleanAllTables()
		require.Error(t, err)
	})

	t.Run("must_clean_all_tables", func(t *testing.T) {
		require.Panics(t, func() {
			b.MustCleanAllTables()
		})
	})
}

func TestMySQLBoxDefaultConfig(t *testing.T) {
	b, err := mysqlbox.Start(&mysqlbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := b.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})

	db, err := b.DB()
	require.NoError(t, err)
	require.NotNil(t, db)

	require.NotPanics(t, func() {
		db = b.MustDB()
		require.NotNil(t, db)
	})

	dsn, err := b.DSN()
	require.NoError(t, err)
	require.NotEmpty(t, dsn)

	require.NotPanics(t, func() {
		dsn = b.MustDSN()
		require.NotEmpty(t, dsn)
	})

	containerName, err := b.ContainerName()
	require.NoError(t, err)
	require.NotEmpty(t, containerName)

	require.NotPanics(t, func() {
		containerName = b.MustContainerName()
		require.NotEmpty(t, containerName)
	})

	row := db.QueryRow("SELECT NOW()")
	var now time.Time
	err = row.Scan(&now)
	if err != nil {
		t.Error(err)
	}

	if now.IsZero() {
		t.Error("time is zero")
	}
}

func TestPanicRecoverCleanup(t *testing.T) {
	b, err := mysqlbox.Start(&mysqlbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		recover()
		err := b.Stop()
		if err != nil {
			t.Fatal(err)
		}
	}()

	panic("panic!")
}

func TestMySQLBoxWithInitialSchema(t *testing.T) {
	t.Run("with file reader", func(t *testing.T) {
		schemaFile, err := os.Open("./testdata/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			schemaFile.Close()
		}()

		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL: mysqlbox.DataFromReader(schemaFile),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = db.Exec(query, "U-TEST1", "user1@example.com", now, now)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("with file", func(t *testing.T) {
		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL: mysqlbox.DataFromFile("./testdata/schema.sql"),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = db.Exec(query, "U-TEST1", "user1@example.com", now, now)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("with buffer", func(t *testing.T) {
		sql := []byte(`
			CREATE TABLE users
			(
				id         varchar(128) NOT NULL,
				email      varchar(128) NOT NULL,
				created_at datetime     NOT NULL,
				updated_at datetime     NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY users_email_uindex (email),
				UNIQUE KEY users_id_uindex (id)
			) ENGINE = InnoDB
			DEFAULT CHARSET = utf8mb4;
		`)

		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL: mysqlbox.DataFromBuffer(sql),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = db.Exec(query, "U-TEST1", "user1@example.com", now, now)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestStartBadSchema(t *testing.T) {
	schemaFile, err := os.Open("./testdata/bad-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		schemaFile.Close()
	}()

	b, err := mysqlbox.Start(&mysqlbox.Config{
		InitialSQL: mysqlbox.DataFromReader(schemaFile),
	})
	if err == nil {
		t.Error("mysql box should not start")
	}

	if b != nil {
		t.Error("Start should not return a mysql box")
	}
}

func TestCleanTables(t *testing.T) {
	t.Run("with no protected tables", func(t *testing.T) {
		schemaFile, err := os.Open("./testdata/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			schemaFile.Close()
		}()

		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL: mysqlbox.DataFromReader(schemaFile),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatal(err)
		}

		now := time.Now()
		for n := 0; n < 10; n++ {
			_, err := stmt.Exec(fmt.Sprintf("U-TEST%d", n), fmt.Sprintf("user%d@example.com", n), now, now)
			require.NoError(t, err)
		}

		// Check inserted rows
		var count uint
		row := db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean all tables
		err = b.CleanAllTables()
		require.NoError(t, err)

		// Check inserted rows
		row = db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check rows fom initial schema
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}
	})

	t.Run("with protected tables", func(t *testing.T) {
		schemaFile, err := os.Open("./testdata/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			schemaFile.Close()
		}()

		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL:       mysqlbox.DataFromReader(schemaFile),
			DoNotCleanTables: []string{"categories"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatal(err)
		}

		now := time.Now()
		for n := 0; n < 10; n++ {
			_, err := stmt.Exec(fmt.Sprintf("U-TEST%d", n), fmt.Sprintf("user%d@example.com", n), now, now)
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
		}

		// Check inserted rows
		var count uint
		row := db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean all tables
		err = b.CleanAllTables()
		require.NoError(t, err)

		// Check inserted rows
		row = db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check rows fom initial schema
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}
	})

	t.Run("specific tables", func(t *testing.T) {
		schemaFile, err := os.Open("./testdata/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			schemaFile.Close()
		}()

		b, err := mysqlbox.Start(&mysqlbox.Config{
			InitialSQL:       mysqlbox.DataFromReader(schemaFile),
			DoNotCleanTables: []string{"categories"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := b.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})

		db, err := b.DB()
		require.NoError(t, err)

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatal(err)
		}

		now := time.Now()
		for n := 0; n < 10; n++ {
			_, err := stmt.Exec(fmt.Sprintf("U-TEST%d", n), fmt.Sprintf("user%d@example.com", n), now, now)
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
		}

		// Check inserted rows
		var count uint
		row := db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean tables
		err = b.CleanTables("categories", "non_existent")
		require.NoError(t, err)

		// Check users table
		row = db.QueryRow("SELECT COUNT(*) FROM users")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check categories table
		row = db.QueryRow("SELECT COUNT(*) FROM categories")
		err = row.Scan(&count)
		require.NoError(t, err)

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}
	})
}

func TestContainerLogs(t *testing.T) {
	cout := bytes.NewBuffer(nil)
	cerr := bytes.NewBuffer(nil)
	box, err := mysqlbox.Start(&mysqlbox.Config{
		Stdout: cout,
		Stderr: cerr,
	})
	require.NoError(t, err)

	err = box.Stop()
	require.NoError(t, err)
	require.NotEmpty(t, cout.Bytes())
	require.NotEmpty(t, cerr.Bytes())
}

func TestErrorLogs(t *testing.T) {
	// Provide an invalid initial SQL script to trigger an error:
	initialSQL := `
		SELECT * FROM sales WHERE id = 1;
	`
	loggedErrors := []string{}

	_, err := mysqlbox.Start(&mysqlbox.Config{
		InitialSQL:   mysqlbox.DataFromBuffer([]byte(initialSQL)),
		LoggedErrors: &loggedErrors,
	})
	require.Error(t, err)
	require.Len(t, loggedErrors, 1)
	require.Equal(t, "ERROR 1146 (42S02) at line 2: Table 'testing.sales' doesn't exist", loggedErrors[0])
}

func TestMultipleDatabases(t *testing.T) {
	box, err := mysqlbox.Start(&mysqlbox.Config{
		// multiple-db.sql creates two databases: db_one and db_two.
		InitialSQL: mysqlbox.DataFromFile("./testdata/multiple-db.sql"),
	})
	require.NoError(t, err)
	t.Cleanup(box.MustStop)

	db1, dsn1, err := box.ConnectDB("db_one")
	require.NoError(t, err)
	require.NotEmpty(t, dsn1)

	// db_one has a table called 'users'
	_, err = db1.Query("SELECT * FROM users")
	require.NoError(t, err)

	db2, dsn2, err := box.ConnectDB("db_two")
	require.NoError(t, err)
	require.NotEmpty(t, dsn2)

	// db_two has a table called 'products'
	_, err = db2.Query("SELECT * FROM products")
	require.NoError(t, err)

	t.Run("connect_db_non_existent", func(t *testing.T) {
		db3, dsn3, err := box.ConnectDB("db_three")
		require.NoError(t, err)
		require.NotEmpty(t, dsn3)

		_, err = db3.Query("SELECT * FROM articles")
		require.Error(t, err)
	})
}

func TestDBProperties(t *testing.T) {
	box, err := mysqlbox.Start(&mysqlbox.Config{
		RootPassword: "root_pass",
	})
	require.NoError(t, err)
	t.Cleanup(box.MustStop)

	t.Run("db_addr", func(t *testing.T) {
		require.NotEmpty(t, box.DBAddr())
		host, port, err := net.SplitHostPort(box.DBAddr())
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", host)
		require.NotEmpty(t, port)
	})

	t.Run("root_password", func(t *testing.T) {
		require.Equal(t, "root_pass", box.RootPassword())
	})
}
