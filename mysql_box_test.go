package mysqlbox_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/virgild/testutils/mysqlbox"
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
	var b *mysqlbox.MySQLBox

	t.Run("url", func(t *testing.T) {
		_, err := b.URL()
		require.Error(t, err)
	})

	t.Run("db", func(t *testing.T) {
		_, err := b.DB()
		require.Error(t, err)
	})

	t.Run("dbx", func(t *testing.T) {
		_, err := b.DBx()
		require.Error(t, err)
	})

	t.Run("stop", func(t *testing.T) {
		err := b.Stop()
		require.Error(t, err)
	})

	t.Run("container_name", func(t *testing.T) {
		_, err := b.ContainerName()
		require.Error(t, err)
	})

	t.Run("clean_tables", func(t *testing.T) {
		err := b.CleanTables("testing")
		require.Error(t, err)
	})

	t.Run("clean_all_tables", func(t *testing.T) {
		err := b.CleanAllTables()
		require.Error(t, err)
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

	dbx, err := b.DBx()
	require.NoError(t, err)
	require.NotNil(t, dbx)

	db, err := b.DB()
	require.NoError(t, err)
	require.NotNil(t, db)

	dburl, err := b.URL()
	require.NoError(t, err)
	require.NotEmpty(t, dburl)

	containerName, err := b.ContainerName()
	require.NoError(t, err)
	require.NotEmpty(t, containerName)

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
	t.Run("with file", func(t *testing.T) {
		schemaFile, err := os.Open("../testdata/schema.sql")
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

	t.Run("with bad schema", func(t *testing.T) {
		schemaFile, err := os.Open("../testdata/bad-schema.sql")
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
	})
}

func TestCleanTables(t *testing.T) {
	t.Run("with no protected tables", func(t *testing.T) {
		schemaFile, err := os.Open("../testdata/schema.sql")
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
		schemaFile, err := os.Open("../testdata/schema.sql")
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
		schemaFile, err := os.Open("../testdata/schema.sql")
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
