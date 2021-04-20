package mysqlbox

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func ExampleStart() {
	// Start the MySQL server container
	b, err := Start(&Config{})
	if err != nil {
		log.Printf("MySQLBox failed to start: %s\n", err.Error())
		return
	}

	// Query the database
	_, err = b.DB().Query("SELECT * FROM users LIMIT 5")
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

func TestMySQLBoxDefaultConfig(t *testing.T) {
	b, err := Start(&Config{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := b.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})

	if b.DBx() == nil {
		t.Error("DBx() returns nil")
	}

	if b.DB() == nil {
		t.Error("DB() returns nil")
	}

	if b.URL() == "" {
		t.Error("URL() returns blank string")
	}

	if b.ContainerName() == "" {
		t.Error("ContainerName() returns blank string")
	}
}

func TestPanicRecoverCleanup(t *testing.T) {
	b, err := Start(&Config{})
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

		b, err := Start(&Config{
			InitialSchema: InitialSchemaFromReader(schemaFile),
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

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = b.DB().Exec(query, "U-TEST1", "user1@example.com", now, now)
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

		b, err := Start(&Config{
			InitialSchema: InitialSchemaFromBuffer(sql),
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

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = b.DB().Exec(query, "U-TEST1", "user1@example.com", now, now)
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

		b, err := Start(&Config{
			InitialSchema: InitialSchemaFromReader(schemaFile),
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

		b, err := Start(&Config{
			InitialSchema: InitialSchemaFromReader(schemaFile),
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

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := b.DB().Prepare(query)
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
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean all tables
		b.CleanAllTables()

		// Check inserted rows
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check rows fom initial schema
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

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

		b, err := Start(&Config{
			InitialSchema:    InitialSchemaFromReader(schemaFile),
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

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := b.DB().Prepare(query)
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
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean all tables
		b.CleanAllTables()

		// Check inserted rows
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check rows fom initial schema
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

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

		b, err := Start(&Config{
			InitialSchema:    InitialSchemaFromReader(schemaFile),
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

		// Insert rows
		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		stmt, err := b.DB().Prepare(query)
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
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check other rows from the initial schema
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 5 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Clean tables
		b.CleanTables("categories", "non_existent")

		// Check users table
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM users")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 10 {
			t.Error("select count does not match")
			t.FailNow()
		}

		// Check categories table
		err = b.DBx().Get(&count, "SELECT COUNT(*) FROM categories")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if count != 0 {
			t.Error("select count does not match")
			t.FailNow()
		}
	})
}
