package mysqlbox

import (
	"os"
	"testing"
	"time"
)

func TestPanicRecoverCleanup(t *testing.T) {
	b, err := Start(&Config{})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		recover()
		b.StopFunc()()
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
		t.Cleanup(b.StopFunc())

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
			InitialSchema:      InitialSchemaFromBuffer(sql),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.StopFunc())

		query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
		now := time.Now()
		_, err = b.DB().Exec(query, "U-TEST1", "user1@example.com", now, now)
		if err != nil {
			t.Error(err)
		}
	})
}
