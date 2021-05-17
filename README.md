# mysqlbox

![MySQLBox logo](https://github.com/virgild/blob/main/static/logo.png?raw=true)

**mysqlbox** creates a ready to use MySQL server in a Docker container that can be
used in Go tests. The `Start()` function returns a `MySQLBox` that has a container running MySQL server. 
It has a `Stop()` function that stops the container. The `DB()` function returns a connected 
`sql.DB` that can be used to send queries to MySQL.



### Install mysqlbox

```sh
go get github.com/virgild/mysqlbox
```

### Basic usage

```go
package mytests

import (
	"testing"
	"time"

	"github.com/virgild/mysqlbox"
)

func TestMyCode(t *testing.T) {
	// Start MySQL container
	box, err := mysqlbox.Start(&mysqlbox.Config{})
	if err != nil {
	    t.Fatal(err)
	}

	// Register the stop func to stop the container after the test.
	t.Cleanup(func() {
	    err := box.Stop()
	    if err != nil {
	        t.Fatal(err)
	    }
	})
	
	// Use the box's sql.DB object to query the database.
	row := box.MustDB().QueryRow("SELECT NOW()")
	
	var now time.Time
	err = row.Scan(&row)
	if err != nil {
	    t.Error(err)
	}
	
	if now.IsZero() {
	    t.Error("now is zero")
	}
}
```

### Other Features

#### Initial script

MySQL server can be started with an initial SQL script that is run after the service starts. It can be provided as an `io.Reader` or a `[]byte` buffer.

##### Specifying the initial script from a file/reader

```go
schemaFile, err := os.Open("testdata/schema.sql")
if err != nil {
    t.Fatal(err)
}
defer schemaFile.Close()

box, err = mysqlbox.Start(&Config{
    InitialSQL: mysqlbox.DataFromReader(schemaFile),
}
if err != nil {
    t.Fatal(err)
}

t.Cleanup(func() {
    err := b.Stop()
    if err != nil {
        t.Fatal(err)
    }
})
```

##### Specifying the initial script from a byte buffer

```go
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

b, err := mysqlbox.Start(&Config{
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

query := "INSERT INTO users (id, email, created_at, updated_at) VALUES (?, ?, ?, ?)"
now := time.Now()
_, err = b.DB().Exec(query, "U-TEST1", "user1@example.com", now, now)
if err != nil {
	t.Error(err)
}
```

#### Cleaning tables

All tables can be truncated by calling `CleanAllTables()`. This runs `TRUNCATE` on all tables in the database, except for those specified in the `Config.DoNotCleanTables` array. Another function called `CleanTables()` can  be used to truncate just specific tables you want to clean. Any table passed to `CleanTables()` will always truncate it even if it is included in the `DoNotCleanTables` list.

### Using MySQLBox outside tests

It is not recommended to use MySQLBox as a normal MySQL database. This component is designed to be ephemeral, and no precautions are implemented to protect the database data.

### Troubleshooting and other notes

* I forgot to call `Stop()` and now I have a several containers that are still running.

    The following command can be run to stop the containers started by `MySQLBox`.

    ```sh
    docker ps -a -f "label=com.github.virgild.mysqlbox" --format '{{.ID}}' | xargs docker stop
    ```