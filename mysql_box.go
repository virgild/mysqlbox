package mysqlbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// Config contains MySQLBox settings.
type Config struct {
	// ContainerName specifies the MySQL container name. If blank, it will be generated as "mysql-test-<random id>".
	ContainerName string

	// Image specifies what Docker image to use. If blank, it defaults to "mysql:8".
	Image string

	// Database specifies the name of the database to create. If blank, it defaults to "testing".
	Database string

	// RootPassword specifies the password of the MySQL root user. If blank, the password is set to empty unless
	// RandomRootPassword is true.
	RootPassword string

	// RandomRootPassword sets the password of the MySQL root user to a random value.
	RandomRootPassword bool

	// MySQLPort specifies which port the MySQL server port (3306) will be bound to in the container.
	MySQLPort int

	// InitialSQL specifies an SQL script stored in a file or a buffer that will be run against the Database
	// when the MySQL server container is started.
	InitialSQL *Data

	// DoNotCleanTables specifies a list of MySQL tables in Database that will not be cleaned when CleanAllTables()
	// is called.
	DoNotCleanTables []string
}

// LoadDefaults initializes some blank attributes of Config to default values.
func (c *Config) LoadDefaults() {
	if c.Image == "" {
		c.Image = "mysql:8"
	}

	if c.Database == "" {
		c.Database = "testing"
	}

	if c.ContainerName == "" {
		c.ContainerName = fmt.Sprintf("mysql-test-%s", randomID())
	}
}

type MySQLBox struct {
	url           string
	databaseName  string
	db            *sqlx.DB
	containerName string
	stopFunc      func() error
	// logBuf is where the mysql logs are stored (these are logs coming from the library and are not the server logs)
	logBuf *bytes.Buffer
	// port is the assigned port to the container that maps to the mysqld port
	port             int
	doNotCleanTables []string
}

// Start creates a Docker container that will run a MySQL server. The passed Config object contains settings
// for the container, the MySQL service, and initial data. To stop the created container, call the function returned
// by StopFunc.
func Start(c *Config) (*MySQLBox, error) {
	var envVars []string

	// Load config
	if c == nil {
		c = &Config{}
	}

	c.LoadDefaults()

	// mysql log buffer
	logbuf := bytes.NewBuffer(nil)
	mylog := newMySQLLogger(logbuf)

	// Initial schema - write to file so it can be passed to docker
	var tmpf *os.File
	if c.InitialSQL != nil && (c.InitialSQL.reader != nil || c.InitialSQL.buf != nil) {
		var err error
		tmpf, err = ioutil.TempFile(os.TempDir(), "schema-*.sql")
		if err != nil {
			return nil, err
		}
		defer func() {
			tmpf.Close()
			os.Remove(tmpf.Name())
		}()

		var src io.Reader

		if c.InitialSQL.reader != nil {
			src = c.InitialSQL.reader
		} else if c.InitialSQL.buf != nil {
			src = c.InitialSQL.buf
		}

		_, err = io.Copy(tmpf, src)
		if err != nil {
			return nil, err
		}
	}

	// Create docker client
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	// Load container env vars
	envVars = append(envVars, fmt.Sprintf("MYSQL_DATABASE=%s", c.Database))

	if c.RandomRootPassword {
		envVars = append(envVars, "MYSQL_RANDOM_ROOT_PASSWORD=1")
	} else if c.RootPassword == "" {
		envVars = append(envVars, "MYSQL_ALLOW_EMPTY_PASSWORD=1")
	} else {
		envVars = append(envVars, fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", c.RootPassword))
	}

	// Container config
	cfg := &container.Config{
		Image: c.Image,
		Env:   envVars,
		Cmd: []string{
			"--default-authentication-plugin=mysql_native_password",
			"--general-log=1",
			"--general-log-file=/var/lib/mysql/general-log.log",
		},
		ExposedPorts: map[nat.Port]struct{}{
			"3306/tcp": {},
		},
		Labels: map[string]string{
			"com.github.virgild.testutils.mysqlbox": "1",
		},
	}

	portBinding := nat.PortBinding{
		HostIP:   "127.0.0.1",
		HostPort: "0",
	}

	if c.MySQLPort != 0 {
		portBinding.HostPort = fmt.Sprintf("%d", c.MySQLPort)
	}

	var mounts []mount.Mount
	if tmpf != nil {
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   tmpf.Name(),
			Target:   "/docker-entrypoint-initdb.d/schema.sql",
			ReadOnly: true,
		})
	}

	// Host config
	hostCfg := &container.HostConfig{
		AutoRemove: true,
		PortBindings: map[nat.Port][]nat.PortBinding{
			"3306/tcp": {
				portBinding,
			},
		},
		Mounts: mounts,
	}

	// Create container
	created, err := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, c.ContainerName)
	if err != nil {
		return nil, err
	}

	// Create container stopper function
	stopFunc := func() error {
		timeout := time.Second * 60
		err := cli.ContainerStop(context.Background(), created.ID, &timeout)
		if err != nil {
			return err
		}

		return nil
	}

	// Set mysql logger
	_ = mysql.SetLogger(mylog)

	// Start container
	err = cli.ContainerStart(ctx, created.ID, types.ContainerStartOptions{})
	if err != nil {
		return nil, err
	}

	// Get port binding
	cr, err := cli.ContainerInspect(ctx, created.ID)
	if err != nil {
		return nil, err
	}

	ports := cr.NetworkSettings.Ports["3306/tcp"]
	if len(ports) == 0 {
		return nil, errors.New("no port bindings")
	}

	portStr := ports[0].HostPort
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	// Connect to db
	url := fmt.Sprintf("root:%s@tcp(127.0.0.1:%d)/%s?parseTime=true", c.RootPassword, port, c.Database)
	db, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	// Ping DB n times
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	for {
		err := db.PingContext(ctx)
		if err == nil {
			cancel()
			break
		}
		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			return nil, fmt.Errorf("could not connect to mysql")
		}
		time.Sleep(time.Millisecond * 500)
	}

	b := &MySQLBox{
		db:               db,
		url:              url,
		stopFunc:         stopFunc,
		port:             port,
		logBuf:           logbuf,
		containerName:    c.ContainerName,
		databaseName:     c.Database,
		doNotCleanTables: c.DoNotCleanTables,
	}

	return b, nil
}

// Stop stops the MySQL container.
func (b *MySQLBox) Stop() error {
	if b.stopFunc == nil {
		return errors.New("mysqlbox has no stop func")
	}

	return b.stopFunc()
}

// DBx returns an sqlx.DB connected to the running MySQL server.
func (b *MySQLBox) DBx() *sqlx.DB {
	return b.db
}

// DB returns an sql.DB connected to the running MySQL server.
func (b *MySQLBox) DB() *sql.DB {
	return b.db.DB
}

// URL returns the MySQL database URL that can be used to connect tohe MySQL service.
func (b *MySQLBox) URL() string {
	return b.url
}

// ContainerName returns the name of the created container.
func (b *MySQLBox) ContainerName() string {
	return b.containerName
}

// CleanAllTables truncates all tables in the Database, except those provided in Config.DoNotCleanTables.
func (b *MySQLBox) CleanAllTables() {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	rows, err := b.db.Queryx(query, b.databaseName)
	if err != nil {
		panic(err)
	}
	defer func() {
		rows.Close()
	}()

	excludedTables := map[string]bool{}
	for _, table := range b.doNotCleanTables {
		excludedTables[table] = true
	}

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			panic(err)
		}

		if excludedTables[table] {
			continue
		}

		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		_, err = b.db.Exec(query)
		if err != nil {
			panic(err)
		}
	}
}

// CleanTables truncates the specified tables in the Database.
func (b *MySQLBox) CleanTables(tables ...string) {
	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		_, err := b.db.Exec(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "truncate table failed (%s): %s\n", table, err.Error())
		}
	}
}
