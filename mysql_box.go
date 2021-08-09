package mysqlbox

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
)

const startTimeout = time.Second * 30
const containerStopTimeoutDur = time.Second * 60
const defaultMySQLImage = "mysql:8"

// Config contains MySQLBox settings.
type Config struct {
	// ContainerName specifies the MySQL container name. If blank, it will be generated as "mysqlbox-<random name>".
	ContainerName string

	// Image specifies what Docker image to use. If blank, it defaults to "mysql:8".
	Image string

	// Database specifies the name of the database to create. If blank, it defaults to "testing".
	Database string

	// RootPassword specifies the password of the MySQL root user.
	RootPassword string

	// MySQLPort specifies which port the MySQL server port (3306) will be bound to in the container.
	MySQLPort int

	// InitialSQL specifies an SQL script stored in a file or a buffer that will be run against the Database
	// when the MySQL server container is started.
	InitialSQL *Data

	// DoNotCleanTables specifies a list of MySQL tables in Database that will not be cleaned when CleanAllTables()
	// is called.
	DoNotCleanTables []string

	// Stdout is an optional writer where the container log stdout will be sent to.
	Stdout io.Writer
	// Stderr is an optional writer where the container log stderr will be sent to.
	Stderr io.Writer

	// LoggedErrors is an optional list of strings that will contain error messages from the container stderr logs.
	LoggedErrors *[]string
}

// LoadDefaults initializes some blank attributes of Config to default values.
func (c *Config) LoadDefaults() {
	if c.Image == "" {
		c.Image = defaultMySQLImage
	}

	if c.Database == "" {
		c.Database = "testing"
	}

	if c.ContainerName == "" {
		c.ContainerName = fmt.Sprintf("mysqlbox-%s", randomID())
	}
}

// MySQLBox is an interface to a MySQL server running in a Docker container.
type MySQLBox struct {
	dsn          string
	databaseName string
	db           *sql.DB

	cli           *client.Client
	containerName string
	containerID   string
	schemaFile    *os.File

	// stoppedCh receives the signal when the container is stopped.
	stoppedCh chan bool

	// logBuf is where the mysql logs are stored (these are logs coming from the client library and are not the server logs)
	logBuf *bytes.Buffer
	cout   io.Writer
	cerr   io.Writer

	// port is the assigned port to the container that maps to the mysqld port
	port             int
	doNotCleanTables []string
}

// Start creates a Docker container that runs an instance of MySQL server. The passed Config object contains settings
// for the container, the MySQL service, and initial data. To stop the created container, call the function returned
// by Stop().
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
	var schemaFile *os.File
	if c.InitialSQL != nil && (c.InitialSQL.reader != nil || c.InitialSQL.buf != nil) {
		var err error
		schemaFile, err = ioutil.TempFile(os.TempDir(), "schema-*.sql")
		if err != nil {
			return nil, err
		}

		var src io.Reader

		if c.InitialSQL.reader != nil {
			src = c.InitialSQL.reader
		} else if c.InitialSQL.buf != nil {
			src = c.InitialSQL.buf
		}

		_, err = io.Copy(schemaFile, src)
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

	if c.RootPassword == "" {
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
			"com.github.virgild.mysqlbox": "1",
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
	if schemaFile != nil {
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   schemaFile.Name(),
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
	created, createErr := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, c.ContainerName)
	if client.IsErrNotFound(createErr) {
		err := pullImage(ctx, cli, c.Image)
		if err != nil {
			return nil, fmt.Errorf("failed to pull image: %w", err)
		}

		created, createErr = cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, c.ContainerName)
	}
	if createErr != nil {
		return nil, fmt.Errorf("error creating container: %w", err)
	}

	// Create stopped channel
	stoppedCh := make(chan bool, 1)

	// Channel for container closed
	containerClosed := make(chan bool, 1)

	// Set mysql logger
	_ = mysql.SetLogger(mylog)

	// Start container
	err = cli.ContainerStart(ctx, created.ID, types.ContainerStartOptions{})
	if err != nil {
		return nil, err
	}

	// Get container logs
	cout := c.Stdout
	cerr := c.Stderr
	go readContainerLogs(ctx, cli, created.ID, cout, cerr, c.LoggedErrors, containerClosed)

	// Get port binding
	port, err := containerMySQLPort(ctx, cli, created.ID)
	if err != nil {
		return nil, err
	}

	// Connect to DB
	db, dsn, err := connectDB(port, c.Database, c.RootPassword)
	if err != nil {
		return nil, err
	}

	b := &MySQLBox{
		db:               db,
		dsn:              dsn,
		port:             port,
		logBuf:           logbuf,
		cli:              cli,
		containerID:      created.ID,
		containerName:    c.ContainerName,
		schemaFile:       schemaFile,
		databaseName:     c.Database,
		doNotCleanTables: c.DoNotCleanTables,
		cout:             cout,
		cerr:             cerr,
		stoppedCh:        stoppedCh,
	}

	// Wait for db
	err = b.waitForDB(startTimeout, containerClosed)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// MustStart is the same as Start() but panics instead of returning an error.
func (b *MySQLBox) MustStart(c *Config) *MySQLBox {
	box, err := Start(c)
	if err != nil {
		panic(err)
	}

	return box
}

// Stop stops the MySQL container.
func (b *MySQLBox) Stop() error {
	if b == nil {
		return errors.New("mysqlbox is nil")
	}

	// Clean up files
	defer b.cleanupFiles()

	// Stop container
	err := b.stopContainer()
	if err != nil {
		return err
	}

	// Wait for container to be removed
	msgCh, errCh := b.cli.ContainerWait(context.Background(), b.containerID, container.WaitConditionRemoved)
Wait:
	for {
		select {
		case <-msgCh:
			break Wait
		case err := <-errCh:
			if errdefs.IsNotFound(err) {
				break Wait
			} else {
				return err
			}
		}
	}

	return nil
}

// MustStop stops the MySQL container.
func (b *MySQLBox) MustStop() {
	err := b.Stop()
	if err != nil {
		panic(err)
	}
}

func (b *MySQLBox) stopContainer() error {
	timeout := containerStopTimeoutDur
	err := b.cli.ContainerStop(context.Background(), b.containerID, &timeout)
	if err != nil {
		return err
	}

	return nil
}

// DB returns an sql.DB connected to the running MySQL server.
func (b *MySQLBox) DB() (*sql.DB, error) {
	if b == nil {
		return nil, errors.New("mysqlbox is nil")
	}

	return b.db, nil
}

// MustDB returns an sql.DB conencted to the running MySQL server.
func (b *MySQLBox) MustDB() *sql.DB {
	db, err := b.DB()
	if err != nil {
		panic(err)
	}

	return db
}

// DSN returns the MySQL database DSN that can be used to connect to the MySQL service.
func (b *MySQLBox) DSN() (string, error) {
	if b == nil {
		return "", errors.New("mysqlbox is nil")
	}

	return b.dsn, nil
}

// MustDSN returns the MySQL database DSN that can be used to connect to the MySQL service.
func (b *MySQLBox) MustDSN() string {
	dsn, err := b.DSN()
	if err != nil {
		panic(err)
	}

	return dsn
}

// ContainerName returns the name of the created container.
func (b *MySQLBox) ContainerName() (string, error) {
	if b == nil {
		return "", errors.New("mysqlbox is nil")
	}

	return b.containerName, nil
}

// MustContainerName returns the name of the created container.
func (b *MySQLBox) MustContainerName() string {
	name, err := b.ContainerName()
	if err != nil {
		panic(err)
	}

	return name
}

// CleanAllTables truncates all tables in the Database, except those provided in Config.DoNotCleanTables.
func (b *MySQLBox) CleanAllTables() error {
	if b == nil {
		return errors.New("mysqlbox is nil")
	}

	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	rows, err := b.db.Query(query, b.databaseName)
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

	return nil
}

// MustCleanAllTables truncates all tables in the Database, except those provided in Config.DoNotCleanTables.
func (b *MySQLBox) MustCleanAllTables() {
	err := b.CleanAllTables()
	if err != nil {
		panic(err)
	}
}

// CleanTables truncates the specified tables in the Database.
func (b *MySQLBox) CleanTables(tables ...string) error {
	if b == nil {
		return errors.New("mysqlbox is nil")
	}

	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		_, err := b.db.Exec(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "truncate table failed (%s): %s\n", table, err.Error())
		}
	}

	return nil
}

// MustCleanTables truncates the specified tables in the Database.
func (b *MySQLBox) MustCleanTables(tables ...string) {
	err := b.CleanTables(tables...)
	if err != nil {
		panic(err)
	}
}

// cleanupFiles removes all temporary files created in the host space.
func (b *MySQLBox) cleanupFiles() {
	// Delete the schema file
	if b.schemaFile != nil {
		b.schemaFile.Close()
		os.Remove(b.schemaFile.Name())
	}
}

// connectDB returns a DB connection to the MySQL server.
func connectDB(port int, dbName string, rootPass string) (*sql.DB, string, error) {
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.Net = "tcp"
	mysqlCfg.ParseTime = true
	mysqlCfg.Addr = net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port))
	mysqlCfg.DBName = dbName
	mysqlCfg.User = "root"
	mysqlCfg.Passwd = rootPass

	dsn := mysqlCfg.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, "", err
	}

	return db, dsn, nil
}

// containerMYSQLPort returns the MySQL port number of the running container.
func containerMySQLPort(ctx context.Context, cli *client.Client, containerID string) (int, error) {
	cr, err := cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return 0, err
	}

	ports := cr.NetworkSettings.Ports["3306/tcp"]
	if len(ports) == 0 {
		return 0, errors.New("no port bindings")
	}

	port, err := strconv.Atoi(ports[0].HostPort)
	if err != nil {
		return 0, err
	}

	return port, nil
}

// readContainerLogs starts reading a container log's two streams (stdout and stderr), and copies
// them to the provider cout and cerr writers. While the stderr is being read, it also scanned
// line by line. If a line starts with "ERROR", it is copied to the passed errors list.
func readContainerLogs(ctx context.Context,
	cli *client.Client,
	containerID string,
	cout io.Writer,
	cerr io.Writer,
	errors *[]string,
	containerExit chan<- bool) {
	if cout == nil {
		cout = io.Discard
	}

	if cerr == nil {
		cerr = io.Discard
	}

	// Get container log reader
	clog, err := cli.ContainerLogs(ctx, containerID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return
	}

	pr, pw := io.Pipe()
	mw := io.MultiWriter(cerr, pw)

	// Go routine to scan the pipe reader for mysql errors:
	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "ERROR") {
				if errors != nil {
					*errors = append(*errors, line)
				}
			}
		}
	}()

	// Multiplex container logs to cout and the cerr pipe.
	// Receiving a signal in the clogClose channel will close the reader and exit this loop.
	_, err = stdcopy.StdCopy(cout, mw, clog)
	if err != nil {
		if err.Error() != "http: read on closed response body" {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}

		return
	}
	clog.Close()
	pw.Close()
	containerExit <- true
}

func (b *MySQLBox) waitForDB(timeout time.Duration, containerClosed <-chan bool) error {
	if b == nil {
		return errors.New("mysqlbox is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	for {
		err := b.db.PingContext(ctx)
		if err == nil {
			cancel()
			break
		}
		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			return errors.New("could not connect to mysql")
		}
		time.Sleep(time.Millisecond * 500)

		select {
		case <-containerClosed:
			cancel()
			return errors.New("container closed")
		default:
		}
	}

	return nil
}

func pullImage(ctx context.Context, cli *client.Client, image string) error {
	if image == "" {
		return errors.New("image is blank")
	}

	fmt.Printf("pulling Docker image %s...\n", image)
	reader, err := cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("docker image pull error: %w", err)
	}
	defer reader.Close()

	err = jsonmessage.DisplayJSONMessagesStream(reader, os.Stderr, 0, false, nil)
	if err != nil {
		return fmt.Errorf("docker image pull stream error: %w", err)
	}
	fmt.Printf("Docker image %s pulled.\n", image)

	return nil
}
