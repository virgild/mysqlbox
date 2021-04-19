package mysqlbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
	"github.com/oklog/ulid/v2"
)

type Config struct {
	ContainerName      string
	Image              string
	Database           string
	RootPassword       string
	RandomRootPassword bool
	MySQLPort          int
	InitialSchema      *InitialSchema
}

type InitialSchema struct {
	buf    *bytes.Buffer
	reader io.Reader
}

func InitialSchemaFromReader(reader io.Reader) *InitialSchema {
	return &InitialSchema{
		reader: reader,
	}
}

func InitialSchemaFromBuffer(buf []byte) *InitialSchema {
	return &InitialSchema{
		buf: bytes.NewBuffer(buf),
	}
}

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
	db            *sqlx.DB
	containerName string
	stopFunc      func()
	// logBuf is where the mysql logs are stored (these are logs coming from the library and are not the server logs)
	logBuf *bytes.Buffer
	// port is the assigned port to the container that maps to the mysqld port
	port int
}

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
	if c.InitialSchema != nil && (c.InitialSchema.reader != nil || c.InitialSchema.buf != nil) {
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

		if c.InitialSchema.reader != nil {
			src = c.InitialSchema.reader
		} else if c.InitialSchema.buf != nil {
			src = c.InitialSchema.buf
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
	stopFunc := func() {
		timeout := time.Second * 60
		err := cli.ContainerStop(context.Background(), created.ID, &timeout)
		if err != nil {
			fmt.Printf("stop container error: %s\n", err.Error())
			return
		}
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
		db:            db,
		url:           url,
		stopFunc:      stopFunc,
		port:          port,
		logBuf:        logbuf,
		containerName: c.ContainerName,
	}

	return b, nil
}

func (b *MySQLBox) StopFunc() func() {
	return b.stopFunc
}

func (b *MySQLBox) DB() *sqlx.DB {
	return b.db
}

func (b *MySQLBox) URL() string {
	return b.url
}

func (b *MySQLBox) ContainerName() string {
	return b.containerName
}

func (b *MySQLBox) CleanTables(table ...string) {
	panic("not implemented")
}

type mysqlLogger struct {
	buf *bytes.Buffer
	lg  *log.Logger
}

func newMySQLLogger(buf *bytes.Buffer) *mysqlLogger {
	lg := log.New(buf, "mysql: ", 0)
	lg.SetOutput(buf)
	ml := &mysqlLogger{
		buf: buf,
		lg:  lg,
	}
	return ml
}

func (l *mysqlLogger) Print(args ...interface{}) {
	l.lg.Print(args[0])
}

var entropy = ulid.Monotonic(rand.Reader, 0)

func randomID() string {
	t := time.Now()
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}
