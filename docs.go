// Package mysqlbox provides MySQLBox, a utility for starting a MySQL server in a Docker container.
//
// It creates a ready to use MySQL server in a Docker container that can be
// used in tests. The Start() function returns a MySQLBox that has a running MySQL server.
// It has a Stop() function that stops the container. The DB() function returns a connected
// sql.DB that can be used to send queries to MySQL.
//
package mysqlbox
