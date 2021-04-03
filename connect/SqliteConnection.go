package connect

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
)

/**
 * PostgreSQL connection using plain driver.
 *
 * By defining a connection and sharing it through multiple persistence components
 * you can reduce number of used database connections.
 *
 * ### Configuration parameters ###
 *
 * - connection(s):
 *   - discovery_key:             (optional) a key to retrieve the connection from [[IDiscovery]]
 *   - database:                  path to database file
 *   - uri:                       resource URI or connection string with all parameters in it
 * ### References ###
 *
 * - \*:logger:\*:\*:1.0           (optional) [[ILogger]] components to pass log messages
 * - \*:discovery:\*:\*:1.0        (optional) [[ IDiscovery]] services
 * - \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
 *
 */
type SqliteConnection struct {
	defaultConfig *cconf.ConfigParams
	// The logger.
	Logger *clog.CompositeLogger
	// The connection resolver.
	ConnectionResolver *SqliteConnectionResolver
	// The configuration options.
	Options *cconf.ConfigParams
	// The PostgreSQL connection pool object.
	Connection *sql.DB
	// The PostgreSQL database name.
	DatabaseName string
}

// NewSqliteConnection creates a new instance of the connection component.
func NewSqliteConnection() *SqliteConnection {
	c := &SqliteConnection{
		defaultConfig:      cconf.NewEmptyConfigParams(),
		Logger:             clog.NewCompositeLogger(),
		ConnectionResolver: NewSqliteConnectionResolver(),
		Options:            cconf.NewEmptyConfigParams(),
	}
	return c
}

// Configures component by passing configuration parameters.
//   - config    configuration parameters to be set.
func (c *SqliteConnection) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.ConnectionResolver.Configure(config)
	c.Options = c.Options.Override(config.GetSection("options"))
}

// Sets references to dependent components.
//  - references 	references to locate the component dependencies.
func (c *SqliteConnection) SetReferences(references cref.IReferences) {
	c.Logger.SetReferences(references)
	c.ConnectionResolver.SetReferences(references)
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *SqliteConnection) IsOpen() bool {
	return c.Connection != nil
}

// Opens the component.
//  - correlationId 	(optional) transaction id to trace execution through call chain.
//  - Return 			error or nil no errors occured.
func (c *SqliteConnection) Open(correlationId string) error {

	config, err := c.ConnectionResolver.Resolve(correlationId)

	if err != nil {
		c.Logger.Error(correlationId, err, "Failed to resolve Sqlite connection")
		return nil
	}
	database, ok := config["database"].(string)
	if !ok || database == "" {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Database name is empty. Connection to sqlite failed")
		return err
	}

	c.Logger.Debug(correlationId, "Connecting to sqlite")

	con, err := sql.Open("sqlite3", database)

	if err != nil || con == nil {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to sqlite failed").WithCause(err)
	} else {

		c.Connection = con
		c.DatabaseName = database
	}
	return err
}

// Closes component and frees used resources.
//  - correlationId 	(optional) transaction id to trace execution through call chain.
// Return			 error or nil no errors occured
func (c *SqliteConnection) Close(correlationId string) error {
	if c.Connection == nil {
		return nil
	}
	err := c.Connection.Close()
	if err != nil {
		c.Logger.Error(correlationId, err, "Error while closing SQLite database %s", c.DatabaseName)
		return err
	}
	c.Logger.Debug(correlationId, "Disconnected from sqlite database %s", c.DatabaseName)
	c.Connection = nil
	c.DatabaseName = ""
	return nil
}

func (c *SqliteConnection) GetConnection() *sql.DB {
	return c.Connection
}

func (c *SqliteConnection) GetDatabaseName() string {
	return c.DatabaseName
}
