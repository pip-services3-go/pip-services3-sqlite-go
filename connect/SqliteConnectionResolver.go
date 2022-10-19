package connect

import (
	"strings"
	"sync"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
)

/*
SqliteConnectionResolver a helper struct  that resolves Sqlite connection and credential parameters,
validates them and generates a connection URI.
It is able to process multiple connections to Sqlite cluster nodes.

Configuration parameters

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from IDiscovery
  - host:                        host name or IP address
  - port:                        port number (default: 27017)
  - database:                    database name
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from ICredentialStore
  - username:                    user name
  - password:                    user password

 References

- *:discovery:*:*:1.0             (optional) IDiscovery services
- *:credential-store:*:*:1.0      (optional) Credential stores to resolve credentials
*/
type SqliteConnectionResolver struct {
	//The connections resolver.
	ConnectionResolver ccon.ConnectionResolver
	//The credentials resolver.
	CredentialResolver cauth.CredentialResolver
}

// NewSqliteConnectionResolver creates new connection resolver
// Retruns *SqliteConnectionResolver
func NewSqliteConnectionResolver() *SqliteConnectionResolver {
	mongoCon := SqliteConnectionResolver{}
	mongoCon.ConnectionResolver = *ccon.NewEmptyConnectionResolver()
	mongoCon.CredentialResolver = *cauth.NewEmptyCredentialResolver()
	return &mongoCon
}

// Configure is configures component by passing configuration parameters.
// Parameters:
// 	- config  *cconf.ConfigParams
//  configuration parameters to be set.
func (c *SqliteConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

// SetReferences is sets references to dependent components.
// Parameters:
// 	- references crefer.IReferences
//	references to locate the component dependencies.
func (c *SqliteConnectionResolver) SetReferences(references crefer.IReferences) {
	c.ConnectionResolver.SetReferences(references)
	c.CredentialResolver.SetReferences(references)
}

func (c *SqliteConnectionResolver) validateConnection(correlationId string, connection *ccon.ConnectionParams) error {
	uri := connection.Uri()
	if uri != "" {
		if !strings.HasPrefix(uri, "file://") {
			return cerr.NewConfigError(correlationId, "WRONG_PROTOCOL", "Connection protocol must be file://")
		}
		return nil
	}

	database := connection.GetAsNullableString("database")
	if *database == "" {
		return cerr.NewConfigError(correlationId, "NO_DATABASE", "Connection database is not set")
	}
	return nil
}

func (c *SqliteConnectionResolver) validateConnections(correlationId string, connections []*ccon.ConnectionParams) error {
	if connections == nil || len(connections) == 0 {
		return cerr.NewConfigError(correlationId, "NO_CONNECTION", "Database connection is not set")
	}
	for _, connection := range connections {
		err := c.validateConnection(correlationId, connection)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *SqliteConnectionResolver) composeConfig(connections []*ccon.ConnectionParams, credential *cauth.CredentialParams) (map[string]interface{}, error) {

	var e error
	config := make(map[string]interface{}, 0)

	// Define connection part
	for _, connection := range connections {
		uri := connection.Uri()
		if uri != "" {

			// Removing file://
			config["database"] = uri[7:]

		}

		database := connection.GetAsNullableString("database")
		if database != nil && *database != "" {
			config["database"] = *database
		}
	}

	return config, e

}

//   Resolves SQLite connection URI from connection and credential parameters.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//  Return 			     resolved config or error.
func (c *SqliteConnectionResolver) Resolve(correlationId string) (config map[string]interface{}, err error) {
	var connections []*ccon.ConnectionParams
	var credential *cauth.CredentialParams
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		connections, err = c.ConnectionResolver.ResolveAll(correlationId)
		// Validate connections
		if err == nil {
			err = c.validateConnections(correlationId, connections)
		}
	}()

	go func() {
		defer wg.Done()
		credential, err = c.CredentialResolver.Lookup(correlationId)
		// Credentials are not validated right now
	}()
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return c.composeConfig(connections, credential)

}
