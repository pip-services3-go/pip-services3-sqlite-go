package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	sliteconn "github.com/pip-services3-go/pip-services3-sqlite-go/connect"
)

// Creates Sqlite components by their descriptors.
// See [[Factory]]
// See [[SqliteConnection]]
type DefaultSqliteFactory struct {
	cbuild.Factory
	Descriptor                 *cref.Descriptor
	SqliteConnectionDescriptor *cref.Descriptor
}

//	Create a new instance of the factory.
func NewDefaultSqliteFactory() *DefaultSqliteFactory {

	c := &DefaultSqliteFactory{

		Descriptor:                 cref.NewDescriptor("pip-services", "factory", "sqlite", "default", "1.0"),
		SqliteConnectionDescriptor: cref.NewDescriptor("pip-services", "connection", "sqlite", "*", "1.0"),
	}
	c.RegisterType(c.SqliteConnectionDescriptor, sliteconn.NewSqliteConnection)
	return c
}
