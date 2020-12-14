package test

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	ppersist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
	"github.com/stretchr/testify/assert"
)

func TestDummySqliteConnection(t *testing.T) {

	var persistence *DummySqlitePersistence
	var fixture tf.DummyPersistenceFixture
	var connection *ppersist.SqliteConnection

	sqliteDatabase := os.Getenv("SQLITE_DB")
	if sqliteDatabase == "" {
		sqliteDatabase = "../../data/test.db"
	}

	if sqliteDatabase == "" {
		panic("Connection params losse")
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.database", sqliteDatabase,
	)

	connection = ppersist.NewSqliteConnection()
	connection.Configure(dbConfig)

	persistence = NewDummySqlitePersistence()
	descr := cref.NewDescriptor("pip-services", "connection", "sqlite", "default", "1.0")
	ref := cref.NewReferencesFromTuples(descr, connection)
	persistence.SetReferences(ref)

	fixture = *tf.NewDummyPersistenceFixture(persistence)

	opnErr := connection.Open("")
	if opnErr != nil {
		t.Error("Error opened connection", opnErr)
		return
	}
	defer connection.Close("")

	opnErr = persistence.Open("")
	if opnErr != nil {
		t.Error("Error opened persistence", opnErr)
		return
	}
	defer persistence.Close("")

	opnErr = persistence.Clear("")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("Connection", func(t *testing.T) {
		assert.NotNil(t, connection.GetConnection())
		assert.NotNil(t, connection.GetDatabaseName())
		assert.NotEqual(t, "", connection.GetDatabaseName())
	})

	t.Run("DummySqliteConnection:CRUD", fixture.TestCrudOperations)

	opnErr = persistence.Clear("")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummySqliteConnection:Batch", fixture.TestBatchOperations)

}
