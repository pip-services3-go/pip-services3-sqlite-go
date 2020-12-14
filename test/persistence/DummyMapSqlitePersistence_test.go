package test

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
)

func TestDummyMapSqlitePersistence(t *testing.T) {

	var persistence *DummyMapSqlitePersistence
	var fixture tf.DummyMapPersistenceFixture

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
	persistence = NewDummyMapSqlitePersistence()
	persistence.Configure(dbConfig)

	fixture = *tf.NewDummyMapPersistenceFixture(persistence)

	opnErr := persistence.Open("")
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

	t.Run("DummyMapSqlitePersistence:CRUD", fixture.TestCrudOperations)

	opnErr = persistence.Clear("")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummyMapSqlitePersistence:Batch", fixture.TestBatchOperations)

}
