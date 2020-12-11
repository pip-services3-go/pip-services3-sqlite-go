package test

// import (
// 	"os"
// 	"testing"

// 	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
// 	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
// )

// func TestDummyRefSqlitePersistence(t *testing.T) {

// 	var persistence *DummyRefSqlitePersistence
// 	var fixture tf.DummyRefPersistenceFixture

// sqliteDatabase := os.Getenv("SQLITE_DB")
// 	if sqliteDatabase == "" {
// 		sqliteDatabase = "./test.db"
// 	}

// 	if sqliteDatabase == "" {
// 		panic("Connection params losse")
// 	}

// 	dbConfig := cconf.NewConfigParamsFromTuples(
// 		"connection.database", sqliteDatabase,
// 	)

// 	persistence = NewDummyRefSqlitePersistence()
// 	persistence.Configure(dbConfig)

// 	fixture = *tf.NewDummyRefPersistenceFixture(persistence)

// 	opnErr := persistence.Open("")
// 	if opnErr != nil {
// 		t.Error("Error opened persistence", opnErr)
// 		return
// 	}
// 	defer persistence.Close("")

// 	opnErr = persistence.Clear("")
// 	if opnErr != nil {
// 		t.Error("Error cleaned persistence", opnErr)
// 		return
// 	}

// 	t.Run("DummyRefSqlitePersistence:CRUD", fixture.TestCrudOperations)

// 	opnErr = persistence.Clear("")
// 	if opnErr != nil {
// 		t.Error("Error cleaned persistence", opnErr)
// 		return
// 	}

// 	t.Run("DummyRefSqlitePersistence:Batch", fixture.TestBatchOperations)

// }
