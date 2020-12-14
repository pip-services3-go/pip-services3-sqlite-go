package test

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	ppersist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	"github.com/stretchr/testify/assert"
)

func TestSqliteConnection(t *testing.T) {
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
	err := connection.Open("")
	assert.Nil(t, err)
	defer connection.Close("")

	assert.NotNil(t, connection.GetConnection())
	assert.NotNil(t, connection.GetDatabaseName())
	assert.NotEqual(t, "", connection.GetDatabaseName())
}
