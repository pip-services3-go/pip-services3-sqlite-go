package test_connect

import (
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	pcon "github.com/pip-services3-go/pip-services3-sqlite-go/connect"
	"github.com/stretchr/testify/assert"
)

func TestSqliteConnectionResolverConnectionConfigWithParams(t *testing.T) {

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.database", "../../data/test.db",
	)

	resolver := pcon.NewSqliteConnectionResolver()
	resolver.Configure(dbConfig)

	config, err := resolver.Resolve("")
	assert.Nil(t, err)

	assert.NotNil(t, config)
	assert.Equal(t, "../../data/test.db", config["database"])

}

func TestSqliteConnectionResolverConnectionConfigWithURI(t *testing.T) {

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.uri", "file://../../data/test.db?_mutex=full",
	)

	resolver := pcon.NewSqliteConnectionResolver()
	resolver.Configure(dbConfig)

	config, err := resolver.Resolve("")
	assert.Nil(t, err)

	assert.NotNil(t, config)
	assert.Equal(t, "../../data/test.db?_mutex=full", config["database"])

}
