package persistence

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cconv "github.com/pip-services3-go/pip-services3-commons-go/convert"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
	conn "github.com/pip-services3-go/pip-services3-sqlite-go/connect"
)

type ISqlitePersistenceOverrides interface {
	DefineSchema()
	ConvertFromPublic(item interface{}) interface{}
	ConvertToPublic(item *sql.Rows) interface{}
	ConvertFromPublicPartial(item interface{}) interface{}
}

/*
Abstract persistence component that stores data in PostgreSQL using plain driver.

This is the most basic persistence component that is only
able to store data items of any type. Specific CRUD operations
over the data items must be implemented in child classes by
accessing c._db or c._collection properties.

### Configuration parameters ###

- collection:                  (optional) PostgreSQL collection name
- connection(s):
  - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
  - host:                      host name or IP address
  - port:                      port number (default: 27017)
  - uri:                       resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
  - username:                  (optional) user name
  - password:                  (optional) user password

### References ###

- \*:logger:\*:\*:1.0           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages
- \*:discovery:\*:\*:1.0        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials

### Example ###

    class MySqlitePersistence extends SqlitePersistence<MyData> {

      func (c * SqlitePersistence) constructor() {
          base("mydata");
      }

      func (c * SqlitePersistence) getByName(correlationId: string, name: string, callback: (err, item) => void) {
        let criteria = { name: name };
        c._model.findOne(criteria, callback);
      });

      func (c * SqlitePersistence) set(correlatonId: string, item: MyData, callback: (err) => void) {
        let criteria = { name: item.name };
        let options = { upsert: true, new: true };
        c._model.findOneAndUpdate(criteria, item, options, callback);
      }

    }

    let persistence = new MySqlitePersistence();
    persistence.configure(ConfigParams.fromTuples(
        "host", "localhost",
        "port", 27017
    ));

    persitence.open("123", (err) => {
         ...
    });

    persistence.set("123", { name: "ABC" }, (err) => {
        persistence.getByName("123", "ABC", (err, item) => {
            console.log(item);                   // Result: { name: "ABC" }
        });
    });
*/

type SqlitePersistence struct {
	Overrides ISqlitePersistenceOverrides
	Prototype reflect.Type

	defaultConfig    *cconf.ConfigParams
	config           *cconf.ConfigParams
	references       cref.IReferences
	opened           bool
	localConnection  bool
	schemaStatements []string

	//The dependency resolver.
	DependencyResolver *cref.DependencyResolver
	//The logger.
	Logger *clog.CompositeLogger
	//The PostgreSQL connection component.
	Connection *conn.SqliteConnection
	//The PostgreSQL connection pool object.
	Client *sql.DB
	//The PostgreSQL database name.
	DatabaseName string
	//The PostgreSQL table object.
	TableName   string
	MaxPageSize int
}

// Creates a new instance of the persistence component.
// - overrides a references to child class that overrides virtual methods
// - tableName    (optional) a table name.
func InheritSqlitePersistence(overrides ISqlitePersistenceOverrides, proto reflect.Type, tableName string) *SqlitePersistence {
	c := &SqlitePersistence{
		Overrides: overrides,
		Prototype: proto,
		defaultConfig: cconf.NewConfigParamsFromTuples(
			"collection", nil,
			"dependencies.connection", "*:connection:sqlite:*:1.0",
		),
		schemaStatements: make([]string, 0),
		Logger:           clog.NewCompositeLogger(),
		MaxPageSize:      100,
		TableName:        tableName,
	}

	c.DependencyResolver = cref.NewDependencyResolver()
	c.DependencyResolver.Configure(c.defaultConfig)

	return c
}

// Configures component by passing configuration parameters.
// - config    configuration parameters to be set.
func (c *SqlitePersistence) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.config = config

	c.DependencyResolver.Configure(config)

	c.TableName = config.GetAsStringWithDefault("collection", c.TableName)
	c.TableName = config.GetAsStringWithDefault("table", c.TableName)
	c.MaxPageSize = config.GetAsIntegerWithDefault("options.max_page_size", c.MaxPageSize)
}

// Sets references to dependent components.
// - references 	references to locate the component dependencies.
func (c *SqlitePersistence) SetReferences(references cref.IReferences) {
	c.references = references
	c.Logger.SetReferences(references)

	// Get connection
	c.DependencyResolver.SetReferences(references)
	result := c.DependencyResolver.GetOneOptional("connection")
	if dep, ok := result.(*conn.SqliteConnection); ok {
		c.Connection = dep
	}
	// Or create a local one
	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	} else {
		c.localConnection = false
	}
}

// Unsets (clears) previously set references to dependent components.
func (c *SqlitePersistence) UnsetReferences() {
	c.Connection = nil
}

func (c *SqlitePersistence) createConnection() *conn.SqliteConnection {
	connection := conn.NewSqliteConnection()
	if c.config != nil {
		connection.Configure(c.config)
	}
	if c.references != nil {
		connection.SetReferences(c.references)
	}
	return connection
}

// Adds index definition to create it on opening
// - keys index keys (fields)
// - options index options
func (c *SqlitePersistence) EnsureIndex(name string, keys map[string]string, options map[string]string) {
	builder := "CREATE"
	if options == nil {
		options = make(map[string]string, 0)
	}

	if options["unique"] != "" {
		builder += " UNIQUE"
	}

	builder += " INDEX IF NOT EXISTS " + name + " ON " + c.QuoteIdentifier(c.TableName)

	if options["type"] != "" {
		builder += " " + options["type"]
	}

	fields := ""
	for key, _ := range keys {
		if fields != "" {
			fields += ", "
		}
		//fields += c.QuoteIdentifier(key)
		fields += key
		asc := keys[key]
		if asc != "1" {
			fields += " DESC"
		}
	}

	builder += "(" + fields + ")"

	c.EnsureSchema(builder)
}

// Defines database schema for the persistence
func (c *SqlitePersistence) DefineSchema() {
	// Override in child classes
}

// Adds a statement to schema definition
//   - schemaStatement a statement to be added to the schema
func (c *SqlitePersistence) EnsureSchema(schemaStatement string) {
	c.schemaStatements = append(c.schemaStatements, schemaStatement)
}

// Clears all auto-created objects
func (c *SqlitePersistence) ClearSchema() {
	c.schemaStatements = []string{}
}

// Converts object value from internal to func (c * SqlitePersistence) format.
// - value     an object in internal format to convert.
// Returns converted object in func (c * SqlitePersistence) format.
func (c *SqlitePersistence) ConvertToPublic(rows *sql.Rows) interface{} {

	columns, err := rows.Columns()
	if err != nil || columns == nil || len(columns) == 0 {
		return nil
	}
	values := make([]interface{}, len(columns))
	pointers := make([]interface{}, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	err = rows.Scan(pointers...)
	if err != nil {
		return nil
	}

	buf := make(map[string]interface{}, 0)

	for index, column := range columns {
		buf[column] = values[index]
	}
	docPointer := c.NewObjectByPrototype()
	jsonBuf, _ := json.Marshal(buf)
	json.Unmarshal(jsonBuf, docPointer.Interface())
	return c.DereferenceObject(docPointer)
}

// Convert object value from func (c * SqlitePersistence) to internal format.
// - value     an object in func (c * SqlitePersistence) format to convert.
// Returns converted object in internal format.
func (c *SqlitePersistence) ConvertFromPublic(value interface{}) interface{} {
	return value
}

// Converts the given object from the public partial format.
// - value     the object to convert from the public partial format.
// Returns the initial object.
func (c *SqlitePersistence) ConvertFromPublicPartial(value interface{}) interface{} {
	return c.Overrides.ConvertFromPublic(value)
}

func (c *SqlitePersistence) QuoteIdentifier(value string) string {
	if value == "" {
		return value
	}
	if value[0] == '\'' {
		return value
	}
	return "\"" + value + "\""
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *SqlitePersistence) IsOpen() bool {
	return c.opened
}

// Opens the component.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - Returns 			 error or nil no errors occured.
func (c *SqlitePersistence) Open(correlationId string) (err error) {
	if c.opened {
		return nil
	}

	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	}

	if c.localConnection {
		err = c.Connection.Open(correlationId)
	}

	if err == nil && c.Connection == nil {
		err = cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "PostgreSQL connection is missing")
	}

	if err == nil && !c.Connection.IsOpen() {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "PostgreSQL connection is not opened")
	}

	c.opened = false

	if err != nil {
		return err
	}
	c.Client = c.Connection.GetConnection()
	c.DatabaseName = c.Connection.GetDatabaseName()

	// Define database schema
	c.Overrides.DefineSchema()

	// Recreate objects
	err = c.CreateSchema(correlationId)
	if err != nil {
		c.Client = nil
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to sqlite failed").WithCause(err)
	} else {
		c.opened = true
		c.Logger.Debug(correlationId, "Connected to sqlite database %s, collection %s", c.DatabaseName, c.QuoteIdentifier(c.TableName))
	}

	return err

}

// Closes component and frees used resources.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - Returns 			error or nil no errors occured.
func (c *SqlitePersistence) Close(correlationId string) (err error) {
	if !c.opened {
		return nil
	}

	if c.Connection == nil {
		return cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "Sqlite connection is missing")
	}

	if c.localConnection {
		err = c.Connection.Close(correlationId)
	}
	if err != nil {
		return err
	}
	c.opened = false
	c.Client = nil
	return nil
}

// Clears component state.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - Returns 			error or nil no errors occured.
func (c *SqlitePersistence) Clear(correlationId string) error {
	// Return error if collection is not set
	if c.TableName == "" {
		return errors.New("Table name is not defined")
	}

	query := "DELETE FROM " + c.QuoteIdentifier(c.TableName)

	_, err := c.Client.Exec(query)
	if err != nil {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to sqlite failed").
			WithCause(err)
	}

	return err
}

func (c *SqlitePersistence) CreateSchema(correlationId string) (err error) {
	if c.schemaStatements == nil || len(c.schemaStatements) == 0 {
		return nil
	}

	// Check if table exist to determine weither to auto create objects
	query := "SELECT * FROM '" + c.TableName + "' LIMIT 1"
	_, qErr := c.Client.Exec(query)
	if qErr == nil || !strings.Contains(qErr.Error(), "no such table") {
		return nil
	}

	c.Logger.Debug(correlationId, "Table "+c.TableName+" does not exist. Creating database objects...")

	for _, dml := range c.schemaStatements {
		_, err := c.Client.Exec(dml)
		if err != nil {
			c.Logger.Error(correlationId, err, "Failed to autocreate database object")
			return err
		}
	}

	return nil
}

// Generates a list of column names to use in SQL statements like: "column1,column2,column3"
// - values an array with column values or a key-value map
// Returns a generated list of column names
func (c *SqlitePersistence) GenerateColumns(values interface{}) string {

	items := c.convertToMap(values)
	if items == nil {
		return ""
	}
	result := strings.Builder{}
	for item := range items {
		if result.String() != "" {
			result.WriteString(",")
		}
		result.WriteString(c.QuoteIdentifier(item))
	}
	return result.String()

}

// Generates a list of value parameters to use in SQL statements like: "$1,$2,$3"
// - values an array with values or a key-value map
// Returns a generated list of value parameters
func (c *SqlitePersistence) GenerateParameters(values interface{}) string {

	result := strings.Builder{}
	// String arrays
	if val, ok := values.([]interface{}); ok {
		for index := 1; index <= len(val); index++ {
			if result.String() != "" {
				result.WriteString(",")
			}
			result.WriteString("$")
			result.WriteString(strconv.FormatInt((int64)(index), 16))
		}

		return result.String()
	}

	items := c.convertToMap(values)
	if items == nil {
		return ""
	}

	for index := 1; index <= len(items); index++ {
		if result.String() != "" {
			result.WriteString(",")
		}
		result.WriteString("$")
		result.WriteString(strconv.FormatInt((int64)(index), 16))
	}

	return result.String()
}

// Generates a list of column sets to use in UPDATE statements like: column1=$1,column2=$2
// - values a key-value map with columns and values
// Returns a generated list of column sets
func (c *SqlitePersistence) GenerateSetParameters(values interface{}) (params string, columns string) {

	items := c.convertToMap(values)
	if items == nil {
		return "", ""
	}
	setParamsBuf := strings.Builder{}
	columnBuf := strings.Builder{}
	index := 1
	for column := range items {
		if setParamsBuf.String() != "" {
			setParamsBuf.WriteString(",")
			columnBuf.WriteString(",")
		}
		setParamsBuf.WriteString(c.QuoteIdentifier(column) + "=$" + strconv.FormatInt((int64)(index), 16))
		columnBuf.WriteString(c.QuoteIdentifier(column))
		index++
	}
	return setParamsBuf.String(), columnBuf.String()
}

// Generates a list of column parameters
// - values a key-value map with columns and values
// Returns a generated list of column values
func (c *SqlitePersistence) GenerateValues(columns string, values interface{}) []interface{} {
	results := make([]interface{}, 0, 1)

	items := c.convertToMap(values)
	if items == nil {
		return nil
	}

	if columns == "" {
		panic("GenerateValues: Columns must be set for properly convert")
	}

	columnNames := strings.Split(strings.ReplaceAll(columns, "\"", ""), ",")
	for _, item := range columnNames {
		results = append(results, items[item])
	}
	return results
}

func (c *SqlitePersistence) convertToMap(values interface{}) map[string]interface{} {
	mRes, mErr := json.Marshal(values)
	if mErr != nil {
		c.Logger.Error("SqlitePersistence", mErr, "Error data convertion")
		return nil
	}
	items := make(map[string]interface{}, 0)
	mErr = json.Unmarshal(mRes, &items)
	if mErr != nil {
		c.Logger.Error("SqlitePersistence", mErr, "Error data convertion")
		return nil
	}
	return items
}

// Gets a page of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c * SqlitePersistence) getPageByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object
// - paging            (optional) paging parameters
// - sort              (optional) sorting JSON object
// - select            (optional) projection JSON object
// - Returns           receives a data page or error.
func (c *SqlitePersistence) GetPageByFilter(correlationId string, filter interface{}, paging *cdata.PagingParams,
	sort interface{}, sel interface{}) (page *cdata.DataPage, err error) {

	query := "SELECT * FROM " + c.QuoteIdentifier(c.TableName)
	if sel != nil {
		if slct, ok := sel.(string); ok && slct != "" {
			query = "SELECT " + slct + " FROM " + c.QuoteIdentifier(c.TableName)
		}
	}

	// Adjust max item count based on configurationpaging
	if paging == nil {
		paging = cdata.NewEmptyPagingParams()
	}
	skip := paging.GetSkip(-1)
	take := paging.GetTake((int64)(c.MaxPageSize))
	pagingEnabled := paging.Total

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	if sort != nil {
		if srt, ok := sort.(string); ok && srt != "" {
			query += " ORDER BY " + srt
		}
	}

	query += " LIMIT " + strconv.FormatInt(take, 10)

	if skip >= 0 {
		query += " OFFSET " + strconv.FormatInt(skip, 10)
	}

	qResult, qErr := c.Client.Query(query)

	if qErr != nil {
		return nil, qErr
	}

	defer qResult.Close()

	items := make([]interface{}, 0, 0)
	for qResult.Next() {
		item := c.Overrides.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}

	if pagingEnabled {
		query := "SELECT COUNT(*) AS count FROM " + c.QuoteIdentifier(c.TableName)
		if filter != nil {
			if flt, ok := sel.(string); ok && flt != "" {
				query += " WHERE " + flt
			}
		}

		qResult2, qErr2 := c.Client.Query(query)
		if qErr2 != nil {
			return nil, qErr2
		}
		defer qResult2.Close()
		var count int64 = 0
		if qResult2.Next() {
			var cnt interface{}
			err := qResult2.Scan(&cnt)
			if err != nil {
				cnt = 0
			}
			count = cconv.LongConverter.ToLong(cnt)
		}
		page = cdata.NewDataPage(&count, items)
		return page, qResult2.Err()
	}
	var total int64 = 0
	page = cdata.NewDataPage(&total, items)
	return page, qResult.Err()
}

// Gets a number of data items retrieved by a given filter.
// This method shall be called by a func (c * SqlitePersistence) getCountByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object
// - Returns           data page or error.
func (c *SqlitePersistence) GetCountByFilter(correlationId string, filter interface{}) (count int64, err error) {

	query := "SELECT COUNT(*) AS count FROM " + c.QuoteIdentifier(c.TableName)

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	qResult, qErr := c.Client.Query(query)
	if qErr != nil {
		return 0, qErr
	}
	defer qResult.Close()
	count = 0
	if qResult != nil && qResult.Next() {
		var cnt interface{}
		err := qResult.Scan(&cnt)
		if err != nil {
			cnt = 0
		}
		count = cconv.LongConverter.ToLong(cnt)
	}
	if count != 0 {
		c.Logger.Trace(correlationId, "Counted %d items in %s", count, c.TableName)
	}

	return count, qResult.Err()
}

// Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c * SqlitePersistence) getListByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId    (optional) transaction id to trace execution through call chain.
// - filter           (optional) a filter JSON object
// - paging           (optional) paging parameters
// - sort             (optional) sorting JSON object
// - select           (optional) projection JSON object
// - Returns          data list or error.
func (c *SqlitePersistence) GetListByFilter(correlationId string, filter interface{}, sort interface{}, sel interface{}) (items []interface{}, err error) {

	query := "SELECT * FROM " + c.QuoteIdentifier(c.TableName)
	if sel != nil {
		if slct, ok := sel.(string); ok && slct != "" {
			query = "SELECT " + slct + " FROM " + c.QuoteIdentifier(c.TableName)
		}
	}

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	if sort != nil {
		if srt, ok := sort.(string); ok && srt != "" {
			query += " ORDER BY " + srt
		}
	}

	qResult, qErr := c.Client.Query(query)

	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	items = make([]interface{}, 0, 1)
	for qResult.Next() {
		item := c.Overrides.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}
	return items, qResult.Err()
}

// Gets a random item from items that match to a given filter.
// This method shall be called by a func (c * SqlitePersistence) getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object
// - Returns            random item or error.
func (c *SqlitePersistence) GetOneRandom(correlationId string, filter interface{}) (item interface{}, err error) {

	query := "SELECT COUNT(*) AS count FROM " + c.QuoteIdentifier(c.TableName)

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	qResult, qErr := c.Client.Query(query)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()

	query = "SELECT * FROM " + c.QuoteIdentifier(c.TableName)
	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	var count int64 = 0
	if !qResult.Next() {
		return nil, qResult.Err()
	}

	var cnt interface{}
	err = qResult.Scan(&cnt)
	if err != nil {
		cnt = 0
	}
	count = cconv.LongConverter.ToLong(cnt)

	rand.Seed(time.Now().UnixNano())
	pos := rand.Int63n(int64(count))
	query += " OFFSET " + strconv.FormatInt(pos, 10) + " LIMIT 1"
	qResult2, qErr2 := c.Client.Query(query)
	if qErr2 != nil {
		return nil, qErr
	}
	defer qResult2.Close()
	if !qResult2.Next() {
		c.Logger.Trace(correlationId, "Random item wasn't found from %s", c.TableName)
		return nil, qResult2.Err()
	}

	item = c.Overrides.ConvertToPublic(qResult2)
	c.Logger.Trace(correlationId, "Retrieved random item from %s", c.TableName)
	return item, nil

}

// Creates a data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - item              an item to be created.
// - Returns          (optional) callback function that receives created item or error.
func (c *SqlitePersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {

	if item == nil {
		return nil, nil
	}

	row := c.Overrides.ConvertFromPublic(item)
	columns := c.GenerateColumns(row)
	params := c.GenerateParameters(row)
	values := c.GenerateValues(columns, row)
	query := "INSERT INTO " + c.QuoteIdentifier(c.TableName) + " (" + columns + ") VALUES (" + params + ")"
	qResult, qErr := c.Client.Query(query, values...)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	qResult.Next()
	if qResult.Err() != nil {
		return nil, qResult.Err()
	}
	newitem := cmpersist.CloneObjectForResult(item, c.Prototype)
	id := cmpersist.GetObjectId(newitem)
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.TableName, id)
	return newitem, nil

}

// Deletes data items that match to a given filter.
// This method shall be called by a func (c * SqlitePersistence) deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object.
// - Returns           error or nil for success.
func (c *SqlitePersistence) DeleteByFilter(correlationId string, filter string) (err error) {
	query := "DELETE FROM " + c.QuoteIdentifier(c.TableName)
	if filter != "" {
		query += " WHERE " + filter
	}

	qResult, qErr := c.Client.Exec(query)

	if qErr != nil {
		return qErr
	}

	count, err := qResult.RowsAffected()
	if err != nil {
		return err
	}

	c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.TableName)
	return nil
}

// service function for return pointer on new prototype object for unmarshaling
func (c *SqlitePersistence) NewObjectByPrototype() reflect.Value {
	proto := c.Prototype
	if proto.Kind() == reflect.Ptr {
		proto = proto.Elem()
	}
	return reflect.New(proto)
}

func (c *SqlitePersistence) DereferenceObject(docPointer reflect.Value) interface{} {
	item := docPointer.Elem().Interface()
	if c.Prototype.Kind() == reflect.Ptr {
		return docPointer.Interface()
	}
	return item
}
