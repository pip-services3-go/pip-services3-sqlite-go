package persistence

import (
	"reflect"
	"strconv"

	cconv "github.com/pip-services3-go/pip-services3-commons-go/convert"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
)

/*
Abstract persistence component that stores data in PostgreSQL
and implements a number of CRUD operations over data items with unique ids.
The data items must implement IIdentifiable interface.
 *
In basic scenarios child classes shall only override [[getPageByFilter]],
[[getListByFilter]] or [[deleteByFilter]] operations with specific filter function.
All other operations can be used out of the box.
 *
In complex scenarios child classes can implement additional operations by
accessing c._collection and c._model properties.

### Configuration parameters ###
 *
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
- options:
  - connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
  - idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
  - max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
 *
### References ###
 *
- \*:logger:\*:\*:1.0           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages components to pass log messages
- \*:discovery:\*:\*:1.0        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
 *
### Example ###
 *
    class MySqlitePersistence extends IdentifiableSqlitePersistence<MyData, string> {
 *
    public constructor() {
        base("mydata", new MyDataSqliteSchema());
    }
 *
    private composeFilter(filter: FilterParams): any {
        filter = filter || new FilterParams();
        let criteria = [];
        let name = filter.getAsNullableString('name');
        if (name != null)
            criteria.push({ name: name });
        return criteria.length > 0 ? { $and: criteria } : null;
    }
 *
    public getPageByFilter(correlationId: string, filter: FilterParams, paging: PagingParams,
        callback: (err: any, page: DataPage<MyData>) => void): void {
        base.getPageByFilter(correlationId, c.composeFilter(filter), paging, null, null, callback);
    }
 *
    }
 *
    let persistence = new MySqlitePersistence();
    persistence.configure(ConfigParams.fromTuples(
        "host", "localhost",
        "port", 27017
    ));
 *
    persitence.open("123", (err) => {
        ...
    });
 *
    persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
        persistence.getPageByFilter(
            "123",
            FilterParams.fromTuples("name", "ABC"),
            null,
            (err, page) => {
                console.log(page.data);          // Result: { id: "1", name: "ABC" }
 *
                persistence.deleteById("123", "1", (err, item) => {
                   ...
                });
            }
        )
    });
*/
type IdentifiableSqlitePersistence struct {
	*SqlitePersistence
}

//    Creates a new instance of the persistence component.
//    - collection    (optional) a collection name.
func NewIdentifiableSqlitePersistence(proto reflect.Type, tableName string) *IdentifiableSqlitePersistence {
	c := &IdentifiableSqlitePersistence{
		SqlitePersistence: NewSqlitePersistence(proto, tableName),
	}

	if tableName == "" {
		panic("Table name could not be empty")
	}
	return c
}

// Gets a list of data items retrieved by given unique ids.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - ids               ids of data items to be retrieved
// Returns          a data list or error.
func (c *IdentifiableSqlitePersistence) GetListByIds(correlationId string, ids []interface{}) (items []interface{}, err error) {

	params := c.GenerateParameters(ids)
	query := "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE \"id\" IN(" + params + ")"

	qResult, qErr := c.Client.Query(query, ids...)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	items = make([]interface{}, 0, 0)
	for qResult.Next() {

		item := c.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}

	return items, qResult.Err()
}

// Gets a data item by its unique id.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be retrieved.
// Returns           data item or error.
func (c *IdentifiableSqlitePersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {

	query := "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE \"id\"=$1"

	qResult, qErr := c.Client.Query(query, id)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return nil, qResult.Err()
	}

	result := c.ConvertToPublic(qResult)
	if result == nil {
		c.Logger.Trace(correlationId, "Nothing found from %s with id = %s", c.TableName, id)
	} else {
		c.Logger.Trace(correlationId, "Retrieved from %s with id = %s", c.TableName, id)
	}
	return result, nil

}

// Creates a data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - item              an item to be created.
// Returns          (optional)  created item or error.
func (c *IdentifiableSqlitePersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	// Assign unique id
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	cmpersist.GenerateObjectId(&newItem)

	return c.SqlitePersistence.Create(correlationId, newItem)
}

// Sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - item              a item to be set.
// Returns          (optional)  updated item or error.
func (c *IdentifiableSqlitePersistence) Set(correlationId string, item interface{}) (result interface{}, err error) {

	if item == nil {
		return nil, nil
	}

	// Assign unique id
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	cmpersist.GenerateObjectId(&newItem)

	row := c.ConvertFromPublic(item)
	columns := c.GenerateColumns(row)
	params := c.GenerateParameters(row)
	setParams, col := c.GenerateSetParameters(row)
	values := c.GenerateValues(col, row)
	id := cmpersist.GetObjectId(newItem)

	query := "INSERT INTO " + c.QuoteIdentifier(c.TableName) + " (" + columns + ")" +
		" VALUES (" + params + ")" +
		" ON CONFLICT (\"id\") DO UPDATE SET " + setParams

	qResult, qErr := c.Client.Query(query, values...)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	qResult.Next()
	if qResult.Err() != nil {
		return nil, qResult.Err()
	}

	query = "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE id=$1"
	qResult2, qErr2 := c.Client.Query(query, id)
	if qErr2 != nil {
		return nil, qErr2
	}
	defer qResult2.Close()
	if !qResult2.Next() {
		return nil, qResult2.Err()
	}

	result = c.ConvertToPublic(qResult2)
	c.Logger.Trace(correlationId, "Set in %s with id = %s", c.TableName, id)
	return result, nil
}

// Updates a data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - item              an item to be updated.
// Returns          (optional)  updated item or error.
func (c *IdentifiableSqlitePersistence) Update(correlationId string, item interface{}) (result interface{}, err error) {

	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	id := cmpersist.GetObjectId(newItem)

	row := c.ConvertFromPublic(newItem)
	params, col := c.GenerateSetParameters(row)
	values := c.GenerateValues(col, row)
	values = append(values, id)

	query := "UPDATE " + c.QuoteIdentifier(c.TableName) +
		" SET " + params + " WHERE \"id\"=$" + strconv.FormatInt((int64)(len(values)), 16)

	qResult, qErr := c.Client.Query(query, values...)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	qResult.Next()
	if qResult.Err() != nil {
		return nil, qResult.Err()
	}
	query = "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE id=$1"
	qResult2, qErr2 := c.Client.Query(query, id)
	if qErr2 != nil {
		return nil, qErr2
	}
	defer qResult2.Close()
	if !qResult2.Next() {
		return nil, qResult2.Err()
	}

	result = c.ConvertToPublic(qResult2)
	c.Logger.Trace(correlationId, "Updated in %s with id = %s", c.TableName, id)
	return result, nil

}

// Updates only few selected fields in a data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be updated.
// - data              a map with fields to be updated.
// Returns           updated item or error.
func (c *IdentifiableSqlitePersistence) UpdatePartially(correlationId string, id interface{}, data *cdata.AnyValueMap) (result interface{}, err error) {

	if id == nil {
		return nil, nil
	}

	row := c.ConvertFromPublicPartial(data.Value())
	params, col := c.GenerateSetParameters(row)
	values := c.GenerateValues(col, row)
	values = append(values, id)

	query := "UPDATE " + c.QuoteIdentifier(c.TableName) +
		" SET " + params + " WHERE \"id\"=$" + strconv.FormatInt((int64)(len(values)), 16)

	qResult, qErr := c.Client.Query(query, values...)

	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	qResult.Next()
	if qResult.Err() != nil {
		return nil, qResult.Err()
	}
	query = "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE id=$1"
	qResult2, qErr2 := c.Client.Query(query, id)
	if qErr2 != nil {
		return nil, qErr2
	}
	defer qResult2.Close()
	if !qResult2.Next() {
		return nil, qResult2.Err()
	}

	result = c.ConvertToPublic(qResult2)
	c.Logger.Trace(correlationId, "Updated partially in %s with id = %s", c.TableName, id)
	return result, nil

}

// Deleted a data item by it's unique id.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of the item to be deleted
// Returns          (optional)  deleted item or error.
func (c *IdentifiableSqlitePersistence) DeleteById(correlationId string, id interface{}) (result interface{}, err error) {

	query := "SELECT * FROM " + c.QuoteIdentifier(c.TableName) + " WHERE id=$1"
	qResult, qErr := c.Client.Query(query, id)
	if qErr != nil {
		return nil, qErr
	}
	if !qResult.Next() {
		defer qResult.Close()
		return nil, qResult.Err()
	}
	result = c.ConvertToPublic(qResult)
	qResult.Close()

	query = "DELETE FROM " + c.QuoteIdentifier(c.TableName) + " WHERE \"id\"=$1"
	_, qErr2 := c.Client.Exec(query, id)
	if qErr2 != nil {
		return nil, qErr2
	}
	c.Logger.Trace(correlationId, "Deleted from %s with id = %s", c.TableName, id)
	return result, nil
}

// Deletes multiple data items by their unique ids.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - ids               ids of data items to be deleted.
// Returns          (optional)  error or null for success.
func (c *IdentifiableSqlitePersistence) DeleteByIds(correlationId string, ids []interface{}) error {

	params := c.GenerateParameters(ids)
	query := "DELETE FROM " + c.QuoteIdentifier(c.TableName) + " WHERE \"id\" IN(" + params + ")"

	qResult, qErr := c.Client.Query(query, ids...)

	if qErr != nil {
		return qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return qResult.Err()
	}
	var count int64 = 0

	var cnt interface{}
	err := qResult.Scan(&cnt)
	if err != nil {
		cnt = 0
	}
	count = cconv.LongConverter.ToLong(cnt)
	if count != 0 {
		c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.TableName)
	}
	return nil
}
