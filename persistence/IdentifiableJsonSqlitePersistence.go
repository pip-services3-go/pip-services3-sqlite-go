package persistence

import (
	"database/sql"
	"encoding/json"
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
)

///*
// Abstract persistence component that stores data in PostgreSQL in JSON or JSONB fields
// and implements a number of CRUD operations over data items with unique ids.
// The data items must implement IIdentifiable interface.
//
// The JSON table has only two fields: id and data.
//
// In basic scenarios child classes shall only override [[getPageByFilter]],
// [[getListByFilter]] or [[deleteByFilter]] operations with specific filter function.
// All other operations can be used out of the box.
//
// In complex scenarios child classes can implement additional operations by
// accessing c._collection and c._model properties.

// ### Configuration parameters ###
//
// - collection:                  (optional) PostgreSQL collection name
// - connection(s):
//   - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
//   - host:                      host name or IP address
//   - port:                      port number (default: 27017)
//   - uri:                       resource URI or connection string with all parameters in it
// - credential(s):
//   - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
//   - username:                  (optional) user name
//   - password:                  (optional) user password
// - options:
//   - connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
//   - idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
//   - max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
//
// ### References ###
//
// - \*:logger:\*:\*:1.0           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages components to pass log messages
// - \*:discovery:\*:\*:1.0        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
// - \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
//
// ### Example ###
//
//     type MySqlitePersistence struct {
//     IdentifiableSqliteJsonPersistence
//}
//      func NewMySqlitePersistence() * MySqlitePersistence {
//		 return &MySqlitePersistence{
// 			IdentifiableSqliteJsonPersistence: NewIdentifiableSqliteJsonPersistence("mydata")
// 		}
//
//      func (c*MySqlitePersistence)composeFilter(filter *cdata.FilterParams) interface{} {
//         if filter == nil {
// 			filter = NewFilterParams();
// 			}
//         let criteria = [];
//         let name = filter.getAsNullableString('name');
//         if (name != null)
//             criteria.push({ name: name });
//         return criteria.length > 0 ? { $and: criteria } : null;
//     }
//
//      GetPageByFilter(correlationId: string, filter: FilterParams, paging: PagingParams,
//         callback: (err: any, page: DataPage<MyData>) => void): void {
//         base.getPageByFilter(correlationId, c.composeFilter(filter), paging, null, null, callback);
//     }
//
//     }
//
//     let persistence = new MySqlitePersistence();
//     persistence.configure(ConfigParams.fromTuples(
//         "host", "localhost",
//         "port", 27017
//     ));
//
//     persitence.open("123", (err) => {
//         ...
//     });
//
//     persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
//         persistence.getPageByFilter(
//             "123",
//             FilterParams.fromTuples("name", "ABC"),
//             null,
//             (err, page) => {
//                 console.log(page.data);          // Result: { id: "1", name: "ABC" }
//
//                 persistence.deleteById("123", "1", (err, item) => {
//                    ...
//                 });
//             }
//         )
//     });
//  */
type IdentifiableJsonSqlitePersistence struct {
	IdentifiableSqlitePersistence
}

// Creates a new instance of the persistence component.
// - collection    (optional) a collection name.
func NewIdentifiableJsonSqlitePersistence(proto reflect.Type, tableName string) *IdentifiableJsonSqlitePersistence {
	c := &IdentifiableJsonSqlitePersistence{
		IdentifiableSqlitePersistence: *NewIdentifiableSqlitePersistence(proto, tableName),
	}
	c.ConvertFromPublic = c.PerformConvertFromPublic
	c.ConvertToPublic = c.PerformConvertToPublic
	c.ConvertFromPublicPartial = c.PerformConvertFromPublic
	return c
}

// Adds DML statement to automatically create JSON(B) table
// - idType type of the id column (default: TEXT)
// - dataType type of the data column (default: JSONB)
func (c *IdentifiableJsonSqlitePersistence) EnsureTable(idType string, dataType string) {
	if idType == "" {
		idType = "VARCHAR(32)"
	}
	if dataType == "" {
		dataType = "JSON"
	}

	query := "CREATE TABLE IF NOT EXISTS " + c.QuoteIdentifier(c.TableName) +
		" (id " + idType + " PRIMARY KEY, data " + dataType + ")"

	c.AutoCreateObject(query)
}

// Converts object value from internal to public format.
// - value     an object in internal format to convert.
// Returns converted object in public format.
func (c *IdentifiableJsonSqlitePersistence) PerformConvertToPublic(rows *sql.Rows) interface{} {

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

	data, ok := buf["data"]
	if !ok {
		data = buf
	}

	docPointer := c.NewObjectByPrototype()
	jsonBuf, ok := data.(string)
	if ok {
		json.Unmarshal(([]byte)(jsonBuf), docPointer.Interface())
		return c.DereferenceObject(docPointer)
	}
	return nil

}

//  Convert object value from public to internal format.
//  - value     an object in public format to convert.
//  Returns converted object in internal format.
func (c *IdentifiableJsonSqlitePersistence) PerformConvertFromPublic(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	id := cmpersist.GetObjectId(value)

	json, _ := json.Marshal(value)

	result := map[string]interface{}{
		"id":   id,
		"data": (string)(json),
	}
	return result
}

//  Updates only few selected fields in a data item.
//  - correlation_id    (optional) transaction id to trace execution through call chain.
//  - id                an id of data item to be updated.
//  - data              a map with fields to be updated.
//  Returns          callback function that receives updated item or error.
func (c *IdentifiableJsonSqlitePersistence) UpdatePartially(correlationId string, id interface{}, data *cdata.AnyValueMap) (result interface{}, err error) {

	if data == nil {
		return nil, nil
	}

	query := "UPDATE " + c.QuoteIdentifier(c.TableName) + " SET data=JSON_PATCH(data,?) WHERE id=?"
	jsonBuf, err := json.Marshal(data.Value())
	if err != nil {
		return nil, err
	}
	values := []interface{}{(string)(jsonBuf), id}

	_, qErr := c.Client.Exec(query, values...)

	if qErr != nil {
		return nil, qErr
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
