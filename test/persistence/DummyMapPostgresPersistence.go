package test

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	ppersist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
)

type DummyMapSqlitePersistence struct {
	ppersist.IdentifiableSqlitePersistence
}

func NewDummyMapSqlitePersistence() *DummyMapSqlitePersistence {
	var t map[string]interface{}
	proto := reflect.TypeOf(t)
	c := &DummyMapSqlitePersistence{*ppersist.NewIdentifiableSqlitePersistence(proto, "dummies")}
	c.AutoCreateObject("CREATE TABLE dummies (\"id\" TEXT PRIMARY KEY, \"key\" TEXT, \"content\" TEXT)")
	c.EnsureIndex("dummies_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
	return c
}

func (c *DummyMapSqlitePersistence) Create(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableSqlitePersistence.Create(correlationId, item)
	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapSqlitePersistence) GetListByIds(correlationId string, ids []string) (items []map[string]interface{}, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableSqlitePersistence.GetListByIds(correlationId, convIds)
	items = make([]map[string]interface{}, len(result))
	for i, v := range result {
		val, _ := v.(map[string]interface{})
		items[i] = val
	}
	return items, err
}

func (c *DummyMapSqlitePersistence) GetOneById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableSqlitePersistence.GetOneById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapSqlitePersistence) Update(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableSqlitePersistence.Update(correlationId, item)

	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapSqlitePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableSqlitePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapSqlitePersistence) DeleteById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableSqlitePersistence.DeleteById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapSqlitePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableSqlitePersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyMapSqlitePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *tf.MapPage, err error) {

	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	filterObj := ""
	if key != nil && *key != "" {
		filterObj += "key='" + *key + "'"
	}
	sorting := ""

	tempPage, err := c.IdentifiableSqlitePersistence.GetPageByFilter(correlationId, filterObj, paging,
		sorting, nil)
	dataLen := int64(len(tempPage.Data))
	data := make([]map[string]interface{}, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(map[string]interface{})
	}
	dataPage := tf.NewMapPage(&dataLen, data)
	return dataPage, err
}

func (c *DummyMapSqlitePersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {

	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	filterObj := ""
	if key != nil && *key != "" {
		filterObj += "key='" + *key + "'"
	}
	return c.IdentifiableSqlitePersistence.GetCountByFilter(correlationId, filterObj)
}
