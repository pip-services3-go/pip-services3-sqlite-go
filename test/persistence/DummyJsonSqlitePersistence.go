package test

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	persist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
)

type DummyJsonSqlitePersistence struct {
	persist.IdentifiableJsonSqlitePersistence
}

func NewDummyJsonSqlitePersistence() *DummyJsonSqlitePersistence {
	proto := reflect.TypeOf(tf.Dummy{})
	c := &DummyJsonSqlitePersistence{}
	c.IdentifiableJsonSqlitePersistence = *persist.InheritIdentifiableJsonSqlitePersistence(c, proto, "dummies_json")

	return c
}

func (c *DummyJsonSqlitePersistence) DefineSchema() {
	c.ClearSchema()
	c.EnsureTable("", "")
	c.EnsureIndex(c.TableName+"_json_key", map[string]string{"(data->'key')": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyJsonSqlitePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *tf.DummyPage, err error) {
	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	filterObj := ""
	if key != nil && *key != "" {
		filterObj += "data->key='" + *key + "'"
	}

	tempPage, err := c.IdentifiableSqlitePersistence.GetPageByFilter(correlationId,
		filterObj, paging,
		nil, nil)
	// Convert to DummyPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]tf.Dummy, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(tf.Dummy)
	}
	page = tf.NewDummyPage(&dataLen, data)
	return page, err
}

func (c *DummyJsonSqlitePersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {
	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	filterObj := ""

	if key != nil && *key != "" {
		filterObj += "data->key='" + *key + "'"
	}

	return c.IdentifiableSqlitePersistence.GetCountByFilter(correlationId, filterObj)
}

func (c *DummyJsonSqlitePersistence) Create(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyJsonSqlitePersistence) GetListByIds(correlationId string, ids []string) (items []tf.Dummy, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableSqlitePersistence.GetListByIds(correlationId, convIds)
	items = make([]tf.Dummy, len(result))
	for i, v := range result {
		val, _ := v.(tf.Dummy)
		items[i] = val
	}
	return items, err
}

func (c *DummyJsonSqlitePersistence) GetOneById(correlationId string, id string) (item tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyJsonSqlitePersistence) Update(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyJsonSqlitePersistence) Set(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Set(correlationId, item)
	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyJsonSqlitePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item tf.Dummy, err error) {
	// In json persistence this method must call from IdentifiableJsonSqlitePersistence
	result, err := c.IdentifiableJsonSqlitePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyJsonSqlitePersistence) DeleteById(correlationId string, id string) (item tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyJsonSqlitePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableSqlitePersistence.DeleteByIds(correlationId, convIds)
}
