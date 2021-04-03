package test

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	persist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
)

type DummySqlitePersistence struct {
	persist.IdentifiableSqlitePersistence
}

func NewDummySqlitePersistence() *DummySqlitePersistence {
	proto := reflect.TypeOf(tf.Dummy{})
	c := &DummySqlitePersistence{}
	c.IdentifiableSqlitePersistence = *persist.InheritIdentifiableSqlitePersistence(c, proto, "dummies")
	return c
}

func (c *DummySqlitePersistence) DefineSchema() {
	c.ClearSchema()
	// Row name must be in double quotes for properly case!!!
	c.EnsureSchema("CREATE TABLE \"" + c.TableName + "\" (\"id\" VARCHAR(32) PRIMARY KEY, \"key\" VARCHAR(50), \"content\" TEXT)")
	c.EnsureIndex(c.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummySqlitePersistence) Create(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummySqlitePersistence) GetListByIds(correlationId string, ids []string) (items []tf.Dummy, err error) {
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

func (c *DummySqlitePersistence) GetOneById(correlationId string, id string) (item tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummySqlitePersistence) Update(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummySqlitePersistence) Set(correlationId string, item tf.Dummy) (result tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Set(correlationId, item)
	if value != nil {
		val, _ := value.(tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummySqlitePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummySqlitePersistence) DeleteById(correlationId string, id string) (item tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummySqlitePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableSqlitePersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummySqlitePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *tf.DummyPage, err error) {

	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	filterObj := ""
	if key != nil && *key != "" {
		filterObj += "key='" + *key + "'"
	}
	sorting := ""

	tempPage, err := c.IdentifiableSqlitePersistence.GetPageByFilter(correlationId,
		filterObj, paging,
		sorting, nil)
	// Convert to DummyPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]tf.Dummy, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(tf.Dummy)
	}
	page = tf.NewDummyPage(&dataLen, data)
	return page, err
}

func (c *DummySqlitePersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {

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
