package test

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	ppersist "github.com/pip-services3-go/pip-services3-sqlite-go/persistence"
	tf "github.com/pip-services3-go/pip-services3-sqlite-go/test/fixtures"
)

// extends IdentifiableSqlitePersistence<Dummy, string>
// implements IDummyPersistence {
type DummyRefSqlitePersistence struct {
	ppersist.IdentifiableSqlitePersistence
}

func NewDummyRefSqlitePersistence() *DummyRefSqlitePersistence {

	proto := reflect.TypeOf(&tf.Dummy{})
	return &DummyRefSqlitePersistence{*ppersist.NewIdentifiableSqlitePersistence(proto, "dummies")}
}

func (c *DummyRefSqlitePersistence) Create(correlationId string, item *tf.Dummy) (result *tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(*tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefSqlitePersistence) GetListByIds(correlationId string, ids []string) (items []*tf.Dummy, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableSqlitePersistence.GetListByIds(correlationId, convIds)
	items = make([]*tf.Dummy, len(result))
	for i, v := range result {
		val, _ := v.(*tf.Dummy)
		items[i] = val
	}
	return items, err
}

func (c *DummyRefSqlitePersistence) GetOneById(correlationId string, id string) (item *tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(*tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefSqlitePersistence) Update(correlationId string, item *tf.Dummy) (result *tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(*tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefSqlitePersistence) Set(correlationId string, item *tf.Dummy) (result *tf.Dummy, err error) {
	value, err := c.IdentifiableSqlitePersistence.Set(correlationId, item)
	if value != nil {
		val, _ := value.(*tf.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefSqlitePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item *tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(*tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefSqlitePersistence) DeleteById(correlationId string, id string) (item *tf.Dummy, err error) {
	result, err := c.IdentifiableSqlitePersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(*tf.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefSqlitePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableSqlitePersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyRefSqlitePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *tf.DummyRefPage, err error) {

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
	// Convert to DummyRefPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]*tf.Dummy, dataLen)
	for i := range tempPage.Data {
		temp := tempPage.Data[i].(*tf.Dummy)
		data[i] = temp
	}
	page = tf.NewDummyRefPage(&dataLen, data)
	return page, err
}

func (c *DummyRefSqlitePersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {

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
