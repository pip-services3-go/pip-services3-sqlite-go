package test

import (
	"testing"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	"github.com/stretchr/testify/assert"
)

type DummyPersistenceFixture struct {
	dummy1      Dummy
	dummy2      Dummy
	persistence IDummyPersistence
}

func NewDummyPersistenceFixture(persistence IDummyPersistence) *DummyPersistenceFixture {
	c := DummyPersistenceFixture{}
	c.dummy1 = Dummy{Id: "", Key: "Key 11", Content: "Content 1"}
	c.dummy2 = Dummy{Id: "", Key: "Key 2", Content: "Content 2"}
	c.persistence = persistence
	return &c
}

func (c *DummyPersistenceFixture) TestCrudOperations(t *testing.T) {
	var dummy1 Dummy
	var dummy2 Dummy

	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	// Create another dummy by send pointer
	result, err = c.persistence.Create("", c.dummy2)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy2 = result
	assert.NotNil(t, dummy2)
	assert.NotNil(t, dummy2.Id)
	assert.Equal(t, c.dummy2.Key, dummy2.Key)
	assert.Equal(t, c.dummy2.Content, dummy2.Content)

	page, errp := c.persistence.GetPageByFilter("", cdata.NewEmptyFilterParams(), cdata.NewPagingParams(0, 5, true))
	if errp != nil {
		t.Errorf("GetPageByFilter method error %v", err)
	}
	assert.NotNil(t, page)
	assert.Len(t, page.Data, 2)
	assert.NotNil(t, page.Total)
	assert.Equal(t, *page.Total, (int64)(2))

	item1 := page.Data[0]
	assert.Equal(t, item1.Key, dummy1.Key)
	item2 := page.Data[1]
	assert.Equal(t, item2.Key, dummy2.Key)

	page, err = c.persistence.GetPageByFilter("",
		cdata.NewFilterParamsFromTuples("Key", "Key 11"),
		cdata.NewPagingParams(0, 5, true),
	)
	assert.Nil(t, err)

	assert.Len(t, page.Data, 1)
	assert.Equal(t, *page.Total, int64(1))

	assert.Equal(t, page.Data[0].Key, dummy1.Key)

	// Update the dummy
	dummy1.Content = "Updated Content 1"
	result, err = c.persistence.Update("", dummy1)
	if err != nil {
		t.Errorf("Update method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, dummy1.Content, result.Content)

	// Set the dummy (updating)
	dummy1.Content = "Updated Content 2"
	result, err = c.persistence.Set("", dummy1)
	if err != nil {
		t.Errorf("Set method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, dummy1.Content, result.Content)

	// Set the dummy (creating)
	dummy2.Id = "New_id"
	dummy2.Key = "New_key"
	result, err = c.persistence.Set("", dummy2)
	if err != nil {
		t.Errorf("Set method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy2.Id, result.Id)
	assert.Equal(t, dummy2.Key, result.Key)
	assert.Equal(t, dummy2.Content, result.Content)

	// Partially update the dummy
	updateMap := cdata.NewAnyValueMapFromTuples("content", "Partially Updated Content 1")
	result, err = c.persistence.UpdatePartially("", dummy1.Id, updateMap)
	if err != nil {
		t.Errorf("UpdatePartially method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Get the dummy by Id
	result, err = c.persistence.GetOneById("", dummy1.Id)
	if err != nil {
		t.Errorf("GetOneById method error %v", err)
	}
	// Try to get item
	assert.NotNil(t, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Delete the dummy
	result, err = c.persistence.DeleteById("", dummy1.Id)
	if err != nil {
		t.Errorf("DeleteById method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Get the deleted dummy
	result, err = c.persistence.GetOneById("", dummy1.Id)
	if err != nil {
		t.Errorf("GetOneById method error %v", err)
	}
	// Try to get item, must be an empty Dummy struct
	temp := Dummy{}
	assert.Equal(t, temp, result)
}

func (c *DummyPersistenceFixture) TestBatchOperations(t *testing.T) {
	var dummy1 Dummy
	var dummy2 Dummy

	// Create one dummy
	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	// Create another dummy
	result, err = c.persistence.Create("", c.dummy2)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy2 = result
	assert.NotNil(t, dummy2)
	assert.NotNil(t, dummy2.Id)
	assert.Equal(t, c.dummy2.Key, dummy2.Key)
	assert.Equal(t, c.dummy2.Content, dummy2.Content)

	// Read batch
	items, err := c.persistence.GetListByIds("", []string{dummy1.Id, dummy2.Id})
	if err != nil {
		t.Errorf("GetListByIds method error %v", err)
	}
	//assert.isArray(t,items)
	assert.NotNil(t, items)
	assert.Len(t, items, 2)

	// Delete batch
	err = c.persistence.DeleteByIds("", []string{dummy1.Id, dummy2.Id})
	if err != nil {
		t.Errorf("DeleteByIds method error %v", err)
	}
	assert.Nil(t, err)

	// Read empty batch
	items, err = c.persistence.GetListByIds("", []string{dummy1.Id, dummy2.Id})
	if err != nil {
		t.Errorf("GetListByIds method error %v", err)
	}
	assert.NotNil(t, items)
	assert.Len(t, items, 0)

}
