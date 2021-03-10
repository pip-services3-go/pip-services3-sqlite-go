package test

import (
	"testing"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	"github.com/stretchr/testify/assert"
)

type DummyMapPersistenceFixture struct {
	dummy1      map[string]interface{}
	dummy2      map[string]interface{}
	persistence IDummyMapPersistence
}

func NewDummyMapPersistenceFixture(persistence IDummyMapPersistence) *DummyMapPersistenceFixture {
	c := DummyMapPersistenceFixture{}
	c.dummy1 = map[string]interface{}{"id": "", "key": "Key 11", "content": "Content 1"}
	c.dummy2 = map[string]interface{}{"id": "", "key": "Key 2", "content": "Content 2"}
	c.persistence = persistence
	return &c
}

func (c *DummyMapPersistenceFixture) TestCrudOperations(t *testing.T) {
	var dummy1 map[string]interface{}
	var dummy2 map[string]interface{}

	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1["id"])
	assert.Equal(t, c.dummy1["key"], dummy1["key"])
	assert.Equal(t, c.dummy1["content"], dummy1["content"])

	// Create another dummy by set pointer
	result, err = c.persistence.Create("", c.dummy2)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy2 = result
	assert.NotNil(t, dummy2)
	assert.NotNil(t, dummy2["id"])
	assert.Equal(t, c.dummy2["key"], dummy2["key"])
	assert.Equal(t, c.dummy2["content"], dummy2["content"])

	page, errp := c.persistence.GetPageByFilter("", cdata.NewEmptyFilterParams(), cdata.NewEmptyPagingParams())
	if errp != nil {
		t.Errorf("GetPageByFilter method error %v", err)
	}
	assert.NotNil(t, page)
	assert.Len(t, page.Data, 2)
	//Testing default sorting by Key field len

	item1 := page.Data[0]
	assert.Equal(t, item1["key"], dummy1["key"])
	item2 := page.Data[1]
	assert.Equal(t, item2["key"], dummy2["key"])

	// Update the dummy
	dummy1["content"] = "Updated Content 1"
	result, err = c.persistence.Update("", dummy1)
	if err != nil {
		t.Errorf("Update method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1["id"], result["id"])
	assert.Equal(t, dummy1["key"], result["key"])
	assert.Equal(t, dummy1["content"], result["content"])

	// Set the dummy (updating)
	dummy1["content"] = "Updated Content 2"
	result, err = c.persistence.Set("", dummy1)
	if err != nil {
		t.Errorf("Set method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1["id"], result["id"])
	assert.Equal(t, dummy1["key"], result["key"])
	assert.Equal(t, dummy1["content"], result["content"])

	// Set the dummy (creating)
	dummy2["id"] = "New_id"
	dummy2["key"] = "New_key"
	result, err = c.persistence.Set("", dummy2)
	if err != nil {
		t.Errorf("Set method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy2["id"], result["id"])
	assert.Equal(t, dummy2["key"], result["key"])
	assert.Equal(t, dummy2["content"], result["content"])

	// Partially update the dummy
	updateMap := cdata.NewAnyValueMapFromTuples("content", "Partially Updated Content 1")
	result, err = c.persistence.UpdatePartially("", dummy1["id"].(string), updateMap)
	if err != nil {
		t.Errorf("UpdatePartially method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1["id"], result["id"])
	assert.Equal(t, dummy1["key"], result["key"])
	assert.Equal(t, "Partially Updated Content 1", result["content"])

	// Get the dummy by Id
	result, err = c.persistence.GetOneById("", dummy1["id"].(string))
	if err != nil {
		t.Errorf("GetOneById method error %v", err)
	}
	// Try to get item
	assert.NotNil(t, result)
	assert.Equal(t, dummy1["id"], result["id"])
	assert.Equal(t, dummy1["key"], result["key"])
	assert.Equal(t, "Partially Updated Content 1", result["content"])

	// Delete the dummy
	result, err = c.persistence.DeleteById("", dummy1["id"].(string))
	if err != nil {
		t.Errorf("DeleteById method error %v", err)
	}
	assert.NotNil(t, result)
	assert.Equal(t, dummy1["id"], result["id"])
	assert.Equal(t, dummy1["key"], result["key"])
	assert.Equal(t, "Partially Updated Content 1", result["content"])

	// Get the deleted dummy
	result, err = c.persistence.GetOneById("", dummy1["id"].(string))
	if err != nil {
		t.Errorf("GetOneById method error %v", err)
	}
	// Try to get item
	assert.Nil(t, result)
}

func (c *DummyMapPersistenceFixture) TestBatchOperations(t *testing.T) {
	var dummy1 map[string]interface{}
	var dummy2 map[string]interface{}

	// Create one dummy
	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1["id"])
	assert.Equal(t, c.dummy1["key"], dummy1["key"])
	assert.Equal(t, c.dummy1["content"], dummy1["content"])

	// Create another dummy
	result, err = c.persistence.Create("", c.dummy2)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}
	dummy2 = result
	assert.NotNil(t, dummy2)
	assert.NotNil(t, dummy2["id"])
	assert.Equal(t, c.dummy2["key"], dummy2["key"])
	assert.Equal(t, c.dummy2["content"], dummy2["content"])

	// Read batch
	items, err := c.persistence.GetListByIds("", []string{dummy1["id"].(string), dummy2["id"].(string)})
	if err != nil {
		t.Errorf("GetListByIds method error %v", err)
	}
	//assert.isArray(t,items)
	assert.NotNil(t, items)
	assert.Len(t, items, 2)

	// Delete batch
	err = c.persistence.DeleteByIds("", []string{dummy1["id"].(string), dummy2["id"].(string)})
	if err != nil {
		t.Errorf("DeleteByIds method error %v", err)
	}
	assert.Nil(t, err)

	// Read empty batch
	items, err = c.persistence.GetListByIds("", []string{dummy1["id"].(string), dummy2["id"].(string)})
	if err != nil {
		t.Errorf("GetListByIds method error %v", err)
	}
	assert.NotNil(t, items)
	assert.Len(t, items, 0)

}
