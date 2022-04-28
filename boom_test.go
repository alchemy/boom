package boom

import (
	"sort"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

const dbName = "boom_test.db"

type Customer struct {
	BaseDataObj
	CompanyName string `boom:"index"`
}

type Order struct {
	Id         uint64
	Date       *time.Time
	CustomerId uint64 `boom:"fk:Customer"`
}

type OrderRow struct {
	Id      uint64
	OrderId uint64 `book:"fk:Order"`
}

func TestPutGet(t *testing.T) {
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	customer := &Customer{BaseDataObj: BaseDataObj{Id: 1}, CompanyName: "ACME Labs"}
	dao := NewDao[Customer]()
	err = db.Update(func(tx *bolt.Tx) error {
		dao.CreateBucketIfNotExists(tx)
		return dao.Put(tx, customer)
	})
	if err != nil {
		t.Fatal(err)
	}
	var value *Customer
	err = db.View(func(tx *bolt.Tx) error {
		value, err = dao.Get(tx, customer.Key())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if *value != *customer {
		t.Fatalf("structs are different")
	}
}

func TestGetBy(t *testing.T) {
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dao := NewDao[Customer]()
	customer := &Customer{BaseDataObj: BaseDataObj{Id: 1}, CompanyName: "ACME Labs"}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := dao.CreateBucketIfNotExists(tx)
		if err != nil {
			return err
		}
		return dao.Put(tx, customer)
	})
	if err != nil {
		t.Fatal(err)
	}
	customer.Id = 2
	err = db.Update(func(tx *bolt.Tx) error {
		return dao.Put(tx, customer)
	})
	if err != nil {
		t.Fatal(err)
	}
	var values []*Customer
	err = db.View(func(tx *bolt.Tx) error {
		values, err = dao.GetBy(tx, "CompanyName", "ACME Labs")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("wrong number of results")
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Id < values[j].Id })
	for i, value := range values {
		customer.Id = uint64(i + 1)
		if *value != *customer {
			t.Fatalf("structs are different")
		}
	}
	customer.Id = 1
	err = db.Update(func(tx *bolt.Tx) error {
		return dao.Delete(tx, customer.Key())
	})
	if err != nil {
		t.Fatal(err)
	}
	customer.Id = 2
	err = db.View(func(tx *bolt.Tx) error {
		values, err = dao.GetBy(tx, "CompanyName", "ACME Labs")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 {
		t.Fatalf("wrong number of results")
	}
	if *values[0] != *customer {
		t.Fatalf("structs are different")
	}
}
