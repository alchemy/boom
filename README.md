boom - BbOlt Object Mapper
==========================

Experimental package to make it easier to persist go structs in a [bbolt](https://github.com/etcd-io/bbolt) key/value store using generics.

Example:
```go
import (
    "log"

    bolt "go.etcd.io/bbolt"
    "github.com/alchemy/boom"
)

const dbName "bbolt.db"

func main() {
    db, err := bolt.Open(dbName, 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    customer := &Customer{BaseDataObj: BaseDataObj{Id: 1}, CompanyName: "ACME Labs"}
    dao := boom.NewDao[Customer]()
    err = db.Update(func(tx *bolt.Tx) error {
        dao.CreateBucketIfNotExists(tx)
        return dao.Put(tx, customer)
    })
    if err != nil {
        log.Fatal(err)
    }
    var value *Customer
    err = db.View(func(tx *bolt.Tx) error {
        value, err = dao.Get(tx, customer.Key())
        return err
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Println(value)
}
```
