package boom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"

	bolt "go.etcd.io/bbolt"
)

type DataDescriptor interface {
	Name() []byte
	Key() []byte
	//SetId([]byte)
}

type BaseDataObj struct {
	Id uint64
}

func (bdo BaseDataObj) Key() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bdo.Id)
	return b
}

// func (bdo *BaseDataObj) SetId(id []byte) {
// 	bdo.id = binary.BigEndian.Uint64(id)
// }

func (bdo BaseDataObj) Name() []byte {
	return nil
}

type void struct{}

type keySet map[string]void

func (ks keySet) Add(key []byte) {
	ks[string(key)] = void{}
}

func (ks keySet) Del(key []byte) {
	delete(ks, string(key))
}

func (ks keySet) Keys() [][]byte {
	keys := make([][]byte, 0, len(ks))
	for k := range ks {
		keys = append(keys, []byte(k))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	return keys
}

type IndexDescription struct {
	bucketName     string
	destBucketName string
}

type Dao[T DataDescriptor] struct {
	BucketName []byte
	indexes    []string
}

func (dao *Dao[T]) analyzeValueType() {
	value := (*T)(nil)
	typeT := reflect.TypeOf(value).Elem()
	if typeT.Kind() == reflect.Ptr {
		typeT = typeT.Elem()
	}
	if len(dao.BucketName) <= 0 {
		dao.BucketName = []byte(typeT.Name())
	}

	// We only consider structs
	if typeT.Kind() != reflect.Struct {
		return
	}

	fields := reflect.VisibleFields(typeT)
	for _, field := range fields {
		if !field.IsExported() {
			continue
		}
		if boom, ok := field.Tag.Lookup("boom"); ok {
			switch boom {
			case "index":
				dao.indexes = append(dao.indexes, field.Name)
			}
		}
	}
}

func (dao *Dao[T]) getFieldAsBytes(obj *T, fieldName string) ([]byte, error) {
	objT := reflect.ValueOf(obj).Elem()
	if objT.Kind() != reflect.Struct {
		return nil, fmt.Errorf("value not a struct: %v", obj)
	}
	field := objT.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}
	value := field.Interface()
	return GobEncode(value)
}

func NewDao[T DataDescriptor]() *Dao[T] {
	dao := &Dao[T]{}
	dao.analyzeValueType()
	return dao
}

func (dao *Dao[T]) Encode(value *T) ([]byte, error) {
	return GobEncode(value)
}

func (dao *Dao[T]) Decode(data []byte) (*T, error) {
	obj := new(T)
	err := GobDecode(data, obj)
	return obj, err
}

func (dao *Dao[T]) CreateBucketIfNotExists(tx *bolt.Tx) (*bolt.Bucket, error) {
	b, err := tx.CreateBucketIfNotExists(dao.BucketName)
	if err != nil {
		return nil, err
	}
	for _, id := range dao.indexes {
		_, err := b.CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return nil, err
		}

	}
	return b, nil
}

func (dao *Dao[T]) Bucket(tx *bolt.Tx) *bolt.Bucket {
	return tx.Bucket(dao.BucketName)
}

func (dao *Dao[T]) Put(tx *bolt.Tx, obj *T) error {
	b := dao.Bucket(tx)
	d, err := dao.Encode(obj)
	if err != nil {
		return err
	}
	// Update indexes
	for _, idx := range dao.indexes {
		ib := b.Bucket([]byte(idx))
		if ib == nil {
			return fmt.Errorf("%s index bucket not found: %s", dao.BucketName, idx)
		}
		idxValue, err := dao.getFieldAsBytes(obj, idx)
		if err != nil {
			return err
		}
		keys := make(keySet, 0)
		encodedKeys := ib.Get(idxValue)
		if encodedKeys != nil {
			err = GobDecode(encodedKeys, &keys)
			if err != nil {
				return err
			}
		}
		keys.Add((*obj).Key())
		encodedKeys, err = GobEncode(keys)
		if err != nil {
			return err
		}
		err = ib.Put(idxValue, encodedKeys)
		if err != nil {
			return err
		}
	}
	return b.Put((*obj).Key(), d)
}

func (dao *Dao[T]) Get(tx *bolt.Tx, key []byte) (*T, error) {
	b := dao.Bucket(tx)
	data := b.Get(key)
	obj, err := dao.Decode(data)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (dao *Dao[T]) GetBy(tx *bolt.Tx, idxName string, idxValue interface{}) ([]*T, error) {
	b := dao.Bucket(tx)
	if b == nil {
		return nil, fmt.Errorf("cannot find bucket: %s", dao.BucketName)
	}
	ib := b.Bucket([]byte(idxName))
	if ib == nil {
		return nil, fmt.Errorf("cannot find index bucket: %s", idxName)
	}
	idxBytes, err := GobEncode(idxValue)
	if err != nil {
		return nil, err
	}
	values := make([]*T, 0)
	ks := make(keySet, 0)
	encodedKeys := ib.Get(idxBytes)
	err = GobDecode(encodedKeys, &ks)
	if err != nil {
		return nil, err
	}
	for k := range ks {
		data := b.Get([]byte(k))
		if data == nil {
			continue
		}
		obj, err := dao.Decode(data)
		if err != nil {
			return nil, err
		}
		values = append(values, obj)
	}
	return values, nil
}

func (dao *Dao[T]) First(tx *bolt.Tx) (*T, error) {
	b := dao.Bucket(tx)
	c := b.Cursor()
	_, v := c.First()
	if v == nil {
		return nil, nil
	}
	obj, err := dao.Decode(v)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (dao *Dao[T]) Next(tx *bolt.Tx) (*T, error) {
	b := dao.Bucket(tx)
	c := b.Cursor()
	_, v := c.Next()
	if v == nil {
		return nil, nil
	}
	obj, err := dao.Decode(v)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (dao *Dao[T]) Prev(tx *bolt.Tx) (*T, error) {
	b := dao.Bucket(tx)
	c := b.Cursor()
	_, v := c.Prev()
	if v == nil {
		return nil, nil
	}
	obj, err := dao.Decode(v)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (dao *Dao[T]) Last(tx *bolt.Tx) (*T, error) {
	b := dao.Bucket(tx)
	c := b.Cursor()
	_, v := c.Last()
	if v == nil {
		return nil, nil
	}
	obj, err := dao.Decode(v)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (dao *Dao[T]) Delete(tx *bolt.Tx, key []byte) error {
	b := dao.Bucket(tx)
	if b == nil {
		return fmt.Errorf("bucket not found: %s", dao.BucketName)
	}
	// Update indexes
	data := b.Get(key)
	obj, err := dao.Decode(data)
	if err != nil {
		return err
	}
	for _, idx := range dao.indexes {
		ib := b.Bucket([]byte(idx))
		if ib == nil {
			return fmt.Errorf("%s index bucket not found: %s", dao.BucketName, idx)
		}
		idxValue, err := dao.getFieldAsBytes(obj, idx)
		if err != nil {
			return err
		}
		keys := make(keySet, 0)
		encodedKeys := ib.Get(idxValue)
		if encodedKeys != nil {
			err = GobDecode(encodedKeys, &keys)
			if err != nil {
				return err
			}
		}
		keys.Del(key)
		encodedKeys, err = GobEncode(keys)
		if err != nil {
			return err
		}
		err = ib.Put(idxValue, encodedKeys)
		if err != nil {
			return err
		}
	}
	return b.Delete(key)
}
