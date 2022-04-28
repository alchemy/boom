package boom

import (
	"testing"
	"time"
)

type nestedObjType struct {
	NestedField string
}

type EmbeddedStruct struct {
	EmbeddedField string
}

type objType struct {
	IntField       int
	FloatField     float64
	TimeField      time.Time
	StrField       string
	BoolField      bool
	NestedObjField nestedObjType
	EmbeddedStruct
}

func TestEncodeDecode(t *testing.T) {
	now := time.Now().Round(0)
	obj := objType{
		IntField:       123,
		FloatField:     123.456,
		TimeField:      now,
		StrField:       "object",
		BoolField:      true,
		NestedObjField: nestedObjType{NestedField: "nested object"},
		EmbeddedStruct: EmbeddedStruct{EmbeddedField: "embedded object"},
	}
	encoded, err := GobEncode(obj)
	if err != nil {
		t.Fatal(err)
	}
	var decoded objType
	err = GobDecode(encoded, &decoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != obj {
		t.Fatalf("original and decoded objects differs:\n%+v\n%+v", obj, decoded)
	}
}
