package boom

import (
	"bytes"
	"encoding/gob"
)

func GobEncode(e interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(e)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GobDecode(b []byte, e interface{}) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(e)
	if err != nil {
		return err
	}
	return nil
}
