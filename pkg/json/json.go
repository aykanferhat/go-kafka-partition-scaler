package json

import (
	"github.com/json-iterator/go"
)

var customJSON = NewJSON()

func Marshal(obj interface{}) ([]byte, error) {
	return customJSON.Marshal(obj)
}

func Unmarshal(data []byte, obj interface{}) error {
	return customJSON.Unmarshal(data, obj)
}

type JSON struct {
	iter jsoniter.API
}

func NewJSON() *JSON {
	return &JSON{
		iter: jsoniter.ConfigDefault,
	}
}

func (j *JSON) Unmarshal(data []byte, obj interface{}) error {
	return j.iter.Unmarshal(data, obj)
}

func (j *JSON) Marshal(obj interface{}) ([]byte, error) {
	return j.iter.Marshal(obj)
}
