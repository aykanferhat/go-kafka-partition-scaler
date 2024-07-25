package csmap

import "testing"

func TestSyncMapWrapper(t *testing.T) {
	p := Create[string, string](0)
	p.Store("key", "value")

	if value, ok := p.Load("key"); ok {
		if value != "value" {
			t.Errorf("value must be 'value'")
		}
	} else {
		t.Errorf("key not exist")
	}
}
