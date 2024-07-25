package uuid

import (
	"github.com/hashicorp/go-uuid"
)

func GenerateUUID() string {
	key, _ := uuid.GenerateUUID()
	return key
}
