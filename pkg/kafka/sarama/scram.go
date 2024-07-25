package sarama

import (
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

var sHA512 scram.HashGeneratorFcn = sha512.New

type xDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xDGSCRAMClient) Begin(userName string, password string, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
