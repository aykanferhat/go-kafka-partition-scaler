package cron

import "github.com/robfig/cron/v3"

type Cron struct {
	*cron.Cron
}

func NewCron() *Cron {
	return &Cron{Cron: cron.New()}
}

func (c *Cron) AddFunc(spec string, cmd func()) error {
	_, err := c.Cron.AddFunc(spec, cmd)
	return err
}

func (c *Cron) Start() {
	c.Cron.Start()
}
