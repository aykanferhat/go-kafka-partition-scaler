package viper

import (
	"github.com/spf13/viper"
)

func ReadFile(conf any, filePath string) error {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return err
	}
	return nil
}

func ReadFileWithProfile(profile string, conf any, filePath string) error {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	c := viper.Sub(profile)
	if err := c.Unmarshal(conf); err != nil {
		return err
	}
	return nil
}
