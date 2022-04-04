package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type BrokerConfig struct {
	Address string
	Port    uint16
}

type CredentialsConfig struct {
	Username string
	Password string
}

type AmqConnectionConfig struct {
	Broker      BrokerConfig
	Credentials CredentialsConfig
}

type AmqChannelConfig struct {
	Exchange     string
	ExchangeType string `yaml:"exchange_type"`
	Queue        string
}

type PubSubConfig struct {
	Source AmqChannelConfig
	Target AmqChannelConfig
}

type RedisConfig struct {
	Address string
	Port    uint16
	Credentials CredentialsConfig
}
type Config struct {
	Redis RedisConfig
	AmqSource AmqConnectionConfig       `yaml:"amq_source"`
	AmqTarget AmqConnectionConfig       `yaml:"amq_target"`
	PubSub    []map[string]PubSubConfig `yaml:"pub_sub"`
}

func Parse(file string) (*Config, error) {
	var c Config
	yamlFile, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}
