package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	AmqReceive struct {
		Broker struct {
			Address string
			Port    uint16
		}
		Credentials struct {
			Username string
			Password string
		}
	} `yaml:"amq_receive"`

	AmqForward struct {
		Broker struct {
			Address string
			Port    uint16
		}
		Credentials struct {
			Username string
			Password string
		}
	} `yaml:"amq_forward"`
	PubSub []map[string]struct{Receive struct {
		Exchange     string
		ExchangeType string `yaml:"exchange_type"`
		Queue        string
	}
	Forward struct {
		Exchange     string
		ExchangeType string `yaml:"exchange_type"`
		Queue        string
	}} `yaml:"pub_sub"`
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
