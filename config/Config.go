package config

import (
	"encoding/json"
	"fmt"
	"github.com/astaxie/beego/logs"
	"gopkg.in/ini.v1"
)

const FilePath = "/etc/amqp_tools.ini"

type RabbitMQConfig struct {
	Username string
	Password string
	Host     string
	Port     uint
	Vhost    string
}

type RabbitMQAdvancedConfig struct {
	Exchange     string
	ExchangeType string
	Queue        string
	RoutingKey   string
	Reliable     bool
	Tag          string
}

type Config struct {
	RabbitMQConfig
	RabbitMQAdvancedConfig
}

var GlobalConfig = new(Config)

func (c *Config) InitRabbitMQConfig(file *ini.File) bool {
	var err error
	ConfigMQ := file.Section("MQ")
	c.Username = ConfigMQ.Key("Username").String()
	c.Password = ConfigMQ.Key("Password").String()
	c.Host = ConfigMQ.Key("Host").String()
	c.Port, err = ConfigMQ.Key("Port").Uint()
	c.Vhost = ConfigMQ.Key("VHost").String()
	if len(c.Username) == 0 || len(c.Password) == 0 || len(c.Host) == 0 || err != nil || len(c.Vhost) == 0 {
		logs.Error("Failed to get MQ configuration file")
		return false
	}
	return true
}

// MQ详细配置 如有需要请手动修改
func (c *Config) InitRabbitMQAdvancedConfig() bool {
	c.Exchange = "HAHAHA-EXCHANGE"
	c.ExchangeType = "topic"
	c.RoutingKey = "HAHA"
	c.Reliable = false
	c.Queue = "HAHAHA-QUEUE"
	c.Tag = ""
	return true
}

func LoadConfig() bool {
	if file, err := ini.Load(FilePath); err == nil {
		if GlobalConfig.InitRabbitMQConfig(file) && GlobalConfig.InitRabbitMQAdvancedConfig() {
			return true
		}
		logs.Error("Load advanced configuration encounter an error")
		return false

	}
	logs.Error(fmt.Sprintf("Failed to get configuration file，File Path is %s", FilePath))
	return false
}

func CheckConfig() {
	success := LoadConfig()
	if success {
		bytes, err := json.MarshalIndent(GlobalConfig, "", "  ")
		if err != nil {
			logs.Error(err)
		} else {
			logs.Info("Check MQ configuration file successfully\n", string(bytes))
		}
	}
}
