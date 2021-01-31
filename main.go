package main

import (
	"amqp_tools/config"
	"amqp_tools/net"
	"flag"
	"fmt"
	"github.com/astaxie/beego/logs"
	"strings"
	"time"
)

func main() {
	action := flag.String("action", "", `执行操作:
check      		检查配置文件
run      		启动syncEngine
`)
	flag.Parse()
	actionType := strings.ToLower(*action)

	if len(actionType) == 0 {
		flag.PrintDefaults()
		return
	}

	if err := logs.SetLogger("console"); err != nil {
		fmt.Println(fmt.Sprintf("syncEngine日志配置异常，err：%s", err.Error()))
		return
	}

	if err := logs.SetLogger(logs.AdapterFile,
		`{"filename":"/var/log/syncEngine.log","maxlines":0,"maxsize":0,"daily":true,"maxdays":10,"color":true}`,
	); err != nil {
		fmt.Println(fmt.Sprintf("syncEngine日志配置异常，err：%s", err.Error()))
		return
	}

	switch actionType {
	case "check":
		config.CheckConfig()
		return
	case "run":
		break
	default:
		flag.PrintDefaults()
		return
	}

	if ok := config.LoadConfig(); !ok {
		return
	}
	logs.Info(fmt.Sprintf("MQ配置文件加载完毕Host %s:%d Username[%s] Vhost[%s]", config.GlobalConfig.Host,
		config.GlobalConfig.Port,
		config.GlobalConfig.Username,
		config.GlobalConfig.Vhost))

	net.MQInit()
	time.Sleep(time.Second * 5)
	net.SendMessage([]byte("hello world from amqp_tools"))

	forever := make(chan bool)
	logs.Info(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever

}
