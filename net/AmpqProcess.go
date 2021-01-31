package net

import "github.com/astaxie/beego/logs"

type MsgProcess struct {
}

func (p *MsgProcess) doProcess(body []byte) error {
	logs.Info("receive msg: ", string(body))
	return nil
}
