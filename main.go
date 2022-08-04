package main

import (
	_ "go.uber.org/automaxprocs"

	"github.com/golang/etcd-task-manager/server"
)

func main() {
	server.InitServer()

	// 注册服务
	// register server

	// 启动服务
	// serve
}
