package server

import (
    "github.com/golang/etcd-task-manager/pkg/config"
    "github.com/golang/etcd-task-manager/pkg/modules/cos"
    "github.com/golang/etcd-task-manager/service/messageHandler"
)

func InitServer() {
    if err := config.Init(); err != nil {
        panic(err)
    }
    if err := messageHandler.Init(); err != nil {
        panic(err)
    }
    if err := cos.Init(); err != nil {
        panic(err)
    }
}
