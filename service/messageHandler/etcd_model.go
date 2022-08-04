package messageHandler

import (
    "fmt"
    "sync"

    "github.com/golang/etcd-task-manager/etcd_util"
    pb "github.com/golang/etcd-task-manager/pb"
    "github.com/golang/protobuf/proto"
)

var (
	DefaultTaskConsumeHandler *EtcdConsumeHandler = nil
)

func Init() error {
    DefaultTaskConsumeHandler = NewEtcdConsumeHandler("human_seg", "namespace_human_seg")
    return nil
}

type TimeSlice = pb.TimeSlice
type TVideoSliceTask = pb.TVideoSliceTask
type TAlgorithmTask = pb.TAlgorithmTask


type isTask interface {
    proto.Message
    GetSlice() *TimeSlice
}

type EtcdResult struct {
    HasError  bool   `json:"has_error"`
    ErrorInfo string `json:"error_info"`
}

type EtcdConsumeHandler struct {
    etcdName       string
    etcdNamespace  string
    etcdCli        *etcd_util.EtcdConnector
    currentContext sync.Map
    feedIdSlice	   []string
    normalGroup    map[string][]*TimeSlice
    damageError    map[string]chan error
    GlobalMutex    map[string]*sync.Mutex
}

func NewEtcdConsumeHandler(etcdName string, namespace string) *EtcdConsumeHandler {
    handler := &EtcdConsumeHandler{
        etcdName: etcdName,
        etcdNamespace: namespace,
    }
    var err error
    handler.etcdCli, err = etcd_util.NewEtcdConnectorByKey(etcdName, namespace)
    if err != nil {
        panic(fmt.Errorf("etcd config is not exists. [%+v]", err))
    }
    handler.currentContext = sync.Map{}
    return handler
}
