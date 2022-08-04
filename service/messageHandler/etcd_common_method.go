package messageHandler

import (
    "context"
    "fmt"
    "log"
    "sync"

    pb "github.com/golang/etcd-task-manager/pb"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
)

func (handler *EtcdConsumeHandler) Refresh() {
    if handler.damageError == nil {
        handler.damageError = make(map[string]chan error)
    }
    if handler.GlobalMutex == nil {
        handler.GlobalMutex = make(map[string]*sync.Mutex)
    }
    if handler.normalGroup == nil {
        handler.normalGroup = make(map[string][]*TimeSlice)
    }
}

// 清理队列
func (handler *EtcdConsumeHandler) finalizeResource(innerFeedId string) {
    if len(handler.feedIdSlice) > 0 {
        for i, v := range handler.feedIdSlice {
            if v == innerFeedId {
                handler.feedIdSlice = append(handler.feedIdSlice[:i], handler.feedIdSlice[i+1:]...)
            }
        }
    }
    //handler.etcdCli.DestroyQueue(ctx, fmt.Sprintf("taskQueue/%s/human_seg", innerFeedId))
    handler.etcdCli.DestroyQueue(context.TODO(), "taskQueue/" + innerFeedId + "/human_seg")
    //handler.etcdCli.DeleteKVPrefix(ctx, fmt.Sprintf("taskError/%s", innerFeedId))
    handler.etcdCli.DeleteKVPrefix(context.TODO(), "taskError/" + innerFeedId)
}

func (handler *EtcdConsumeHandler) pushDamageErrorRemote(ctx context.Context, innerFeedId string, detail string, err error) {
    log.Printf("detail:[%s], err:[%v]", detail, err)
    //errorKey := fmt.Sprintf("taskError/damageError/%s", innerFeedId)
    errorKey := "taskError/damageError/" + innerFeedId
    //errorValue := fmt.Sprintf("%s: %v", detail, err)
    errorValue := detail + ": " + err.Error()
    _, errPut := handler.etcdCli.PutKV(ctx, errorKey, errorValue)
    if errPut != nil {
        log.Printf("[%s] 存储错误 [%v] 到 etcd 发生错误", detail, err)
    }
}

func (handler *EtcdConsumeHandler) watchRemoteDamageError(ctx context.Context, innerFeedId string, errHandler func(err error)) chan struct{} {
    closeChan := make(chan struct{}, 0)
    //rch := handler.etcdCli.WatchKV(ctx, fmt.Sprintf("taskError/damageError/%s", innerFeedId))
    rch := handler.etcdCli.WatchKV(ctx, "taskError/damageError/" + innerFeedId)

    go func() {
        defer close(closeChan)

    Loop:
        for {
            select {
            case <-ctx.Done():
                log.Printf("[watchRemoteDamageError] context超时")
                break Loop
            case _, ok := <-rch:
                if !ok {
                    log.Printf("[watchRemoteDamageError] watch被关闭")
                    errHandler(fmt.Errorf("[watchRemoteDamageError] watch被关闭错误，退出"))
                    break Loop
                }
                //rsp, err := handler.etcdCli.GetKV(ctx, fmt.Sprintf("taskError/damageError/%s", innerFeedId))
                rsp, err := handler.etcdCli.GetKV(ctx, "taskError/damageError/" + innerFeedId)
                if err != nil {
                    log.Printf("[watchRemoteDamageError] GetKV遇到错误 %v", err)
                    errHandler(err)
                } else if len(rsp.Kvs) > 0 {
                    errDetail := fmt.Errorf("%s", string(rsp.Kvs[0].Value))
                    log.Printf("[watchRemoteDamageError] Receive remote error: %v", errDetail)
                    errHandler(errDetail)
                } else {
                    log.Printf("[watchRemoteDamageError] 未知错误，退出 [%+v]", rsp)
                    errHandler(fmt.Errorf("[watchRemoteDamageError] 未知错误，退出"))
                }
                break Loop
            }
        }
        log.Printf("[watchRemoteDamageError] 对当前请求的错误不再监听，监听退出")
    }()
    return closeChan
}

func (handler *EtcdConsumeHandler) watchRemoteContext(ctx context.Context) chan struct{} {
    closeChan := make(chan struct{}, 0)
    go func() {
        defer close(closeChan)
    Loop:
        for {
            select {
            case <-ctx.Done():
                log.Printf("[watchRemoteContext] context超时")
                break Loop
            }
        }
        log.Printf("[watchRemoteContext] 对当前请求的 Context 不再监听，监听退出")
    }()
    return closeChan
}


// GetTaskContext 从当前任务 sync.map 中获取任务
func (handler *EtcdConsumeHandler) GetTaskContext(innerFeedId string) *pb.TaskContext {
    v, ok1 := handler.currentContext.Load(innerFeedId)
    if ok1 {
        taskContext, ok2 := v.(*pb.TaskContext)
        if ok2 {
            return taskContext
        }
    }
    return nil
}

// GetCurrentContext 获取当前节点的任务，没有则获取全局任务
func (handler *EtcdConsumeHandler) GetCurrentContext() []*pb.TaskContext {
    var res []*pb.TaskContext
    for _, feedId := range handler.feedIdSlice {
        taskContext := handler.GetTaskContext(feedId)
        if taskContext != nil && taskContext.GetInnerFeedID() != "" {
            res = append(res, taskContext)
        }
    }
    if len(res) > 0 {
        return common.SortedContext(res)
    }
    return nil
}

func (handler *EtcdConsumeHandler) GlobalLock(key string) (func(), error) {
    return handler.etcdCli.MutexLock(key)
}
