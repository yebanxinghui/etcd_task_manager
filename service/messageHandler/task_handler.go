package messageHandler

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    pb "github.com/golang/etcd-task-manager/pb"
    "github.com/golang/etcd-task-manager/pkg/modules/cos"
    "github.com/golang/etcd-task-manager/pkg/repo/component"
    "github.com/golang/etcd-task-manager/pkg/repo/constant"
    "github.com/golang/etcd-task-manager/pkg/repo/model"
)

func (handler *EtcdConsumeHandler) PushTask(task *pb.TaskContext) error {
    ctx, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(150))
    defer cancel()
    // 开启分布式锁，防止任务冲突
    cancelLock, err := handler.GlobalLock(constant.GlobalCurrentContextKey)
    if err != nil {
        log.Printf("get global lock err: [%v]", err)
        return err
    }
    defer cancelLock()
    //queueName := fmt.Sprintf("taskQueue/%s/mutex", task.InnerFeedID)
    queueName := "taskQueue/" + task.InnerFeedID + "/mutex"
    res, err := handler.etcdCli.TryPopQueue(queueName)
    if err != nil {
        log.Printf("get GlobalCurrentContextKey err: [%v]", err)
        return err
    }
    if res != nil {
        return fmt.Errorf("task is already running")
    }

    // 使用 kv 存已有任务信息貌似还是会引起冲突，改用etcd队列
    err = handler.etcdCli.PushQueue(ctx, queueName, "mutex")
    if err != nil {
        log.Printf("Etcd添加任务-全局KV修改失败")
        return err
    }
    // 正常添加任务
    handler.Refresh()
    handler.damageError[task.InnerFeedID] = make(chan error, 100)
    handler.currentContext.Store(task.InnerFeedID, task)
    handler.feedIdSlice = append(handler.feedIdSlice, task.InnerFeedID)
    handler.GlobalMutex[task.InnerFeedID] = &sync.Mutex{}
    return nil
}

// 等待任务分片的函数
func (handler *EtcdConsumeHandler) pendingTask(ctx context.Context, task *pb.TaskContext) error {
    ctxInner, cancel := context.WithCancel(ctx)

    ctxErrorExit := handler.watchRemoteDamageError(ctxInner, task.InnerFeedID, func(err error) {
        handler.damageError[task.InnerFeedID] <- err
    })
    defer func() { <-ctxErrorExit }()
    ctxRemoteExit := handler.watchRemoteContext(ctxInner)
    defer func() { <-ctxRemoteExit }()
    defer cancel()

    startTime := time.Now()
    // execute slice task
    handler.divideSliceTaskUsingLocalContext(task)
    relativePts, err := handler.processSliceTask(task.InnerFeedID)

    if err != nil {
        log.Printf("[%s] divide slice err: [%v]", task.SourceKey, err)
        return err
    }
    log.Printf("[PendingTask] [%v] 分片处理完成, 总耗时: [%v]", task.SourceKey, time.Since(startTime))

    // 处理算法模块
    if algorithmErr := handler.ProcessAlgorithm(ctxInner, task); algorithmErr != nil {
        return algorithmErr
    }

    // 写结构化数据
    writter := &component.AlgorithmStructuredDataWritter{
        MediaInfo: &model.MediaInfo{
            Type: "tencentVideo",
            Vid:  task.SourceKey,
        },
        Durations:       handler.normalGroup[task.InnerFeedID],
        CosPrefix:       cos.CosPrefix(task),
        FeedID:          task.FeedID,
        Cid:             task.TencentVideoCid,
        VideoFormat:     task.TencentVideoFormat,
        AlgorithmResult: nil,
        RelativeSlice:   relativePts,
    }

    if err = writter.WriteRemote(ctxInner); err != nil {
        log.Printf("write structured data to remote err: [%v]", err)
        return err
    }
    log.Printf("处理视频 [%s] 全流程成功，全流程耗时 %v, 视频总时长: [%v], 占比总时长 [%v]",
        task.SourceKey, time.Since(startTime), task.VideoDurationMs,
        float32(time.Since(startTime).Milliseconds())/float32(task.VideoDurationMs))
    return nil
}

func (handler *EtcdConsumeHandler) PendingTask(ctx context.Context, task *pb.TaskContext) error {
    // 前置处理
    handler.etcdCli.DestroyQueue(ctx, "taskQueue/"+task.InnerFeedID+"/human_seg")

    err := handler.pendingTask(ctx, task)

    // 后置处理
    //handler.etcdCli.DestroyQueue(ctx, fmt.Sprintf("taskQueue/%s/mutex", task.InnerFeedID))
    handler.etcdCli.DestroyQueue(context.TODO(), "taskQueue/"+task.InnerFeedID+"/mutex")
    delete(handler.normalGroup, task.InnerFeedID)
    delete(handler.GlobalMutex, task.InnerFeedID)
    handler.currentContext.Delete(task.InnerFeedID)
    return err
}

func (handler *EtcdConsumeHandler) ProcessAlgorithm(ctx context.Context, task *pb.TaskContext) error {
    defer handler.finalizeResource(task.InnerFeedID)
    // execute human_seg task
    HumanSegStartTime := time.Now()
    errDetect := handler.divideAlgorithmTaskUsingLocalContext(ctx, task.InnerFeedID)
    if errDetect != nil {
        log.Printf("切分弹幕分割任务时错误: %v", errDetect)
        return errDetect
    }
    errDetect = handler.processAlgorithmTask(task.InnerFeedID)
    if errDetect != nil {
        log.Printf("处理弹幕分割任务时错误: %v", errDetect)
        return errDetect
    }
    log.Printf("video [%+v] 弹幕分割处理完成, 总耗时: [%v]", task.SourceKey, time.Since(HumanSegStartTime))
    return nil
}
