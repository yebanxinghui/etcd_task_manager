package messageHandler

import (
    "context"
    "fmt"
    "log"
    "strconv"
    "time"

    "github.com/golang/etcd-task-manager/pkg/modules/cos"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
)

func (handler *EtcdConsumeHandler) divideAlgorithmTaskUsingLocalContext(ctx context.Context, innerFeedId string) error {
    taskContext := handler.GetTaskContext(innerFeedId)
    startTime := time.Now()

    log.Printf("切分算法任务开始，任务数量 [%d]", len(handler.normalGroup[innerFeedId]))
    //AlgorithmTaskQueueKey := fmt.Sprintf("taskQueue/%s/algorithm", innerFeedId)
    AlgorithmTaskQueueKey := "taskQueue/" + innerFeedId + "/algorithm"
    for _, duration := range handler.normalGroup[innerFeedId] {
        task := &TAlgorithmTask{
            Slice:      duration,
            Context:    taskContext,
            VideoClips: nil,
            RetryTime:  0,
        }
        err := handler.PushEtcdQueue(ctx, AlgorithmTaskQueueKey, task, cos.CosPrefix(taskContext))
        if err != nil {
            log.Printf("Etcd算法任务切分-任务添加失败")
            return err
        }
    }
    log.Printf("切分算法任务切分推送完成，共推送任务数量 [%d] 个，推送到远端队列耗时 [%v]", len(handler.normalGroup[innerFeedId]), time.Since(startTime))
    return nil
}

func (handler *EtcdConsumeHandler) AlgorithmDone(task *TAlgorithmTask, err error) {
    ctx, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(150))
    defer cancel()

    //queueKey := fmt.Sprintf("taskQueue/%s/algorithm", task.Context.InnerFeedID)
    queueKey := "taskQueue/" + task.Context.InnerFeedID + "/algorithm"
    if !handler.etcdCli.GetQueue(ctx, queueKey) {
        return
    }
    defer func() {
        err := handler.etcdCli.PushToDoneQueue(ctx, queueKey, task.EtcdIndex)
        if err != nil && handler.etcdCli.GetQueue(ctx, queueKey) {
            handler.pushDamageErrorRemote(ctx, task.Context.InnerFeedID, "[AlgorithmDone] 报告任务完成遇到错误", err)
        }
    }()
    if err == nil {
        return
    }

    var errInner error
    retry := task.RetryTime
    if retry < 3 {
        log.Printf("Etcd检测任务完成-有错误并重试")
        task.RetryTime++
        // clear large data
        task.VideoClips = nil

        errInner = handler.PushEtcdQueue(ctx, queueKey, task, cos.CosPrefix(task.Context))
    }
    if retry >= 3 || errInner != nil {
        if errInner == nil {
            errInner = fmt.Errorf("task.RetryTime(%d) >= StageMaxRetryTime(%d)", task.RetryTime, 3)
        }
        log.Printf("Etcd检测任务完成-有错误放弃重试, err: [%v]", errInner)
        //errorKey := fmt.Sprintf("taskError/%s/algorithm/%d", task.Context.InnerFeedID, task.GetSlice().GetBeginPtsMs())
        errorKey := "taskError/" + task.Context.InnerFeedID + "/algorithm/" + strconv.FormatInt(int64(task.GetSlice().GetBeginPtsMs()), 10)
        errorValue := common.MustEncodeToJsonString(EtcdResult{HasError: true, ErrorInfo: errInner.Error()})
        _, errPut := handler.etcdCli.PutKV(ctx, errorKey, errorValue)
        if errPut != nil {
            log.Printf("[AlgorithmDone] 推送错误遇到错误 %v", errPut)
            handler.pushDamageErrorRemote(ctx, task.Context.InnerFeedID, "[AlgorithmDone] 推送错误遇到错误", errPut)
        }
    }
}

func (handler *EtcdConsumeHandler) processAlgorithmTask(innerFeedId string) error {
    ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*time.Duration(1))
    defer cancel()

    //queueKey := fmt.Sprintf("taskQueue/%s/algorithm", innerFeedId)
    queueKey := "taskQueue/" + innerFeedId + "/algorithm"
    defer handler.etcdCli.DestroyQueue(ctx, queueKey)
    errWait := make(chan error, 2)
    go func() {
        err := handler.etcdCli.WaitQueueTaskDone(ctx, queueKey)
        errWait <- err
    }()

    var ansErr error
    select {
    case <-ctx.Done():
        ansErr = fmt.Errorf("[processAlgorithmTask] context过期")
    case err, ok := <-handler.damageError[innerFeedId]:
        if ok {
            ansErr = err
        } else {
            ansErr = fmt.Errorf("[processAlgorithmTask] damageError 被关闭")
        }
    case err := <-errWait:
        ansErr = err
    }

    if ansErr != nil {
        log.Printf("Etcd处理检测-等待失败, err: [%v]", ansErr)
        return ansErr
    }

    for _, v := range handler.normalGroup[innerFeedId] {
        //errKey := fmt.Sprintf("taskError/%s/algorithm/%d", innerFeedId, v.BeginPtsMs)
        errKey := "taskError/" + innerFeedId + "/algorithm/" + strconv.FormatInt(int64(v.BeginPtsMs), 10)
        rsp, err := handler.etcdCli.GetKV(ctx, errKey)
        if err != nil {
            log.Printf("cannot read error: key [%s] err: %v", errKey, err)
            continue
        }
        if len(rsp.Kvs) != 0 {
            log.Printf("Etcd算法-获取到错误")
            return fmt.Errorf("%s", string(rsp.Kvs[0].Value))
        }
    }

    return nil
}

// 尝试获取一个算法任务
func (handler *EtcdConsumeHandler) TryPopAlgorithmTask(ctx context.Context) (*TAlgorithmTask, bool) {
    remoteCurrentContext := handler.GetCurrentContext()
    if len(remoteCurrentContext) == 0 {
        return nil, false
    }

    for _, taskCtx := range remoteCurrentContext {
        task := &TAlgorithmTask{}
        //queueKey := fmt.Sprintf("taskQueue/%s/algorithm", taskCtx.InnerFeedID)
        queueKey := "taskQueue/" + taskCtx.InnerFeedID + "/algorithm"
        msg, err := handler.tryPopEtcdQueue(ctx, queueKey, taskCtx.InnerFeedID, task)
        if err != nil {
            log.Printf("[TryPopAlgorithmTask] err: [%v]", err)
            continue
        }
        if msg == nil {
            continue
        }
        if task.GetSlice() == nil {
            return nil, false
        }
        task.VideoClips, err = cos.GetSliceResultCos(ctx, cos.GetSliceCosPrefix(taskCtx), task.GetSlice().GetBeginPtsMs())
        if err != nil {
            log.Printf("get slice info failed: err: [%v]", err)
            handler.AlgorithmDone(task, err)
            return nil, false
        }
        task.EtcdIndex = msg.Index
        task.EtcdNamespace = handler.etcdNamespace
        return task, true
    }
    return nil, false
}
