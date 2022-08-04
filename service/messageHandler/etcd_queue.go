package messageHandler

import (
    "context"
    "io"
    "log"
    "strconv"

    "github.com/golang/etcd-task-manager/etcd_util"
    "github.com/golang/etcd-task-manager/pkg/modules/cos"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
    "github.com/golang/etcd-task-manager/pkg/repo/constant"
)

func (handler *EtcdConsumeHandler) PushEtcdQueue(ctx context.Context, queueName string, detail isTask, cosPrefix string) error {
    //path := fmt.Sprintf("%s/%s/%d.json", cosPrefix, queueName, detail.GetSlice().GetBeginPtsMs())
    path := cosPrefix + "/" + queueName + "/" + strconv.FormatInt(int64(detail.GetSlice().GetBeginPtsMs()), 10) + ".json"
    var err error
    // 存储任务到 cos
    for i := 0; i < 3; i++ {
        err = cos.PutStringToCos(ctx, path, common.MustDumpsPBToString(detail))
        if err == nil {
            break
        }
    }
    if err != nil {
        log.Printf("[pushEtcdQueue] 推送Etcd队列: 将任务信息存储到cos发生错误 %v", err)
        return err
    }

    // 把任务推送到 etcd 队列
    for i := 0; i < 3; i++ {
        err = handler.etcdCli.PushQueue(ctx, queueName, path)
        if err == nil {
            break
        }
    }
    if err != nil {
        log.Printf("[pushEtcdQueue] 推送Etcd队列: 将任务链接存储到推送到etcd发生错误 %v", err)
        return err
    }
    log.Printf("推送成功，分片起始时间:[%v], key: [%v]", detail.GetSlice().BeginPtsMs, queueName)
    return nil
}

func (handler *EtcdConsumeHandler) rePushEtcdQueue(ctx context.Context, key string) {
    feedId, taskType, beginMs := common.SplitKey(key)
    taskCtx := handler.GetTaskContext(feedId)
    if feedId == "" || taskType == "" || taskCtx == nil {
        return
    }
    var duration *TimeSlice
    for _, timeSlice := range handler.normalGroup[feedId] {
        if timeSlice.BeginPtsMs == beginMs {
            duration = timeSlice
        }
    }
    if duration == nil {
        return
    }
    switch taskType {
    case constant.AtomAlgorithm:
        //queueKey := fmt.Sprintf("taskQueue/%s/algorithm", feedId)
        queueKey := "taskQueue/" + feedId + "/algorithm"
        task := &TAlgorithmTask {
            Slice:      duration,
            Context:    taskCtx,
            VideoClips: nil,
            RetryTime:  0,
        }
        handler.PushEtcdQueue(ctx, queueKey, task, cos.CosPrefix(taskCtx))
        return
    }
}

func (handler *EtcdConsumeHandler) tryPopEtcdQueue(ctx context.Context, queueName string, innerFeedId string, detail isTask) (*etcd_util.QueueItem, error) {
    item, err := handler.etcdCli.TryPopQueue(queueName)
    if err != nil && handler.etcdCli.GetQueue(ctx, queueName){
        //handler.pushDamageErrorRemote(ctx, innerFeedId, fmt.Sprintf("[tryPopEtcdQueue] 从队列 [%s] 中取任务遇到错误", queueName), err)
        handler.pushDamageErrorRemote(ctx, innerFeedId, "[tryPopEtcdQueue] 从队列 ["+ queueName +"] 中取任务遇到错误", err)
        return nil, err
    }

    if item == nil {
        return nil, nil
    }

    bs, err := cos.GetBytesFromCos(ctx, item.Value)
    if err != nil {
        //handler.pushDamageErrorRemote(ctx, innerFeedId, fmt.Sprintf("[tryPopEtcdQueue] 拉取队列元素 [%s] 遇到错误", item.Value), err)
        handler.pushDamageErrorRemote(ctx, innerFeedId, "[tryPopEtcdQueue] 拉取队列元素 ["+ item.Value +"] 遇到错误", err)
        return item, err
    }
    err = common.LoadsJsonToPb(bs, detail)
    if err != nil {
        if err == io.EOF {
            handler.rePushEtcdQueue(ctx, item.Value)
            return nil, nil
        }
        //handler.pushDamageErrorRemote(ctx, innerFeedId, fmt.Sprintf("[tryPopEtcdQueue] 解析队列元素 [%s] 遇到错误", item.Value), err)
        handler.pushDamageErrorRemote(ctx, innerFeedId, "[tryPopEtcdQueue] 解析队列元素 [" + item.Value +"] 遇到错误", err)
        return item, err
    }
    return item, nil
}
