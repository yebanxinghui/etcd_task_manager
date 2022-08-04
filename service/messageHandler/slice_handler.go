package messageHandler

import (
    "context"
    "fmt"
    "log"
    "strconv"
    "sync"
    "time"

    pb "github.com/golang/etcd-task-manager/pb"
    "github.com/golang/etcd-task-manager/pkg/config"
    "github.com/golang/etcd-task-manager/pkg/modules/cos"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
    "github.com/golang/etcd-task-manager/pkg/repo/model"
)

// 切分片任务，注意只能在Master节点调用
func (handler *EtcdConsumeHandler) divideSliceTaskUsingLocalContext(task *pb.TaskContext) {
    handler.normalGroup[task.InnerFeedID] = nil
    for currentPts := int32(0); currentPts < task.VideoDurationMs; currentPts = currentPts + config.GlobalConfig.Algorithm.Duration {
        handler.normalGroup[task.InnerFeedID] = append(handler.normalGroup[task.InnerFeedID], &TimeSlice{
            BeginPtsMs: currentPts,
            EndPtsMs:   currentPts + config.GlobalConfig.Algorithm.Duration,
        })
    }

    log.Printf("分组成功 sourceKey: [%v], 分组数量 [%d]", task.SourceKey, len(handler.normalGroup[task.InnerFeedID]))
}

// 组织一批分片任务的请求
func (handler *EtcdConsumeHandler) getSliceTaskBatchRequest(innerFeedId string, sliceTask chan *TVideoSliceTask) *pb.SyncProcessRequest {
    taskContext := handler.GetTaskContext(innerFeedId)
    req := &pb.SyncProcessRequest{
        Appid:        taskContext.GetAppid(),
        FeedId:       taskContext.GetFeedID(),
        CosUrl:       cos.PartOfCosURL,
        CosSecretId:  config.GlobalConfig.Cos.CosID,
        CosSecretKey: config.GlobalConfig.Cos.CosKey,
    }

    common.TaskCommonVideoInfoToSliceVideoInfo(req, taskContext)
    req.FileNamePrefix = cos.GetSliceCosPrefix(taskContext)
    req.Formats = []int32{taskContext.GetTencentVideoFormat()}
    task := <-sliceTask
    req.Durations = nil
    req.Durations = append(req.Durations, &pb.VideoSliceDurationInfo{
        Index:   task.GetIndex(),
        BeginMs: task.GetSlice().GetBeginPtsMs(),
        EndMs:   task.GetSlice().GetEndPtsMs(),
    })
    log.Printf("分片推送索引: [%v]", task.Index)
    return req
}

// 批量执行分片任务
func (handler *EtcdConsumeHandler) executeSliceTaskBatch(ctx context.Context, innerFeedId string, sliceTask chan *TVideoSliceTask) ([]int32, error) {
    sliceTime := time.Now()
    // 请求和重试
    var err error
    req := handler.getSliceTaskBatchRequest(innerFeedId, sliceTask)
    var rsp *pb.SyncProcessResponse
    var e1 error
    for retry := 0; retry < 6; retry++ {
        //TODO：调用切片服务
        //rsp, e1 = SyncProcess(ctx, req)
        // 下面是测试数据
        rsp, e1 = nil, nil
        err = func() error {
            if e1 != nil {
                log.Printf("video slice process [%+v] error [%+v]", req, e1)
                return e1
            }
            if rsp == nil || rsp.Ret != 0 {
                log.Printf("video slice process [%+v] rsp!=0 [%+v]", req, rsp)
                return fmt.Errorf("video slice process [%+v] error [%+v]", req, rsp)
            }
            if rsp.Data == nil || len(rsp.Data.Infos) == 0 || rsp.Data.Infos[0].Duration == nil {
                log.Printf("video slice rsp.Data is nil [%+v] [%+v]", req, rsp)
                return fmt.Errorf("video slice rsp.Data is nil [%+v]", rsp)
            }
            return nil
        }()
        if err == nil {
            break
        }
        common.TimeBlocker()
    }

    if err != nil {
        log.Printf("Etcd请求分片服务-请求下游服务失败")
        return nil, err
    }
    //将所有去头去尾的 RelativePts 放入返回结果中
    var duration []int32
    for _, v := range rsp.Data.Infos {
        for _, t := range v.Duration.RelativePtsMs {
            if t >= (v.Duration.Index-1)*config.GlobalConfig.Algorithm.Duration && t <= v.Duration.Index*config.GlobalConfig.Algorithm.Duration {
                duration = append(duration, t)
            }
        }
    }
    // 写入结果
    var putResultWg sync.WaitGroup
    var putResultError error = nil
    taskContext := handler.GetTaskContext(innerFeedId)
    for _, v := range rsp.Data.Infos {
        // 一般来说，调用分片服务对durations长度为1，返回对infos长度也为1
        // 如果relativePts最大值都没有开始时间(用索引算出来对整分片值)大，说明可能是把videoinfo这个服务返回的分片所有时间加起来多了一些帧，导致会多出一个分片
        putResultWg.Add(1)
        if v.Duration.EndMs < (v.Duration.Index-1)*config.GlobalConfig.Algorithm.Duration {
            handler.GlobalMutex[innerFeedId].Lock()
            length := len(handler.normalGroup[innerFeedId])
            if length > 1 && taskContext != nil {
                taskContext.VideoDurationMs = (v.Duration.Index-1)*config.GlobalConfig.Algorithm.Duration - 1

                handler.normalGroup[innerFeedId] = handler.normalGroup[innerFeedId][:length-1]
            }
            handler.GlobalMutex[innerFeedId].Unlock()
            putResultWg.Done()
            break
        }
        go func(v *pb.VideoSliceInfo) {
            defer putResultWg.Done()
            var err error = nil
            relativePts := common.RemoveDuplication(v.GetDuration().GetRelativePtsMs())
            for i := 0; i < 5; i++ {
                err = cos.PutSliceResultToCos(ctx, cos.GetSliceCosPrefix(taskContext),
                    (v.Duration.Index-1)*config.GlobalConfig.Algorithm.Duration, &pb.VideoClipInfo{
                        SourceKey:         taskContext.GetSourceKey(),
                        BeginPtsMs:        v.GetDuration().GetBeginMs(),
                        EndPtsMs:          v.GetDuration().GetEndMs(),
                        RelativePtsMs:     relativePts,
                        VideoClipURL:      v.GetUrl(),
                        Width:             v.GetFrame().GetWidth(),
                        Height:            v.GetFrame().GetHeight(),
                        ProcessBeginPtsMs: (v.GetDuration().GetIndex() - 1) * config.GlobalConfig.Algorithm.Duration,
                        ProcessEndPtsMs:   v.GetDuration().GetIndex() * config.GlobalConfig.Algorithm.Duration,
                    })
                if err == nil {
                    break
                }
            }
            if err != nil {
                putResultError = err
                log.Printf("Etcd请求分片服务-存储结果失败, slice: [%v], err: [%v]", v.GetDuration().GetBeginMs(), err)
            }
        }(v)
    }
    putResultWg.Wait()
    if putResultError != nil {
        log.Printf("Etcd请求分片服务-存储结果失败")
        return nil, putResultError
    }

    log.Printf("切分片任务时的耗时: [%v], slice: [%+v], sourceKey: [%v]",
        time.Since(sliceTime), req.Durations, taskContext.GetSourceKey())
    return duration, nil
}

func (handler *EtcdConsumeHandler) processSliceTask(innerFeedId string) (*model.DurationPts, error) {
    taskContext := handler.GetTaskContext(innerFeedId)
    ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*time.Duration(30))
    defer cancel()
    startTime := time.Now()
    var taskWg sync.WaitGroup

    taskLen := len(handler.normalGroup[innerFeedId])
    errs := make([]error, taskLen)
    durations := make([][]int32, taskLen)
    sliceTask := make(chan *TVideoSliceTask, taskLen)

    log.Printf("开始视频分片，任务数量 %d", taskLen)
    taskWg.Add(taskLen)

    // 限流 chan
    limitChan := make(chan struct{}, 10)
    // 错误记录情况
    flag := false
    for i, duration := range handler.normalGroup[innerFeedId] {
        task := &TVideoSliceTask{
            //TaskID:    fmt.Sprintf("%s_%d", innerFeedId, i),
            TaskID:    innerFeedId + "_" + strconv.Itoa(i),
            SourceKey: taskContext.GetSourceKey(),
            Context:   taskContext,
            Index:     int32(i + 1),
            Slice:     duration,
        }
        sliceTask <- task
        limitChan <- struct{}{}

        go func(i int) {
            defer func() {
                taskWg.Done()
                <-limitChan
            }()
            if flag {
                return
            }
            duration, err := handler.executeSliceTaskBatch(ctx, innerFeedId, sliceTask)
            errs[i] = err
            durations[i] = duration
            if err != nil {
                flag = true
            }
        }(i)
    }
    taskWg.Wait()
    var finalErr error
    var errNum int = 0
    for i, e := range errs {
        if e != nil {
            log.Printf("处理第 %d 个分片时发生错误，该分片的时间范围为[%d, %d)", i,
                int32(i)*config.GlobalConfig.Algorithm.Duration, int32(i+1)*config.GlobalConfig.Algorithm.Duration)
            if finalErr == nil {
                finalErr = e
                errNum++
            }
        }
    }

    log.Printf("分片任务处理完毕，共有 %d 个错误", errNum)
    if finalErr != nil {
        log.Printf("Etcd处理切片-失败调用量 %v", errNum)
        return nil, finalErr
    }

    // 提前计算需要保存的relativePts分片长度
    ptsLength := 0
    for i := range durations {
        ptsLength += len(durations[i])
    }
    relativeTime := make([]int32, 0, ptsLength)
    for i := range durations {
        relativeTime = append(relativeTime, durations[i]...)
    }
    relativeTime = common.SortedInt32s(common.RemoveDuplication(relativeTime))
    rp := &model.DurationPts{
        RelativePts: relativeTime,
    }
    log.Printf("切分片任务处理完毕，切分片任务耗时:[%v]", time.Since(startTime))
    return rp, nil
}
