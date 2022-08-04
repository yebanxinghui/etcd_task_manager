package component

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
    "github.com/golang/etcd-task-manager/pkg/repo/constant"
    "github.com/golang/etcd-task-manager/pkg/repo/model"
)

type AlgorithmStructuredDataComponent struct {
    SegData	string `json:"segData"`
}

type AlgorithmStructuredDataWritter struct {
    MediaInfo         *model.MediaInfo
    Durations         []*pb.TimeSlice
    CosPrefix         string
    FeedID            string
    Cid				  string
    VideoFormat		  int32
    AlgorithmResult    []*pb.SingleAlgorithmResult
    RelativeSlice	  *model.DurationPts
}

func (w *AlgorithmStructuredDataWritter) prepareData(ctx context.Context) error {
    var wg sync.WaitGroup
    ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
    defer cancel()
    errs := make([]error, len(w.Durations))
    results := make([][]*pb.SingleAlgorithmResult, len(w.Durations))
    for i, v := range w.Durations {
        wg.Add(1)
        go func(i int, beginPtsMs int32) {
            defer wg.Done()

            var (
            	rsp *pb.FrameAlgorithmResult
                err error
            )
            for i := 0; i < 3; i++ {
                rsp, err = cos.GetAlgorithmResultCos(ctx, w.CosPrefix, beginPtsMs)
                if err == nil {
                    break
                }
            }
            errs[i] = err
            results[i] = rsp.GetResults()
        }(i, v.BeginPtsMs)
    }
    wg.Wait()
    for i, e := range errs {
        if e != nil {
            log.Printf("[prepareData] get beginms: [%d] of algorithm result err: [%v]", w.Durations[i].BeginPtsMs, e)
            return e
        }
    }
    // 提前计算申请的切片容量
    sliceSize := 0
    for i := range results {
        sliceSize += len(results[i])
    }
    AlgorithmResults := make([]*pb.SingleAlgorithmResult, 0, sliceSize)
    for i := range results {
        AlgorithmResults = append(AlgorithmResults, results[i]...)
    }
    w.AlgorithmResult = AlgorithmResults
    return nil
}

func (w *AlgorithmStructuredDataWritter) makeStructuredData() (*model.Result, error) {
    ans := make([]*model.StructuredDataRaw, 0)
    DocumentIdCounter := 0
    /**
        beg
        end
    pts 0    40   80   120  160  200  240  280  320
    res 0    40   120  160  240  280
        ptr
     */
    if len(w.AlgorithmResult) == 0 || len(w.RelativeSlice.RelativePts) == 0{
        log.Printf("[makeStructuredData] nil res")
        return nil, nil
    } else if w.AlgorithmResult[0].BeginPtsMs < w.RelativeSlice.RelativePts[0] {
        log.Printf("[makeStructuredData] res begin pts < relative pts begin")
        return nil, fmt.Errorf("[makeStructuredData] res begin pts < relative pts begin")
    } else if len(w.AlgorithmResult) == 1 {
        ans = append(ans, &model.StructuredDataRaw{
            Namespace:  constant.StructuredDataNamespace,
            // fmt.Sprintf("%s_%d_algorithm", w.FeedID, DocumentIdCounter),
            DocumentId: w.FeedID + "_" + strconv.Itoa(DocumentIdCounter) + "_algorithm",
            Type:       constant.StructuredDataAlgorithmType,
            Version:    constant.AlgorithmAIVersion,
            MediaInfo: &model.MediaInfo{
                Type: w.MediaInfo.Type,
                Vid:  w.MediaInfo.Vid,
            },
            StartTime: uint32(w.AlgorithmResult[0].BeginPtsMs),
            EndTime:   uint32(w.AlgorithmResult[0].BeginPtsMs+1),
            Region: &model.Region{
                StartX: 0,
                StartY: 0,
                EndX:   1,
                EndY:   1,
            },
            Track:      constant.StructuredDataAlgorithmTrack,
            Confidence: 100,
            Component: common.MustEncodeToJsonString(&AlgorithmStructuredDataComponent{
                SegData:  w.AlgorithmResult[0].GetSegData(),
            }),
            Env: []*model.Argument{
                &model.Argument{
                    Key:   "decode",
                    Value: config.GlobalConfig.Algorithm.Decode,
                },
                &model.Argument{
                    Key: "videoFormat",
                    Value: strconv.FormatInt(int64(w.VideoFormat), 10),
                },
            },
        })
        res := &model.Result{
            RawDataList: ans,
        }
        return res, nil
    }

    // 当 algorithm 结果 >= 2 时
    var (
        ptr     int   = 1   // Algorithm 索引
        begin   int32 = w.AlgorithmResult[0].BeginPtsMs  // relativePts 双指针开始端
        end     int32 = w.AlgorithmResult[0].BeginPtsMs  // relativePts 双指针结束端
        index   int   = 0  // relativePts 索引
    )

    /**
      防止出现
            beg
            end
        pts 0   40   80   120  160  200  240  280  320
        res 40  120  160  240  280
            ptr
    */
    for index = 0; index < len(w.RelativeSlice.RelativePts); index++ {
        if w.RelativeSlice.RelativePts[index] > w.AlgorithmResult[0].BeginPtsMs {
            break
        }
    }

    for ; index < len(w.RelativeSlice.RelativePts) && ptr < len(w.AlgorithmResult); index++ {
        if w.AlgorithmResult[ptr].GetBeginPtsMs() == w.RelativeSlice.RelativePts[index] {
            DocumentIdCounter++
            end = w.RelativeSlice.RelativePts[index]
            ans = append(ans, &model.StructuredDataRaw{
                Namespace:  constant.StructuredDataNamespace,
                // fmt.Sprintf("%s_%d_algorithm", w.FeedID, DocumentIdCounter),
                DocumentId: w.FeedID + "_" + strconv.Itoa(DocumentIdCounter) + "_algorithm",
                Type:       constant.StructuredDataAlgorithmType,
                Version:    constant.AlgorithmAIVersion,
                MediaInfo: &model.MediaInfo{
                    Type: w.MediaInfo.Type,
                    Vid:  w.MediaInfo.Vid,
                },
                StartTime: uint32(begin),
                EndTime:   uint32(end),
                Region: &model.Region{
                    StartX: 0,
                    StartY: 0,
                    EndX:   1,
                    EndY:   1,
                },
                Track:      constant.StructuredDataAlgorithmTrack,
                Confidence: 100,
                Component: common.MustEncodeToJsonString(&AlgorithmStructuredDataComponent{
                    SegData:  w.AlgorithmResult[ptr-1].GetSegData(),
                }),
                Env: []*model.Argument{
                    &model.Argument{
                        Key:   "decode",
                        Value: config.GlobalConfig.Algorithm.Decode,
                    },
                    &model.Argument{
                        Key: "videoFormat",
                        Value: strconv.FormatInt(int64(w.VideoFormat), 10),
                    },
                },
            })
            begin = end
            ptr++
            continue
        }
    }
    DocumentIdCounter++
    if w.RelativeSlice.RelativePts[len(w.RelativeSlice.RelativePts)-1] == end {
        end += 1
    } else {
        end = w.RelativeSlice.RelativePts[len(w.RelativeSlice.RelativePts)-1]
    }
    ans = append(ans, &model.StructuredDataRaw{
        Namespace:  constant.StructuredDataNamespace,
        // fmt.Sprintf("%s_%d_algorithm", w.FeedID, DocumentIdCounter),
        DocumentId: w.FeedID + "_" + strconv.Itoa(DocumentIdCounter) + "_algorithm",
        Type:       constant.StructuredDataAlgorithmType,
        Version:    constant.AlgorithmAIVersion,
        MediaInfo: &model.MediaInfo{
            Type: w.MediaInfo.Type,
            Vid:  w.MediaInfo.Vid,
        },
        StartTime: uint32(begin),
        EndTime:   uint32(end),
        Region: &model.Region{
            StartX: 0,
            StartY: 0,
            EndX:   1,
            EndY:   1,
        },
        Track:      constant.StructuredDataAlgorithmTrack,
        Confidence: 100,
        Component: common.MustEncodeToJsonString(&AlgorithmStructuredDataComponent{
            SegData:  w.AlgorithmResult[len(w.AlgorithmResult)-1].GetSegData(),
        }),
        Env: []*model.Argument{
            &model.Argument{
                Key: "videoFormat",
                Value: strconv.FormatInt(int64(w.VideoFormat), 10),
            },
        },
    })
    res := &model.Result{
        RawDataList: ans,
    }
    return res, nil
}

func (w *AlgorithmStructuredDataWritter) WriteRemote(ctx context.Context) error {
    log.Printf("开始推送 [%v] 结构化数据", w.MediaInfo.Vid)
    writeStructureDataTime := time.Now()
    err := w.prepareData(ctx)
    if err != nil {
        return err
    }
    datas, err := w.makeStructuredData()
    if err != nil {
        return err
    }

    // 现在不写es，改用cos
    if w.CosPrefix != "" {
        starJsonStr := common.MustDumpsPBToString(datas)
        //AlgorithmUrl := fmt.Sprintf("%s/algorithm.json", w.CosPrefix)
        AlgorithmUrl := w.CosPrefix + "/algorithm.json"
        err := cos.PutStringToCos(ctx, AlgorithmUrl, starJsonStr)
        if err != nil {
            log.Printf("上传 algorithm 失败 [%s] err: [%v]", AlgorithmUrl, err)
            return err
        } else {
            log.Printf("上传 algorithm 成功 [%s]", AlgorithmUrl)
        }
    }
    log.Printf("推送 [%v] 结构化数据成功, 耗时: [%v]", w.MediaInfo.Vid, time.Since(writeStructureDataTime))
    return nil
}
