package cos

import (
    "context"
    "io/ioutil"
    "net/http"
    "net/url"
    "strconv"
    "strings"

    pb "github.com/golang/etcd-task-manager/pb"
    "github.com/golang/etcd-task-manager/pkg/config"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
    "github.com/tencentyun/cos-go-sdk-v5"
)

const (
	GlobalCosURL = "算法结果存储的地址"
	PartOfCosURL = "算法结果存储的地址"
    GlobalCosURLOut = "算法结果存储的地址"
)

var (
    GlobalResultCosClient *cos.Client
)

func Init() error {
    u, _ := url.Parse(GlobalCosURL)
    b := &cos.BaseURL{BucketURL: u}
    GlobalResultCosClient = cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:  config.GlobalConfig.Cos.CosID,
            SecretKey: config.GlobalConfig.Cos.CosKey,
        },
    })
    return nil
}

func GetSliceCosPrefix(taskCtx *pb.TaskContext) string {
    //return fmt.Sprintf("%s/slice_video/%ds_per_clips", cos.CosPrefix(taskCtx), constant.VideoClipDurationMs/1000)
    return CosPrefix(taskCtx) + "/slice_video/" + strconv.FormatInt(int64(config.GlobalConfig.Algorithm.Duration)/1000, 10) + "s_per_clips"
}

// 根据task.GetVideoInfo的类型编写前缀
func CosPrefix(taskCtx *pb.TaskContext) string {
    if taskCtx == nil {
        return "error"
    }

    switch taskCtx.GetVideoInfo().(type) {
    case *pb.TaskContext_TencentVideoVid:
        //return fmt.Sprintf("video_human_seg/tencentVID/%s/%s", taskCtx.GetTencentVideoVid(), taskCtx.GetInnerFeedID())
        return "video_human_seg/tencentVID/" + taskCtx.GetTencentVideoVid() + "/" + taskCtx.GetInnerFeedID()
    case *pb.TaskContext_VideoUrl:
        //return fmt.Sprintf("video_human_seg/url/%s", taskCtx.GetInnerFeedID())
        return "video_human_seg/url/" + taskCtx.GetInnerFeedID()
    }
    return "error"
}

func PutSliceResultToCos(ctx context.Context, cosPrefix string, beginMs int32, result *pb.VideoClipInfo) error {
    //resultKey := fmt.Sprintf("%s/%d.json", cosPrefix, beginMs)
    resultKey := cosPrefix + "/" + strconv.FormatInt(int64(beginMs), 10) + ".json"
    resultValue := common.MustDumpsPBToString(result)
    resultValueReader := strings.NewReader(resultValue)

    opt := &cos.ObjectPutOptions{
        ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
            ContentType: "application/json",
        },
    }
    var err error
    for retryTime := 0; retryTime < 3; retryTime++ {
        _, err = GlobalResultCosClient.Object.Put(ctx, resultKey, resultValueReader, opt)
        if err == nil {
            break
        }
    }
    return err
}

func GetSliceResultCos(ctx context.Context, cosPrefix string, beginMs int32) (*pb.VideoClipInfo, error) {
    //resultKey := fmt.Sprintf("%s/%d.json", cosPrefix, beginMs)
    resultKey := cosPrefix + "/" + strconv.FormatInt(int64(beginMs), 10) + ".json"
    resp, err := GlobalResultCosClient.Object.Get(ctx, resultKey, nil)
    if err != nil {
        return nil, err
    }

    bs, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    body := &pb.VideoClipInfo{}
    err = common.LoadsJsonToPb(bs, body)
    if err != nil {
        return nil, err
    }
    return body, nil
}

func PutAlgorithmResultToCos(ctx context.Context, cosPrefix string, beginMs int32, result *pb.SingleAlgorithmResult) error {
    //resultKey := fmt.Sprintf("%s/human_seg_result/%d.json", cosPrefix, beginMs)
    resultKey := cosPrefix + "/human_seg_result/" + strconv.FormatInt(int64(beginMs), 10) + ".json"
    resultValue := common.MustDumpsPBToString(result)
    resultValueReader := strings.NewReader(resultValue)

    opt := &cos.ObjectPutOptions{
        ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
            ContentType: "application/json",
        },
    }
    var err error
    for retryTime := 0; retryTime < 3; retryTime++ {
        _, err = GlobalResultCosClient.Object.Put(ctx, resultKey, resultValueReader, opt)
        if err == nil {
            break
        }
    }
    return err
}

func GetAlgorithmResultCos(ctx context.Context, cosPrefix string, beginMs int32) (*pb.FrameAlgorithmResult, error) {
    //resultKey := fmt.Sprintf("%s/final_result/%d.json", cosPrefix, beginMs)
    resultKey := cosPrefix + "/final_result/" + strconv.FormatInt(int64(beginMs), 10) + ".json"
    resp, err := GlobalResultCosClient.Object.Get(ctx, resultKey, nil)
    if err != nil {
        return nil, err
    }

    bs, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var body = &pb.FrameAlgorithmResult{}
    err = common.LoadsJsonToPb(bs, body)
    if err != nil {
        return nil, err
    }
    return body, nil
}

func PutStringToCos(ctx context.Context, uri string, value string) error {
    valueReader := strings.NewReader(value)

    opt := &cos.ObjectPutOptions{
        ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
            ContentType: "application/json",
        },
    }

    var err error
    for retryTime := 0; retryTime < 3; retryTime++ {
        _, err = GlobalResultCosClient.Object.Put(ctx, uri, valueReader, opt)
        if err == nil {
            break
        }
    }

    if err != nil {
        return err
    }
    return nil
}

func GetBytesFromCos(ctx context.Context, uri string) ([]byte, error) {
    resp, err := GlobalResultCosClient.Object.Get(ctx, uri, nil)
    if err != nil {
        return nil, err
    }

    bs, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    return bs, nil
}
