package etcd_util

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "strconv"
    "strings"
    "time"

    "github.com/coreos/etcd/clientv3"
    "github.com/coreos/etcd/clientv3/concurrency"
    "github.com/go-basic/uuid"
    "github.com/golang/etcd-task-manager/pkg/repo/common"
)

// 0、链接
type EtcdConnector struct {
    namespace string
    machine   string
    cli       *clientv3.Client
    kv        clientv3.KV
    watcher   clientv3.Watcher
}

func CreateEtcdGetDefaultIntFromKvFunc(cli *EtcdConnector, ctx context.Context, queuePrefix string) func(key string, def int64) (int64, error) {
    return func(key string, def int64) (int64, error) {
        //rsp, err := cli.kv.Get(ctx, fmt.Sprintf("%s/infos/%s", queuePrefix, key))
        rsp, err := cli.kv.Get(ctx, queuePrefix+"/infos/"+key)
        if err != nil {
            return 0, err
        }
        if len(rsp.Kvs) == 0 {
            _, err := cli.kv.Put(ctx, queuePrefix+"/infos/"+key, fmt.Sprintf("%020d", def))
            return def, err
        }

        i, err := strconv.ParseInt(string(rsp.Kvs[0].Value), 10, 64)
        return i, err
    }
}

func CreateEtcdGetIntFromKvFunc(cli *EtcdConnector, ctx context.Context, queuePrefix string) func(key string) (int64, error) {
    return func(key string) (int64, error) {
        //queueKey := fmt.Sprintf("%s/infos/%s", queuePrefix, key)
        queueKey := queuePrefix + "/infos/" + key
        rsp, err := cli.kv.Get(ctx, queueKey)
        if err != nil {
            return 0, err
        }
        if len(rsp.Kvs) == 0 {
            return 0, fmt.Errorf("queue not exists or metadata had been destory [key = %s]", queueKey)
        }
        i, err := strconv.ParseInt(string(rsp.Kvs[0].Value), 10, 64)
        return i, err
    }
}

func NewEtcdConnectorByKey(key, machine string) (*EtcdConnector, error) {
    cfg := GetEtcdConfig(key)
    if cfg == nil {
        return nil, fmt.Errorf("cannot find key [%s] config", key)
    }
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{cfg.Addr},
        DialTimeout: 5 * time.Second,
        TLS:         cfg.TLSConfig,
    })
    if err != nil {
        return nil, err
    }
    return &EtcdConnector{
        namespace: key,
        machine:   machine,
        cli:       cli,
        kv:        clientv3.NewKV(cli),
        watcher:   clientv3.NewWatcher(cli),
    }, nil
}

func (cli *EtcdConnector) Close() {
    cli.cli.Close()
}

func (cli *EtcdConnector) Machine() string {
    return cli.machine
}

func (cli *EtcdConnector) putKV(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
    //rsp, err := cli.kv.Put(ctx, fmt.Sprintf("/%s/KV/%s", cli.namespace, key), value)
    rsp, err := cli.kv.Put(ctx, "/"+cli.namespace+"/KV/"+key, value)
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func (cli *EtcdConnector) PutKV(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
    rsp, err := cli.putKV(ctx, key, value)
    return rsp, err
}

func (cli *EtcdConnector) getKVPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
    //rsp, err := cli.kv.Get(ctx, fmt.Sprintf("/%s/KV/%s", cli.namespace, prefix), clientv3.WithPrefix())
    rsp, err := cli.kv.Get(ctx, "/"+cli.namespace+"/KV/"+prefix, clientv3.WithPrefix())
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func (cli *EtcdConnector) GetKVPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
    rsp, err := cli.getKVPrefix(ctx, prefix)
    return rsp, err
}

func (cli *EtcdConnector) getKV(ctx context.Context, key string) (*clientv3.GetResponse, error) {
    //rsp, err := cli.kv.Get(ctx, fmt.Sprintf("/%s/KV/%s", cli.namespace, key))
    rsp, err := cli.kv.Get(ctx, "/"+cli.namespace+"/KV/"+key)
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func (cli *EtcdConnector) GetKV(ctx context.Context, key string) (*clientv3.GetResponse, error) {
    rsp, err := cli.getKV(ctx, key)
    return rsp, err
}

func (cli *EtcdConnector) deleteKV(ctx context.Context, key string) (*clientv3.DeleteResponse, error) {
    //rsp, err := cli.kv.Delete(ctx, fmt.Sprintf("/%s/KV/%s", cli.namespace, key))
    rsp, err := cli.kv.Delete(ctx, "/"+cli.namespace+"/KV/"+key)
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func (cli *EtcdConnector) DeleteKV(ctx context.Context, key string) (*clientv3.DeleteResponse, error) {
    rsp, err := cli.deleteKV(ctx, key)
    return rsp, err
}

func (cli *EtcdConnector) deleteKVPrefix(ctx context.Context, prefix string) (*clientv3.DeleteResponse, error) {
    //rsp, err := cli.kv.Delete(ctx, fmt.Sprintf("/%s/KV/%s", cli.namespace, prefix), clientv3.WithPrefix())
    rsp, err := cli.kv.Delete(ctx, "/"+cli.namespace+"/KV/"+prefix, clientv3.WithPrefix())
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func (cli *EtcdConnector) DeleteKVPrefix(ctx context.Context, prefix string) (*clientv3.DeleteResponse, error) {
    rsp, err := cli.deleteKVPrefix(ctx, prefix)
    return rsp, err
}

func (cli *EtcdConnector) LowLevelWatch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
    return cli.watcher.Watch(ctx, key, opts...)
}

func (cli *EtcdConnector) WatchKV(ctx context.Context, key string) clientv3.WatchChan {
    //realKey := fmt.Sprintf("/%s/KV/%s", cli.namespace, key)
    return cli.watcher.Watch(ctx, "/"+cli.namespace+"/KV/"+key)
}

func (cli *EtcdConnector) LenPrefix(key string) int {
    //realKey := fmt.Sprintf("/%s/KV/%s/", cli.namespace, key)
    return len("/" + cli.namespace + "/KV/" + key)
}

// 1、选主
// Prefix: /<namespace>/Campaign/
// Return true -> Win in campaign, other mean lose
func (cli *EtcdConnector) Campaign(ctx context.Context, key string, expire time.Duration) (bool, error) {
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(5))
    if err != nil {
        return false, err
    }

    ctx, cancel := context.WithTimeout(ctx, expire)
    defer cancel() // 配置过期时间

    e := concurrency.NewElection(s, fmt.Sprintf("/%s/Campaign/%s", cli.namespace, key))
    err = e.Campaign(ctx, cli.machine)
    if err == context.Canceled {
        return false, fmt.Errorf("Etcd Campaign: timeout")
    }

    if err != nil { // 竞争失败
        return false, nil
    }

    // e.Resign(ctx)    // 在这个模型中不要主动卸任，除非做完任务了
    return true, nil // 竞争成功
}

// 2、队列
// etcd 模拟队列目录结构
// ${queue_name}
// ├── lock -> formal.Consumer.SZ1 (consumer id)
// ├── infos
// │   ├── head -> 10                 (item中key最大的任务)
// │   ├── min_ready -> 4             (status/ready 中key最小的任务)
// │   ├── tail -> 2                  (item 中 key 最小的任务)
// │   └── consecutive_last_done -> 0 (status/done 中『连续』最小的任务)
// └── items ( task 0 and task 1 is expire ,auto delete )
// │   ├── 0000000000000000002 -> {}
// │   ├── 0000000000000000003
// │   ├── 0000000000000000004
// │   ├── ...
// │   └── 0000000000000000010
// └── status
//     ├── doing
//     │   ├── 0000000000000000002 {"push_time":0}
//     │   └── 0000000000000000003
//     ├── done
//     └── ready

type QueueStatus struct {
    PushTime int64 `json:"push_time"`
    PopTime  int64 `json:"pop_time"`
    DoneTime int64 `json:"done_time"`
}

type QueueItem struct {
    QueueStatus
    Index int64  `json:"index"`
    Value string `json:"value"`
}

func (cli *EtcdConnector) PushQueue(ctx context.Context, queueName string, value string) error {
    // 配置过期时间
    ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(60))
    defer cancel()
    // 锁的租约也配置为5s，避免机器挂掉锁的长期占用
    timoutGrant, e := cli.cli.Grant(ctx, 5)
    if e != nil {
        log.Printf(e.Error())
        return e
    }
    // 创建连接
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(5), concurrency.WithLease(timoutGrant.ID))
    if err != nil {
        return err
    }

    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    //mutex := concurrency.NewMutex(s, fmt.Sprintf("%s/lock", queuePrefix))
    mutex := concurrency.NewMutex(s, queuePrefix+"/lock")

    err = mutex.Lock(ctx)
    if err != nil {
        return err
    }
    defer mutex.Unlock(context.TODO())

    // default with write
    getDefaultInt := CreateEtcdGetDefaultIntFromKvFunc(cli, ctx, queuePrefix)

    head, err := getDefaultInt("head", 0)
    if err != nil {
        return err
    }

    _, err = getDefaultInt("tail", 0)
    if err != nil {
        return err
    }

    _, err = getDefaultInt("min_ready", 0)
    if err != nil {
        return err
    }

    _, err = cli.kv.Put(ctx, fmt.Sprintf("%s/items/%020d", queuePrefix, head), value)
    if err != nil {
        return err
    }

    //_, err = cli.kv.Put(ctx, fmt.Sprintf("%s/infos/head", queuePrefix), strconv.FormatInt(head,10))
    _, err = cli.kv.Put(ctx, queuePrefix+"/infos/head", strconv.FormatInt(head, 10))
    if err != nil {
        return err
    }

    status := &QueueStatus{PushTime: time.Now().Unix()} // 首次插入
    _, err = cli.kv.Put(ctx, fmt.Sprintf("%s/status/ready/%020d", queuePrefix, head), common.MustEncodeToJsonString(status))
    if err != nil {
        return err
    }

    head = head + 1
    //_, err = cli.kv.Put(ctx, fmt.Sprintf("%s/infos/head", queuePrefix), fmt.Sprintf("%020d", head))
    _, err = cli.kv.Put(ctx, queuePrefix+"/infos/head", fmt.Sprintf("%020d", head))
    if err != nil {
        return err
    }

    return nil
}

// convert ready item to doing
func (cli *EtcdConnector) TryPopQueue(queueName string) (*QueueItem, error) {
    ctx := context.TODO()
    if !cli.GetQueue(ctx, queueName) {
        return nil, nil
    }
    //// 配置过期时间
    //ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(60))
    //defer cancel()
    // 锁的租约也配置为5s，避免机器挂掉锁的长期占用
    timoutGrant, e := cli.cli.Grant(ctx, 5)
    if e != nil {
        //return nil, nil
        return nil, e
    }
    // 创建连接
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(5), concurrency.WithLease(timoutGrant.ID))
    if err != nil {
        return nil, err
    }

    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    //mutex := concurrency.NewMutex(s, fmt.Sprintf("%s/lock", queuePrefix))
    mutex := concurrency.NewMutex(s, queuePrefix+"/lock")

    err = mutex.Lock(ctx)
    if err != nil {
        return nil, nil
        //return nil, fmt.Errorf("queue lock error: %v", err)
    }
    defer mutex.Unlock(context.TODO())

    // default without write
    getDefaultInt := CreateEtcdGetDefaultIntFromKvFunc(cli, ctx, queuePrefix)

    head, err := getDefaultInt("head", 0)
    if err != nil {
        log.Printf("get head index error: %v", err)
        //return nil, nil
        return nil, fmt.Errorf("get head index error: %v", err)
    }
    min_ready, err := getDefaultInt("min_ready", 0)
    if err != nil {
        log.Printf("get min_ready index error: %v", err)
        //return nil, nil
        return nil, fmt.Errorf("get min_ready index error: %v", err)
    }

    // if head==0 && min_ready==0 => message 0 need consume.
    // if head==0 && min_ready==1 => message 1 need consume.
    if head < min_ready {
        return nil, nil
    }

    ans := &QueueItem{
        Index: min_ready,
    }
    // get {queuePrefix}/items/{index} -> string
    rsp, err := cli.kv.Get(ctx, fmt.Sprintf("%s/items/%020d", queuePrefix, min_ready))
    if err != nil {
        return nil, fmt.Errorf("get item detail index error: %v", err)
    }
    if len(rsp.Kvs) != 1 { // task is empty
        // todo: wait for task to [waitTimeout]
        return nil, nil
    }
    ans.Value = string(rsp.Kvs[0].Value)
    // get {queuePrefix}/ready/{index} -> QueueStatus
    rsp, err = cli.kv.Get(ctx, fmt.Sprintf("%s/status/ready/%020d", queuePrefix, min_ready))
    if err != nil {
        return nil, fmt.Errorf("get ready index index error: %v", err)
    }
    if len(rsp.Kvs) != 1 {
        if head == min_ready {
            return nil, nil
        } else {
            log.Printf("queue [%s] meta data(ready) has been damage!, rsp: [%+v], head: [%v], min_ready: [%v]", queueName, rsp, head, min_ready)
            return nil, fmt.Errorf("queue [%s] meta data(ready) has been damage", queueName)
        }
    }
    var status QueueStatus
    err = json.Unmarshal(rsp.Kvs[0].Value, &status)
    if err != nil {
        return nil, fmt.Errorf("queue %s meta data(status) has been damage! status meta data = %s, parse json err = %+v", queueName, string(rsp.Kvs[0].Value), err)
    }
    status.PopTime = time.Now().Unix()
    ans.QueueStatus = status
    new_min_ready := min_ready + 1
    _, err = cli.kv.Put(ctx, queuePrefix+"/infos/min_ready", fmt.Sprintf("%020d", new_min_ready))
    if err != nil {
        return nil, fmt.Errorf("put min_ready self plus index error: %v", err)
    }

    _, err = cli.kv.Put(ctx, fmt.Sprintf("%s/status/doing/%020d", queuePrefix, min_ready), common.MustEncodeToJsonString(status))
    if err != nil {
        return nil, fmt.Errorf("put doing item index error: %v", err)
    }
    return ans, nil
}

func (cli *EtcdConnector) PushToDoneQueue(ctx context.Context, queueName string, index int64) error {
    // 配置过期时间
    ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(60))
    defer cancel()
    // 锁的租约也配置为5s，避免机器挂掉锁的长期占用
    timoutGrant, e := cli.cli.Grant(ctx, 5)
    if e != nil {
        log.Print(e.Error())
        return e
    }
    // 创建连接
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(10), concurrency.WithLease(timoutGrant.ID))
    if err != nil {
        return err
    }

    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    //mutex := concurrency.NewMutex(s, fmt.Sprintf("%s/lock", queuePrefix))
    mutex := concurrency.NewMutex(s, queuePrefix+"/lock")

    err = mutex.Lock(ctx)
    if err != nil {
        return err
    }
    defer mutex.Unlock(context.TODO())

    // default without write
    getDefaultInt := CreateEtcdGetDefaultIntFromKvFunc(cli, ctx, queuePrefix)

    rsp, err := cli.kv.Get(ctx, fmt.Sprintf("%s/status/doing/%020d", queuePrefix, index))
    if err != nil {
        return err
    }

    if len(rsp.Kvs) != 1 {
        return fmt.Errorf("queue [%s] doesn't have doing item index %d.", queueName, index)
    }

    var status QueueStatus
    err = json.Unmarshal(rsp.Kvs[0].Value, &status)
    if err != nil {
        return fmt.Errorf("queue [%s] item index %d meta data damage, json parse err %v", queueName, index, err)
    }

    status.DoneTime = int64(time.Now().Unix())
    _, err = cli.kv.Put(ctx, fmt.Sprintf("%s/status/done/%020d", queuePrefix, index), common.MustEncodeToJsonString(status))
    if err != nil {
        return err
    }

    head, err := getDefaultInt("head", 0)
    if err != nil {
        return err
    }
    consecutiveLast, err := getDefaultInt("consecutive_last_done", -1)

    if err != nil {
        log.Printf("get consecutive_last_done var meet error")
        return err
    }
    if index == consecutiveLast+1 {
        _, err = cli.kv.Put(ctx, queuePrefix+"/infos/consecutive_last_done", fmt.Sprintf("%020d", index))
        if err != nil {
            return err
        }

        nextConsecutiveItem := int64(-1)
        for subIndex := index + 1; subIndex < head; subIndex++ {
            itemKey := fmt.Sprintf("%s/status/done/%020d", queuePrefix, subIndex)
            rsp, err := cli.kv.Get(ctx, itemKey)
            if err != nil || len(rsp.Kvs) == 0 {
                break
            }
            nextConsecutiveItem = subIndex
        }

        if nextConsecutiveItem != -1 {
            //_, err = cli.kv.Put(ctx, fmt.Sprintf("%s/infos/consecutive_last_done", queuePrefix), fmt.Sprintf("%020d", nextConsecutiveItem))
            _, err = cli.kv.Put(ctx, queuePrefix+"/infos/consecutive_last_done", fmt.Sprintf("%020d", nextConsecutiveItem))
            if err != nil {
                return err
            }
        }
    }
    //_, _ = cli.kv.Delete(ctx, fmt.Sprintf("%s/status/doing/%020d", queuePrefix, index))
    return err
}

func (cli *EtcdConnector) ShowQueue(ctx context.Context, queueName string, cb ...func(string)) {
    mcb := func(s string) {
        fmt.Println("Queue:", queueName, " ", s)
    }
    if len(cb) != 0 {
        mcb = cb[0]
    }
    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    rsp, err := cli.kv.Get(ctx, queuePrefix, clientv3.WithPrefix())
    if err != nil {
        mcb("meet error: " + err.Error())
        return
    }

    if len(rsp.Kvs) == 0 {
        //mcb(fmt.Sprintf("queue %s is empty.", queueName))
        mcb("queue " + queueName + "is empty")
        return
    }

    mt := NewMenuTree()
    for _, v := range rsp.Kvs {
        var key string = string(v.Key)
        //var value string = fmt.Sprintf("%s [lease %v]", v.Value, v.Lease)
        var value string = string(v.Value) + "[lease " + strconv.FormatInt(v.Lease, 10) + "]"
        mt.WriteTo(key, value)
    }

    for _, v := range mt.Show() {
        mcb(v)
    }

}

func (cli *EtcdConnector) DestroyQueue(ctx context.Context, queueName string) error {
    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    _, err := cli.kv.Delete(ctx, queuePrefix, clientv3.WithPrefix())
    if err != nil {
        return err
    }
    return nil
}

func (cli *EtcdConnector) GetQueue(ctx context.Context, queueName string) bool {
    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    rsp, err := cli.kv.Get(ctx, queuePrefix, clientv3.WithPrefix())
    if err != nil {
        return false
    }
    if len(rsp.Kvs) == 0 {
        return false
    }
    return true
}

type WatchQueueResponse struct {
    HeadIndex     int64 `json:"head"`
    MinReadyIndex int64 `json:"min_ready"`
    TailIndex     int64 `json:"tail"`
    LastDoneIndex int64 `json:"consecutive_last_done"`
}

func (cli *EtcdConnector) GetQueueStatus(ctx context.Context, queueName string) (*WatchQueueResponse, error) {
    // 配置过期时间
    ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(60))
    defer cancel()
    // 锁的租约也配置为5s，避免机器挂掉锁的长期占用
    timoutGrant, e := cli.cli.Grant(ctx, 5)
    if e != nil {
        log.Printf(e.Error())
        return nil, e
    }
    // 创建连接
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(5), concurrency.WithLease(timoutGrant.ID))
    if err != nil {
        return nil, err
    }

    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    //mutex := concurrency.NewMutex(s, fmt.Sprintf("%s/lock", queuePrefix))
    mutex := concurrency.NewMutex(s, queuePrefix+"/lock")

    err = mutex.Lock(ctx)
    if err != nil {
        return nil, err
    }
    defer mutex.Unlock(context.TODO())

    //getInt := CreateEtcdGetIntFromKvFunc(cli, ctx, queuePrefix)
    getDefaultInt := CreateEtcdGetDefaultIntFromKvFunc(cli, ctx, queuePrefix)

    ans := &WatchQueueResponse{}
    ans.HeadIndex, err = getDefaultInt("head", 0)
    if err != nil {
        return nil, err
    }
    ans.LastDoneIndex, err = getDefaultInt("consecutive_last_done", -1)
    if err != nil {
        return nil, err
    }
    return ans, nil
}

func (cli *EtcdConnector) WatchQueue(ctx context.Context, queueName string, notify func()) (func(), error) {
    //queuePrefix := fmt.Sprintf("/%s/Queue/%s", cli.namespace, queueName)
    queuePrefix := "/" + cli.namespace + "/Queue/" + queueName
    ctxI, cancel := context.WithCancel(ctx)
    rch := cli.LowLevelWatch(ctxI, queuePrefix+"/infos", clientv3.WithPrefix())
    closeChan := make(chan struct{}, 0)
    // ticker := time.NewTicker(time.Second * time.Duration(5))
    notify()
    go func() {
        defer func(closeChan chan struct{}) {
            // 防止通道被重复关闭
            if !isChanClose(closeChan) {
                close(closeChan)
            }
        }(closeChan)
        for {
            select {
            case _, ok := <-rch:
                if !ok {
                    return
                }
                // case <-ticker.C:
            }
            notify()
        }
    }()

    return func() {
        cancel()
        <-closeChan
    }, nil
}

func isChanClose(ch chan struct{}) bool {
    select {
    case _, received := <-ch:
        return !received
    default:
    }
    return false
}

func (cli *EtcdConnector) WaitQueueTaskDone(ctx context.Context, queueName string) error {
    isClosed := false
    closeChan := make(chan struct{}, 0)
    // fmt.Printf("wait queue [%s] task done...\n", queueName)
    var rsp *WatchQueueResponse
    var errQueue error
    close2, err := cli.WatchQueue(ctx, queueName, func() {
        rsp, errQueue = cli.GetQueueStatus(ctx, queueName)
        if errQueue != nil {
            log.Printf("notify meet error: %v", errQueue)
            if !isChanClose(closeChan) {
                close(closeChan)
                isClosed = true
            }
        } else {
            if rsp.LastDoneIndex+1 == rsp.HeadIndex {
                if !isClosed {
                    close(closeChan)
                    isClosed = true
                }
            }
        }
    })
    if err != nil {
        if !isClosed {
            close(closeChan)
            isClosed = true
        }
        return err
    }

    if errQueue != nil {
        if !isClosed {
            close(closeChan)
            isClosed = true
        }
        return errQueue
    }
    <-closeChan
    close2()
    return nil
}

// 3.0、 任务模型
type EtcdTaskModel struct {
    TaskName string // 任务名称

    BeginTimestampSecond   int64 // 开始时间戳(秒)
    TimeoutTimestampSecond int64 // 超时时间戳(秒)
    RetryTime              int64 // 当前重试次数
    MaxRetryTime           int64 // 最大重试次数

    TaskID   string // 临时生成ID 没什么用
    TaskInfo string // 任务数据
}

type TaskOpt func(*EtcdTaskModel)

func NewEtcdTaskModel(taskName string, timeoutSecond int64, taskInfo string, opts ...TaskOpt) *EtcdTaskModel {
    task := &EtcdTaskModel{
        TaskName:               taskName,
        BeginTimestampSecond:   time.Now().Unix(),
        TimeoutTimestampSecond: time.Now().Unix() + timeoutSecond,
        MaxRetryTime:           1,
    }
    task.TaskID = uuid.New()
    for _, v := range opts {
        v(task)
    }
    return task
}

func (task *EtcdTaskModel) String() string {
    return common.MustEncodeToJsonString(task)
}

func PushTaskToEtcd(ctx context.Context, cli *EtcdConnector, task *EtcdTaskModel) error {
    err := cli.PushQueue(ctx, task.TaskName, task.String())
    if err != nil {
        return err
    }
    return nil
}

type MenuTree struct {
    m map[string]interface{}
    // 目录类型 map[string]interface{}
    // 文本类型 string
}

func NewMenuTree() *MenuTree {
    return &MenuTree{
        m: map[string]interface{}{},
    }
}

func (MenuTree) ProcessDir(p string) []string {
    if p[0] == '/' {
        p = p[1:]
    }
    return strings.Split(p, "/")
}

func (this *MenuTree) WriteTo(dir string, txt string) error {
    paths := this.ProcessDir(dir)
    var root interface{} = this.m
    for _, v := range paths[:len(paths)-1] {
        if _, ok := root.(map[string]interface{})[v]; ok {
            if _, ok := root.(map[string]interface{})[v].(map[string]interface{}); !ok {
                return fmt.Errorf("path %s is not directory", dir)
            }
        } else {
            // fmt.Println("mkdir: ", v, " to ", this.m)
            root.(map[string]interface{})[v] = map[string]interface{}{}
        }
        root = root.(map[string]interface{})[v]
    }
    root.(map[string]interface{})[paths[len(paths)-1]] = txt
    return nil
}

func innerMenuTreeShow(m map[string]interface{}, indent string) []string {
    lines := make([]string, 0)
    last := -1

    for k, v := range m {
        singleLines := make([]string, 0)
        switch v.(type) {
        case string:
            singleLines = append(singleLines, fmt.Sprintf("%s├── %s -> (%s)", indent, k, v.(string)))
        case map[string]interface{}:
            singleLines = append(singleLines, fmt.Sprintf("%s├── %s", indent, k))
            singleLines = append(singleLines, innerMenuTreeShow(v.(map[string]interface{}), indent+"│    ")...)
        }
        last = len(lines)
        lines = append(lines, singleLines...)
    }

    if last >= 0 && strings.HasPrefix(lines[last], indent+"├") {
        lines[last] = indent + "└" + strings.TrimPrefix(lines[last], indent+"├")
    }
    for i := last + 1; i < len(lines); i++ {
        if strings.HasPrefix(lines[i], indent+"│") {
            lines[i] = indent + " " + strings.TrimPrefix(lines[i], indent+"│")
        }
    }
    return lines
}

func (this *MenuTree) Show() []string {
    return innerMenuTreeShow(this.m, "")
}

func (cli *EtcdConnector) MutexLock(key string) (func(), error) {
    // 锁的租约也配置为5s，避免机器挂掉锁的长期占用
    ctx := context.TODO()
    timoutGrant, e := cli.cli.Grant(ctx, 5)
    if e != nil {
        log.Printf("mutex lock err:[%v]", e)
        return nil, e
    }
    // 创建连接
    s, err := concurrency.NewSession(cli.cli, concurrency.WithTTL(5), concurrency.WithLease(timoutGrant.ID))
    if err != nil {
        log.Printf("create session err: [%v]", err)
        return nil, err
    }

    //mutex := concurrency.NewMutex(s, fmt.Sprintf("/%s/Mutex/%s", cli.namespace, key))
    mutex := concurrency.NewMutex(s, "/"+cli.namespace+"/Mutex/"+key)
    err = mutex.Lock(ctx)
    if err != nil {
        log.Printf("create mutex err: [%v]", err)
        return nil, err
    }
    return func() {
        mutex.Unlock(ctx)
    }, nil
}
