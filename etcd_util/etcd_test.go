package etcd_util

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/smartystreets/goconvey/convey"
)

// const GlobalCurrectContextKey = "boxaiface_global_currect_context"

// func GetCurrectContext(conn *EtcdConnector, ctx context.Context) (*pb_common.TaskContext, error) {
// 	rspGet, err := conn.GetKV(ctx, GlobalCurrectContextKey)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if len(rspGet.Kvs) == 0 {
// 		return nil, fmt.Errorf("global currect context is empty.")
// 	}

// 	task := &pb_common.TaskContext{}
// 	err = LoadsJsonToPb(rspGet.Kvs[0].Value, task)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return task, err
// }

// go test -timeout 30s -run ^TestEtcdConnect$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestEtcdConnect(t *testing.T) {
	convey.Convey("etcd conn", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(120))
		defer cancel()
		conn, err := NewEtcdConnectorByKey("boxai_test", "default")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn.Close()
		rsp0, err := conn.PutKV(ctx, "test_insert_key", "hello")
		convey.So(err, convey.ShouldEqual, nil)
		convey.Printf("%+v\n", rsp0)
		rsp, err := conn.GetKV(ctx, "test_insert_key")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(rsp.Kvs), convey.ShouldEqual, 1)
		convey.Printf("%+v\n", rsp)
		convey.So(string(rsp.Kvs[0].Value), convey.ShouldEqual, "hello")
	})
}

// go test -timeout 30s -run ^TestEtcdGetNil$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestEtcdGetNil(t *testing.T) {
	convey.Convey("etcd conn", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(120))
		defer cancel()
		conn, err := NewEtcdConnectorByKey("boxai_test", "default")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn.Close()
		rsp, err := conn.GetKV(ctx, "test_insert_key0")
		convey.So(err, convey.ShouldEqual, nil)
		convey.Printf("%+v\n", rsp)
		convey.So(len(rsp.Kvs), convey.ShouldEqual, 0)
	})
}

// go test -timeout 30s -run ^TestEtcdCampaign$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestEtcdCampaign(t *testing.T) {
	convey.Convey("etcd campaign", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(120))
		defer cancel()

		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		conn2, err := NewEtcdConnectorByKey("boxai_test", "machine_2")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn2.Close()

		conn3, err := NewEtcdConnectorByKey("boxai_test", "machine_3")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn3.Close()

		for i := 0; i < 5; i++ {
			wg := sync.WaitGroup{}
			m := sync.Mutex{}
			leaders := make(map[string]bool)
			errs := make(map[string]error)

			c := func(conn *EtcdConnector, i int) {
				defer wg.Done()
				leader, err := conn.Campaign(ctx, fmt.Sprintf("cp_key2_%d", i), time.Millisecond*time.Duration(500))
				m.Lock()
				defer m.Unlock()
				leaders[conn.Machine()] = leader
				errs[conn.Machine()] = err
				if err != nil {
					fmt.Printf("err in c: %v\n", err)
				}
			}

			wg.Add(3)
			go c(conn1, i)
			go c(conn2, i)
			go c(conn3, i)
			wg.Wait()

			convey.Println(leaders)
			convey.Println(errs)

			leaderNum := 0
			errNum := 0
			for k, v := range leaders {
				if v {
					leaderNum++
				}
				if errs[k] != nil {
					errNum++
				}
			}
			convey.So(errNum, convey.ShouldEqual, 0)
			convey.So(leaderNum, convey.ShouldEqual, 1)
			// convey.So(leaderNum, convey.ShouldBeLessThanOrEqualTo, 1)
			// time.Sleep(time.Millisecond * time.Duration(100))
		}
	})
}

// go test -timeout 30s -run ^TestEtcdTaskPushAndGet$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
// func TestEtcdTaskPushAndGet(t *testing.T) {
// 	convey.Convey("etcd task model", t, func() {
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
// 		defer cancel()

// 		conn0, err := NewEtcdConnectorByKey("boxai_test", "machine_produce")
// 		convey.So(err, convey.ShouldEqual, nil)
// 		defer conn0.Close()

// 		task := NewEtcdTaskModel("defaultTask", WithTaskName("001task"), WithTaskInfo("001"))
// 		err = conn0.PutTask(ctx, task)
// 		convey.So(err, convey.ShouldEqual, nil)

// 	})
// }

// go test -timeout 30s -run ^TestEndianCode$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestEndianCode(t *testing.T) {
	agentFlag := 0x01
	bossid := 8903

	buffer := bytes.NewBufferString("test")
	binary.Write(buffer, binary.BigEndian, uint8(agentFlag))
	binary.Write(buffer, binary.BigEndian, uint32(bossid))

	for _, v := range buffer.Bytes() {
		fmt.Printf("%x -> %c\n", v, v)
	}
}

// go test -timeout 30s -run ^TestTree$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestTree(t *testing.T) {
	convey.Convey("tree test", t, func() {
		mt := NewMenuTree()
		mt.WriteTo("/path/your/code", "hello")
		fmt.Println(mt.m)
		convey.So(mt.m["path"].(map[string]interface{})["your"].(map[string]interface{})["code"].(string), convey.ShouldEqual, "hello")
		convey.Println("")
		for _, v := range mt.Show() {
			convey.Println(v)
		}
	})
}

// go test -timeout 30s -run ^TestPushQueue$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestPushQueue(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()
		err = conn1.PushQueue(ctx, "test_queue_name", "test_queue_value")
		convey.So(err, convey.ShouldEqual, nil)
	})
}

// go test -timeout 30s -run ^TestPopQueue$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestPopQueue(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		rsp, err := conn1.TryPopQueue("test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.Println(rsp)
	})
}

// go test -timeout 30s -run ^TestAllQueueOp git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestAllQueueOp(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		err = conn1.DestroyQueue(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)

		for i := 0; i < 10; i++ {
			err = conn1.PushQueue(ctx, "test_queue_name", fmt.Sprintf("test_queue_item_%d", i))
			convey.So(err, convey.ShouldEqual, nil)
		}

		conn1.ShowQueue(ctx, "test_queue_name")
		for i := int64(0); i < 5; i++ {
			s, err := conn1.TryPopQueue("test_queue_name")
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(s, convey.ShouldNotEqual, nil)
			convey.So(s.Index, convey.ShouldEqual, i)
		}

		conn1.ShowQueue(ctx, "test_queue_name")
		var avr *WatchQueueResponse
		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 2)
		convey.So(err, convey.ShouldEqual, nil)
		avr, err = conn1.GetQueueStatus(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(avr.LastDoneIndex, convey.ShouldEqual, -1)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 0)
		convey.So(err, convey.ShouldEqual, nil)
		avr, err = conn1.GetQueueStatus(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(avr.LastDoneIndex, convey.ShouldEqual, 0)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 1)
		convey.So(err, convey.ShouldEqual, nil)
		avr, err = conn1.GetQueueStatus(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(avr.LastDoneIndex, convey.ShouldEqual, 2)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 3)
		convey.So(err, convey.ShouldEqual, nil)
		avr, err = conn1.GetQueueStatus(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(avr.LastDoneIndex, convey.ShouldEqual, 3)

		conn1.ShowQueue(ctx, "test_queue_name")
	})
}

// go test -timeout 30s -run ^TestShowQueue git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestShowQueue(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		convey.Println("")
		conn1.ShowQueue(ctx, "test_queue_name")
	})
}

// go test -timeout 30s -run ^TestShowQueue2 git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestShowQueue2(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "unittest")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		convey.Println("")
		conn1.ShowQueue(ctx, "taskQueue/innerfeedid/scene")
		conn1.ShowQueue(ctx, "taskQueue/innerfeedid/detect")
		conn1.ShowQueue(ctx, "taskQueue/innerfeedid/pre_index")
		conn1.ShowQueue(ctx, "taskQueue/innerfeedid/index")
	})
}

func ShowKvs(conn *EtcdConnector, ctx context.Context, prefix string) {
	rsp, err := conn.GetKVPrefix(ctx, prefix)
	if err != nil {
		fmt.Println("ShouKvs meet error: ", err)
	} else {
		for _, v := range rsp.Kvs {
			fmt.Println(string(v.Key), "->", string(v.Value))
		}
	}
}

// go test -timeout 30s -run ^TestShowQueue3 git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestShowQueue3(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "1a6d502a_BoxAIFace_TaskManager")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()
		conn1.PutKV(ctx, "boxaiface_global_currect_context", "done")
		rsp, err := conn1.GetKV(ctx, "boxaiface_global_currect_context")

		if rsp == nil || len(rsp.Kvs) == 0 {
			fmt.Println("no_task")
		} else {
			convey.Println("task detail : ", rsp)
			//
			//task := &pb_common.TaskContext{}
			//LoadsJsonToPb(rsp.Kvs[0].Value, task)
			//conn1.ShowQueue(ctx, fmt.Sprintf("taskQueue/%s/scene", task.InnerFeedID))
			//conn1.ShowQueue(ctx, fmt.Sprintf("taskQueue/%s/detect", task.InnerFeedID))
			//conn1.ShowQueue(ctx, fmt.Sprintf("taskQueue/%s/pre_index", task.InnerFeedID))
			//conn1.ShowQueue(ctx, fmt.Sprintf("taskQueue/%s/index", task.InnerFeedID))
			//
			//ShowKvs(conn1, ctx, fmt.Sprintf("taskError/%s", task.InnerFeedID))
		}
	})
}

// go test -timeout 30s -run ^TestShowKvs git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestShowKvs(t *testing.T) {
	convey.Convey("get kvs test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "unittest")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		rsp, err := conn1.GetKVPrefix(ctx, "taskError")
		convey.So(err, convey.ShouldEqual, nil)
		convey.Println(rsp.Kvs)
	})
}

// go test -timeout 30s -run ^TestDestroyQueue$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestDestroyQueue(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()
		err = conn1.DestroyQueue(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
	})
}

// go test -timeout 30s -run ^TestGetPointerFromInterface$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestGetPointerFromInterface(t *testing.T) {
	convey.Convey("TestGetPointerFromInterface campaign", t, func() {
		m := map[string]interface{}{}
		mp := &m
		var mi interface{} = m
		var mip = (*map[string]interface{})(unsafe.Pointer(&mi))
		fmt.Printf("mp %v mip %v\n", unsafe.Pointer(mp), unsafe.Pointer(mip))
		// convey.So(mp, convey.ShouldEqual, mip)
	})
}

// go test -timeout 30s -run ^TestWatch$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestWatch(t *testing.T) {
	convey.Convey("watch", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		var t = 0
		cancel2, err := conn1.WatchQueue(ctx, "test_queue_name", func() {
			fmt.Println("notify...")
			t++
		})
		convey.So(err, convey.ShouldEqual, nil)
		defer cancel2()
		time.Sleep(time.Second * 10)
		convey.Println(t)
	})
}

// go test -timeout 30s -run ^TestQueueProduceAndConsumerModel$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestQueueProduceAndConsumerModel(t *testing.T) {
	// 创建测试队列
	conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
	if err != nil {
		t.Errorf("NewEtcdConnectorByKey meet error: %v", err)
	}
	defer conn1.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
	defer cancel()

	err = conn1.DestroyQueue(ctx, "test_queue_name")
	if err != nil {
		t.Errorf("DestroyQueue meet error: %v", err)
	}

	// produce
	go func() {
		for i := 0; i < 10; i++ {
			err = conn1.PushQueue(ctx, "test_queue_name", fmt.Sprintf("test_queue_item_%d", i))
			if err != nil {
				t.Errorf("produce meet error: %v", err)
				return
			}
			time.Sleep(time.Millisecond * time.Duration(10))
		}
	}()
	time.Sleep(time.Millisecond * time.Duration(10))
	// consume
	go func() {
		for {
			task, err := conn1.TryPopQueue("test_queue_name")
			if err != nil {
				t.Errorf("consumer error %v", err)
				break
			}
			if task == nil {
				break
			}
			time.Sleep(time.Millisecond * time.Duration(10))
			err = conn1.PushToDoneQueue(ctx, "test_queue_name", task.Index)
			if err != nil {
				t.Errorf("consumer error %v", err)
			}
		}
		fmt.Println("consumer has no task.")
	}()
	err = conn1.WaitQueueTaskDone(ctx, "test_queue_name")
	if err != nil {
		t.Errorf("wait meet error %v", err)
	}
	time.Sleep(time.Second)
}

// go test -timeout 30s -run ^TestClearAllEtcd$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestClearAllEtcd(t *testing.T) {
	// 创建测试队列
	conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
	if err != nil {
		t.Errorf("NewEtcdConnectorByKey meet error: %v", err)
	}
	defer conn1.Close()
	rsp, err := conn1.kv.Delete(context.Background(), "", clientv3.WithPrefix())
	fmt.Println(rsp)
	fmt.Println(err)
}

// go test -timeout 30s -run ^TestPopQueue2$ git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=1
func TestPopQueue2(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		rsp, err := conn1.TryPopQueue("test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.Println(rsp)
	})
}

// go test -timeout 30s -run ^TestAllQueueOp2 git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v -count=5
func TestAllQueueOp2(t *testing.T) {
	convey.Convey("push queue test", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "machine_1")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(50))
		defer cancel()

		err = conn1.DestroyQueue(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)

		for i := 0; i < 10; i++ {
			err = conn1.PushQueue(ctx, "test_queue_name", fmt.Sprintf("test_queue_item_%d", i))
			convey.So(err, convey.ShouldEqual, nil)
		}

		// conn1.ShowQueue(ctx, "test_queue_name")
		for i := int64(0); i < 5; i++ {
			s, err := conn1.TryPopQueue("test_queue_name")
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(s, convey.ShouldNotEqual, nil)
			convey.So(s.Index, convey.ShouldEqual, i)
		}

		// conn1.ShowQueue(ctx, "test_queue_name")
		var avr *WatchQueueResponse
		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 0)
		convey.So(err, convey.ShouldEqual, nil)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 1)
		convey.So(err, convey.ShouldEqual, nil)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 2)
		convey.So(err, convey.ShouldEqual, nil)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 4)
		convey.So(err, convey.ShouldEqual, nil)

		err = conn1.PushToDoneQueue(ctx, "test_queue_name", 3)
		convey.So(err, convey.ShouldEqual, nil)

		avr, err = conn1.GetQueueStatus(ctx, "test_queue_name")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(avr.LastDoneIndex, convey.ShouldEqual, 4)

		conn1.ShowQueue(ctx, "test_queue_name")
	})
}

// go test -timeout 3000s -run ^TestWatchKey git.woa.com/video_ai/VideoSegment/TaskManager/etcd_util -v
func TestWatchKey(t *testing.T) {
	convey.Convey("watch", t, func() {
		conn1, err := NewEtcdConnectorByKey("boxai_test", "1a6d502a_BoxAIFace_TaskManager")
		convey.So(err, convey.ShouldEqual, nil)
		defer conn1.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(9000000))
		defer cancel()

		rch := conn1.WatchKV(ctx, "boxaiface_global_currect_context")
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("context 过期\n")
				break
			case rsp, ok := <-rch:
				if !ok {
					fmt.Printf("监听异常\n")
					break
				}
				fmt.Printf("检测到变化 %+v \n", rsp)
			}
		}
		fmt.Printf("检测完毕\n")
	})
}
