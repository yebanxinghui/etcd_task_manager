package messageHandler

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/golang/etcd-task-manager/pb"
	"github.com/golang/etcd-task-manager/pkg/modules/cos"
	"google.golang.org/grpc"
)

type StreamServerImpl struct{}

func (s *StreamServerImpl) algorithmHandler(stream grpc.ServerStream, task *TAlgorithmTask) error {
	rsp := &pb.ConsumerTaskReply{
		Context:  task.Context,
		Fine:     false,
		TaskType: &pb.TaskTypeEnum{Tasktype: &pb.TaskTypeEnum_TAlgorithm{}},
		Slice: &pb.ProcessSlice{
			BeginPtsMs: task.GetSlice().GetBeginPtsMs(),
			EndPtsMs:   task.GetSlice().GetEndPtsMs(),
			Clips:      []*pb.VideoClipInfo{task.GetVideoClips()},
		},
		//BaseUrl: fmt.Sprintf("%s/%s", cos.GlobalCosURL, cos.CosPrefix(task.Context)),
		BaseUrl: cos.GlobalCosURL + "/" + cos.CosPrefix(task.Context),
	}
	log.Printf("发送弹幕分割任务 vid [%s] 片段开始时间 [%d] EtcdIndex [%d]", rsp.GetContext().GetSourceKey(), rsp.GetSlice().GetBeginPtsMs(), task.EtcdIndex)
	startTime := time.Now()
	err := stream.SendMsg(rsp) //向客户端返回任务信息
	if err != nil {
		err = stream.SendMsg(rsp) //重试
		if err != nil {
			log.Printf("human_seg task send error, err: [%v]", err)
			return err
		}
	}

	// 等待客户端处理完毕
	var processResult *pb.ConsumerTaskRequest
	for {
		err := stream.RecvMsg(processResult)
		if err != nil && err != io.EOF{
			log.Printf("[AlgorithmHandler] wait process result error:%v", err)
			return err
		}

		if processResult.GetDone() {
			log.Printf("弹幕分割任务 vid [%s] 片段开始时间 [%d] 结果接收完毕, 弹幕分割任务分片耗时:[%v]", rsp.GetContext().GetSourceKey(), rsp.GetSlice().GetBeginPtsMs(), time.Since(startTime).Seconds())
			break
		}
		if processResult.GetRetCode() != 1 {
			log.Printf("human_seg task return retCode: [%v], slice: [%v], vid: [%v]", processResult.GetRetCode(), task.GetSlice().GetBeginPtsMs(), rsp.GetContext().GetSourceKey())
			return fmt.Errorf("[AlgorithmHandler] result return nil")
		}
	}

	fine := &pb.ConsumerTaskReply{
		Fine: true,
	}
	stream.SendMsg(fine)
	return nil
}

func (s *StreamServerImpl) AlgorithmHandler(stream grpc.ServerStream, task *TAlgorithmTask) error {
	err := s.algorithmHandler(stream, task)
	DefaultTaskConsumeHandler.AlgorithmDone(task, err)
	return err
}

// StreamConsumer 流式处理
func (s *StreamServerImpl) streamConsumer(stream grpc.ServerStream) error {
	// 接收到一个请求
	var wantGetTaskReq *pb.ConsumerTaskRequest
	err := stream.RecvMsg(wantGetTaskReq)
	if err != nil {
		log.Printf("receive get task stage meet error:%v", err)
		return err
	}
	// 如果支持 Algorithm 任务
	if SupportAlgorithmTask(wantGetTaskReq) {
		waitTaskCtx, cancel := context.WithTimeout(stream.Context(), time.Second*time.Duration(60))
		defer cancel()
		task, ok := DefaultTaskConsumeHandler.TryPopAlgorithmTask(waitTaskCtx)
		// 有任务
		if ok {
			return s.AlgorithmHandler(stream, task)
		}
	}

	// 没有任务
	rsp := &pb.ConsumerTaskReply{
		Context: nil,
		Fine:    true,
	}
	stream.SendMsg(rsp)
	for {
		var tmp interface{}
		err := stream.RecvMsg(tmp)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("receive failed: %s", err.Error())
			return err
		}
	}
	return nil
}

func (s *StreamServerImpl) StreamConsumer(stream grpc.ServerStream) error {
	err := s.streamConsumer(stream)
	if err == io.EOF {
		log.Printf("client close connection.")
	}
	if err != nil && err != io.EOF {
		log.Printf("Process task meet error: %v", err)
	}
	return nil
}

func SupportAlgorithmTask(wantGetTask *pb.ConsumerTaskRequest) bool {
	for _, v := range wantGetTask.GetTaskTypes().GetOpt() {
		switch v.GetTasktype().(type) {
		case *pb.TaskTypeEnum_TAlgorithm:
			return true
		}
	}
	return false
}
