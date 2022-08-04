package messageHandler

import (
	"context"
	"log"

	pb "github.com/golang/etcd-task-manager/pb"
	"github.com/golang/etcd-task-manager/pkg/repo/common"
)

// 只有返回成功nil，才会确认消费成功，返回err不会确认成功，会等待3s重新消费，会有重复消息，一定要保证处理函数幂等性
func TaskProcessHandler(ctx context.Context, value []byte) {
	// 解析参数
	taskCtx := &pb.TaskContext{}
	err := common.LoadsJsonToPb(value, taskCtx)
	if err != nil {
		return
	}
	log.Printf("receive tdmq task: [%v]", taskCtx)

	// 把请求记录在全局任务map里
	err = DefaultTaskConsumeHandler.PushTask(taskCtx)
	if err != nil {
		return
	}
	// 从全局任务map里取出任务处理
	err = DefaultTaskConsumeHandler.PendingTask(ctx, taskCtx)
	if err != nil {
		log.Printf("处理任务时发生错误 [%v], SourceKey: [%v]", err, taskCtx.SourceKey)
		return
	}
	log.Printf("处理任务成功 [%v]", taskCtx.SourceKey)
}

type TdmqConsumer struct {}
func (c *TdmqConsumer) Handle(ctx context.Context) error {
	//TaskProcessHandler(ctx, message)
	// 消费消息队列消息
	return nil
}

