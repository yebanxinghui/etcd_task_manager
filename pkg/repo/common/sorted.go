package common

import (
	"sort"

	pb "github.com/golang/etcd-task-manager/pb"
)

type Int32s []int32

func (s Int32s) Len() int { return len(s) }

func (s Int32s) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s Int32s) Less(i, j int) bool { return s[i] < s[j] }

func SortedInt32s(datas []int32) []int32 {
	is := datas
	sort.Sort(Int32s(is))
	return is
}

type ContextCompare []*pb.TaskContext
func (s ContextCompare) Len() int { return len(s) }

func (s ContextCompare) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ContextCompare) Less(i, j int) bool { return s[i].GetInnerFeedID() < s[j].GetInnerFeedID() }

func SortedContext(datas []*pb.TaskContext) []*pb.TaskContext {
	is := datas
	sort.Sort(ContextCompare(is))
	return is
}
