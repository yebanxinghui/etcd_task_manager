package common

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"

	pb "github.com/golang/etcd-task-manager/pb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

func TaskCommonVideoInfoToSliceVideoInfo(dst *pb.SyncProcessRequest, src *pb.TaskContext) {
	switch src.VideoInfo.(type) {
	case *pb.TaskContext_TencentVideoVid:
		dst.VideoInfo = &pb.SyncProcessRequest_Vid{Vid: src.GetTencentVideoVid()}
	case *pb.TaskContext_VideoUrl:
		dst.VideoInfo = &pb.SyncProcessRequest_VideoUrl{VideoUrl: src.GetVideoUrl()}
	}
}

func DumpsPBToFile(body interface{}, fileName string) error {
	Marshaler := jsonpb.Marshaler{EmitDefaults: true, OrigName: true, EnumsAsInts: true}
	input, ok := body.(proto.Message)
	if !ok {
		return errors.New("not pb json")
	}
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("open file err: [%v]", err)
		return err
	}
	defer f.Close()

	err = Marshaler.Marshal(f, input)
	if err != nil {
		log.Printf("marshal err: [%v]", err)
		return err
	}
	return nil
}

func MustDumpsPBToBytes(body interface{}) []byte {
	input, ok := body.(proto.Message)
	if !ok {
		return MustEncodeToJson(body)
	}

	var err error
	Marshaler := jsonpb.Marshaler{EmitDefaults: true, OrigName: true, EnumsAsInts: true}
	var buf []byte
	w := bytes.NewBuffer(buf)
	err = Marshaler.Marshal(w, input)
	if err != nil {
		return []byte(fmt.Sprintf("{\"err\":\"[MustDumpsPBToString] error: %v\"}", err))
	}
	return w.Bytes()
}

func MustDumpsPBToString(body interface{}) string {
	return string(MustDumpsPBToBytes(body))
}

func LoadsJsonToPb(in []byte, body interface{}) error {
	var Unmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: true}
	input, ok := body.(proto.Message)
	if !ok {
		return DecodeBytesToStruct(in, body)
	}
	return Unmarshaler.Unmarshal(bytes.NewReader(in), input)
}
