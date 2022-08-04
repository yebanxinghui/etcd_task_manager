package common

import (
    "strconv"
    "strings"
)

// RemoveDuplication 去掉relativePts里的重复帧
func RemoveDuplication(arr []int32) []int32 {
    arrCopy := make([]int32, 0, len(arr))
    set := make(map[int32]struct{}, len(arr))
    for _, v := range arr {
        if _, ok := set[v]; ok {
            continue
        }
        set[v] = struct{}{}
        arrCopy = append(arrCopy, v)
    }
    return arrCopy
}

func SplitKey(s string) (string, string, int32){
    slice := strings.Split(s, "/")
    length := len(slice)
    var (
        innerFeedId string = ""
        taskType string = ""
        duration int = 0
    )
    if length >= 3 {
        innerFeedId = slice[length-3]
        taskType =  slice[length-2]
        durationStr := slice[length-1]
        durationStr = durationStr[:strings.Index(durationStr, ".")]
        duration, _ = strconv.Atoi(durationStr)
    }
    return innerFeedId, taskType, int32(duration)
}
