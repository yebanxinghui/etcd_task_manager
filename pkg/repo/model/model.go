package model

// 保存每个分片的起始时间，结束时间与内部的相关帧时间
type DurationPts struct {
    RelativePts []int32
}

// 结构化数据
type Result struct {
    RawDataList []*StructuredDataRaw `json:"datas"`
}

type StructuredDataRaw struct {
    Component  string                               `json:"component"`
    Confidence float32                              `json:"confidence"`
    DocumentId string                               `json:"documentID"`
    Env        []*Argument                          `json:"env"`
    StartTime  uint32                               `json:"startTime"`
    EndTime    uint32                               `json:"endTime"`
    MediaInfo  *MediaInfo                           `json:"mediaInfo"`
    Namespace  string                               `json:"namespace"`
    Region     *Region                              `json:"region"`
    Tag        []*Argument                          `json:"tag"`
    Track      string                               `json:"track"`
    Type       string                               `json:"type"`
    Version    string                               `json:"version"`
}

type Argument struct {
    Key   string `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}

type MediaInfo struct {
    Type          string `json:"type,omitempty"`
    Url           string `json:"url,omitempty"`
    Vid           string `json:"vid,omitempty"`
    Aid           string `json:"aid,omitempty"`
    Mid           string `json:"mid,omitempty"`
}

type Region struct {
    StartX        float32 `json:"startX,omitempty"`
    StartY        float32 `json:"startY,omitempty"`
    EndX          float32 `json:"endX,omitempty"`
    EndY          float32 `json:"endY,omitempty"`
}
