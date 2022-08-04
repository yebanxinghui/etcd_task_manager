package common


import (
    "encoding/json"
    "fmt"
    "reflect"
)

func decodeBytesToStruct(bs []byte, obj interface{}) error {
    return json.Unmarshal(bs, obj)
}

func DecodeBytesToStruct(bs []byte, obj interface{}) error {
    return decodeBytesToStruct(bs, obj)
}

func decodeStruct(js string, obj interface{}) error {
    return decodeBytesToStruct([]byte(js), obj)
}

func DecodeToStruct(js string, obj interface{}) error {
    return decodeStruct(js, obj)
}

func decodeByteArrayToInterface(bs []byte) (interface{}, error) {
    var obj interface{}
    err := json.Unmarshal(bs, &obj)
    if err != nil {
        return nil, err
    }
    return obj, nil
}

func decodeToInterface(js string) (interface{}, error) {
    return decodeByteArrayToInterface([]byte(js))
}

func DecodeBytesToInterface(bs []byte) (interface{}, error) {
    return decodeByteArrayToInterface(bs)
}

func DecodeToInterface(js string) (interface{}, error) {
    return decodeToInterface(js)
}

func MustDecodeByteArrayToInterface(bs []byte) interface{} {
    ins, err := DecodeBytesToInterface(bs)
    if err != nil {
        return struct{}{}
    }
    return ins
}

func MustDecodeToInterface(js string) interface{} {
    ins, err := DecodeToInterface(js)
    if err != nil {
        return struct{}{}
    }
    return ins
}

func DecodeByteArrayToMap(bs []byte) (map[string]interface{}, error) {
    ins, err := DecodeBytesToInterface(bs)
    if err != nil {
        return map[string]interface{}{}, err
    }
    if !isMapStringInterface(ins) {
        return map[string]interface{}{}, fmt.Errorf("cannot convert %v to map[string]interface{}", reflect.TypeOf(ins))
    }
    return ins.(map[string]interface{}), nil
}

func DecodeToMap(js string) (map[string]interface{}, error) {
    ins, err := DecodeToInterface(js)
    if err != nil {
        return map[string]interface{}{}, err
    }
    if !isMapStringInterface(ins) {
        return map[string]interface{}{}, fmt.Errorf("cannot convert %v to map[string]interface{}", reflect.TypeOf(ins))
    }
    return ins.(map[string]interface{}), nil
}

func MustDecodeToMap(js string) map[string]interface{} {
    ins, err := DecodeToMap(js)
    if err != nil {
        return map[string]interface{}{"err": err.Error()}
    }
    return ins
}

func encodeToJson(ins interface{}) ([]byte, error) {
    bs, err := json.Marshal(ins)
    if err != nil {
        return []byte("{}"), err
    }
    return bs, nil
}

func encodeToJsonString(ins interface{}) (string, error) {
    bs, err := encodeToJson(ins)
    if err != nil {
        return "{}", err
    }
    return string(bs), nil
}

func EncodeToJson(ins interface{}) ([]byte, error) {
    return encodeToJson(ins)
}

func EncodeToJsonString(ins interface{}) (string, error) {
    return encodeToJsonString(ins)
}

func MustEncodeToJson(ins interface{}) []byte {
    bs, err := encodeToJson(ins)
    if err != nil {
        return []byte("{}")
    }
    return bs
}

func MustEncodeToJsonString(ins interface{}) string {
    s, err := encodeToJsonString(ins)
    if err != nil {
        return "{}"
    }
    return s
}

func deepCopyMap(src map[string]interface{}, dst map[string]interface{}) {
    for key, value := range src {
        switch src[key].(type) {
        case map[string]interface{}:
            dst[key] = map[string]interface{}{}
            deepCopyMap(src[key].(map[string]interface{}), dst[key].(map[string]interface{}))
        default:
            dst[key] = value
        }
    }
}

func DeepCopyMap(src map[string]interface{}) map[string]interface{} {
    var dst = make(map[string]interface{})
    deepCopyMap(src, dst)
    return dst
}

func GetEmptyType(i interface{}) interface{} {
    switch i.(type) {
    case int:
        return (int)(0)
    case int8:
        return (int8)(0)
    case int16:
        return (int16)(0)
    case int32:
        return (int32)(0)
    case int64:
        return (int64)(0)
    case uint:
        return (uint)(0)
    case uint8:
        return (uint8)(0)
    case uint16:
        return (uint16)(0)
    case uint32:
        return (uint32)(0)
    }
    return nil
}

func isMapStringInterface(i interface{}) bool {
    if reflect.ValueOf(i).Type().ConvertibleTo(reflect.ValueOf(map[string]interface{}{}).Type()) {
        return true
    }
    return false
}