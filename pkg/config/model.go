package config

type Config struct {
    Cos       CosStruct       `yaml:"cos"`
    Algorithm AlgorithmStruct `yaml:"algorithm"`
}

type CosStruct struct {
    CosID  string `yaml:"cos_id"`
    CosKey string `yaml:"cos_key"`
    Region string `yaml:"region"`
    Bucket string `yaml:"bucket"`
    Prefix string `yaml:"prefix"`
    Domain string `yaml:"domain"`
}

type AlgorithmStruct struct {
    Decode   string `yaml:"decode"`
    Duration int32  `yaml:"duration"`
}
