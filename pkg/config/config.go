package config

var (
	GlobalConfig *Config
	//cfg atomic.Value
)

func Init() error {
    // 初始化配置项
    GlobalConfig = new(Config)

    // 监听配置是否发生修改
    //watchConf()

    return nil
}

