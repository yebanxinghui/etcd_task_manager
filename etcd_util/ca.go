package etcd_util

import (
    "crypto/tls"
    "crypto/x509"
    "embed"
    "encoding/base64"
    "encoding/pem"
    "fmt"
    "log"
    "strings"
)

//go:embed "*.info" "*.ca" "*.ce" "*.pr"
var fs embed.FS

type EtcdCA struct {
    Key       string
    Addr      string
    TLSConfig *tls.Config
}

var cas = make(map[string]EtcdCA)

func GetEtcdConfig(key string) *EtcdCA {
    if i, ok := cas[key]; ok {
        return &i
    }
    return nil
}

func Base64Decode(bs []byte) ([]byte, error) {
    decoded, err := base64.StdEncoding.DecodeString(string(bs))
    return decoded, err
}

func DecodeCACertFile(bs []byte) (*x509.CertPool, error) {
    block, pemByte := pem.Decode(bs)
    if block == nil || pemByte == nil {
        return nil, fmt.Errorf("DecodeCACertFile: decode faild.")
    }
    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
        return nil, fmt.Errorf("DecodeCACertFile: parse faild: %v", err)
    }
    certPool := x509.NewCertPool()
    certPool.AddCert(cert)
    return certPool, nil
}

func ParseTLSConfig(pool *x509.CertPool, cert []byte, privateKey []byte) (*tls.Config, error) {
    cer, err := tls.X509KeyPair(cert, privateKey)
    if err != nil {
        return nil, fmt.Errorf("ParseTLSConfig: pair: %v", err)
    }
    return &tls.Config{
        RootCAs:      pool,
        Certificates: []tls.Certificate{cer},
    }, nil
}

func LoadConfig(fs *embed.FS, key string) error {
    var err error

    //binfo, err := fs.ReadFile(fmt.Sprintf("%s.info", key))
    binfo, err := fs.ReadFile(key + ".info")
    if err != nil {
        return fmt.Errorf("cannot find %s.info CACert File: %v", key, err)
    }

    //bca, err := fs.ReadFile(fmt.Sprintf("%s.ca", key))
    bca, err := fs.ReadFile(key + ".ca")
    if err != nil {
        return fmt.Errorf("cannot find %s.ca CACert File: %v", key, err)
    }
    bcar, err := Base64Decode(bca)
    if err != nil {
        return fmt.Errorf("cannot decode base64 %s.ca CACert File: %v", key, err)
    }

    pool, err := DecodeCACertFile(bcar)
    if err != nil {
        return fmt.Errorf("cannot decode pem %s.ca CACert File: %v", key, err)
    }

    //bce, err := fs.ReadFile(fmt.Sprintf("%s.ce", key))
    bce, err := fs.ReadFile(key + ".ce")
    if err != nil {
        return fmt.Errorf("cannot find %s.ce CACert File: %v", key, err)
    }
    bcer, err := Base64Decode(bce)
    if err != nil {
        return fmt.Errorf("cannot decode base64 %s.ce CACert File: %v", key, err)
    }

    //bpr, err := fs.ReadFile(fmt.Sprintf("%s.pr", key))
    bpr, err := fs.ReadFile(key + ".pr")
    if err != nil {
        return fmt.Errorf("cannot find %s.pr CACert File: %v", key, err)
    }
    bprr, err := Base64Decode(bpr)
    if err != nil {
        return fmt.Errorf("cannot decode base64 %s.pr CACert File: %v", key, err)
    }

    cfg, err := ParseTLSConfig(pool, bcer, bprr)
    if err != nil {
        return fmt.Errorf("ParseTLSConfig key %s error: %v", key, err)
    }

    cas[key] = EtcdCA{
        Key:       key,
        Addr:      string(binfo),
        TLSConfig: cfg,
    }
    return nil
}

func init() {
    ds, err := fs.ReadDir(".")
    if err != nil {
        panic(err)
    }

    for _, v := range ds {
        if !strings.Contains(v.Name(), ".info") {
            continue
        }

        key := strings.Split(v.Name(), ".")[0]
        err := LoadConfig(&fs, key)
        if err != nil {
            panic(fmt.Errorf("load etcd ca key %s meet error %v", key, err))
        }
        log.Printf("init etcd ca config: %s success.", v.Name())
    }
}
