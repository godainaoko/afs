// pkg/meta/config.go

package meta

import "time"

// Config for clients.
type Config struct {
    Strict      bool // update ctime
    Retries     int
    CaseInsensi bool
    ReadOnly    bool
    OpenCache   time.Duration
    MountPoint  string
    Subdir      string
}

type Format struct {
    Name        string
    UUID        string
    Storage     string
    Bucket      string
    AccessKey   string
    SecretKey   string `json:",omitempty"`
    BlockSize   int
    Compression string
    Shards      int
    Partitions  int
    Capacity    uint64
    Inodes      uint64
    EncryptKey  string `json:",omitempty"`
}

func (f *Format) RemoveSecret() {
    if f.SecretKey != "" {
        f.SecretKey = "removed"
    }
    if f.EncryptKey != "" {
        f.EncryptKey = "removed"
    }
}
