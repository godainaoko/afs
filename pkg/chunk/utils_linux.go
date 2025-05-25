// pkg/chunk/utils_linux.go

package chunk

import (
    "os"
    "syscall"
    "time"
)

func getAtime(fi os.FileInfo) time.Time {
    if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
        return time.Unix(sst.Atim.Unix())
    }
    return fi.ModTime()
}
