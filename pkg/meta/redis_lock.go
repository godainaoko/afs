// pkg/meta/redis_lock.go

package meta

import (
    "AveFS/pkg/utils"
    "errors"
    "github.com/redis/go-redis/v9"
    "strconv"
    "strings"
    "syscall"
    "time"
)

func (rm *redisMeta) Flock(ctx Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
    ikey := rm.flockKey(inode)
    lkey := rm.ownerKey(owner)
    if ltype == F_UNLCK {
        _, err := rm.rdb.HDel(ctx, ikey, lkey).Result()
        return errno(err)
    }
    var err syscall.Errno
    for {
        err = rm.txn(ctx, func(tx *redis.Tx) error {
            owners, err := tx.HGetAll(ctx, ikey).Result()
            if err != nil {
                return err
            }
            if ltype == F_RDLCK {
                for _, v := range owners {
                    if v == "W" {
                        return syscall.EAGAIN
                    }
                }
                _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                    pipe.HSet(ctx, ikey, lkey, "R")
                    return nil
                })
                return err
            }
            delete(owners, lkey)
            if len(owners) > 0 {
                return syscall.EAGAIN
            }
            _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                pipe.HSet(ctx, ikey, lkey, "W")
                pipe.SAdd(ctx, rm.lockedKey(rm.sid), ikey)
                return nil
            })
            return err
        }, ikey)

        if !block || !errors.Is(err, syscall.EAGAIN) {
            break
        }
        if ltype == F_WRLCK {
            time.Sleep(time.Millisecond * 1)
        } else {
            time.Sleep(time.Millisecond * 10)
        }
        if ctx.Canceled() {
            return syscall.EINTR
        }
    }
    return err
}

type plockRecord struct {
    ltype uint32
    pid   uint32
    start uint64
    end   uint64
}

func loadLocks(d []byte) []plockRecord {
    var ls []plockRecord
    rb := utils.FromBuffer(d)
    for rb.HasMore() {
        ls = append(ls, plockRecord{rb.Get32(), rb.Get32(), rb.Get64(), rb.Get64()})
    }
    return ls
}

func dumpLocks(ls []plockRecord) []byte {
    wb := utils.NewBuffer(uint32(len(ls)) * 24)
    for _, l := range ls {
        wb.Put32(l.ltype)
        wb.Put32(l.pid)
        wb.Put64(l.start)
        wb.Put64(l.end)
    }
    return wb.Bytes()
}

func insertLocks(ls []plockRecord, i int, nl plockRecord) []plockRecord {
    nls := make([]plockRecord, len(ls)+1)
    copy(nls[:i], ls[:i])
    nls[i] = nl
    copy(nls[i+1:], ls[i:])
    ls = nls
    return ls
}

func updateLocks(ls []plockRecord, nl plockRecord) []plockRecord {
    // ls is ordered by l.start without overlap
    var i int
    for i < len(ls) && nl.end > nl.start {
        l := ls[i]
        if l.end < nl.start {
        } else if l.start < nl.start {
            ls = insertLocks(ls, i+1, plockRecord{nl.ltype, nl.pid, nl.start, l.end})
            ls[i].end = nl.start
            i++
            nl.start = l.end
        } else if l.end < nl.end {
            ls[i].ltype = nl.ltype
            ls[i].start = nl.start
            nl.start = l.end
        } else if l.start < nl.end {
            ls = insertLocks(ls, i, nl)
            ls[i+1].start = nl.end
            nl.start = nl.end
        } else {
            ls = insertLocks(ls, i, nl)
            nl.start = nl.end
        }
        i++
    }
    if nl.start < nl.end {
        ls = append(ls, nl)
    }
    i = 0
    for i < len(ls) {
        if ls[i].ltype == F_UNLCK || ls[i].start == ls[i].end {
            // remove the empty one
            copy(ls[i:], ls[i+1:])
            ls = ls[:len(ls)-1]
        } else {
            if i+1 < len(ls) && ls[i].ltype == ls[i+1].ltype && ls[i].pid == ls[i+1].pid && ls[i].end == ls[i+1].start {
                // combine continuous range
                ls[i].end = ls[i+1].end
                ls[i+1].start = ls[i+1].end
            }
            i++
        }
    }
    return ls
}

func (rm *redisMeta) Getlk(ctx Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
    if *ltype == F_UNLCK {
        *start = 0
        *end = 0
        *pid = 0
        return 0
    }
    lkey := rm.ownerKey(owner)
    owners, err := rm.rdb.HGetAll(ctx, rm.plockKey(inode)).Result()
    if err != nil {
        return errno(err)
    }
    delete(owners, lkey) // exclude itself
    for k, d := range owners {
        ls := loadLocks([]byte(d))
        for _, l := range ls {
            // find conflicted locks
            if (*ltype == F_WRLCK || l.ltype == F_WRLCK) && *end > l.start && *start < l.end {
                *ltype = l.ltype
                *start = l.start
                *end = l.end
                sid, _ := strconv.Atoi(strings.Split(k, "_")[0])
                if int64(sid) == rm.sid {
                    *pid = l.pid
                } else {
                    *pid = 0
                }
                return 0
            }
        }
    }
    *ltype = F_UNLCK
    *start = 0
    *end = 0
    *pid = 0
    return 0
}

func (rm *redisMeta) Setlk(ctx Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
    ikey := rm.plockKey(inode)
    lkey := rm.ownerKey(owner)
    var err syscall.Errno
    lock := plockRecord{ltype, pid, start, end}
    for {
        err = rm.txn(ctx, func(tx *redis.Tx) error {
            if ltype == F_UNLCK {
                d, err := tx.HGet(ctx, ikey, lkey).Result()
                if err != nil {
                    return err
                }
                ls := loadLocks([]byte(d))
                if len(ls) == 0 {
                    return nil
                }
                ls = updateLocks(ls, lock)
                _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                    if len(ls) == 0 {
                        pipe.HDel(ctx, ikey, lkey)
                    } else {
                        pipe.HSet(ctx, ikey, lkey, dumpLocks(ls))
                    }
                    return nil
                })
                return err
            }
            owners, err := tx.HGetAll(ctx, ikey).Result()
            if err != nil {
                return err
            }
            ls := loadLocks([]byte(owners[lkey]))
            delete(owners, lkey)
            for _, d := range owners {
                ls := loadLocks([]byte(d))
                for _, l := range ls {
                    // find conflicted locks
                    if (ltype == F_WRLCK || l.ltype == F_WRLCK) && end > l.start && start < l.end {
                        return syscall.EAGAIN
                    }
                }
            }
            ls = updateLocks(ls, lock)
            _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                pipe.HSet(ctx, ikey, lkey, dumpLocks(ls))
                pipe.SAdd(ctx, rm.lockedKey(rm.sid), ikey)
                return nil
            })
            return err
        }, ikey)

        if !block || !errors.Is(err, syscall.EAGAIN) {
            break
        }
        if ltype == F_WRLCK {
            time.Sleep(time.Millisecond * 1)
        } else {
            time.Sleep(time.Millisecond * 10)
        }
        if ctx.Canceled() {
            return syscall.EINTR
        }
    }
    return err
}
