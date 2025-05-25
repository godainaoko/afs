// pkg/meta/redis.go

package meta

import (
	"AveFS/pkg/utils"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

var logger = utils.GetLogger("avefs")

const usedSpace = "usedSpace"
const totalInodes = "totalInodes"
const delfiles = "delfiles"
const allSessions = "sessions"
const sessionInfos = "sessionInfos"
const sliceRefs = "sliceRef"

const nextInode = "nextinode"
const nextChunk = "nextchunk"
const nextSession = "nextsession"

type redisMeta struct {
	sync.Mutex
	conf    *Config
	fmt     Format
	rdb     *redis.Client
	txlocks [1024]sync.Mutex // Pessimistic locks to reduce conflict on Redis

	root         Ino
	sid          int64
	usedSpace    uint64
	usedInodes   uint64
	of           *openfiles
	removedFiles map[Ino]bool
	compacting   map[uint64]bool
	deleting     chan int
	symlinks     *sync.Map
	msgCallbacks *msgCallbacks

	shaLookup  string // The SHA returned by Redis for the loaded `scriptLookup`
	shaResolve string // The SHA returned by Redis for the loaded `scriptResolve`
}

var _ Meta = &redisMeta{}

type msgCallbacks struct {
	sync.Mutex
	callbacks map[uint32]MsgCallback
}

func init() {
	Register("redis", newRedisMeta)
}

// newRedisMeta return a meta-store using Redis.
func newRedisMeta(driver, addr string, conf *Config) (Meta, error) {
	url := driver + "://" + addr
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", url, err)
	}

	var rdb *redis.Client
	if strings.Contains(opt.Addr, ",") {
		var fopt redis.FailoverOptions
		ps := strings.Split(opt.Addr, ",")
		fopt.MasterName = ps[0]
		fopt.SentinelAddrs = ps[1:]

		defaultSentinelPort := "26379"
		for i, saddr := range fopt.SentinelAddrs {
			h, p, err := net.SplitHostPort(saddr)
			if err != nil {
				// If SplitHostPort fails, assume it's just a host and add the default port
				fopt.SentinelAddrs[i] = net.JoinHostPort(saddr, defaultSentinelPort)
			} else if p == "" {
				fopt.SentinelAddrs[i] = net.JoinHostPort(h, defaultSentinelPort)
			}
		}

		fopt.Username = opt.Username
		fopt.Password = opt.Password
		if fopt.Password == "" && os.Getenv("REDIS_PASSWORD") != "" {
			fopt.Password = os.Getenv("REDIS_PASSWORD")
		}
		fopt.SentinelPassword = os.Getenv("SENTINEL_PASSWORD")
		fopt.DB = opt.DB
		fopt.TLSConfig = opt.TLSConfig
		fopt.MaxRetries = conf.Retries
		fopt.MinRetryBackoff = time.Millisecond * 100
		fopt.MaxRetryBackoff = time.Minute * 1
		fopt.ReadTimeout = time.Second * 30
		fopt.WriteTimeout = time.Second * 5
		rdb = redis.NewFailoverClient(&fopt)
	} else {
		if opt.Password == "" && os.Getenv("REDIS_PASSWORD") != "" {
			opt.Password = os.Getenv("REDIS_PASSWORD")
		}
		opt.MaxRetries = conf.Retries
		opt.MinRetryBackoff = time.Millisecond * 100
		opt.MaxRetryBackoff = time.Minute * 1
		opt.ReadTimeout = time.Second * 30
		opt.WriteTimeout = time.Second * 5
		rdb = redis.NewClient(opt)
	}

	m := &redisMeta{
		conf:         conf,
		rdb:          rdb,
		of:           newOpenFiles(conf.OpenCache),
		removedFiles: make(map[Ino]bool),
		compacting:   make(map[uint64]bool),
		deleting:     make(chan int, 2),
		symlinks:     &sync.Map{},
		msgCallbacks: &msgCallbacks{
			callbacks: make(map[uint32]MsgCallback),
		},
	}

	m.checkServerConfig()
	m.root = 1
	m.root, err = lookupSubdir(m, conf.Subdir)
	return m, err
}

func lookupSubdir(m Meta, subdir string) (Ino, error) {
	var root Ino = 1
	for subdir != "" {
		ps := strings.SplitN(subdir, "/", 2)
		if ps[0] != "" {
			var attr Attr
			r := m.Lookup(Background, root, ps[0], &root, &attr)
			if r != 0 {
				return 0, fmt.Errorf("lookup subdir %s: %s", ps[0], r)
			}
			if attr.Typ != TypeDirectory {
				return 0, fmt.Errorf("%s is not a directory", ps[0])
			}
		}
		if len(ps) == 1 {
			break
		}
		subdir = ps[1]
	}
	return root, nil
}

func (rm *redisMeta) checkRoot(inode Ino) Ino {
	if inode == 1 {
		return rm.root
	}
	return inode
}

func (rm *redisMeta) Name() string {
	return "redis"
}

func (rm *redisMeta) Init(format Format, force bool) error {
	body, err := rm.rdb.Get(Background, "setting").Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if err == nil {
		var old Format
		err = json.Unmarshal(body, &old)
		if err != nil {
			logger.Fatalf("existing format is broken: %s", err)
		}
		if force {
			old.SecretKey = "removed"
			logger.Warnf("Existing volume will be overwrited: %+v", old)
		} else {
			format.UUID = old.UUID
			old.AccessKey = format.AccessKey
			old.SecretKey = format.SecretKey
			old.Capacity = format.Capacity
			old.Inodes = format.Inodes
			if format != old {
				old.SecretKey = ""
				format.SecretKey = ""
				return fmt.Errorf("cannot update format from %+v to %+v", old, format)
			}
		}
	}

	data, err := json.MarshalIndent(format, "", "")
	if err != nil {
		logger.Fatalf("json: %s", err)
	}
	err = rm.rdb.Set(Background, "setting", data, 0).Err()
	if err != nil {
		return err
	}
	rm.fmt = format
	if body != nil {
		return nil
	}

	// root inode
	var attr Attr
	attr.Typ = TypeDirectory
	attr.Mode = 0777
	ts := time.Now().Unix()
	attr.Atime = ts
	attr.Mtime = ts
	attr.Ctime = ts
	attr.Nlink = 2
	attr.Length = 4 << 10
	attr.Parent = 1
	return rm.rdb.Set(Background, rm.inodeKey(1), rm.marshal(&attr), 0).Err()
}

func (rm *redisMeta) Load() (*Format, error) {
	body, err := rm.rdb.Get(Background, "setting").Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("database is not formatted")
	}
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &rm.fmt)
	if err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	return &rm.fmt, nil
}

func (rm *redisMeta) NewSession() error {
	go rm.refreshUsage()
	if rm.conf.ReadOnly {
		return nil
	}
	var err error
	rm.sid, err = rm.rdb.Incr(Background, nextSession).Result()
	if err != nil {
		return fmt.Errorf("create session: %s", err)
	}
	logger.Debugf("session is %d", rm.sid)
	rm.rdb.ZAdd(Background, allSessions, redis.Z{Score: float64(time.Now().Unix()), Member: strconv.Itoa(int(rm.sid))})
	info, err := newSessionInfo()
	if err != nil {
		return fmt.Errorf("new session info: %s", err)
	}
	info.MountPoint = rm.conf.MountPoint
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("json: %s", err)
	}
	rm.rdb.HSet(Background, sessionInfos, rm.sid, data)

	rm.shaLookup, err = rm.rdb.ScriptLoad(Background, scriptLookup).Result()
	if err != nil {
		logger.Warnf("load scriptLookup: %v", err)
		rm.shaLookup = ""
	}
	rm.shaResolve, err = rm.rdb.ScriptLoad(Background, scriptResolve).Result()
	if err != nil {
		logger.Warnf("load scriptResolve: %v", err)
		rm.shaResolve = ""
	}

	go rm.refreshSession()
	go rm.cleanupDeletedFiles()
	go rm.cleanupSlices()
	return nil
}

func (rm *redisMeta) refreshUsage() {
	for {
		used, _ := rm.rdb.IncrBy(Background, usedSpace, 0).Result()
		atomic.StoreUint64(&rm.usedSpace, uint64(used))
		inodes, _ := rm.rdb.IncrBy(Background, totalInodes, 0).Result()
		atomic.StoreUint64(&rm.usedInodes, uint64(inodes))
		time.Sleep(time.Second * 10)
	}
}

func (rm *redisMeta) checkQuota(size, inodes int64) bool {
	if size > 0 && rm.fmt.Capacity > 0 && atomic.LoadUint64(&rm.usedSpace)+uint64(size) > rm.fmt.Capacity {
		return true
	}
	return inodes > 0 && rm.fmt.Inodes > 0 && atomic.LoadUint64(&rm.usedInodes)+uint64(inodes) > rm.fmt.Inodes
}

func (rm *redisMeta) getSession(sid string, detail bool) (*Session, error) {
	ctx := Background
	info, err := rm.rdb.HGet(ctx, sessionInfos, sid).Bytes()
	if errors.Is(err, redis.Nil) { // legacy client has no info
		info = []byte("{}")
	} else if err != nil {
		return nil, fmt.Errorf("HGet %s %s: %s", sessionInfos, sid, err)
	}
	var s Session
	if err := json.Unmarshal(info, &s); err != nil {
		return nil, fmt.Errorf("corrupted session info; json error: %s", err)
	}
	s.Sid, _ = strconv.ParseUint(sid, 10, 64)
	if detail {
		inodes, err := rm.rdb.SMembers(ctx, rm.sustained(int64(s.Sid))).Result()
		if err != nil {
			return nil, fmt.Errorf("SMembers %s: %s", sid, err)
		}
		s.Sustained = make([]Ino, 0, len(inodes))
		for _, sinode := range inodes {
			inode, _ := strconv.ParseUint(sinode, 10, 64)
			s.Sustained = append(s.Sustained, Ino(inode))
		}

		locks, err := rm.rdb.SMembers(ctx, rm.lockedKey(int64(s.Sid))).Result()
		if err != nil {
			return nil, fmt.Errorf("SMembers %s: %s", sid, err)
		}
		s.Flocks = make([]Flock, 0, len(locks)) // greedy
		s.Plocks = make([]Plock, 0, len(locks))
		for _, lock := range locks {
			owners, err := rm.rdb.HGetAll(ctx, lock).Result()
			if err != nil {
				return nil, fmt.Errorf("HGetAll %s: %s", lock, err)
			}
			isFlock := strings.HasPrefix(lock, "lockf")
			inode, _ := strconv.ParseUint(lock[5:], 10, 64)
			for k, v := range owners {
				parts := strings.Split(k, "_")
				if parts[0] != sid {
					continue
				}
				owner, _ := strconv.ParseUint(parts[1], 16, 64)
				if isFlock {
					s.Flocks = append(s.Flocks, Flock{Ino(inode), owner, v})
				} else {
					s.Plocks = append(s.Plocks, Plock{Ino(inode), owner, []byte(v)})
				}
			}
		}
	}
	return &s, nil
}

func (rm *redisMeta) GetSession(sid uint64) (*Session, error) {
	key := strconv.FormatUint(sid, 10)
	score, err := rm.rdb.ZScore(Background, allSessions, key).Result()
	if err != nil {
		return nil, err
	}
	s, err := rm.getSession(key, true)
	if err != nil {
		return nil, err
	}
	s.Heartbeat = time.Unix(int64(score), 0)
	return s, nil
}

func (rm *redisMeta) ListSessions() ([]*Session, error) {
	keys, err := rm.rdb.ZRangeWithScores(Background, allSessions, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	sessions := make([]*Session, 0, len(keys))
	for _, k := range keys {
		s, err := rm.getSession(k.Member.(string), false)
		if err != nil {
			logger.Errorf("get session: %s", err)
			continue
		}
		s.Heartbeat = time.Unix(int64(k.Score), 0)
		sessions = append(sessions, s)
	}
	return sessions, nil
}

func (rm *redisMeta) OnMsg(mtype uint32, cb MsgCallback) {
	rm.msgCallbacks.Lock()
	defer rm.msgCallbacks.Unlock()
	rm.msgCallbacks.callbacks[mtype] = cb
}

func (rm *redisMeta) newMsg(mid uint32, args ...interface{}) error {
	rm.msgCallbacks.Lock()
	cb, ok := rm.msgCallbacks.callbacks[mid]
	rm.msgCallbacks.Unlock()
	if ok {
		return cb(args...)
	}
	return fmt.Errorf("message %d is not supported", mid)
}

func (rm *redisMeta) sustained(sid int64) string {
	return "session" + strconv.FormatInt(sid, 10)
}

func (rm *redisMeta) lockedKey(sid int64) string {
	return "locked" + strconv.FormatInt(sid, 10)
}

func (rm *redisMeta) symKey(inode Ino) string {
	return "s" + inode.String()
}

func (rm *redisMeta) inodeKey(inode Ino) string {
	return "i" + inode.String()
}

func (rm *redisMeta) entryKey(parent Ino) string {
	return "d" + parent.String()
}

func (rm *redisMeta) chunkKey(inode Ino, idx uint32) string {
	return "c" + inode.String() + "_" + strconv.FormatInt(int64(idx), 10)
}

func (rm *redisMeta) sliceKey(chunkid uint64, size uint32) string {
	return "k" + strconv.FormatUint(chunkid, 10) + "_" + strconv.FormatUint(uint64(size), 10)
}

func (rm *redisMeta) xattrKey(inode Ino) string {
	return "x" + inode.String()
}

func (rm *redisMeta) flockKey(inode Ino) string {
	return "lockf" + inode.String()
}

func (rm *redisMeta) ownerKey(owner uint64) string {
	return fmt.Sprintf("%d_%016X", rm.sid, owner)
}

func (rm *redisMeta) plockKey(inode Ino) string {
	return "lockp" + inode.String()
}

func (rm *redisMeta) nextInode() (Ino, error) {
	ino, err := rm.rdb.Incr(Background, nextInode).Uint64()
	if ino == 1 {
		ino, err = rm.rdb.Incr(Background, nextInode).Uint64()
	}
	return Ino(ino), err
}

func (rm *redisMeta) packEntry(_type uint8, inode Ino) []byte {
	wb := utils.NewBuffer(9)
	wb.Put8(_type)
	wb.Put64(uint64(inode))
	return wb.Bytes()
}

func (rm *redisMeta) parseEntry(buf []byte) (uint8, Ino) {
	if len(buf) != 9 {
		panic("invalid entry")
	}
	return buf[0], Ino(binary.BigEndian.Uint64(buf[1:]))
}

func (rm *redisMeta) parseAttr(buf []byte, attr *Attr) {
	if attr == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	attr.Flags = rb.Get8()
	attr.Mode = rb.Get16()
	attr.Typ = uint8(attr.Mode >> 12)
	attr.Mode &= 0xfff
	attr.Uid = rb.Get32()
	attr.Gid = rb.Get32()
	attr.Atime = int64(rb.Get64())
	attr.Atimensec = rb.Get32()
	attr.Mtime = int64(rb.Get64())
	attr.Mtimensec = rb.Get32()
	attr.Ctime = int64(rb.Get64())
	attr.Ctimensec = rb.Get32()
	attr.Nlink = rb.Get32()
	attr.Length = rb.Get64()
	attr.Rdev = rb.Get32()
	if rb.Left() >= 8 {
		attr.Parent = Ino(rb.Get64())
	}
	attr.Full = true
	logger.Tracef("attr: %+v -> %+v", buf, attr)
}

func (rm *redisMeta) marshal(attr *Attr) []byte {
	w := utils.NewBuffer(36 + 24 + 4 + 8)
	w.Put8(attr.Flags)
	w.Put16((uint16(attr.Typ) << 12) | (attr.Mode & 0xfff))
	w.Put32(attr.Uid)
	w.Put32(attr.Gid)
	w.Put64(uint64(attr.Atime))
	w.Put32(attr.Atimensec)
	w.Put64(uint64(attr.Mtime))
	w.Put32(attr.Mtimensec)
	w.Put64(uint64(attr.Ctime))
	w.Put32(attr.Ctimensec)
	w.Put32(attr.Nlink)
	w.Put64(attr.Length)
	w.Put32(attr.Rdev)
	w.Put64(uint64(attr.Parent))
	logger.Tracef("attr: %+v -> %+v", attr, w.Bytes())
	return w.Bytes()
}

func align4K(length uint64) int64 {
	if length == 0 {
		return 1 << 12
	}
	return int64((((length - 1) >> 12) + 1) << 12)
}

func (rm *redisMeta) StatFS(ctx Context, totalspace, availspace, iused, iavail *uint64) syscall.Errno {
	if rm.fmt.Capacity > 0 {
		*totalspace = rm.fmt.Capacity
	} else {
		*totalspace = 1 << 50
	}
	c, cancel := context.WithTimeout(ctx, time.Millisecond*300)
	defer cancel()
	used, _ := rm.rdb.IncrBy(c, usedSpace, 0).Result()
	if used < 0 {
		used = 0
	}
	used = ((used >> 16) + 1) << 16 // aligned to 64K
	if rm.fmt.Capacity > 0 {
		if used > int64(*totalspace) {
			*totalspace = uint64(used)
		}
	} else {
		for used*10 > int64(*totalspace)*8 {
			*totalspace *= 2
		}
	}
	*availspace = *totalspace - uint64(used)
	inodes, _ := rm.rdb.IncrBy(c, totalInodes, 0).Result()
	if inodes < 0 {
		inodes = 0
	}
	*iused = uint64(inodes)
	if rm.fmt.Inodes > 0 {
		if *iused > rm.fmt.Inodes {
			*iavail = 0
		} else {
			*iavail = rm.fmt.Inodes - *iused
		}
	} else {
		*iavail = 10 << 20
	}
	return 0
}

func GetSummary(r Meta, ctx Context, inode Ino, summary *Summary) syscall.Errno {
	var attr Attr
	if st := r.GetAttr(ctx, inode, &attr); st != 0 {
		return st
	}
	if attr.Typ == TypeDirectory {
		var entries []*Entry
		if st := r.Readdir(ctx, inode, 1, &entries); st != 0 {
			return st
		}
		for _, e := range entries {
			if e.Inode == inode || len(e.Name) == 2 && bytes.Equal(e.Name, []byte("..")) {
				continue
			}
			if e.Attr.Typ == TypeDirectory {
				if st := GetSummary(r, ctx, e.Inode, summary); st != 0 {
					return st
				}
			} else {
				summary.Files++
				summary.Length += e.Attr.Length
				summary.Size += uint64(align4K(e.Attr.Length))
			}
		}
		summary.Dirs++
		summary.Size += 4096
	} else {
		summary.Files++
		summary.Length += attr.Length
		summary.Size += uint64(align4K(attr.Length))
	}
	return 0
}

func (rm *redisMeta) resolveCase(ctx Context, parent Ino, name string) *Entry {
	var entries []*Entry
	_ = rm.Readdir(ctx, parent, 0, &entries)
	for _, e := range entries {
		n := string(e.Name)
		if strings.EqualFold(name, n) {
			return e
		}
	}
	return nil
}

func (rm *redisMeta) Lookup(ctx Context, parent Ino, name string, inode *Ino, attr *Attr) syscall.Errno {
	var foundIno Ino
	var encodedAttr []byte
	var err error
	parent = rm.checkRoot(parent)
	entryKey := rm.entryKey(parent)
	if len(rm.shaLookup) > 0 && attr != nil && !rm.conf.CaseInsensi {
		var res interface{}
		res, err = rm.rdb.EvalSha(ctx, rm.shaLookup, []string{entryKey, name}).Result()
		if err != nil {
			if strings.Contains(err.Error(), "NOSCRIPT") {
				var err2 error
				rm.shaLookup, err2 = rm.rdb.ScriptLoad(Background, scriptLookup).Result()
				if err2 != nil {
					logger.Warnf("load scriptLookup: %s", err2)
				} else {
					logger.Info("loaded script for lookup")
				}
				return rm.Lookup(ctx, parent, name, inode, attr)
			}
			if strings.Contains(err.Error(), "Error running script") {
				logger.Warnf("eval lookup: %s", err)
				rm.shaLookup = ""
				return rm.Lookup(ctx, parent, name, inode, attr)
			}
			return errno(err)
		}
		vals, ok := res.([]interface{})
		if !ok {
			return errno(fmt.Errorf("invalid script result: %v", res))
		}
		returnedIno, ok := vals[0].(int64)
		if !ok {
			return errno(fmt.Errorf("invalid script result: %v", res))
		}
		returnedAttr, ok := vals[1].(string)
		if !ok {
			return errno(fmt.Errorf("invalid script result: %v", res))
		}
		if returnedAttr == "" {
			return syscall.ENOENT
		}
		foundIno = Ino(returnedIno)
		encodedAttr = []byte(returnedAttr)
	} else {
		var buf []byte
		buf, err = rm.rdb.HGet(ctx, entryKey, name).Bytes()
		if err == nil {
			_, foundIno = rm.parseEntry(buf)
		}
		if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi {
			e := rm.resolveCase(ctx, parent, name)
			if e != nil {
				foundIno = e.Inode
				err = nil
			}
		}
		if err != nil {
			return errno(err)
		}
		if attr != nil {
			encodedAttr, err = rm.rdb.Get(ctx, rm.inodeKey(foundIno)).Bytes()
		}
	}

	if err == nil && attr != nil {
		rm.parseAttr(encodedAttr, attr)
	}
	if inode != nil {
		*inode = foundIno
	}
	return errno(err)
}

func (rm *redisMeta) Resolve(ctx Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	if len(rm.shaResolve) == 0 || rm.conf.CaseInsensi {
		return syscall.ENOTSUP
	}
	parent = rm.checkRoot(parent)
	args := []string{parent.String(), path,
		strconv.FormatUint(uint64(ctx.Uid()), 10),
		strconv.FormatUint(uint64(ctx.Gid()), 10)}
	res, err := rm.rdb.EvalSha(ctx, rm.shaResolve, args).Result()
	if err != nil {
		fields := strings.Fields(err.Error())
		lastError := fields[len(fields)-1]
		switch lastError {
		case "ENOENT":
			return syscall.ENOENT
		case "EACCESS":
			return syscall.EACCES
		case "ENOTDIR":
			return syscall.ENOTDIR
		case "ENOTSUP":
			return syscall.ENOTSUP
		default:
			logger.Warnf("resolve %d %s: %s", parent, path, err)
			rm.shaResolve = ""
			return syscall.ENOTSUP
		}
	}
	vals, ok := res.([]interface{})
	if !ok {
		logger.Errorf("invalid script result: %v", res)
		return syscall.ENOTSUP
	}
	returnedIno, ok := vals[0].(int64)
	if !ok {
		logger.Errorf("invalid script result: %v", res)
		return syscall.ENOTSUP
	}
	returnedAttr, ok := vals[1].(string)
	if !ok {
		logger.Errorf("invalid script result: %v", res)
		return syscall.ENOTSUP
	}
	if returnedAttr == "" {
		return syscall.ENOENT
	}
	if inode != nil {
		*inode = Ino(returnedIno)
	}
	rm.parseAttr([]byte(returnedAttr), attr)
	return 0
}

func accessMode(attr *Attr, uid uint32, gid uint32) uint8 {
	if uid == 0 {
		return 0x7
	}
	mode := attr.Mode
	if uid == attr.Uid {
		return uint8(mode>>6) & 7
	}
	if gid == attr.Gid {
		return uint8(mode>>3) & 7
	}
	return uint8(mode & 7)
}

func (rm *redisMeta) Access(ctx Context, inode Ino, mmask uint8, attr *Attr) syscall.Errno {
	if ctx.Uid() == 0 {
		return 0
	}

	if attr == nil || !attr.Full {
		if attr == nil {
			attr = &Attr{}
		}
		err := rm.GetAttr(ctx, inode, attr)
		if err != 0 {
			return err
		}
	}

	mode := accessMode(attr, ctx.Uid(), ctx.Gid())
	if mode&mmask != mmask {
		logger.Debugf("Access inode %d %o, mode %o, request mode %o", inode, attr.Mode, mode, mmask)
		return syscall.EACCES
	}
	return 0
}

func (rm *redisMeta) GetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	var c context.Context = ctx
	inode = rm.checkRoot(inode)
	if inode == 1 {
		var cancel func()
		c, cancel = context.WithTimeout(ctx, time.Millisecond*300)
		defer cancel()
	}
	if rm.conf.OpenCache > 0 && rm.of.Check(inode, attr) {
		return 0
	}
	a, err := rm.rdb.Get(c, rm.inodeKey(inode)).Bytes()
	if err == nil {
		rm.parseAttr(a, attr)
		if rm.conf.OpenCache > 0 {
			rm.of.Update(inode, attr)
		}
	}
	if err != nil && inode == 1 {
		err = nil
		attr.Typ = TypeDirectory
		attr.Mode = 0777
		attr.Nlink = 2
		attr.Length = 4 << 10
	}
	return errno(err)
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var eno syscall.Errno
	if errors.As(err, &eno) {
		return eno
	}
	if errors.Is(err, redis.Nil) {
		return syscall.ENOENT
	}
	if strings.HasPrefix(err.Error(), "OOM") {
		return syscall.ENOSPC
	}
	logger.Errorf("error: %s\n%s", err, debug.Stack())
	return syscall.EIO
}

type timeoutError interface {
	Timeout() bool
}

func shouldRetry(err error, retryOnFailure bool) bool {
	switch {
	case errors.Is(err, redis.TxFailedErr):
		return true
	case err == io.EOF, errors.Is(err, io.ErrUnexpectedEOF):
		return retryOnFailure
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}

	if v, ok := err.(timeoutError); ok && v.Timeout() {
		return retryOnFailure
	}

	s := err.Error()
	if s == "ERR max number of clients reached" {
		return true
	}
	ps := strings.SplitN(s, " ", 3)
	switch ps[0] {
	case "LOADING":
	case "READONLY":
	case "CLUSTERDOWN":
	case "TRYAGAIN":
	case "MOVED":
	case "ASK":
	case "ERR":
		if len(ps) > 1 {
			switch ps[1] {
			case "DISABLE":
				fallthrough
			case "NOWRITE":
				fallthrough
			case "NOREAD":
				return true
			}
		}
		return false
	default:
		return false
	}
	return true
}

func (rm *redisMeta) txn(ctx Context, txf func(tx *redis.Tx) error, keys ...string) syscall.Errno {
	if rm.conf.ReadOnly {
		return syscall.EROFS
	}
	var err error
	var khash = fnv.New32()
	_, _ = khash.Write([]byte(keys[0]))
	l := &rm.txlocks[int(khash.Sum32())%len(rm.txlocks)]
	l.Lock()
	defer l.Unlock()
	// TODO: enable retry for some of idempodent transactions
	var retryOnFailure = false
	for i := 0; i < 50; i++ {
		err = rm.rdb.Watch(ctx, txf, keys...)
		if shouldRetry(err, retryOnFailure) {
			time.Sleep(time.Microsecond * 100 * time.Duration(rand.Int()%(i+1)))
			continue
		}
		return errno(err)
	}
	return errno(err)
}

func (rm *redisMeta) Truncate(ctx Context, inode Ino, flags uint8, length uint64, attr *Attr) syscall.Errno {
	f := rm.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() { rm.of.InvalidateChunk(inode, 0xFFFFFFFF) }()
	return rm.txn(ctx, func(tx *redis.Tx) error {
		var t Attr
		a, err := tx.Get(ctx, rm.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		rm.parseAttr(a, &t)
		if t.Typ != TypeFile {
			return syscall.EPERM
		}
		if length == t.Length {
			if attr != nil {
				*attr = t
			}
			return nil
		}
		newSpace := align4K(length) - align4K(t.Length)
		if newSpace > 0 && rm.checkQuota(newSpace, 0) {
			return syscall.ENOSPC
		}
		var zeroChunks []uint32
		var left, right = t.Length, length
		if left > right {
			right, left = left, right
		}
		if (right-left)/ChunkSize >= 100 {
			// super large
			var cursor uint64
			var keys []string
			for {
				keys, cursor, err = tx.Scan(ctx, cursor, fmt.Sprintf("c%d_*", inode), 10000).Result()
				if err != nil {
					return err
				}
				for _, key := range keys {
					idx, err := strconv.Atoi(strings.Split(key, "_")[1])
					if err != nil {
						logger.Errorf("parse %s: %s", key, err)
						continue
					}
					if uint64(idx) > left/ChunkSize && uint64(idx) < right/ChunkSize {
						zeroChunks = append(zeroChunks, uint32(idx))
					}
				}
				if cursor <= 0 {
					break
				}
			}
		} else {
			for i := left/ChunkSize + 1; i < right/ChunkSize; i++ {
				zeroChunks = append(zeroChunks, uint32(i))
			}
		}
		t.Length = length
		now := time.Now()
		t.Mtime = now.Unix()
		t.Mtimensec = uint32(now.Nanosecond())
		t.Ctime = now.Unix()
		t.Ctimensec = uint32(now.Nanosecond())
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&t), 0)
			// zero out from left to right
			var l = uint32(right - left)
			if right > (left/ChunkSize+1)*ChunkSize {
				l = ChunkSize - uint32(left%ChunkSize)
			}
			pipe.RPush(ctx, rm.chunkKey(inode, uint32(left/ChunkSize)), marshalSlice(uint32(left%ChunkSize), 0, 0, 0, l))
			buf := marshalSlice(0, 0, 0, 0, ChunkSize)
			for _, indx := range zeroChunks {
				pipe.RPushX(ctx, rm.chunkKey(inode, indx), buf)
			}
			if right > (left/ChunkSize+1)*ChunkSize && right%ChunkSize > 0 {
				pipe.RPush(ctx, rm.chunkKey(inode, uint32(right/ChunkSize)), marshalSlice(0, 0, 0, 0, uint32(right%ChunkSize)))
			}
			pipe.IncrBy(ctx, usedSpace, newSpace)
			return nil
		})
		if err == nil {
			if attr != nil {
				*attr = t
			}
		}
		return err
	}, rm.inodeKey(inode))
}

const (
	// fallocate
	fallocKeepSize      = 1 << iota // 0x01
	fallocPunchHole                 // 0x02
	_                               // RESERVED: fallocNoHideStale = 0x04
	fallocCollapesRange             // 0x08
	fallocZeroRange                 // 0x10
	fallocInsertRange               // 0x20
)

func (rm *redisMeta) Fallocate(ctx Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	if mode&fallocCollapesRange != 0 && mode != fallocCollapesRange {
		return syscall.EINVAL
	}
	if mode&fallocInsertRange != 0 && mode != fallocInsertRange {
		return syscall.EINVAL
	}
	if mode == fallocInsertRange || mode == fallocCollapesRange {
		return syscall.ENOTSUP
	}
	if mode&fallocPunchHole != 0 && mode&fallocKeepSize == 0 {
		return syscall.EINVAL
	}
	if size == 0 {
		return syscall.EINVAL
	}
	f := rm.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() { rm.of.InvalidateChunk(inode, 0xFFFFFFFF) }()
	return rm.txn(ctx, func(tx *redis.Tx) error {
		var t Attr
		a, err := tx.Get(ctx, rm.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		rm.parseAttr(a, &t)
		if t.Typ == TypeFIFO {
			return syscall.EPIPE
		}
		if t.Typ != TypeFile {
			return syscall.EPERM
		}
		length := t.Length
		if off+size > t.Length {
			if mode&fallocKeepSize == 0 {
				length = off + size
			}
		}

		old := t.Length
		if length > old && rm.checkQuota(align4K(length)-align4K(old), 0) {
			return syscall.ENOSPC
		}
		t.Length = length
		now := time.Now()
		t.Mtime = now.Unix()
		t.Mtimensec = uint32(now.Nanosecond())
		t.Ctime = now.Unix()
		t.Ctimensec = uint32(now.Nanosecond())
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&t), 0)
			if mode&(fallocZeroRange|fallocPunchHole) != 0 {
				if off+size > old {
					size = old - off
				}
				for size > 0 {
					indx := uint32(off / ChunkSize)
					coff := off % ChunkSize
					l := size
					if coff+size > ChunkSize {
						l = ChunkSize - coff
					}
					pipe.RPush(ctx, rm.chunkKey(inode, indx), marshalSlice(uint32(coff), 0, 0, 0, uint32(l)))
					off += l
					size -= l
				}
			}
			pipe.IncrBy(ctx, usedSpace, align4K(length)-align4K(old))
			return nil
		})
		return err
	}, rm.inodeKey(inode))
}

func (rm *redisMeta) SetAttr(ctx Context, inode Ino, set uint16, sugidclearmode uint8, attr *Attr) syscall.Errno {
	inode = rm.checkRoot(inode)
	defer func() { rm.of.InvalidateChunk(inode, 0xFFFFFFFE) }()
	return rm.txn(ctx, func(tx *redis.Tx) error {
		var cur Attr
		a, err := tx.Get(ctx, rm.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		rm.parseAttr(a, &cur)
		if (set&(SetAttrUID|SetAttrGID)) != 0 && (set&SetAttrMode) != 0 {
			attr.Mode |= cur.Mode & 06000
		}
		var changed bool
		if (cur.Mode&06000) != 0 && (set&(SetAttrUID|SetAttrGID)) != 0 {
			if cur.Mode&01777 != cur.Mode {
				cur.Mode &= 01777
				changed = true
			}
			attr.Mode &= 01777
		}
		if set&SetAttrUID != 0 && cur.Uid != attr.Uid {
			cur.Uid = attr.Uid
			changed = true
		}
		if set&SetAttrGID != 0 && cur.Gid != attr.Gid {
			cur.Gid = attr.Gid
			changed = true
		}
		if set&SetAttrMode != 0 {
			if ctx.Uid() != 0 && (attr.Mode&02000) != 0 {
				if ctx.Gid() != cur.Gid {
					attr.Mode &= 05777
				}
			}
			if attr.Mode != cur.Mode {
				cur.Mode = attr.Mode
				changed = true
			}
		}
		now := time.Now()
		if set&SetAttrAtime != 0 && (cur.Atime != attr.Atime || cur.Atimensec != attr.Atimensec) {
			cur.Atime = attr.Atime
			cur.Atimensec = attr.Atimensec
			changed = true
		}
		if set&SetAttrAtimeNow != 0 {
			cur.Atime = now.Unix()
			cur.Atimensec = uint32(now.Nanosecond())
			changed = true
		}
		if set&SetAttrMtime != 0 && (cur.Mtime != attr.Mtime || cur.Mtimensec != attr.Mtimensec) {
			cur.Mtime = attr.Mtime
			cur.Mtimensec = attr.Mtimensec
			changed = true
		}
		if set&SetAttrMtimeNow != 0 {
			cur.Mtime = now.Unix()
			cur.Mtimensec = uint32(now.Nanosecond())
			changed = true
		}
		if !changed {
			*attr = cur
			return nil
		}
		cur.Ctime = now.Unix()
		cur.Ctimensec = uint32(now.Nanosecond())
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&cur), 0)
			return nil
		})
		if err == nil {
			*attr = cur
		}
		return err
	}, rm.inodeKey(inode))
}

func (rm *redisMeta) ReadLink(ctx Context, inode Ino, path *[]byte) syscall.Errno {
	if target, ok := rm.symlinks.Load(inode); ok {
		*path = target.([]byte)
		return 0
	}
	target, err := rm.rdb.Get(ctx, rm.symKey(inode)).Bytes()
	if err == nil {
		*path = target
		rm.symlinks.Store(inode, target)
	}
	return errno(err)
}

func (rm *redisMeta) Symlink(ctx Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return rm.mknod(ctx, parent, name, TypeSymlink, 0644, 022, 0, path, inode, attr)
}

func (rm *redisMeta) Mknod(ctx Context, parent Ino, name string, _type uint8, mode, cumask uint16, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	return rm.mknod(ctx, parent, name, _type, mode, cumask, rdev, "", inode, attr)
}

func (rm *redisMeta) mknod(ctx Context, parent Ino, name string, _type uint8, mode, cumask uint16, rdev uint32, path string, inode *Ino, attr *Attr) syscall.Errno {
	parent = rm.checkRoot(parent)
	if rm.checkQuota(4<<10, 1) {
		return syscall.ENOSPC
	}
	ino, err := rm.nextInode()
	if err != nil {
		return errno(err)
	}
	if attr == nil {
		attr = &Attr{}
	}
	attr.Typ = _type
	attr.Mode = mode & ^cumask
	attr.Uid = ctx.Uid()
	attr.Gid = ctx.Gid()
	if _type == TypeDirectory {
		attr.Nlink = 2
		attr.Length = 4 << 10
	} else {
		attr.Nlink = 1
		if _type == TypeSymlink {
			attr.Length = uint64(len(path))
		} else {
			attr.Length = 0
			attr.Rdev = rdev
		}
	}
	attr.Parent = parent
	attr.Full = true
	if inode != nil {
		*inode = ino
	}

	return rm.txn(ctx, func(tx *redis.Tx) error {
		var pattr Attr
		a, err := tx.Get(ctx, rm.inodeKey(parent)).Bytes()
		if err != nil {
			return err
		}
		rm.parseAttr(a, &pattr)
		if pattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}

		buf, err := tx.HGet(ctx, rm.entryKey(parent), name).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		var foundIno Ino
		var foundType uint8
		if err == nil {
			foundType, foundIno = rm.parseEntry(buf)
		} else if rm.conf.CaseInsensi { // err == redis.Nil
			if entry := rm.resolveCase(ctx, parent, name); entry != nil {
				foundType, foundIno = entry.Attr.Typ, entry.Inode
			}
		}
		if foundIno != 0 {
			if _type == TypeFile {
				a, err = tx.Get(ctx, rm.inodeKey(foundIno)).Bytes()
				if err == nil {
					rm.parseAttr(a, attr)
				} else if errors.Is(err, redis.Nil) {
					*attr = Attr{Typ: foundType, Parent: parent} // corrupt entry
				} else {
					return err
				}
				if inode != nil {
					*inode = foundIno
				}
			}
			return syscall.EEXIST
		}

		now := time.Now()
		if _type == TypeDirectory {
			pattr.Nlink++
		}
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())
		attr.Atime = now.Unix()
		attr.Atimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		if ctx.Value(CtxKey("behavior")) == "Hadoop" {
			attr.Gid = pattr.Gid
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, rm.entryKey(parent), name, rm.packEntry(_type, ino))
			pipe.Set(ctx, rm.inodeKey(parent), rm.marshal(&pattr), 0)
			pipe.Set(ctx, rm.inodeKey(ino), rm.marshal(attr), 0)
			if _type == TypeSymlink {
				pipe.Set(ctx, rm.symKey(ino), path, 0)
			}
			pipe.IncrBy(ctx, usedSpace, align4K(0))
			pipe.Incr(ctx, totalInodes)
			return nil
		})
		return err
	}, rm.inodeKey(parent), rm.entryKey(parent))
}

func (rm *redisMeta) Mkdir(ctx Context, parent Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *Ino, attr *Attr) syscall.Errno {
	return rm.mknod(ctx, parent, name, TypeDirectory, mode, cumask, 0, "", inode, attr)
}

func (rm *redisMeta) Create(ctx Context, parent Ino, name string, mode uint16, cumask uint16, flags uint32, inode *Ino, attr *Attr) syscall.Errno {
	if attr == nil {
		attr = &Attr{}
	}
	err := rm.mknod(ctx, parent, name, TypeFile, mode, cumask, 0, "", inode, attr)
	if errors.Is(err, syscall.EEXIST) && (flags&syscall.O_EXCL) == 0 && attr.Typ == TypeFile {
		err = 0
	}
	if err == 0 && inode != nil {
		rm.of.Open(*inode, attr)
	}
	return err
}

func (rm *redisMeta) Unlink(ctx Context, parent Ino, name string) syscall.Errno {
	parent = rm.checkRoot(parent)
	buf, err := rm.rdb.HGet(ctx, rm.entryKey(parent), name).Bytes()
	if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi {
		if e := rm.resolveCase(ctx, parent, name); e != nil {
			name = string(e.Name)
			buf = rm.packEntry(e.Attr.Typ, e.Inode)
			err = nil
		}
	}
	if err != nil {
		return errno(err)
	}
	_type, inode := rm.parseEntry(buf)
	if _type == TypeDirectory {
		return syscall.EPERM
	}
	defer func() { rm.of.InvalidateChunk(inode, 0xFFFFFFFE) }()
	var opened bool
	var attr Attr
	eno := rm.txn(ctx, func(tx *redis.Tx) error {
		rs, _ := tx.MGet(ctx, rm.inodeKey(parent), rm.inodeKey(inode)).Result()
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var pattr Attr
		rm.parseAttr([]byte(rs[0].(string)), &pattr)
		if pattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())
		attr = Attr{}
		rm.parseAttr([]byte(rs[1].(string)), &attr)
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		if ctx.Uid() != 0 && pattr.Mode&01000 != 0 && ctx.Uid() != pattr.Uid && ctx.Uid() != attr.Uid {
			return syscall.EACCES
		}

		buf, err := tx.HGet(ctx, rm.entryKey(parent), name).Bytes()
		if err != nil {
			return err
		}
		_type2, inode2 := rm.parseEntry(buf)
		if _type2 != _type || inode2 != inode {
			return syscall.EAGAIN
		}

		attr.Nlink--
		opened = false
		if _type == TypeFile && attr.Nlink == 0 {
			opened = rm.of.IsOpen(inode)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, rm.entryKey(parent), name)
			pipe.Set(ctx, rm.inodeKey(parent), rm.marshal(&pattr), 0)
			pipe.Del(ctx, rm.xattrKey(inode))
			if attr.Nlink > 0 {
				pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&attr), 0)
			} else {
				switch _type {
				case TypeFile:
					if opened {
						pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&attr), 0)
						pipe.SAdd(ctx, rm.sustained(rm.sid), strconv.Itoa(int(inode)))
					} else {
						pipe.ZAdd(ctx, delfiles, redis.Z{Score: float64(now.Unix()), Member: rm.toDelete(inode, attr.Length)})
						pipe.Del(ctx, rm.inodeKey(inode))
						pipe.IncrBy(ctx, usedSpace, -align4K(attr.Length))
						pipe.Decr(ctx, totalInodes)
					}
				case TypeSymlink:
					pipe.Del(ctx, rm.symKey(inode))
					fallthrough
				default:
					pipe.Del(ctx, rm.inodeKey(inode))
					pipe.IncrBy(ctx, usedSpace, -align4K(0))
					pipe.Decr(ctx, totalInodes)
				}
			}
			return nil
		})
		return err
	}, rm.entryKey(parent), rm.inodeKey(parent), rm.inodeKey(inode))
	if eno == 0 && _type == TypeFile && attr.Nlink == 0 {
		if opened {
			rm.Lock()
			rm.removedFiles[inode] = true
			rm.Unlock()
		} else {
			go rm.deleteFile(inode, attr.Length, "")
		}
	}
	return eno
}

func (rm *redisMeta) Rmdir(ctx Context, parent Ino, name string) syscall.Errno {
	if name == "." {
		return syscall.EINVAL
	}
	if name == ".." {
		return syscall.ENOTEMPTY
	}
	parent = rm.checkRoot(parent)
	buf, err := rm.rdb.HGet(ctx, rm.entryKey(parent), name).Bytes()
	if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi {
		if e := rm.resolveCase(ctx, parent, name); e != nil {
			name = string(e.Name)
			buf = rm.packEntry(e.Attr.Typ, e.Inode)
			err = nil
		}
	}
	if err != nil {
		return errno(err)
	}
	typ, inode := rm.parseEntry(buf)
	if typ != TypeDirectory {
		return syscall.ENOTDIR
	}

	return rm.txn(ctx, func(tx *redis.Tx) error {
		rs, _ := tx.MGet(ctx, rm.inodeKey(parent), rm.inodeKey(inode)).Result()
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var pattr, attr Attr
		rm.parseAttr([]byte(rs[0].(string)), &pattr)
		rm.parseAttr([]byte(rs[1].(string)), &attr)
		if pattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Nlink--
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())

		buf, err := tx.HGet(ctx, rm.entryKey(parent), name).Bytes()
		if err != nil {
			return err
		}
		typ, inode = rm.parseEntry(buf)
		if typ != TypeDirectory {
			return syscall.ENOTDIR
		}

		cnt, err := tx.HLen(ctx, rm.entryKey(inode)).Result()
		if err != nil {
			return err
		}
		if cnt > 0 {
			return syscall.ENOTEMPTY
		}
		if ctx.Uid() != 0 && pattr.Mode&01000 != 0 && ctx.Uid() != pattr.Uid && ctx.Uid() != attr.Uid {
			return syscall.EACCES
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, rm.entryKey(parent), name)
			pipe.Set(ctx, rm.inodeKey(parent), rm.marshal(&pattr), 0)
			pipe.Del(ctx, rm.inodeKey(inode))
			pipe.Del(ctx, rm.xattrKey(inode))
			// pipe.Del(ctx, r.entryKey(inode))
			pipe.IncrBy(ctx, usedSpace, -align4K(0))
			pipe.Decr(ctx, totalInodes)
			return nil
		})
		return err
	}, rm.inodeKey(parent), rm.entryKey(parent), rm.inodeKey(inode), rm.entryKey(inode))
}

func emptyDir(r Meta, ctx Context, inode Ino, concurrent chan int) syscall.Errno {
	if st := r.Access(ctx, inode, 3, nil); st != 0 {
		return st
	}
	var entries []*Entry
	if st := r.Readdir(ctx, inode, 0, &entries); st != 0 {
		return st
	}
	var wg sync.WaitGroup
	var status syscall.Errno
	for _, e := range entries {
		if e.Inode == inode || len(e.Name) == 2 && string(e.Name) == ".." {
			continue
		}
		if e.Attr.Typ == TypeDirectory {
			select {
			case concurrent <- 1:
				wg.Add(1)
				go func(child Ino, name string) {
					defer wg.Done()
					e := emptyEntry(r, ctx, inode, name, child, concurrent)
					if e != 0 {
						status = e
					}
					<-concurrent
				}(e.Inode, string(e.Name))
			default:
				if st := emptyEntry(r, ctx, inode, string(e.Name), e.Inode, concurrent); st != 0 {
					return st
				}
			}
		} else {
			if st := r.Unlink(ctx, inode, string(e.Name)); st != 0 {
				return st
			}
		}
	}
	wg.Wait()
	return status
}

func emptyEntry(r Meta, ctx Context, parent Ino, name string, inode Ino, concurrent chan int) syscall.Errno {
	st := emptyDir(r, ctx, inode, concurrent)
	if st == 0 {
		st = r.Rmdir(ctx, parent, name)
		if errors.Is(st, syscall.ENOTEMPTY) {
			st = emptyEntry(r, ctx, parent, name, inode, concurrent)
		}
	}
	return st
}

func Remove(r Meta, ctx Context, parent Ino, name string) syscall.Errno {
	if st := r.Access(ctx, parent, 3, nil); st != 0 {
		return st
	}
	var inode Ino
	var attr Attr
	if st := r.Lookup(ctx, parent, name, &inode, &attr); st != 0 {
		return st
	}
	if attr.Typ != TypeDirectory {
		return r.Unlink(ctx, parent, name)
	}
	concurrent := make(chan int, 50)
	return emptyEntry(r, ctx, parent, name, inode, concurrent)
}

func (rm *redisMeta) Rename(ctx Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, inode *Ino, attr *Attr) syscall.Errno {
	parentSrc = rm.checkRoot(parentSrc)
	parentDst = rm.checkRoot(parentDst)
	buf, err := rm.rdb.HGet(ctx, rm.entryKey(parentSrc), nameSrc).Bytes()
	if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi {
		if e := rm.resolveCase(ctx, parentSrc, nameSrc); e != nil {
			nameSrc = string(e.Name)
			buf = rm.packEntry(e.Attr.Typ, e.Inode)
			err = nil
		}
	}
	if err != nil {
		return errno(err)
	}
	typ, ino := rm.parseEntry(buf)
	if parentSrc == parentDst && nameSrc == nameDst {
		if inode != nil {
			*inode = ino
		}
		return 0
	}
	buf, err = rm.rdb.HGet(ctx, rm.entryKey(parentDst), nameDst).Bytes()
	if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi {
		if e := rm.resolveCase(ctx, parentDst, nameDst); e != nil {
			nameDst = string(e.Name)
			buf = rm.packEntry(e.Attr.Typ, e.Inode)
			err = nil
		}
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return errno(err)
	}
	keys := []string{rm.entryKey(parentSrc), rm.inodeKey(parentSrc), rm.inodeKey(ino), rm.entryKey(parentDst), rm.inodeKey(parentDst)}

	var dino Ino
	var dtyp uint8
	if err == nil {
		dtyp, dino = rm.parseEntry(buf)
		keys = append(keys, rm.inodeKey(dino))
		if dtyp == TypeDirectory {
			keys = append(keys, rm.entryKey(dino))
		}
	}

	var opened bool
	var tattr Attr
	eno := rm.txn(ctx, func(tx *redis.Tx) error {
		rs, _ := tx.MGet(ctx, rm.inodeKey(parentSrc), rm.inodeKey(parentDst), rm.inodeKey(ino)).Result()
		if rs[0] == nil || rs[1] == nil || rs[2] == nil {
			return redis.Nil
		}
		var sattr, dattr, iattr Attr
		rm.parseAttr([]byte(rs[0].(string)), &sattr)
		if sattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		rm.parseAttr([]byte(rs[1].(string)), &dattr)
		if dattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		rm.parseAttr([]byte(rs[2].(string)), &iattr)

		buf, err = tx.HGet(ctx, rm.entryKey(parentDst), nameDst).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		tattr = Attr{}
		opened = false
		if err == nil {
			if ctx.Value(CtxKey("behavior")) == "Hadoop" {
				return syscall.EEXIST
			}
			typ1, dino1 := rm.parseEntry(buf)
			if dino1 != dino || typ1 != dtyp {
				return syscall.EAGAIN
			}
			a, err := tx.Get(ctx, rm.inodeKey(dino)).Bytes()
			if err != nil {
				return err
			}
			rm.parseAttr(a, &tattr)
			if typ1 == TypeDirectory {
				cnt, err := tx.HLen(ctx, rm.entryKey(dino)).Result()
				if err != nil {
					return err
				}
				if cnt != 0 {
					return syscall.ENOTEMPTY
				}
			} else {
				tattr.Nlink--
				if tattr.Nlink > 0 {
					now := time.Now()
					tattr.Ctime = now.Unix()
					tattr.Ctimensec = uint32(now.Nanosecond())
				} else if dtyp == TypeFile {
					opened = rm.of.IsOpen(dino)
				}
			}
			if ctx.Uid() != 0 && dattr.Mode&01000 != 0 && ctx.Uid() != dattr.Uid && ctx.Uid() != tattr.Uid {
				return syscall.EACCES
			}
		} else {
			dino = 0
		}

		buf, err := tx.HGet(ctx, rm.entryKey(parentSrc), nameSrc).Bytes()
		if err != nil {
			return err
		}
		_, ino1 := rm.parseEntry(buf)
		if ino != ino1 {
			return syscall.EAGAIN
		}
		if ctx.Uid() != 0 && sattr.Mode&01000 != 0 && ctx.Uid() != sattr.Uid && ctx.Uid() != iattr.Uid {
			return syscall.EACCES
		}

		now := time.Now()
		sattr.Mtime = now.Unix()
		sattr.Mtimensec = uint32(now.Nanosecond())
		sattr.Ctime = now.Unix()
		sattr.Ctimensec = uint32(now.Nanosecond())
		dattr.Mtime = now.Unix()
		dattr.Mtimensec = uint32(now.Nanosecond())
		dattr.Ctime = now.Unix()
		dattr.Ctimensec = uint32(now.Nanosecond())
		iattr.Parent = parentDst
		iattr.Ctime = now.Unix()
		iattr.Ctimensec = uint32(now.Nanosecond())
		if typ == TypeDirectory && parentSrc != parentDst {
			sattr.Nlink--
			dattr.Nlink++
		}
		if inode != nil {
			*inode = ino
		}
		if attr != nil {
			*attr = iattr
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, rm.entryKey(parentSrc), nameSrc)
			pipe.Set(ctx, rm.inodeKey(parentSrc), rm.marshal(&sattr), 0)
			if dino > 0 {
				if dtyp != TypeDirectory && tattr.Nlink > 0 {
					pipe.Set(ctx, rm.inodeKey(dino), rm.marshal(&tattr), 0)
				} else {
					if dtyp == TypeFile {
						if opened {
							pipe.Set(ctx, rm.inodeKey(dino), rm.marshal(&tattr), 0)
							pipe.SAdd(ctx, rm.sustained(rm.sid), strconv.Itoa(int(dino)))
						} else {
							pipe.ZAdd(ctx, delfiles, redis.Z{Score: float64(now.Unix()), Member: rm.toDelete(dino, tattr.Length)})
							pipe.Del(ctx, rm.inodeKey(dino))
							pipe.IncrBy(ctx, usedSpace, -align4K(tattr.Length))
							pipe.Decr(ctx, totalInodes)
						}
					} else {
						if dtyp == TypeDirectory {
							dattr.Nlink--
						} else if dtyp == TypeSymlink {
							pipe.Del(ctx, rm.symKey(dino))
						}
						pipe.Del(ctx, rm.inodeKey(dino))
						pipe.IncrBy(ctx, usedSpace, -align4K(0))
						pipe.Decr(ctx, totalInodes)
					}
					pipe.Del(ctx, rm.xattrKey(dino))
				}
				pipe.HDel(ctx, rm.entryKey(parentDst), nameDst)
			}
			pipe.HSet(ctx, rm.entryKey(parentDst), nameDst, buf)
			if parentDst != parentSrc {
				pipe.Set(ctx, rm.inodeKey(parentDst), rm.marshal(&dattr), 0)
			}
			pipe.Set(ctx, rm.inodeKey(ino), rm.marshal(&iattr), 0)
			return nil
		})
		return err
	}, keys...)
	if eno == 0 && dino > 0 && dtyp == TypeFile && tattr.Nlink == 0 {
		if opened {
			rm.Lock()
			rm.removedFiles[dino] = true
			rm.Unlock()
		} else {
			go rm.deleteFile(dino, tattr.Length, "")
		}
	}
	return eno
}

func (rm *redisMeta) Link(ctx Context, inode, parent Ino, name string, attr *Attr) syscall.Errno {
	parent = rm.checkRoot(parent)
	defer func() { rm.of.InvalidateChunk(inode, 0xFFFFFFFE) }()
	return rm.txn(ctx, func(tx *redis.Tx) error {
		rs, err := tx.MGet(ctx, rm.inodeKey(parent), rm.inodeKey(inode)).Result()
		if err != nil {
			return err
		}
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var pattr, iattr Attr
		rm.parseAttr([]byte(rs[0].(string)), &pattr)
		if pattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())
		rm.parseAttr([]byte(rs[1].(string)), &iattr)
		if iattr.Typ == TypeDirectory {
			return syscall.EPERM
		}
		iattr.Ctime = now.Unix()
		iattr.Ctimensec = uint32(now.Nanosecond())
		iattr.Nlink++

		err = tx.HGet(ctx, rm.entryKey(parent), name).Err()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		} else if err == nil {
			return syscall.EEXIST
		} else if errors.Is(err, redis.Nil) && rm.conf.CaseInsensi && rm.resolveCase(ctx, parent, name) != nil {
			return syscall.EEXIST
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, rm.entryKey(parent), name, rm.packEntry(iattr.Typ, inode))
			pipe.Set(ctx, rm.inodeKey(parent), rm.marshal(&pattr), 0)
			pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&iattr), 0)
			return nil
		})
		if err == nil && attr != nil {
			*attr = iattr
		}
		return err
	}, rm.inodeKey(inode), rm.entryKey(parent), rm.inodeKey(parent))
}

func (rm *redisMeta) Readdir(ctx Context, inode Ino, plus uint8, entries *[]*Entry) syscall.Errno {
	inode = rm.checkRoot(inode)
	var attr Attr
	if err := rm.GetAttr(ctx, inode, &attr); err != 0 {
		return err
	}
	*entries = []*Entry{
		{
			Inode: inode,
			Name:  []byte("."),
			Attr:  &Attr{Typ: TypeDirectory},
		},
	}
	if attr.Parent > 0 {
		if inode == rm.root {
			attr.Parent = rm.root
		}
		*entries = append(*entries, &Entry{
			Inode: attr.Parent,
			Name:  []byte(".."),
			Attr:  &Attr{Typ: TypeDirectory},
		})
	}

	var keys []string
	var cursor uint64
	var err error
	for {
		keys, cursor, err = rm.rdb.HScan(ctx, rm.entryKey(inode), cursor, "*", 10000).Result()
		if err != nil {
			return errno(err)
		}
		newEntries := make([]Entry, len(keys)/2)
		newAttrs := make([]Attr, len(keys)/2)
		for i := 0; i < len(keys); i += 2 {
			typ, inode := rm.parseEntry([]byte(keys[i+1]))
			ent := &newEntries[i/2]
			ent.Inode = inode
			ent.Name = []byte(keys[i])
			ent.Attr = &newAttrs[i/2]
			ent.Attr.Typ = typ
			*entries = append(*entries, ent)
		}
		if cursor == 0 {
			break
		}
	}

	if plus != 0 {
		fillAttr := func(es []*Entry) error {
			var keys = make([]string, len(es))
			for i, e := range es {
				keys[i] = rm.inodeKey(e.Inode)
			}
			rs, err := rm.rdb.MGet(ctx, keys...).Result()
			if err != nil {
				return err
			}
			for j, re := range rs {
				if re != nil {
					if a, ok := re.(string); ok {
						rm.parseAttr([]byte(a), es[j].Attr)
					}
				}
			}
			return nil
		}
		batchSize := 4096
		nEntries := len(*entries)
		if nEntries <= batchSize {
			err = fillAttr(*entries)
		} else {
			indexCh := make(chan []*Entry, 10)
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for es := range indexCh {
						e := fillAttr(es)
						if e != nil {
							err = e
							break
						}
					}
				}()
			}
			for i := 0; i < nEntries; i += batchSize {
				if i+batchSize > nEntries {
					indexCh <- (*entries)[i:]
				} else {
					indexCh <- (*entries)[i : i+batchSize]
				}
			}
			close(indexCh)
			wg.Wait()
		}
		if err != nil {
			return errno(err)
		}
	}
	return 0
}

func (rm *redisMeta) cleanStaleSession(sid int64) {
	var ctx = Background
	key := rm.sustained(sid)
	inodes, err := rm.rdb.SMembers(ctx, key).Result()
	if err != nil {
		logger.Warnf("SMembers %s: %s", key, err)
		return
	}
	for _, sinode := range inodes {
		inode, _ := strconv.ParseInt(sinode, 10, 0)
		if err := rm.deleteInode(Ino(inode)); err != nil {
			logger.Errorf("Failed to delete inode %d: %s", inode, err)
		} else {
			rm.rdb.SRem(ctx, key, sinode)
		}
	}
	if len(inodes) == 0 {
		rm.rdb.ZRem(ctx, allSessions, strconv.Itoa(int(sid)))
		rm.rdb.HDel(ctx, sessionInfos, strconv.Itoa(int(sid)))
		logger.Infof("cleanup session %d", sid)
	}
}

func (rm *redisMeta) cleanStaleLocks(ssid string) {
	var ctx = Background
	key := "locked" + ssid
	inodes, err := rm.rdb.SMembers(ctx, key).Result()
	if err != nil {
		logger.Warnf("SMembers %s: %s", key, err)
		return
	}
	for _, k := range inodes {
		owners, _ := rm.rdb.HKeys(ctx, k).Result()
		for _, o := range owners {
			if strings.Split(o, "_")[0] == ssid {
				err = rm.rdb.HDel(ctx, k, o).Err()
				logger.Infof("cleanup lock on %s from session %s: %s", k, ssid, err)
			}
		}
		rm.rdb.SRem(ctx, key, k)
	}
}

func (rm *redisMeta) cleanStaleSessions() {
	// TODO: once per minute
	now := time.Now()
	var ctx = Background
	rng := &redis.ZRangeBy{Max: strconv.Itoa(int(now.Add(time.Minute * -5).Unix())), Count: 100}
	staleSessions, _ := rm.rdb.ZRangeByScore(ctx, allSessions, rng).Result()
	for _, ssid := range staleSessions {
		sid, _ := strconv.Atoi(ssid)
		rm.cleanStaleSession(int64(sid))
	}

	rng = &redis.ZRangeBy{Max: strconv.Itoa(int(now.Add(time.Minute * -3).Unix())), Count: 100}
	staleSessions, _ = rm.rdb.ZRangeByScore(ctx, allSessions, rng).Result()
	for _, sid := range staleSessions {
		rm.cleanStaleLocks(sid)
	}
}

func (rm *redisMeta) refreshSession() {
	for {
		time.Sleep(time.Minute)
		rm.rdb.ZAdd(Background, allSessions, redis.Z{Score: float64(time.Now().Unix()), Member: strconv.Itoa(int(rm.sid))})
		if _, err := rm.Load(); err != nil {
			logger.Warnf("reload setting: %s", err)
		}
		go rm.cleanStaleSessions()
	}
}

func (rm *redisMeta) deleteInode(inode Ino) error {
	var attr Attr
	var ctx = Background
	a, err := rm.rdb.Get(ctx, rm.inodeKey(inode)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	rm.parseAttr(a, &attr)
	_, err = rm.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(ctx, delfiles, redis.Z{Score: float64(time.Now().Unix()), Member: rm.toDelete(inode, attr.Length)})
		pipe.Del(ctx, rm.inodeKey(inode))
		pipe.IncrBy(ctx, usedSpace, -align4K(attr.Length))
		pipe.Decr(ctx, totalInodes)
		return nil
	})
	if err == nil {
		go rm.deleteFile(inode, attr.Length, "")
	}
	return err
}

func (rm *redisMeta) Open(ctx Context, inode Ino, flags uint32, attr *Attr) syscall.Errno {
	if rm.conf.ReadOnly && flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return syscall.EROFS
	}
	if rm.conf.OpenCache > 0 && rm.of.OpenCheck(inode, attr) {
		return 0
	}
	var err syscall.Errno
	if attr != nil && !attr.Full {
		err = rm.GetAttr(ctx, inode, attr)
	}
	if err == 0 {
		// TODO: tracking update in Redis
		rm.of.Open(inode, attr)
	}
	return 0
}

func (rm *redisMeta) Close(ctx Context, inode Ino) syscall.Errno {
	if rm.of.Close(inode) {
		rm.Lock()
		defer rm.Unlock()
		if rm.removedFiles[inode] {
			delete(rm.removedFiles, inode)
			go func() {
				if err := rm.deleteInode(inode); err == nil {
					rm.rdb.SRem(ctx, rm.sustained(rm.sid), strconv.Itoa(int(inode)))
				}
			}()
		}
	}
	return 0
}

func (rm *redisMeta) Read(ctx Context, inode Ino, idx uint32, chunks *[]Slice) syscall.Errno {
	f := rm.of.find(inode)
	if f != nil {
		f.RLock()
		defer f.RUnlock()
	}
	if cs, ok := rm.of.ReadChunk(inode, idx); ok {
		*chunks = cs
		return 0
	}
	vals, err := rm.rdb.LRange(ctx, rm.chunkKey(inode, idx), 0, 1000000).Result()
	if err != nil {
		return errno(err)
	}
	ss := readSlices(vals)
	*chunks = buildSlice(ss)
	rm.of.CacheChunk(inode, idx, *chunks)
	if !rm.conf.ReadOnly && (len(vals) >= 5 || len(*chunks) >= 5) {
		go rm.compactChunk(inode, idx, false)
	}
	return 0
}

func (rm *redisMeta) NewChunk(ctx Context, inode Ino, indx uint32, offset uint32, chunkid *uint64) syscall.Errno {
	cid, err := rm.rdb.Incr(ctx, nextChunk).Uint64()
	if err == nil {
		*chunkid = cid
	}
	return errno(err)
}

func (rm *redisMeta) InvalidateChunkCache(ctx Context, inode Ino, idx uint32) syscall.Errno {
	rm.of.InvalidateChunk(inode, idx)
	return 0
}

func (rm *redisMeta) Write(ctx Context, inode Ino, idx uint32, off uint32, slice Slice) syscall.Errno {
	f := rm.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() { rm.of.InvalidateChunk(inode, idx) }()
	var needCompact bool
	eno := rm.txn(ctx, func(tx *redis.Tx) error {
		var attr Attr
		a, err := tx.Get(ctx, rm.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		rm.parseAttr(a, &attr)
		if attr.Typ != TypeFile {
			return syscall.EPERM
		}
		newleng := uint64(idx)*ChunkSize + uint64(off) + uint64(slice.Len)
		var added int64
		if newleng > attr.Length {
			added = align4K(newleng) - align4K(attr.Length)
			attr.Length = newleng
		}
		if rm.checkQuota(added, 0) {
			return syscall.ENOSPC
		}
		now := time.Now()
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		var rpush *redis.IntCmd
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			rpush = pipe.RPush(ctx, rm.chunkKey(inode, idx), marshalSlice(off, slice.Chunkid, slice.Size, slice.Off, slice.Len))
			// most of the chunks are used by single inode, so use that as the default (1 means not exists)
			// pipe.Incr(ctx, r.sliceKey(slice.Chunkid, slice.Size))
			pipe.Set(ctx, rm.inodeKey(inode), rm.marshal(&attr), 0)
			if added > 0 {
				pipe.IncrBy(ctx, usedSpace, added)
			}
			return nil
		})
		if err == nil {
			needCompact = rpush.Val()%100 == 99
		}
		return err
	}, rm.inodeKey(inode))
	if eno == 0 && needCompact {
		go rm.compactChunk(inode, idx, false)
	}
	return eno
}

func (rm *redisMeta) CopyFileRange(ctx Context, fin Ino, offIn uint64, fout Ino, offOut uint64, size uint64, flags uint32, copied *uint64) syscall.Errno {
	f := rm.of.find(fout)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() { rm.of.InvalidateChunk(fout, 0xFFFFFFFF) }()
	return rm.txn(ctx, func(tx *redis.Tx) error {
		rs, err := tx.MGet(ctx, rm.inodeKey(fin), rm.inodeKey(fout)).Result()
		if err != nil {
			return err
		}
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var sattr Attr
		rm.parseAttr([]byte(rs[0].(string)), &sattr)
		if sattr.Typ != TypeFile {
			return syscall.EINVAL
		}
		if offIn >= sattr.Length {
			*copied = 0
			return nil
		}
		if offIn+size > sattr.Length {
			size = sattr.Length - offIn
		}
		var attr Attr
		rm.parseAttr([]byte(rs[1].(string)), &attr)
		if attr.Typ != TypeFile {
			return syscall.EINVAL
		}

		newleng := offOut + size
		var added int64
		if newleng > attr.Length {
			added = align4K(newleng) - align4K(attr.Length)
			attr.Length = newleng
		}
		if rm.checkQuota(added, 0) {
			return syscall.ENOSPC
		}
		now := time.Now()
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		p := tx.Pipeline()
		for i := offIn / ChunkSize; i <= (offIn+size)/ChunkSize; i++ {
			p.LRange(ctx, rm.chunkKey(fin, uint32(i)), 0, 1000000)
		}
		vals, err := p.Exec(ctx)
		if err != nil {
			return err
		}

		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			coff := offIn / ChunkSize * ChunkSize
			for _, v := range vals {
				sv := v.(*redis.StringSliceCmd).Val()
				// Add a zero chunk for the hole
				ss := append([]*slice{{len: ChunkSize}}, readSlices(sv)...)
				cs := buildSlice(ss)
				tpos := coff
				for _, s := range cs {
					pos := tpos
					tpos += uint64(s.Len)
					if pos < offIn+size && pos+uint64(s.Len) > offIn {
						if pos < offIn {
							dec := offIn - pos
							s.Off += uint32(dec)
							pos += dec
							s.Len -= uint32(dec)
						}
						if pos+uint64(s.Len) > offIn+size {
							dec := pos + uint64(s.Len) - (offIn + size)
							s.Len -= uint32(dec)
						}
						doff := pos - offIn + offOut
						idx := uint32(doff / ChunkSize)
						dpos := uint32(doff % ChunkSize)
						if dpos+s.Len > ChunkSize {
							pipe.RPush(ctx, rm.chunkKey(fout, idx), marshalSlice(dpos, s.Chunkid, s.Size, s.Off, ChunkSize-dpos))
							if s.Chunkid > 0 {
								pipe.HIncrBy(ctx, sliceRefs, rm.sliceKey(s.Chunkid, s.Size), 1)
							}

							skip := ChunkSize - dpos
							pipe.RPush(ctx, rm.chunkKey(fout, idx+1), marshalSlice(0, s.Chunkid, s.Size, s.Off+skip, s.Len-skip))
							if s.Chunkid > 0 {
								pipe.HIncrBy(ctx, sliceRefs, rm.sliceKey(s.Chunkid, s.Size), 1)
							}
						} else {
							pipe.RPush(ctx, rm.chunkKey(fout, idx), marshalSlice(dpos, s.Chunkid, s.Size, s.Off, s.Len))
							if s.Chunkid > 0 {
								pipe.HIncrBy(ctx, sliceRefs, rm.sliceKey(s.Chunkid, s.Size), 1)
							}
						}
					}
				}
				coff += ChunkSize
			}
			pipe.Set(ctx, rm.inodeKey(fout), rm.marshal(&attr), 0)
			if added > 0 {
				pipe.IncrBy(ctx, usedSpace, added)
			}
			return nil
		})
		if err == nil {
			*copied = size
		}
		return err
	}, rm.inodeKey(fout), rm.inodeKey(fin))
}

func (rm *redisMeta) cleanupDeletedFiles() {
	for {
		time.Sleep(time.Minute)
		now := time.Now()
		members, _ := rm.rdb.ZRangeByScore(Background, delfiles, &redis.ZRangeBy{Min: strconv.Itoa(0), Max: strconv.Itoa(int(now.Add(-time.Hour).Unix())), Count: 1000}).Result()
		for _, member := range members {
			ps := strings.Split(member, ":")
			inode, _ := strconv.ParseInt(ps[0], 10, 0)
			var length int64 = 1 << 30
			if len(ps) == 2 {
				length, _ = strconv.ParseInt(ps[1], 10, 0)
			} else if len(ps) > 2 {
				length, _ = strconv.ParseInt(ps[2], 10, 0)
			}
			logger.Debugf("cleanup chunks of inode %d with %d bytes (%s)", inode, length, member)
			rm.deleteFile(Ino(inode), uint64(length), member)
		}
	}
}

func (rm *redisMeta) cleanupSlices() {
	for {
		time.Sleep(time.Hour)

		// once per hour
		var ctx = Background
		last, _ := rm.rdb.Get(ctx, "nextCleanupSlices").Uint64()
		now := time.Now().Unix()
		if last+3600 > uint64(now) {
			continue
		}
		rm.rdb.Set(ctx, "nextCleanupSlices", now, 0)

		var ckeys []string
		var cursor uint64
		var err error
		for {
			ckeys, cursor, err = rm.rdb.HScan(ctx, sliceRefs, cursor, "*", 1000).Result()
			if err != nil {
				logger.Errorf("scan slices: %s", err)
				break
			}
			if len(ckeys) > 0 {
				values, err := rm.rdb.HMGet(ctx, sliceRefs, ckeys...).Result()
				if err != nil {
					logger.Warnf("mget slices: %s", err)
					break
				}
				for i, v := range values {
					if v == nil {
						continue
					}
					if strings.HasPrefix(v.(string), "-") { // < 0
						ps := strings.Split(ckeys[i], "_")
						if len(ps) == 2 {
							chunkid, _ := strconv.Atoi(ps[0][1:])
							size, _ := strconv.Atoi(ps[1])
							if chunkid > 0 && size > 0 {
								rm.deleteSlice(ctx, uint64(chunkid), uint32(size))
							}
						}
					} else if v == "0" {
						rm.cleanupZeroRef(ckeys[i])
					}
				}
			}
			if cursor == 0 {
				break
			}
		}
	}
}

func (rm *redisMeta) cleanupZeroRef(key string) {
	var ctx = Background
	_ = rm.txn(ctx, func(tx *redis.Tx) error {
		v, err := tx.HGet(ctx, sliceRefs, key).Int()
		if err != nil {
			return err
		}
		if v != 0 {
			return syscall.EINVAL
		}
		_, err = tx.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.HDel(ctx, sliceRefs, key)
			return nil
		})
		return err
	}, sliceRefs)
}

func (rm *redisMeta) cleanupLeakedChunks() {
	var ctx = Background
	var ckeys []string
	var cursor uint64
	var err error
	for {
		ckeys, cursor, err = rm.rdb.Scan(ctx, cursor, "c*", 1000).Result()
		if err != nil {
			logger.Errorf("scan all chunks: %s", err)
			break
		}
		var ikeys []string
		var rs []*redis.IntCmd
		p := rm.rdb.Pipeline()
		for _, k := range ckeys {
			ps := strings.Split(k, "_")
			if len(ps) != 2 {
				continue
			}
			ino, _ := strconv.ParseInt(ps[0][1:], 10, 0)
			ikeys = append(ikeys, k)
			rs = append(rs, p.Exists(ctx, rm.inodeKey(Ino(ino))))
		}
		if len(rs) > 0 {
			_, err = p.Exec(ctx)
			if err != nil {
				logger.Errorf("check inodes: %s", err)
				return
			}
			for i, rr := range rs {
				if rr.Val() == 0 {
					key := ikeys[i]
					logger.Infof("found leaked chunk %s", key)
					ps := strings.Split(key, "_")
					ino, _ := strconv.ParseInt(ps[0][1:], 10, 0)
					idx, _ := strconv.Atoi(ps[1])
					_ = rm.deleteChunk(Ino(ino), uint32(idx))
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
}

func (rm *redisMeta) cleanupOldSliceRefs() {
	var ctx = Background
	var ckeys []string
	var cursor uint64
	var err error
	for {
		ckeys, cursor, err = rm.rdb.Scan(ctx, cursor, "k*", 1000).Result()
		if err != nil {
			logger.Errorf("scan slices: %s", err)
			break
		}
		if len(ckeys) > 0 {
			values, err := rm.rdb.MGet(ctx, ckeys...).Result()
			if err != nil {
				logger.Warnf("mget slices: %s", err)
				break
			}
			var todel []string
			for i, v := range values {
				if v == nil {
					continue
				}
				if strings.HasPrefix(v.(string), "-") {
					// gc will delete the objects
					todel = append(todel, ckeys[i])
				} else {
					vv, _ := strconv.Atoi(v.(string))
					rm.rdb.HIncrBy(ctx, sliceRefs, ckeys[i], int64(vv))
					rm.rdb.DecrBy(ctx, ckeys[i], int64(vv))
					logger.Infof("move refs %d for slice %s", vv, ckeys[i])
				}
			}
			rm.rdb.Del(ctx, todel...)
		}
		if cursor == 0 {
			break
		}
	}
}

func (rm *redisMeta) toDelete(inode Ino, length uint64) string {
	return inode.String() + ":" + strconv.Itoa(int(length))
}

func (rm *redisMeta) deleteSlice(ctx Context, chunkid uint64, size uint32) {
	rm.deleting <- 1
	defer func() { <-rm.deleting }()
	err := rm.newMsg(DeleteChunk, chunkid, size)
	if err != nil {
		logger.Warnf("delete chunk %d (%d bytes): %s", chunkid, size, err)
	} else {
		_ = rm.rdb.HDel(ctx, sliceRefs, rm.sliceKey(chunkid, size))
	}
}

func (rm *redisMeta) deleteChunk(inode Ino, idx uint32) error {
	var ctx = Background
	key := rm.chunkKey(inode, idx)
	for {
		var slices []*slice
		var rs []*redis.IntCmd
		err := rm.txn(ctx, func(tx *redis.Tx) error {
			slices = nil
			vals, err := tx.LRange(ctx, key, 0, 100).Result()
			if errors.Is(err, redis.Nil) {
				return nil
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, v := range vals {
					rb := utils.ReadBuffer([]byte(v))
					_ = rb.Get32() // pos
					chunkid := rb.Get64()
					size := rb.Get32()
					slices = append(slices, &slice{chunkid: chunkid, size: size})
					pipe.LPop(ctx, key)
					rs = append(rs, pipe.HIncrBy(ctx, sliceRefs, rm.sliceKey(chunkid, size), -1))
				}
				return nil
			})
			return err
		}, key)
		if !errors.Is(err, syscall.Errno(0)) {
			return fmt.Errorf("delete slice from chunk %s fail: %s, retry later", key, err)
		}
		for i, s := range slices {
			if rs[i].Val() < 0 {
				rm.deleteSlice(ctx, s.chunkid, s.size)
			}
		}
		if len(slices) < 100 {
			break
		}
	}
	return nil
}

func (rm *redisMeta) deleteFile(inode Ino, length uint64, tracking string) {
	var ctx = Background
	var idx uint32
	p := rm.rdb.Pipeline()
	for uint64(idx)*ChunkSize < length {
		var keys []string
		for i := 0; uint64(idx)*ChunkSize < length && i < 1000; i++ {
			key := rm.chunkKey(inode, idx)
			keys = append(keys, key)
			_ = p.LLen(ctx, key)
			idx++
		}
		cmds, err := p.Exec(ctx)
		if err != nil {
			logger.Warnf("delete chunks of inode %d: %s", inode, err)
			return
		}
		for i, cmd := range cmds {
			val, err := cmd.(*redis.IntCmd).Result()
			if errors.Is(err, redis.Nil) || val == 0 {
				continue
			}
			idx, _ := strconv.Atoi(strings.Split(keys[i], "_")[1])
			err = rm.deleteChunk(inode, uint32(idx))
			if err != nil {
				logger.Warnf("delete chunk %s: %s", keys[i], err)
				return
			}
		}
	}
	if tracking == "" {
		tracking = inode.String() + ":" + strconv.FormatInt(int64(length), 10)
	}
	_ = rm.rdb.ZRem(ctx, delfiles, tracking)
}

func (rm *redisMeta) compactChunk(inode Ino, idx uint32, force bool) {
	// avoid too much or duplicated compaction
	if !force {
		rm.Lock()
		k := uint64(inode) + (uint64(idx) << 32)
		if len(rm.compacting) > 10 || rm.compacting[k] {
			rm.Unlock()
			return
		}
		rm.compacting[k] = true
		rm.Unlock()
		defer func() {
			rm.Lock()
			delete(rm.compacting, k)
			rm.Unlock()
		}()
	}

	var ctx = Background
	vals, err := rm.rdb.LRange(ctx, rm.chunkKey(inode, idx), 0, 1000).Result()
	if err != nil {
		return
	}

	ss := readSlices(vals)
	skipped := skipSome(ss)
	ss = ss[skipped:]
	pos, size, chunks := compactChunk(ss)
	if len(ss) < 2 || size == 0 {
		return
	}

	chunkid, err := rm.rdb.Incr(ctx, nextChunk).Uint64()
	if err != nil {
		return
	}

	logger.Debugf("compact %d:%d: skipped %d slices (%d bytes) %d slices (%d bytes)", inode, idx, skipped, pos, len(ss), size)
	err = rm.newMsg(CompactChunk, chunks, chunkid)
	if err != nil {
		logger.Warnf("compact %d %d with %d slices: %s", inode, idx, len(ss), err)
		return
	}
	var rs []*redis.IntCmd
	key := rm.chunkKey(inode, idx)
	errno := rm.txn(ctx, func(tx *redis.Tx) error {
		rs = nil
		vals2, err := tx.LRange(ctx, key, 0, int64(len(vals)-1)).Result()
		if err != nil {
			return err
		}
		if len(vals2) != len(vals) {
			return syscall.EINVAL
		}
		for i, val := range vals2 {
			if val != vals[i] {
				return syscall.EINVAL
			}
		}

		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LTrim(ctx, key, int64(len(vals)), -1)
			pipe.LPush(ctx, key, marshalSlice(pos, chunkid, size, 0, size))
			for i := skipped; i > 0; i-- {
				pipe.LPush(ctx, key, vals[i-1])
			}
			pipe.HSet(ctx, sliceRefs, rm.sliceKey(chunkid, size), "0") // create the key to tracking it
			for _, s := range ss {
				rs = append(rs, pipe.HIncrBy(ctx, sliceRefs, rm.sliceKey(s.chunkid, s.size), -1))
			}
			return nil
		})
		return err
	}, key)
	// there could be a false-negative that the compaction is successful, double-check
	if errno != 0 && !errors.Is(errno, syscall.EINVAL) {
		if e := rm.rdb.HGet(ctx, sliceRefs, rm.sliceKey(chunkid, size)).Err(); e == redis.Nil {
			errno = syscall.EINVAL // failed
		} else if e == nil {
			errno = 0 // successful
		}
	}

	if errors.Is(errno, syscall.EINVAL) {
		rm.rdb.HIncrBy(ctx, sliceRefs, rm.sliceKey(chunkid, size), -1)
		logger.Infof("compaction for %d:%d is wasted, delete slice %d (%d bytes)", inode, idx, chunkid, size)
		rm.deleteSlice(ctx, chunkid, size)
	} else if errno == 0 {
		rm.of.InvalidateChunk(inode, idx)
		rm.cleanupZeroRef(rm.sliceKey(chunkid, size))
		for i, s := range ss {
			if rs[i].Err() == nil && rs[i].Val() < 0 {
				rm.deleteSlice(ctx, s.chunkid, s.size)
			}
		}
		if rm.rdb.LLen(ctx, rm.chunkKey(inode, idx)).Val() > 5 {
			go func() {
				// wait for the current compaction to finish
				time.Sleep(time.Millisecond * 10)
				rm.compactChunk(inode, idx, force)
			}()
		}
	} else {
		logger.Warnf("compact %s: %s", key, errno)
	}
}

func (rm *redisMeta) CompactAll(ctx Context) syscall.Errno {
	var cursor uint64
	p := rm.rdb.Pipeline()
	for {
		keys, c, err := rm.rdb.Scan(ctx, cursor, "c*_*", 10000).Result()
		if err != nil {
			logger.Warnf("scan chunks: %s", err)
			return errno(err)
		}
		for _, key := range keys {
			_ = p.LLen(ctx, key)
		}
		cmds, err := p.Exec(ctx)
		if err != nil {
			logger.Warnf("list slices: %s", err)
			return errno(err)
		}
		for i, cmd := range cmds {
			cnt := cmd.(*redis.IntCmd).Val()
			if cnt > 1 {
				var inode uint64
				var idx uint32
				n, err := fmt.Sscanf(keys[i], "c%d_%d", &inode, &idx)
				if err == nil && n == 2 {
					logger.Debugf("compact chunk %d:%d (%d slices)", inode, idx, cnt)
					rm.compactChunk(Ino(inode), idx, true)
				}
			}
		}
		if c == 0 {
			break
		}
		cursor = c
	}
	return 0
}

func (rm *redisMeta) cleanupLeakedInodes(delete bool) {
	var ctx = Background
	var keys []string
	var cursor uint64
	var err error
	var foundInodes = make(map[Ino]struct{})
	cutoff := time.Now().Add(time.Hour * -1)
	for {
		keys, cursor, err = rm.rdb.Scan(ctx, cursor, "d*", 1000).Result()
		if err != nil {
			logger.Errorf("scan dentry: %s", err)
			return
		}
		if len(keys) > 0 {
			for _, key := range keys {
				ino, _ := strconv.Atoi(key[1:])
				var entries []*Entry
				eno := rm.Readdir(ctx, Ino(ino), 0, &entries)
				if !errors.Is(eno, syscall.ENOENT) && eno != 0 {
					logger.Errorf("readdir %d: %s", ino, eno)
					return
				}
				for _, e := range entries {
					foundInodes[e.Inode] = struct{}{}
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
	for {
		keys, cursor, err = rm.rdb.Scan(ctx, cursor, "i*", 1000).Result()
		if err != nil {
			logger.Errorf("scan inodes: %s", err)
			break
		}
		if len(keys) > 0 {
			values, err := rm.rdb.MGet(ctx, keys...).Result()
			if err != nil {
				logger.Warnf("mget inodes: %s", err)
				break
			}
			for i, v := range values {
				if v == nil {
					continue
				}
				var attr Attr
				rm.parseAttr([]byte(v.(string)), &attr)
				ino, _ := strconv.Atoi(keys[i][1:])
				if _, ok := foundInodes[Ino(ino)]; !ok && time.Unix(attr.Atime, 0).Before(cutoff) {
					logger.Infof("found dangling inode: %s %+v", keys[i], attr)
					if delete {
						err = rm.deleteInode(Ino(ino))
						if err != nil {
							logger.Errorf("delete leaked inode %d : %s", ino, err)
						}
					}
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
}

func (rm *redisMeta) ListSlices(ctx Context, slices *[]Slice, delete bool, showProgress func()) syscall.Errno {
	rm.cleanupLeakedInodes(delete)
	rm.cleanupLeakedChunks()
	rm.cleanupOldSliceRefs()
	*slices = nil
	var cursor uint64
	p := rm.rdb.Pipeline()
	for {
		keys, c, err := rm.rdb.Scan(ctx, cursor, "c*_*", 10000).Result()
		if err != nil {
			logger.Warnf("scan chunks: %s", err)
			return errno(err)
		}
		for _, key := range keys {
			_ = p.LRange(ctx, key, 0, 100000000)
		}
		cmds, err := p.Exec(ctx)
		if err != nil {
			logger.Warnf("list slices: %s", err)
			return errno(err)
		}
		for _, cmd := range cmds {
			vals := cmd.(*redis.StringSliceCmd).Val()
			ss := readSlices(vals)
			for _, s := range ss {
				if s.chunkid > 0 {
					*slices = append(*slices, Slice{Chunkid: s.chunkid, Size: s.size})
					if showProgress != nil {
						showProgress()
					}
				}
			}
		}
		if c == 0 {
			break
		}
		cursor = c
	}
	return 0
}

func (rm *redisMeta) GetXattr(ctx Context, inode Ino, name string, vbuff *[]byte) syscall.Errno {
	inode = rm.checkRoot(inode)
	var err error
	*vbuff, err = rm.rdb.HGet(ctx, rm.xattrKey(inode), name).Bytes()
	if errors.Is(err, redis.Nil) {
		err = ENOATTR
	}
	return errno(err)
}

func (rm *redisMeta) ListXattr(ctx Context, inode Ino, names *[]byte) syscall.Errno {
	inode = rm.checkRoot(inode)
	vals, err := rm.rdb.HKeys(ctx, rm.xattrKey(inode)).Result()
	if err != nil {
		return errno(err)
	}
	*names = nil
	for _, name := range vals {
		*names = append(*names, []byte(name)...)
		*names = append(*names, 0)
	}
	return 0
}

func (rm *redisMeta) SetXattr(ctx Context, inode Ino, name string, value []byte) syscall.Errno {
	if name == "" {
		return syscall.EINVAL
	}
	inode = rm.checkRoot(inode)
	_, err := rm.rdb.HSet(ctx, rm.xattrKey(inode), name, value).Result()
	return errno(err)
}

func (rm *redisMeta) RemoveXattr(ctx Context, inode Ino, name string) syscall.Errno {
	if name == "" {
		return syscall.EINVAL
	}
	inode = rm.checkRoot(inode)
	n, err := rm.rdb.HDel(ctx, rm.xattrKey(inode), name).Result()
	if err != nil {
		return errno(err)
	} else if n == 0 {
		return ENOATTR
	} else {
		return 0
	}
}

func (rm *redisMeta) checkServerConfig() {
	rawInfo, err := rm.rdb.Info(Background).Result()
	if err != nil {
		logger.Warnf("parse info: %s", err)
		return
	}
	_, err = checkRedisInfo(rawInfo)
	if err != nil {
		logger.Warnf("parse info: %s", err)
	}

	start := time.Now()
	_ = rm.rdb.Ping(Background)
	logger.Infof("Ping redis: %s", time.Since(start))
}

func (rm *redisMeta) dumpEntry(inode Ino) (*DumpedEntry, error) {
	ctx := Background
	e := &DumpedEntry{}
	st := rm.txn(ctx, func(tx *redis.Tx) error {
		a, err := tx.Get(ctx, rm.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		attr := &Attr{}
		rm.parseAttr(a, attr)
		e.Attr = dumpAttr(attr)
		e.Attr.Inode = inode

		keys, err := tx.HGetAll(ctx, rm.xattrKey(inode)).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			xattrs := make([]*DumpedXattr, 0, len(keys))
			for k, v := range keys {
				xattrs = append(xattrs, &DumpedXattr{k, v})
			}
			sort.Slice(xattrs, func(i, j int) bool { return xattrs[i].Name < xattrs[j].Name })
			e.Xattrs = xattrs
		}

		if attr.Typ == TypeFile {
			for idx := uint32(0); uint64(idx)*ChunkSize < attr.Length; idx++ {
				vals, err := tx.LRange(ctx, rm.chunkKey(inode, idx), 0, 1000000).Result()
				if err != nil {
					return err
				}
				ss := readSlices(vals)
				slices := make([]*DumpedSlice, 0, len(ss))
				for _, s := range ss {
					slices = append(slices, &DumpedSlice{s.pos, s.chunkid, s.size, s.off, s.len})
				}
				e.Chunks = append(e.Chunks, &DumpedChunk{idx, slices})
			}
		} else if attr.Typ == TypeSymlink {
			if e.Symlink, err = tx.Get(ctx, rm.symKey(inode)).Result(); err != nil {
				return err
			}
		}
		return nil
	}, rm.inodeKey(inode))
	if st == 0 {
		return e, nil
	} else {
		return nil, fmt.Errorf("dump entry error: %d", st)
	}
}

func (rm *redisMeta) dumpDir(inode Ino, showProgress func(totalIncr, currentIncr int64)) (map[string]*DumpedEntry, error) {
	ctx := Background
	keys, err := rm.rdb.HGetAll(ctx, rm.entryKey(inode)).Result()
	if err != nil {
		return nil, err
	}
	if showProgress != nil {
		showProgress(int64(len(keys)), 0)
	}
	entries := make(map[string]*DumpedEntry)
	for k, v := range keys {
		typ, inode := rm.parseEntry([]byte(v))
		entry, err := rm.dumpEntry(inode)
		if err != nil {
			return nil, err
		}
		if typ == TypeDirectory {
			if entry.Entries, err = rm.dumpDir(inode, showProgress); err != nil {
				return nil, err
			}
		}
		entries[k] = entry
		if showProgress != nil {
			showProgress(0, 1)
		}
	}
	return entries, nil
}

func (rm *redisMeta) DumpMeta(w io.Writer) error {
	ctx := Background
	zs, err := rm.rdb.ZRangeWithScores(ctx, delfiles, 0, -1).Result()
	if err != nil {
		return err
	}
	dels := make([]*DumpedDelFile, 0, len(zs))
	for _, z := range zs {
		parts := strings.Split(z.Member.(string), ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid delfile string: %s", z.Member.(string))
		}
		inode, _ := strconv.ParseUint(parts[0], 10, 64)
		length, _ := strconv.ParseUint(parts[1], 10, 64)
		dels = append(dels, &DumpedDelFile{Ino(inode), length, int64(z.Score)})
	}

	tree, err := rm.dumpEntry(rm.root)
	if err != nil {
		return err
	}

	var total int64 = 1 // root
	progress, bar := utils.NewDynProgressBar("Dump dir progress: ", false)
	bar.Increment()
	if tree.Entries, err = rm.dumpDir(rm.root, func(totalIncr, currentIncr int64) {
		total += totalIncr
		bar.SetTotal(total, false)
		bar.IncrInt64(currentIncr)
	}); err != nil {
		return err
	}
	if bar.Current() != total {
		logger.Warnf("Dumped %d / total %d, some entries are not dumped", bar.Current(), total)
	}
	bar.SetTotal(0, true) // FIXME: current != total
	progress.Wait()

	format, err := rm.Load()
	if err != nil {
		return err
	}

	rs, _ := rm.rdb.MGet(ctx, []string{usedSpace, totalInodes, nextInode, nextChunk, nextSession}...).Result()
	cs := make([]int64, len(rs))
	for i, r := range rs {
		if r != nil {
			cs[i], _ = strconv.ParseInt(r.(string), 10, 64)
		}
	}

	keys, err := rm.rdb.ZRange(ctx, allSessions, 0, -1).Result()
	if err != nil {
		return err
	}
	sessions := make([]*DumpedSustained, 0, len(keys))
	for _, k := range keys {
		sid, _ := strconv.ParseUint(k, 10, 64)
		ss, err := rm.rdb.SMembers(ctx, rm.sustained(int64(sid))).Result()
		if err != nil {
			return err
		}
		if len(ss) > 0 {
			inodes := make([]Ino, 0, len(ss))
			for _, s := range ss {
				inode, _ := strconv.ParseUint(s, 10, 64)
				inodes = append(inodes, Ino(inode))
			}
			sessions = append(sessions, &DumpedSustained{sid, inodes})
		}
	}

	dm := &DumpedMeta{
		format,
		&DumpedCounters{
			UsedSpace:   cs[0],
			UsedInodes:  cs[1],
			NextInode:   cs[2] + 1, // Redis counter is 1 smaller than sql/tkv
			NextChunk:   cs[3] + 1,
			NextSession: cs[4] + 1,
		},
		sessions,
		dels,
		tree,
	}
	return dm.writeJSON(w)
}

func collectEntry(e *DumpedEntry, entries map[Ino]*DumpedEntry, showProgress func(totalIncr, currentIncr int64)) error {
	typ := typeFromString(e.Attr.Type)
	inode := e.Attr.Inode
	if showProgress != nil {
		if typ == TypeDirectory {
			showProgress(int64(len(e.Entries)), 1)
		} else {
			showProgress(0, 1)
		}
	}

	if exist, ok := entries[inode]; ok {
		attr := e.Attr
		eattr := exist.Attr
		if typ != TypeFile || typeFromString(eattr.Type) != TypeFile {
			return fmt.Errorf("inode conflict: %d", inode)
		}
		eattr.Nlink++
		if eattr.Ctime*1e9+int64(eattr.Ctimensec) < attr.Ctime*1e9+int64(attr.Ctimensec) {
			attr.Nlink = eattr.Nlink
			entries[inode] = e
		}
		return nil
	}
	entries[inode] = e

	if typ == TypeFile {
		e.Attr.Nlink = 1 // reset
	} else if typ == TypeDirectory {
		if inode == 1 { // root inode
			e.Parent = 1
		}
		e.Attr.Nlink = 2
		for name, child := range e.Entries {
			child.Name = name
			child.Parent = inode
			if typeFromString(child.Attr.Type) == TypeDirectory {
				e.Attr.Nlink++
			}
			if err := collectEntry(child, entries, showProgress); err != nil {
				return err
			}
		}
	} else if e.Attr.Nlink != 1 { // nlink should be 1 for other types
		return fmt.Errorf("invalid nlink %d for inode %d type %s", e.Attr.Nlink, inode, e.Attr.Type)
	}
	return nil
}

func (rm *redisMeta) loadEntry(e *DumpedEntry, cs *DumpedCounters, refs map[string]int) error {
	inode := e.Attr.Inode
	logger.Debugf("Loading entry inode %d name %s", inode, e.Name)
	ctx := Background
	attr := loadAttr(e.Attr)
	attr.Parent = e.Parent
	p := rm.rdb.Pipeline()
	if attr.Typ == TypeFile {
		attr.Length = e.Attr.Length
		for _, c := range e.Chunks {
			if len(c.Slices) == 0 {
				continue
			}
			slices := make([]string, 0, len(c.Slices))
			for _, s := range c.Slices {
				slices = append(slices, string(marshalSlice(s.Pos, s.Chunkid, s.Size, s.Off, s.Len)))
				refs[rm.sliceKey(s.Chunkid, s.Size)]++
				if cs.NextChunk < int64(s.Chunkid) {
					cs.NextChunk = int64(s.Chunkid)
				}
			}
			p.RPush(ctx, rm.chunkKey(inode, c.Index), slices)
		}
	} else if attr.Typ == TypeDirectory {
		attr.Length = 4 << 10
		if len(e.Entries) > 0 {
			dentries := make(map[string]interface{})
			for _, c := range e.Entries {
				dentries[c.Name] = rm.packEntry(typeFromString(c.Attr.Type), c.Attr.Inode)
			}
			p.HSet(ctx, rm.entryKey(inode), dentries)
		}
	} else if attr.Typ == TypeSymlink {
		attr.Length = uint64(len(e.Symlink))
		p.Set(ctx, rm.symKey(inode), e.Symlink, 0)
	}
	if inode > 1 {
		cs.UsedSpace += align4K(attr.Length)
		cs.UsedInodes += 1
	}
	if cs.NextInode < int64(inode) {
		cs.NextInode = int64(inode)
	}

	if len(e.Xattrs) > 0 {
		xattrs := make(map[string]interface{})
		for _, x := range e.Xattrs {
			xattrs[x.Name] = x.Value
		}
		p.HSet(ctx, rm.xattrKey(inode), xattrs)
	}
	p.Set(ctx, rm.inodeKey(inode), rm.marshal(attr), 0)
	_, err := p.Exec(ctx)
	return err
}

func (rm *redisMeta) LoadMeta(r io.Reader) error {
	ctx := Background
	dbSize, err := rm.rdb.DBSize(ctx).Result()
	if err != nil {
		return err
	}
	if dbSize > 0 {
		return fmt.Errorf("database %s is not empty", rm.Name())
	}

	dec := json.NewDecoder(r)
	dm := &DumpedMeta{}
	if err = dec.Decode(dm); err != nil {
		return err
	}
	format, err := json.MarshalIndent(dm.Setting, "", "")
	if err != nil {
		return err
	}

	var total int64 = 1 // root
	progress, bar := utils.NewDynProgressBar("CollectEntry progress: ", false)
	dm.FSTree.Attr.Inode = 1
	entries := make(map[Ino]*DumpedEntry)
	if err = collectEntry(dm.FSTree, entries, func(totalIncr, currentIncr int64) {
		total += totalIncr
		bar.SetTotal(total, false)
		bar.IncrInt64(currentIncr)
	}); err != nil {
		return err
	}
	if bar.Current() != total {
		logger.Warnf("Collected %d / total %d, some entries are not collected", bar.Current(), total)
	}
	bar.SetTotal(0, true) // FIXME: current != total
	progress.Wait()

	counters := &DumpedCounters{}
	refs := make(map[string]int)
	for _, entry := range entries {
		if err = rm.loadEntry(entry, counters, refs); err != nil {
			return err
		}
	}
	logger.Infof("Dumped counters: %+v", *dm.Counters)
	logger.Infof("Loaded counters: %+v", *counters)

	p := rm.rdb.Pipeline()
	p.Set(ctx, "setting", format, 0)
	cs := make(map[string]interface{})
	cs[usedSpace] = counters.UsedSpace
	cs[totalInodes] = counters.UsedInodes
	cs[nextInode] = counters.NextInode
	cs[nextChunk] = counters.NextChunk
	cs[nextSession] = counters.NextSession
	p.MSet(ctx, cs)
	if len(dm.DelFiles) > 0 {
		zs := make([]redis.Z, 0, len(dm.DelFiles))
		for _, d := range dm.DelFiles {
			zs = append(zs, redis.Z{
				Score:  float64(d.Expire),
				Member: rm.toDelete(d.Inode, d.Length),
			})
		}
		p.ZAdd(ctx, delfiles, zs...)
	}
	slices := make(map[string]interface{})
	for k, v := range refs {
		if v > 1 {
			slices[k] = v - 1
		}
	}
	if len(slices) > 0 {
		p.HSet(ctx, sliceRefs, slices)
	}
	_, err = p.Exec(ctx)
	return err
}
