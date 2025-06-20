// pkg/meta/interface.go

package meta

import (
	"AveFS/pkg/version"
	"io"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	// ChunkSize is the size of a chunk in bytes (64MB).
	ChunkSize = 1 << 26 // 64M

	// DeleteChunk is a message type to request the deletion of a chunk
	// from the object store.
	DeleteChunk = 1000

	// CompactChunk is a message type to request the compaction of a chunk
	// in the object store.
	CompactChunk = 1001

	// Rmr is a message type to request the recursive removal of a directory.
	Rmr = 1002

	// Info is a message type to request internal information for a file or directory.
	Info = 1003

	// FillCache is a message type to request building cache for target
	// directories or files.
	FillCache = 1004
)

const (
	TypeFile      = iota + 1 // type for regular file
	TypeDirectory            // type for directory
	TypeSymlink              // type for symlink
	TypeFIFO                 // type for FIFO node
	TypeBlockDev             // type for block device
	TypeCharDev              // type for character device
	TypeSocket               // type for socket
)

const (
	// SetAttrMode is a mask to update an attribute of a node.
	// Each constant represents a flag indicating which attribute to set a bit.
	SetAttrMode     = 1 << iota // Bit 0: Set mode
	SetAttrUID                  // Bit 1: Set UID
	SetAttrGID                  // Bit 2: Set GID
	SetAttrSize                 // Bit 3: Set size
	SetAttrAtime                // Bit 4: Set access time
	SetAttrMtime                // Bit 5: Set modification time
	SetAttrCtime                // Bit 6: Set creation time
	SetAttrAtimeNow             // Bit 7: Set access time now
	SetAttrMtimeNow             // Bit 8: Set modification time to now
)

type MsgCallback func(...interface{}) error

type Attr struct {
	Flags     uint8  // reserved flags
	Typ       uint8  // type of node
	Mode      uint16 // permission mode
	Uid       uint32 // owner id
	Gid       uint32 // group id
	Atime     int64  // last access time
	Mtime     int64  // last modified time
	Ctime     int64  // last change time for meta
	Atimensec uint32 // nanosecond part of atime
	Mtimensec uint32 // nanosecond part of mtime
	Ctimensec uint32 // nanosecond part of ctime
	Nlink     uint32 // number of links (subdirectories or hardlinks)
	Length    uint64 // length of a regular file
	Rdev      uint32 // device number

	Parent    Ino  // inode of parent, only for Directory
	Full      bool // the attributes are completed or not
	KeepCache bool // whether to keep the cached page or not
}

func typeToStatType(_type uint8) uint32 {
	switch _type & 0x7F {
	case TypeDirectory:
		return syscall.S_IFDIR
	case TypeSymlink:
		return syscall.S_IFLNK
	case TypeFile:
		return syscall.S_IFREG
	case TypeFIFO:
		return syscall.S_IFIFO
	case TypeSocket:
		return syscall.S_IFSOCK
	case TypeBlockDev:
		return syscall.S_IFBLK
	case TypeCharDev:
		return syscall.S_IFCHR
	default:
		panic(_type)
	}
}

func typeToString(_type uint8) string {
	switch _type {
	case TypeFile:
		return "regular"
	case TypeDirectory:
		return "directory"
	case TypeSymlink:
		return "symlink"
	case TypeFIFO:
		return "fifo"
	case TypeBlockDev:
		return "blockdev"
	case TypeCharDev:
		return "chardev"
	case TypeSocket:
		return "socket"
	default:
		return "unknown"
	}
}
func typeFromString(s string) uint8 {
	switch s {
	case "regular":
		return TypeFile
	case "directory":
		return TypeDirectory
	case "symlink":
		return TypeSymlink
	case "fifo":
		return TypeFIFO
	case "blockdev":
		return TypeBlockDev
	case "chardev":
		return TypeCharDev
	case "socket":
		return TypeSocket
	default:
		panic(s)
	}
}

// SMode is the file mode including type and unix permission.
func (a Attr) SMode() uint32 {
	return typeToStatType(a.Typ) | uint32(a.Mode)
}

// Entry is an entry inside a directory.
type Entry struct {
	Inode Ino
	Name  []byte
	Attr  *Attr
}

// Slice is a slice of a chunk.
// Multiple slices could be combined as a chunk.
type Slice struct {
	Chunkid uint64
	Size    uint32
	Off     uint32
	Len     uint32
}

// Summary represents the total number of files/directories and
// total length of all files inside a directory.
type Summary struct {
	Length uint64
	Size   uint64
	Files  uint64
	Dirs   uint64
}

type SessionInfo struct {
	Version    string
	Hostname   string
	MountPoint string
	ProcessID  int
}

type Flock struct {
	Inode Ino
	Owner uint64
	Ltype string
}

type Plock struct {
	Inode   Ino
	Owner   uint64
	Records []byte // FIXME: loadLocks
}

// Session contains detailed information of a client session
type Session struct {
	Sid       uint64
	Heartbeat time.Time
	SessionInfo
	Sustained []Ino   `json:",omitempty"`
	Flocks    []Flock `json:",omitempty"`
	Plocks    []Plock `json:",omitempty"`
}

// Meta is an interface for a meta-service for the file system.
type Meta interface {
	// Name of the database
	Name() string
	// Init is used to initialize a meta-service.
	Init(format Format, force bool) error
	// Load loads the existing setting of a formatted volume from meta-service.
	Load() (*Format, error)
	// NewSession creates a new client session.
	NewSession() error
	// GetSession retrieves information of the session with sid
	GetSession(sid uint64) (*Session, error)
	// ListSessions returns all client sessions.
	ListSessions() ([]*Session, error)

	// StatFS returns summary statistics of a volume.
	StatFS(ctx Context, totalspace, availspace, iused, iavail *uint64) syscall.Errno
	// Access checks the access permission on a given inode.
	Access(ctx Context, inode Ino, modemask uint8, attr *Attr) syscall.Errno
	// Lookup returns the inode and attributes for the given entry in a directory.
	Lookup(ctx Context, parent Ino, name string, inode *Ino, attr *Attr) syscall.Errno
	// Resolve fetches the inode and attributes for an entry identified by the given path.
	// ENOTSUP will be returned if there's no natural implementation for this operation or
	// if there is any symlink following involved.
	Resolve(ctx Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno
	// GetAttr returns the attributes for a given node.
	GetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno
	// SetAttr updates the attributes for a given node.
	SetAttr(ctx Context, inode Ino, set uint16, sggidclearmode uint8, attr *Attr) syscall.Errno
	// Truncate changes the length for a given file.
	Truncate(ctx Context, inode Ino, flags uint8, attrlength uint64, attr *Attr) syscall.Errno
	// Fallocate preallocate given space for a given file.
	Fallocate(ctx Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno
	// ReadLink returns the target of a symlink.
	ReadLink(ctx Context, inode Ino, path *[]byte) syscall.Errno
	// Symlink creates a symlink in a directory with a given name.
	Symlink(ctx Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno
	// Mknod creates a node in a directory with a given name, type and permissions.
	Mknod(ctx Context, parent Ino, name string, _type uint8, mode uint16, cumask uint16, rdev uint32, inode *Ino, attr *Attr) syscall.Errno
	// Mkdir creates a subdirectory with the given name and mode.
	Mkdir(ctx Context, parent Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *Ino, attr *Attr) syscall.Errno
	// Unlink removes a file entry from a directory.
	// The file will be deleted if it's not linked by any entries and not open by any sessions.
	Unlink(ctx Context, parent Ino, name string) syscall.Errno
	// Rmdir removes an empty subdirectory.
	Rmdir(ctx Context, parent Ino, name string) syscall.Errno
	// Rename move an entry from a source directory to another with a given name.
	// The targeted entry will be overwrited if it's a file or empty directory.
	// For Hadoop, the target should not be overwritten.
	Rename(ctx Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, inode *Ino, attr *Attr) syscall.Errno
	// Link creates an entry for a node.
	Link(ctx Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno
	// Readdir returns all entries for a given directory, which include attributes if plus is true.
	Readdir(ctx Context, inode Ino, wantattr uint8, entries *[]*Entry) syscall.Errno
	// Create creates a file in a directory with a given name.
	Create(ctx Context, parent Ino, name string, mode uint16, cumask uint16, flags uint32, inode *Ino, attr *Attr) syscall.Errno
	// Open checks permission on a node and track it as open.
	Open(ctx Context, inode Ino, flags uint32, attr *Attr) syscall.Errno
	// Close a file.
	Close(ctx Context, inode Ino) syscall.Errno
	// Read returns the list of slices on the given chunk.
	Read(ctx Context, inode Ino, indx uint32, chunks *[]Slice) syscall.Errno
	// NewChunk returns a new id for new data.
	NewChunk(ctx Context, inode Ino, indx uint32, offset uint32, chunkid *uint64) syscall.Errno
	// Write put a slice of data on top of the given chunk.
	Write(ctx Context, inode Ino, indx uint32, off uint32, slice Slice) syscall.Errno
	// InvalidateChunkCache invalidate chunk cache
	InvalidateChunkCache(ctx Context, inode Ino, indx uint32) syscall.Errno
	// CopyFileRange copies part of a file to another one.
	CopyFileRange(ctx Context, fin Ino, offIn uint64, fout Ino, offOut uint64, size uint64, flags uint32, copied *uint64) syscall.Errno

	// GetXattr returns the value of extended attribute for given name.
	GetXattr(ctx Context, inode Ino, name string, vbuff *[]byte) syscall.Errno
	// ListXattr returns all extended attributes of a node.
	ListXattr(ctx Context, inode Ino, dbuff *[]byte) syscall.Errno
	// SetXattr update the extended attribute of a node.
	SetXattr(ctx Context, inode Ino, name string, value []byte) syscall.Errno
	// RemoveXattr removes the extended attribute of a node.
	RemoveXattr(ctx Context, inode Ino, name string) syscall.Errno
	// Flock tries to put a lock on a given file.
	Flock(ctx Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno
	// Getlk returns the current lock owner for a range on a file.
	Getlk(ctx Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno
	// Setlk sets a file range lock on a given file.
	Setlk(ctx Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno

	// CompactAll Compact all the chunks by merge small slices together
	CompactAll(ctx Context) syscall.Errno
	// ListSlices returns all slices used by all files.
	ListSlices(ctx Context, slices *[]Slice, delete bool, showProgress func()) syscall.Errno

	// OnMsg add a callback for the given message type.
	OnMsg(mtype uint32, cb MsgCallback)

	DumpMeta(w io.Writer) error
	LoadMeta(r io.Reader) error
}

func removePassword(uri string) string {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return uri
	}

	if parsedURI.User == nil || !strings.Contains(parsedURI.User.String(), ":") {
		return uri
	}

	parsedURI.User = url.User(parsedURI.User.Username())
	return parsedURI.String()
}

type Creator func(driver, addr string, conf *Config) (Meta, error)

var metaDrivers = make(map[string]Creator)

func Register(name string, register Creator) {
	metaDrivers[name] = register
}

// NewClient creates a Meta client for given uri.
func NewClient(uri string, conf *Config) Meta {
	processedURI := uri
	if !strings.Contains(processedURI, "://") {
		processedURI = "redis://" + processedURI
	}

	logger.Infof("Meta address: %s", removePassword(processedURI))

	envPassword := os.Getenv("META_PASSWORD")
	if envPassword != "" {
		parsedURI, err := url.Parse(processedURI)
		if err != nil {
			logger.Fatalf("invalid uri: %s, error: %v", processedURI, err)
		}
		if parsedURI.User != nil && strings.Contains(parsedURI.User.String(), ":") {
			parsedURI.User = url.UserPassword(parsedURI.User.Username(), envPassword)
			processedURI = parsedURI.String()
		} else if parsedURI.User == nil {
			logger.Fatalf("META_PASSWORD is set, but the URI '%s' does not contain user information.", uri)
		} else {
			parsedURI.User = url.UserPassword(parsedURI.User.Username(), envPassword)
			processedURI = parsedURI.String()
		}
	}
	parsedURI, err := url.Parse(processedURI)
	if err != nil {
		logger.Fatalf("invalid uri: %s, error: %v", processedURI, err)
	}

	driver := parsedURI.Scheme
	address := parsedURI.Host

	f, ok := metaDrivers[driver]
	if !ok {
		logger.Fatalf("Invalid meta driver: %s", driver)
	}

	m, err := f(driver, address, conf)
	if err != nil {
		logger.Fatalf("Meta is not available: %s", err)
	}
	return m
}

func newSessionInfo() (*SessionInfo, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &SessionInfo{Version: version.Version(), Hostname: host, ProcessID: os.Getpid()}, nil
}
