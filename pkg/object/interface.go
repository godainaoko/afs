// pkg/object/interface.go

package object

import (
    "io"
    "time"
)

type Object interface {
    Key() string
    Size() int64
    Mtime() time.Time
    IsDir() bool
}

type obj struct {
    key   string
    size  int64
    mtime time.Time
    isDir bool
}

func (o *obj) Key() string      { return o.key }
func (o *obj) Size() int64      { return o.size }
func (o *obj) Mtime() time.Time { return o.mtime }
func (o *obj) IsDir() bool      { return o.isDir }

type MultipartUpload struct {
    MinPartSize int
    MaxCount    int
    UploadID    string
}

type Part struct {
    Num  int
    Size int
    ETag string
}

type PendingPart struct {
    Key      string
    UploadID string
    Created  time.Time
}

// ObjectStorage is the interface for object storage.
// all of these APIs should be idempotent.
type ObjectStorage interface {
    // String Description of the object storage.
    String() string
    // Create the bucket if not existed.
    Create() error
    // Get the data for the given object specified by a key.
    Get(key string, off, limit int64) (io.ReadCloser, error)
    // Put data read from a reader to an object specified by a key.
    Put(key string, in io.Reader) error
    // Delete a object.
    Delete(key string) error

    // Head returns some information about the object or an error if not found.
    Head(key string) (Object, error)
    // List returns a list of objects.
    List(prefix, marker string, limit int64) ([]Object, error)
    // ListAll returns all the objects as a channel.
    ListAll(prefix, marker string) (<-chan Object, error)

    // CreateMultipartUpload starts to upload a large object part by part.
    CreateMultipartUpload(key string) (*MultipartUpload, error)
    // UploadPart upload a part of an object.
    UploadPart(key string, uploadID string, num int, body []byte) (*Part, error)
    // AbortUpload abort a multipart upload.
    AbortUpload(key string, uploadID string)
    // CompleteUpload finish a multipart upload.
    CompleteUpload(key string, uploadID string, parts []*Part) error
    // ListUploads lists existing multipart uploads.
    ListUploads(marker string) ([]*PendingPart, string, error)
}
