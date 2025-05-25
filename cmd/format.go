// cmd/format.go

package main

import (
	"AveFS/pkg/compress"
	"AveFS/pkg/meta"
	"AveFS/pkg/object"
	"AveFS/pkg/version"
	"bytes"
	crand "crypto/rand"
	"fmt"
	"github.com/urfave/cli/v2"
	"io"
	"math/rand"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
)

func fixObjectSize(s int) int {
	const nim, xam = 64, 16 << 10
	var bits uint
	for s > 1 {
		bits++
		s >>= 1
	}
	s = s << bits
	if s < nim {
		s = nim
	} else if s > xam {
		s = xam
	}
	return s
}

func createStorage(format *meta.Format) (object.ObjectStorage, error) {
	object.UserAgent = "AveFS-" + version.Version()
	var blob object.ObjectStorage
	var err error
	if format.Shards > 1 {
		blob, err = object.NewSharded(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.Shards)
	} else {
		blob, err = object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey)
	}
	if err != nil {
		return nil, err
	}
	blob = object.WithPrefix(blob, format.Name+"/")

	if format.EncryptKey != "" {
		passphrase := os.Getenv("AFS_RSA_PASSPHRASE")
		privKey, err := object.ParseRsaPrivateKeyFromPem(format.EncryptKey, passphrase)
		if err != nil {
			return nil, fmt.Errorf("load private key: %s", err)
		}
		encryptor := object.NewAESEncryptor(object.NewRSAEncryptor(privKey))
		blob = object.NewEncrypted(blob, encryptor)
	}
	return blob, nil
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func doTesting(store object.ObjectStorage, key string, data []byte) error {
	if err := store.Put(key, bytes.NewReader(data)); err != nil {
		if strings.Contains(err.Error(), "Access Denied") {
			return fmt.Errorf("Failed to put: %s", err)
		}
		if err2 := store.Create(); err2 != nil {
			return fmt.Errorf("Failed to create %s: %s,  previous error: %s\nplease create bucket %s manually, then format again",
				store, err2, err, store)
		}
		if err := store.Put(key, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("Failed to put: %s", err)
		}
	}
	p, err := store.Get(key, 0, -1)
	if err != nil {
		return fmt.Errorf("Failed to get: %s", err)
	}
	data2, err := io.ReadAll(p)
	_ = p.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(data, data2) {
		return fmt.Errorf("Read wrong data")
	}
	err = store.Delete(key)
	if err != nil {
		// it's OK to don't have deletion permission
		fmt.Printf("Failed to delete: %s", err)
	}
	return nil
}

func test(store object.ObjectStorage) error {
	key := "testing/" + randSeq(10)
	data := make([]byte, 100)
	crand.Read(data)
	nRetry := 3
	var err error
	for i := 0; i < nRetry; i++ {
		err = doTesting(store, key, data)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i*3+1))
	}
	return err
}

func format(c *cli.Context) error {
	setLoggerLevel(c)
	if c.Args().Len() < 1 {
		logger.Fatalf("Meta URL and name are required")
	}
	m := meta.NewClient(c.Args().Get(0), &meta.Config{Retries: 2})

	if c.Args().Len() < 2 {
		logger.Fatalf("Please give it a name")
	}
	name := c.Args().Get(1)
	validName := regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$`)
	if !validName.MatchString(name) {
		logger.Fatalf("invalid name: %s, only alphabet, number and - are allowed, and the length should be 3 to 63 characters.", name)
	}

	compressor := compress.NewCompressor(c.String("compress"))
	if compressor == nil {
		logger.Fatalf("Unsupported compress algorithm: %s", c.String("compress"))
	}
	if c.Bool("no-update") {
		if _, err := m.Load(); err == nil {
			return nil
		}
	}

	format := meta.Format{
		Name:        name,
		UUID:        uuid.New().String(),
		Storage:     c.String("storage"),
		Bucket:      c.String("bucket"),
		AccessKey:   c.String("access-key"),
		SecretKey:   c.String("secret-key"),
		Shards:      c.Int("shards"),
		Capacity:    c.Uint64("capacity") << 30,
		Inodes:      c.Uint64("inodes"),
		BlockSize:   fixObjectSize(c.Int("block-size")),
		Compression: c.String("compress"),
	}
	if format.AccessKey == "" && os.Getenv("ACCESS_KEY") != "" {
		format.AccessKey = os.Getenv("ACCESS_KEY")
		os.Unsetenv("ACCESS_KEY")
	}
	if format.SecretKey == "" && os.Getenv("SECRET_KEY") != "" {
		format.SecretKey = os.Getenv("SECRET_KEY")
		os.Unsetenv("SECRET_KEY")
	}

	if format.Storage == "file" && !strings.HasSuffix(format.Bucket, "/") {
		format.Bucket += "/"
	}

	keyPath := c.String("encrypt-rsa-key")
	if keyPath != "" {
		pem, err := os.ReadFile(keyPath)
		if err != nil {
			logger.Fatalf("load RSA key from %s: %s", keyPath, err)
		}
		format.EncryptKey = string(pem)
	}

	blob, err := createStorage(&format)
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data uses %s", blob)
	if err := test(blob); err != nil {
		logger.Fatalf("Storage %s is not configured correctly: %s", blob, err)
	}

	err = m.Init(format, c.Bool("force"))
	if err != nil {
		logger.Fatalf("format: %s", err)
	}
	format.RemoveSecret()
	logger.Infof("Volume is formatted as %+v", format)
	return nil
}

func formatFlags() *cli.Command {
	var defaultBucket string
	switch runtime.GOOS {
	case "darwin":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
		}
		defaultBucket = path.Join(homeDir, ".avefs", "local")
	case "windows":
		defaultBucket = path.Join("C:/afs/local")
	default:
		defaultBucket = "/var/afs"
	}
	return &cli.Command{
		Name:      "format",
		Usage:     "format a volume",
		ArgsUsage: "META-URL NAME",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "block-size",
				Value: 4096,
				Usage: "size of block in KiB",
			},
			&cli.Uint64Flag{
				Name:  "capacity",
				Value: 0,
				Usage: "the limit for space in GiB",
			},
			&cli.Uint64Flag{
				Name:  "inodes",
				Value: 0,
				Usage: "the limit for number of inodes",
			},
			&cli.StringFlag{
				Name:  "compress",
				Value: "none",
				Usage: "compression algorithm (lz4, zstd, none)",
			},
			&cli.IntFlag{
				Name:  "shards",
				Value: 0,
				Usage: "store the blocks into N buckets by hash of key",
			},
			&cli.StringFlag{
				Name:  "storage",
				Value: "file",
				Usage: "Object storage type (e.g. s3, gcs, oss, cos)",
			},
			&cli.StringFlag{
				Name:  "bucket",
				Value: defaultBucket,
				Usage: "A bucket URL to store data",
			},
			&cli.StringFlag{
				Name:  "access-key",
				Usage: "Access key for object storage (env ACCESS_KEY)",
			},
			&cli.StringFlag{
				Name:  "secret-key",
				Usage: "Secret key for object storage (env SECRET_KEY)",
			},
			&cli.StringFlag{
				Name:  "encrypt-rsa-key",
				Usage: "A path to RSA private key (PEM)",
			},

			&cli.BoolFlag{
				Name:  "force",
				Usage: "overwrite existing format",
			},
			&cli.BoolFlag{
				Name:  "no-update",
				Usage: "don't update existing volume",
			},
		},
		Action: format,
	}
}
