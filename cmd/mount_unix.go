// cmd/mount_unix.go

package main

import (
	"AveFS/pkg/chunk"
	"AveFS/pkg/fuse"
	"AveFS/pkg/meta"
	"AveFS/pkg/vfs"
	"bytes"
	"github.com/juicedata/godaemon"
	"github.com/urfave/cli/v2"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

func checkMountpoint(name, mp string) {
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * 500)
		st, err := os.Stat(mp)
		if err == nil {
			if sys, ok := st.Sys().(*syscall.Stat_t); ok && sys.Ino == 1 {
				logger.Infof("\033[92mOK\033[0m, %s is ready at %s", name, mp)
				return
			}
		}
		os.Stdout.WriteString(".")
		os.Stdout.Sync()
	}
	os.Stdout.WriteString("\n")
	logger.Fatalf("fail to mount after 10 seconds, please mount in foreground")
}

func makeDaemon(c *cli.Context, name, mp string) error {
	var attrs godaemon.DaemonAttr
	attrs.OnExit = func(stage int) error {
		if stage != 0 {
			return nil
		}
		checkMountpoint(name, mp)
		return nil
	}

	// the current dir will be changed to root in daemon,
	// so the mount point has to be an absolute path.
	if godaemon.Stage() == 0 {
		for i, a := range os.Args {
			if a == mp {
				amp, err := filepath.Abs(mp)
				if err == nil {
					os.Args[i] = amp
				} else {
					logger.Warnf("abs of %s: %s", mp, err)
				}
			}
		}
		var err error
		logfile := c.String("log")
		attrs.Stdout, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Errorf("open log file %s: %s", logfile, err)
		}
	}
	_, _, err := godaemon.MakeDaemon(&attrs)
	return err
}

func mountFlag() []cli.Flag {
	var defaultLogDir = "/var/log"
	switch runtime.GOOS {
	case "darwin":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return nil
		}
		defaultLogDir = path.Join(homeDir, ".avefs")
	}
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "d",
			Aliases: []string{"background"},
			Usage:   "run in background",
		},
		&cli.BoolFlag{
			Name:  "no-syslog",
			Usage: "disable syslog",
		},
		&cli.StringFlag{
			Name:  "log",
			Value: path.Join(defaultLogDir, "avefs.log"),
			Usage: "path of log file when running in background",
		},
		&cli.StringFlag{
			Name:  "o",
			Usage: "other FUSE options",
		},
		&cli.Float64Flag{
			Name:  "attr-cache",
			Value: 1.0,
			Usage: "attributes cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "entry-cache",
			Value: 1.0,
			Usage: "file entry cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "dir-entry-cache",
			Value: 1.0,
			Usage: "dir entry cache timeout in seconds",
		},
		&cli.BoolFlag{
			Name:  "enable-xattr",
			Usage: "enable extended attributes (xattr)",
		},
	}
}

func disableUpdateDb() {
	p := "/etc/updateDb.conf"
	data, err := os.ReadFile(p)
	if err != nil {
		return
	}
	fstype := "fuse.avefs"
	if bytes.Contains(data, []byte(fstype)) {
		return
	}
	// assume that fuse.sshfs is already in PRUNEFS
	knownFS := "fuse.sshfs"
	p1 := bytes.Index(data, []byte("PRUNEFS"))
	p2 := bytes.Index(data, []byte(knownFS))
	if p1 > 0 && p2 > p1 {
		var nd []byte
		nd = append(nd, data[:p2]...)
		nd = append(nd, fstype...)
		nd = append(nd, ' ')
		nd = append(nd, data[p2:]...)
		err = os.WriteFile(p, nd, 0644)
		if err != nil {
			logger.Warnf("update %s: %s", p, err)
		} else {
			logger.Infof("Add %s into PRUNEFS of %s", fstype, p)
		}
	}
}

func mountMain(conf *vfs.Config, m meta.Meta, store chunk.ChunkStore, c *cli.Context) {
	if os.Getuid() == 0 && os.Getpid() != 1 {
		disableUpdateDb()
	}

	logger.Infof("Mounting volume %s at %s ...", conf.Format.Name, conf.Mountpoint)
	err := fuse.Serve(conf, c.String("o"), c.Float64("attr-cache"), c.Float64("entry-cache"), c.Float64("dir-entry-cache"), c.Bool("enable-xattr"))
	if err != nil {
		logger.Fatalf("fuse: %s", err)
	}
}
