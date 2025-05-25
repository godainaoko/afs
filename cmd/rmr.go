// cmd/rmr.go

package main

import (
	"AveFS/pkg/meta"
	"AveFS/pkg/utils"
	"errors"
	"fmt"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

func rmrFlags() *cli.Command {
	return &cli.Command{
		Name:      "rmr",
		Usage:     "remove directories recursively",
		ArgsUsage: "PATH ...",
		Action:    rmr,
	}
}

func openController(path string) *os.File {
	f, err := os.OpenFile(filepath.Join(path, ".control"), os.O_RDWR, 0)
	if err != nil && !os.IsNotExist(err) && !errors.Is(err, syscall.ENOTDIR) {
		logger.Errorf("%s", err)
		return nil
	}
	if err != nil && path != "/" {
		return openController(filepath.Dir(path))
	}
	return f
}

func rmr(ctx *cli.Context) error {
	if runtime.GOOS == "windows" {
		logger.Infof("Windows is not supported")
		return nil
	}
	if ctx.Args().Len() < 1 {
		logger.Infof("PATH is needed")
		return nil
	}
	for i := 0; i < ctx.Args().Len(); i++ {
		path := ctx.Args().Get(i)
		p, err := filepath.Abs(path)
		if err != nil {
			logger.Errorf("abs of %s: %s", path, err)
			continue
		}
		d := filepath.Dir(p)
		name := filepath.Base(p)
		inode, err := utils.GetFileInode(d)
		if err != nil {
			return fmt.Errorf("lookup inode for %s: %s", d, err)
		}
		f := openController(d)
		if f == nil {
			logger.Errorf("%s is not inside AveFS", path)
			continue
		}
		wb := utils.NewBuffer(8 + 8 + 1 + uint32(len(name)))
		wb.Put32(meta.Rmr)
		wb.Put32(8 + 1 + uint32(len(name)))
		wb.Put64(inode)
		wb.Put8(uint8(len(name)))
		wb.Put([]byte(name))
		_, err = f.Write(wb.Bytes())
		if err != nil {
			logger.Fatalf("write message: %s", err)
		}
		var errs = make([]byte, 1)
		n, err := f.Read(errs)
		if err != nil || n != 1 {
			logger.Fatalf("read message: %d %s", n, err)
		}
		if errs[0] != 0 {
			errno := syscall.Errno(errs[0])
			if runtime.GOOS == "windows" {
				errno += 0x20000000
			}
			logger.Fatalf("RMR %s: %s", path, errno)
		}
		_ = f.Close()
	}
	return nil
}
