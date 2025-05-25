// cmd/info.go

package main

import (
	"AveFS/pkg/meta"
	"AveFS/pkg/utils"
	"fmt"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
)

func infoFlags() *cli.Command {
	return &cli.Command{
		Name:      "info",
		Usage:     "show internal information for paths or inodes",
		ArgsUsage: "PATH or INODE",
		Action:    info,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "inode",
				Aliases: []string{"i"},
				Usage:   "use inode instead of path (current dir should be inside AveFS)",
			},
		},
	}
}

func info(ctx *cli.Context) error {
	if runtime.GOOS == "windows" {
		logger.Infof("Windows is not supported")
		return nil
	}
	if ctx.Args().Len() < 1 {
		logger.Infof("DIR or FILE is needed")
		return nil
	}
	for i := 0; i < ctx.Args().Len(); i++ {
		path := ctx.Args().Get(i)
		var d string
		var inode uint64
		var err error
		if ctx.Bool("inode") {
			inode, err = strconv.ParseUint(path, 10, 64)
			d, _ = os.Getwd()
		} else {
			d, err = filepath.Abs(path)
			if err != nil {
				logger.Fatalf("abs of %s: %s", path, err)
			}
			inode, err = utils.GetFileInode(d)
		}
		if err != nil {
			logger.Errorf("lookup inode for %s: %s", path, err)
			continue
		}

		f := openController(d)
		if f == nil {
			logger.Errorf("%s is not inside AveFS", path)
			continue
		}

		wb := utils.NewBuffer(8 + 8)
		wb.Put32(meta.Info)
		wb.Put32(8)
		wb.Put64(inode)
		_, err = f.Write(wb.Bytes())
		if err != nil {
			logger.Fatalf("write message: %s", err)
		}

		data := make([]byte, 4)
		n, err := f.Read(data)
		if err != nil {
			logger.Fatalf("read size: %d %s", n, err)
		}
		if n == 1 && data[0] == byte(syscall.EINVAL&0xff) {
			logger.Fatalf("info is not supported, please upgrade and mount again")
		}
		r := utils.ReadBuffer(data)
		size := r.Get32()
		data = make([]byte, size)
		n, err = f.Read(data)
		if err != nil {
			logger.Fatalf("read info: %s", err)
		}
		fmt.Println(path, ":")
		fmt.Println(string(data[:n]))
		_ = f.Close()
	}

	return nil
}
