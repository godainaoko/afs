// pkg/utils/utils.go

package utils

import (
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func CopyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

// SplitDir splits a path with the default path list separator or comma.
func SplitDir(d string) []string {
	dd := strings.Split(d, string(os.PathListSeparator))
	if len(dd) == 1 {
		dd = strings.Split(dd[0], ",")
	}
	return dd
}

// NewDynProgressBar init a dynamic progress bar,the title will appears at the head of the progress bar
func NewDynProgressBar(title string, quiet bool) (*mpb.Progress, *mpb.Bar) {
	var progress *mpb.Progress
	if !quiet && isatty.IsTerminal(os.Stdout.Fd()) {
		progress = mpb.New(mpb.WithWidth(64))
	} else {
		progress = mpb.New(mpb.WithWidth(64), mpb.WithOutput(nil))
	}
	bar := progress.AddBar(0,
		mpb.PrependDecorators(
			decor.Name(title, decor.WCSyncWidth),
			decor.CountersNoUnit("%d / %d"),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.Percentage(decor.WC{W: 5}), "done"),
		),
	)
	return progress, bar
}
