// pkg/utils/rusage.go

package utils

import "syscall"

type Rusage struct {
	syscall.Rusage
}

func (ru *Rusage) GetUtime() float64 {
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
}

func (ru *Rusage) GetStime() float64 {
	return float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}

func GetRusage() *Rusage {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return &Rusage{ru}
}
