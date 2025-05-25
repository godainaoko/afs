// pkg/utils/logger_syslog.go

package utils

import (
	"fmt"
	"github.com/sirupsen/logrus"
	logrusSyslog "github.com/sirupsen/logrus/hooks/syslog"
	"log/syslog"
	"os"
)

type SyslogHook struct {
	*logrusSyslog.SyslogHook
}

func (hook *SyslogHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read entry, %v", err)
		return err
	}

	// drop the timestamp
	line = line[27:]

	switch entry.Level {
	case logrus.PanicLevel:
		return hook.Writer.Crit(line)
	case logrus.FatalLevel:
		return hook.Writer.Crit(line)
	case logrus.ErrorLevel:
		return hook.Writer.Err(line)
	case logrus.WarnLevel:
		return hook.Writer.Warning(line)
	case logrus.InfoLevel:
		return hook.Writer.Info(line)
	case logrus.DebugLevel:
		return hook.Writer.Debug(line)
	default:
		return nil
	}
}

func InitLoggers(logToSyslog bool) {
	if logToSyslog {
		hook, err := logrusSyslog.NewSyslogHook("", "", syslog.LOG_DEBUG|syslog.LOG_USER, "")
		if err != nil {
			// println("Unable to connect to local syslog daemon")
			return
		}
		syslogHook = &SyslogHook{hook}

		for _, l := range loggers {
			l.Hooks.Add(syslogHook)
		}
	}
}
