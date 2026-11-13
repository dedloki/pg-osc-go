package pgosc

// JSON lines logger, same shape as the Ruby version (ougai / bunyan):
// {"name","hostname","pid","level","time","v","msg",...fields,"version"}

import (
	"bytes"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Version is set at build time (-ldflags "-X .../pgosc.Version=...").
var Version = "0.12.1"

const (
	LevelTrace = 10
	LevelInfo  = 30
	LevelWarn  = 40
	LevelError = 50
	LevelFatal = 60
)

var (
	verbose  atomic.Bool
	hostname = sync.OnceValue(func() string { h, _ := os.Hostname(); return h })
	logMu    sync.Mutex
)

func SetVerbose(v bool) { verbose.Store(v) }

// F is an ordered list of extra fields: key, value, key, value...
type F []any

func logLine(level int, msg string, fields F) {
	if level < LevelInfo && !verbose.Load() {
		return
	}
	var b bytes.Buffer
	b.WriteByte('{')
	first := true
	put := func(k string, v any) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		kj, _ := json.Marshal(k)
		b.Write(kj)
		b.WriteByte(':')
		vj, err := json.Marshal(v)
		if err != nil {
			vj, _ = json.Marshal(err.Error())
		}
		b.Write(vj)
	}
	put("name", "pg-online-schema-change")
	put("hostname", hostname())
	put("pid", os.Getpid())
	put("level", level)
	put("time", time.Now().Format("2006-01-02T15:04:05.000-07:00"))
	put("v", 0)
	put("msg", msg)
	for i := 0; i+1 < len(fields); i += 2 {
		k, _ := fields[i].(string)
		put(k, fields[i+1])
	}
	put("version", Version)
	b.WriteString("}\n")

	// one write per line, unbuffered: lines show up immediately in files and pod logs
	logMu.Lock()
	_, _ = os.Stdout.Write(b.Bytes())
	logMu.Unlock()
}

func Debug(msg string, fields ...any) { logLine(LevelTrace, msg, fields) }
func Info(msg string, fields ...any)  { logLine(LevelInfo, msg, fields) }
func Warn(msg string, fields ...any)  { logLine(LevelWarn, msg, fields) }
func Error(msg string, fields ...any) { logLine(LevelError, msg, fields) }
func Fatal(msg string, fields ...any) { logLine(LevelFatal, msg, fields) }
