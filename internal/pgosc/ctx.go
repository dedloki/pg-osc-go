package pgosc

// State of one `perform` run: options, the connection and every name the
// Ruby version kept in its global Store.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Flags are set from the signal goroutine.
type Flags struct {
	SwapRequested atomic.Bool
	// signal number that asked us to stop, 0 = none
	Terminate atomic.Int32

	mu sync.Mutex
	db *DB
}

func (f *Flags) Terminating() (int, bool) {
	s := f.Terminate.Load()
	return int(s), s != 0
}

// CancelQuery cancels the running query on the main connection, if any.
func (f *Flags) CancelQuery() {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	if db != nil {
		db.Cancel()
	}
}

func (f *Flags) setDB(db *DB) {
	f.mu.Lock()
	f.db = db
	f.mu.Unlock()
}

// Terminated is returned when a TERM/INT/QUIT arrived; the caller cleans up and exits.
type Terminated struct{ Signal int }

func (t Terminated) Error() string { return fmt.Sprintf("terminated by signal %d", t.Signal) }

var ErrLockNotAcquired = errors.New("Couldn't acquire ACCESS EXCLUSIVE lock")

type Ctx struct {
	opts  *Options
	db    *DB
	flags *Flags

	oldPrimaryTable      string
	auditTable           string
	operationTypeColumn  string
	triggerTimeColumn    string
	auditTablePK         string
	shadowTable          string
	auditTablePKSequence *string

	primaryTableStorageParameters   string
	referentialForeignKeyStatements string
	selfForeignKeyStatements        string
	triggerStatements               string
	primaryKey                      *string

	droppedColumns []string
	renamedColumns []Rename
	// (insert into columns, select columns) for the replay, cached
	copyColumnsCache *[2][]string
	// indexes and constraints removed from the shadow table for --defer-indexes
	deferredIndexes []ShadowIndex
}

func newCtx(opts *Options, db *DB, flags *Flags, id string) *Ctx {
	t := strings.ToLower(opts.Table)
	return &Ctx{
		opts:                opts,
		db:                  db,
		flags:               flags,
		oldPrimaryTable:     "pgosc_op_table_" + t,
		auditTable:          fmt.Sprintf("pgosc_at_%s_%s", t, id),
		operationTypeColumn: "operation_type_" + id,
		triggerTimeColumn:   "trigger_time_" + id,
		auditTablePK:        fmt.Sprintf("at_%s_id", id),
		shadowTable:         fmt.Sprintf("pgosc_st_%s_%s", t, id),
	}
}

func (c *Ctx) checkTerminate() error {
	if sig, ok := c.flags.Terminating(); ok {
		return Terminated{sig}
	}
	return nil
}

func randomID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
