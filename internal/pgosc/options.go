package pgosc

// Raw command line arguments and the validated options (the Ruby CLI + Client).

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	PullBatchCount = 1000
	DeltaCount     = 20
	SyncInterval   = 1.0
)

type ConnArgs struct {
	Schema   string
	DBName   string
	Host     string
	Username string
	Port     uint16
	Password string
	SSLMode  string
}

func (c ConnArgs) ConnOpts() ConnOpts {
	return ConnOpts{
		Host:     c.Host,
		Port:     c.Port,
		DBName:   c.DBName,
		User:     c.Username,
		Password: c.Password,
		SSLMode:  c.SSLMode,
	}
}

type PerformArgs struct {
	AlterStatement           string
	Conn                     ConnArgs
	Verbose                  bool
	Drop                     bool
	KillBackends             bool
	WaitTimeForLock          uint64
	CopyStatement            string
	PullBatchCount           int64
	DeltaCount               int64
	SkipForeignKeyValidation bool
	Filter                   string
	WaitForSwap              bool
	SyncInterval             float64
	CopyBatchPages           uint64 // 0 = one INSERT ... SELECT
	DeferIndexes             bool
	MaintenanceWorkMem       string
	SkipVacuum               bool
}

// Options are the validated options for `perform`.
type Options struct {
	AlterStatement           string
	Schema                   string
	Conn                     ConnOpts
	Drop                     bool
	KillBackends             bool
	WaitTimeForLock          uint64
	CopyStatement            string // contents of the file, "" = none
	PullBatchCount           int64
	DeltaCount               int64
	SkipForeignKeyValidation bool
	Filter                   string // "" = none
	WaitForSwap              bool
	SyncInterval             float64
	CopyBatchPages           uint64
	DeferIndexes             bool
	MaintenanceWorkMem       string
	SkipVacuum               bool
	// Table as Postgres sees it (unquoted)
	Table string
	// TableName as it goes into SQL (quoted if needed)
	TableName string
}

func OptionsFromArgs(a PerformArgs) (*Options, error) {
	copyStatement := ""
	if a.CopyStatement != "" {
		p, err := filepath.Abs(a.CopyStatement)
		if err != nil {
			return nil, err
		}
		if st, err := os.Stat(p); err != nil || !st.Mode().IsRegular() {
			return nil, fmt.Errorf("File not found: %s", p)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		copyStatement = string(b)
	}

	alter := a.AlterStatement
	if !IsAlterStatement(alter) {
		return nil, fmt.Errorf("Not a valid ALTER statement: %s", alter)
	}
	if a.DeltaCount > a.PullBatchCount {
		return nil, errors.New("Value for delta_count should be smaller than the value for pull_batch_count")
	}
	if !SameTable(alter) {
		return nil, fmt.Errorf("All statements should belong to the same table: %s", alter)
	}

	filter := NormalizeFilter(a.Filter)
	if filter != "" {
		if copyStatement != "" {
			return nil, errors.New("--filter and --copy-statement can't be used together, put the WHERE clause into the copy statement or drop it")
		}
		if !ValidFilter(filter) {
			return nil, fmt.Errorf("Not a valid filter expression: %s", filter)
		}
	}
	if math.IsNaN(a.SyncInterval) || a.SyncInterval < 0 {
		return nil, errors.New("--sync-interval should be >= 0")
	}
	if a.CopyBatchPages > 0 && copyStatement != "" {
		return nil, errors.New("--copy-batch-pages and --copy-statement can't be used together")
	}

	table, err := Table(alter)
	if err != nil {
		return nil, err
	}
	return &Options{
		AlterStatement:           alter,
		Schema:                   a.Conn.Schema,
		Conn:                     a.Conn.ConnOpts(),
		Drop:                     a.Drop,
		KillBackends:             a.KillBackends,
		WaitTimeForLock:          a.WaitTimeForLock,
		CopyStatement:            copyStatement,
		PullBatchCount:           a.PullBatchCount,
		DeltaCount:               a.DeltaCount,
		SkipForeignKeyValidation: a.SkipForeignKeyValidation,
		Filter:                   filter,
		WaitForSwap:              a.WaitForSwap,
		SyncInterval:             a.SyncInterval,
		CopyBatchPages:           a.CopyBatchPages,
		DeferIndexes:             a.DeferIndexes,
		MaintenanceWorkMem:       strings.TrimSpace(a.MaintenanceWorkMem),
		SkipVacuum:               a.SkipVacuum,
		Table:                    table,
		TableName:                TableName(alter, table),
	}, nil
}
