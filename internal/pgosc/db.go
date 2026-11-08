package pgosc

// Connection wrapper with the transaction semantics of the Ruby Query.run:
// every call opens a transaction, reuse = true leaves it open for the next
// call, an error rolls it back. Simple query protocol, all values as text.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

type ConnOpts struct {
	Host     string
	Port     uint16
	DBName   string
	User     string
	Password string
	SSLMode  string
}

func connValue(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// connString: libpq semantics for sslmode prefer/require (encrypt, don't
// verify the certificate), which is what pgconn does too.
func (o ConnOpts) connString() (string, error) {
	switch o.SSLMode {
	case "disable", "prefer", "require":
	default:
		return "", fmt.Errorf("Unsupported sslmode %s, use disable, prefer or require", o.SSLMode)
	}
	parts := []string{
		"host=" + connValue(o.Host),
		"port=" + strconv.Itoa(int(o.Port)),
		"dbname=" + connValue(o.DBName),
		"user=" + connValue(o.User),
		"sslmode=" + o.SSLMode,
		"application_name=pg-osc",
	}
	if o.Password != "" {
		parts = append(parts, "password="+connValue(o.Password))
	}
	return strings.Join(parts, " "), nil
}

// Row is one result row, values as text (NULL is nil), columns in query order.
type Row struct {
	Columns []string
	Values  []*string
}

func (r Row) Get(column string) (string, bool) {
	for i, c := range r.Columns {
		if c == column {
			if r.Values[i] == nil {
				return "", false
			}
			return *r.Values[i], true
		}
	}
	return "", false
}

// Val returns the value or "" for NULL / missing.
func (r Row) Val(column string) string {
	v, _ := r.Get(column)
	return v
}

func (r Row) Ptr(column string) *string {
	if v, ok := r.Get(column); ok {
		return &v
	}
	return nil
}

type DB struct {
	conn *pgconn.PgConn
}

func Connect(opts ConnOpts) (*DB, error) {
	cs, err := opts.connString()
	if err != nil {
		return nil, err
	}
	conn, err := pgconn.Connect(context.Background(), cs)
	if err != nil {
		return nil, err
	}
	Debug("Connection established")
	return &DB{conn: conn}, nil
}

func (d *DB) Close() { _ = d.conn.Close(context.Background()) }

// Cancel cancels the query running on this connection (from another goroutine).
func (d *DB) Cancel() { _ = d.conn.CancelRequest(context.Background()) }

func (d *DB) exec(sql string) ([]*pgconn.Result, error) {
	return d.conn.Exec(context.Background(), sql).ReadAll()
}

func (d *DB) Run(sql string, reuse bool) ([]Row, error) {
	results, err := d.runResults(sql, reuse)
	if err != nil {
		return nil, err
	}
	return toRows(results), nil
}

// Execute is like Run, returns the number of rows the statements touched.
func (d *DB) Execute(sql string, reuse bool) (int64, error) {
	results, err := d.runResults(sql, reuse)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, r := range results {
		n += r.CommandTag.RowsAffected()
	}
	return n, nil
}

func (d *DB) runResults(sql string, reuse bool) ([]*pgconn.Result, error) {
	Debug("Running query", "query", shorten(sql))
	// BEGIN inside an open transaction is only a warning, same as in the Ruby version
	_, err := d.exec("BEGIN;")
	var results []*pgconn.Result
	if err == nil {
		results, err = d.exec(sql)
	}
	if err != nil {
		Info("Exception raised, rolling back query", "rollback", true, "query", shorten(sql))
		_, _ = d.exec("ROLLBACK;")
		return nil, err
	}
	if !reuse {
		if _, err := d.exec("COMMIT;"); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// ExecRaw runs outside of any transaction (VACUUM can't run in one).
func (d *DB) ExecRaw(sql string) error {
	Debug("Running query", "query", shorten(sql))
	_, err := d.exec(sql)
	return err
}

// FirstValue returns the first column of the first row, nil for NULL / no rows.
func (d *DB) FirstValue(sql string, reuse bool) (*string, error) {
	rows, err := d.Run(sql, reuse)
	if err != nil || len(rows) == 0 || len(rows[0].Values) == 0 {
		return nil, err
	}
	return rows[0].Values[0], nil
}

const loggedQueryLimit = 2000

// shorten: replay batches are thousands of statements, log only the head of them.
func shorten(sql string) string {
	if len(sql) <= loggedQueryLimit {
		return sql
	}
	end := loggedQueryLimit
	for end > 0 && !utf8.RuneStart(sql[end]) {
		end--
	}
	return fmt.Sprintf("%s... (%d bytes total)", sql[:end], len(sql))
}

func toRows(results []*pgconn.Result) []Row {
	var rows []Row
	for _, res := range results {
		cols := make([]string, len(res.FieldDescriptions))
		for i, f := range res.FieldDescriptions {
			cols[i] = f.Name
		}
		for _, raw := range res.Rows {
			vals := make([]*string, len(raw))
			for i, v := range raw {
				if v != nil {
					s := string(v)
					vals[i] = &s
				}
			}
			rows = append(rows, Row{Columns: cols, Values: vals})
		}
	}
	return rows
}

// SQLState of a Postgres error, "" otherwise.
func SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func IsPgError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr)
}
