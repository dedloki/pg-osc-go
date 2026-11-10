package pgosc

// End-to-end tests against a real Postgres (see README: `docker compose up`).
// Connection: PGHOST (127.0.0.1), PGPORT (5432), PGUSER (jamesbond),
// PGDATABASE (postgres), PGPASSWORD. Every test works in its own schema, so
// they run in parallel.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func connArgs(schema string) ConnArgs {
	port, _ := strconv.Atoi(env("PGPORT", "5432"))
	return ConnArgs{
		Schema:   schema,
		DBName:   env("PGDATABASE", "postgres"),
		Host:     env("PGHOST", "127.0.0.1"),
		Username: env("PGUSER", "jamesbond"),
		Port:     uint16(port),
		Password: os.Getenv("PGPASSWORD"),
		SSLMode:  "disable",
	}
}

func conn(schema string) ConnOpts { return connArgs(schema).ConnOpts() }

func args(schema, alter string) PerformArgs {
	return PerformArgs{
		AlterStatement:  alter,
		Conn:            connArgs(schema),
		WaitTimeForLock: 5,
		PullBatchCount:  1000,
		DeltaCount:      20,
		SyncInterval:    0.05,
	}
}

type testDB struct {
	t      *testing.T
	schema string
	db     *DB
}

// newTestDB creates the same tables as the Ruby specs (spec/support/database_helpers.rb).
func newTestDB(t *testing.T, name string) *testDB {
	t.Parallel()
	schema := "pgosc_test_go_" + name
	db, err := Connect(conn(schema))
	if err != nil {
		t.Fatalf("Postgres for tests, see README: %v", err)
	}
	td := &testDB{t: t, schema: schema, db: db}
	td.exec(fmt.Sprintf(`
		DROP SCHEMA IF EXISTS "%[1]s" CASCADE;
		CREATE SCHEMA "%[1]s";
		SET search_path TO "%[1]s";
		CREATE TABLE sellers (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL, "createdOn" TIMESTAMP NOT NULL, last_login TIMESTAMP);
		CREATE TABLE books (
		  user_id serial PRIMARY KEY,
		  username VARCHAR(50) UNIQUE NOT NULL,
		  seller_id SERIAL REFERENCES sellers NOT NULL,
		  password VARCHAR(50) NOT NULL,
		  email VARCHAR(255) UNIQUE NOT NULL,
		  "createdOn" TIMESTAMP NOT NULL,
		  last_login TIMESTAMP
		) WITH (autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000);
		CREATE TABLE book_audits (id serial PRIMARY KEY, book_id SERIAL REFERENCES books NOT NULL, changed_on TIMESTAMP(6) NOT NULL);
		CREATE OR REPLACE FUNCTION email_changes() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
		BEGIN
		  IF NEW.email <> OLD.email THEN
		    INSERT INTO book_audits(book_id, changed_on) VALUES (OLD.user_id, now());
		  END IF;
		  RETURN NEW;
		END; $$;
		CREATE TRIGGER email_changes AFTER UPDATE ON books FOR EACH ROW EXECUTE PROCEDURE email_changes();
		CREATE TABLE chapters (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL, book_id SERIAL REFERENCES books NOT NULL);
		INSERT INTO sellers(name, "createdOn", last_login) VALUES ('local shop', clock_timestamp(), clock_timestamp());
		INSERT INTO books(seller_id, username, password, email, "createdOn", last_login) VALUES
		  (1, 'jamesbond2', '007', 'james1@bond.com', clock_timestamp(), clock_timestamp()),
		  (1, 'jamesbond3', '008', 'james2@bond.com', clock_timestamp(), clock_timestamp()),
		  (1, 'jamesbond4', '009', 'james3@bond.com', clock_timestamp(), clock_timestamp());
		INSERT INTO chapters(name, book_id) VALUES ('ch1', 1);
		CREATE VIEW books_view AS SELECT * FROM books WHERE seller_id = 1;`, schema))
	t.Cleanup(func() {
		_, _ = db.Run(fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schema), false)
		db.Close()
	})
	return td
}

func (d *testDB) exec(sql string) {
	d.t.Helper()
	if _, err := d.db.Run(sql, false); err != nil {
		d.t.Fatalf("%v\n%s", err, sql)
	}
}

func (d *testDB) values(sql string) []string {
	d.t.Helper()
	rows, err := d.db.Run(sql, false)
	if err != nil {
		d.t.Fatalf("%v\n%s", err, sql)
	}
	out := []string{}
	for _, r := range rows {
		parts := make([]string, len(r.Values))
		for i, v := range r.Values {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = *v
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func (d *testDB) pgoscTables() []string {
	return d.values(fmt.Sprintf(`SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = '%s' AND relkind = 'r' AND relname LIKE 'pgosc%%' ORDER BY 1`, d.schema))
}

func (d *testDB) find(prefix string) string {
	for _, n := range d.pgoscTables() {
		if strings.HasPrefix(n, prefix) {
			return n
		}
	}
	return ""
}

func eq(t *testing.T, got, want any, what ...any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v: got %#v, want %#v", what, got, want)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > 30*time.Second {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func mustOptions(t *testing.T, a PerformArgs) *Options {
	t.Helper()
	o, err := OptionsFromArgs(a)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func spawnPerform(t *testing.T, a PerformArgs) (*Flags, <-chan error) {
	flags := &Flags{}
	opts := mustOptions(t, a)
	done := make(chan error, 1)
	go func() { done <- Run(opts, flags) }()
	return flags, done
}

func finished(done <-chan error) bool { return len(done) > 0 }

func wait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		t.Fatal("perform didn't finish")
		return nil
	}
}

// waitForSync waits until the background perform is in its sync loop
// (autovacuum is switched on right before it starts) and the copy is done.
func waitForSync(t *testing.T, d *testDB, minRows int) {
	waitUntil(t, "initial copy", func() bool {
		shadow, audit := d.find("pgosc_st_"), d.find("pgosc_at_")
		if shadow == "" || audit == "" {
			return false
		}
		ready := d.values(fmt.Sprintf("SELECT 1 FROM pg_class WHERE relname = '%s' AND reloptions @> ARRAY['autovacuum_enabled=true']", audit))
		if len(ready) == 0 {
			return false
		}
		n, _ := strconv.Atoi(d.values(fmt.Sprintf(`SELECT count(*) FROM "%s".%s`, d.schema, shadow))[0])
		return n >= minRows
	})
}

func errContains(t *testing.T, err error, s string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), s) {
		t.Fatalf("want error containing %q, got %v", s, err)
	}
}

func TestOptionsValidation(t *testing.T) {
	ok := func(a PerformArgs) error { _, err := OptionsFromArgs(a); return err }

	errContains(t, ok(args("s", "CREATE DATABASE foo")), "Not a valid ALTER statement")

	a := args("s", "ALTER TABLE books ADD COLUMN a int")
	a.DeltaCount, a.PullBatchCount = 100, 50
	errContains(t, ok(a), "delta_count")

	errContains(t, ok(args("s", "ALTER TABLE books ADD COLUMN a int; ALTER TABLE cards ADD COLUMN a int")), "same table")

	a = args("s", "ALTER TABLE books ADD COLUMN a int")
	a.Filter = "x IS NOT NULL; DROP TABLE books"
	errContains(t, ok(a), "Not a valid filter")

	a = args("s", "ALTER TABLE books ADD COLUMN a int")
	a.Filter = "WHERE x IS NOT NULL"
	a.CopyStatement = "sql.go"
	errContains(t, ok(a), "can't be used together")

	a = args("s", "ALTER TABLE books ADD COLUMN a int")
	a.CopyStatement = "no/such/file.sql"
	errContains(t, ok(a), "File not found")

	a = args("s", "ALTER TABLE books ADD COLUMN a int")
	a.CopyBatchPages = 100
	a.CopyStatement = "sql.go"
	errContains(t, ok(a), "can't be used together")

	a = args("s", "ALTER TABLE books ADD COLUMN a int")
	a.OnTableRewrite = "ignore"
	errContains(t, ok(a), "--on-table-rewrite should be fail or restart")

	a = args("s", `ALTER TABLE "Books" ADD COLUMN a int`)
	a.Filter = "  where x IS NOT NULL "
	o := mustOptions(t, a)
	eq(t, o.Filter, "x IS NOT NULL")
	eq(t, [2]string{o.Table, o.TableName}, [2]string{"Books", `"Books"`})
}

func TestPerformAddsColumnAndSwaps(t *testing.T) {
	d := newTestDB(t, "add_column")
	s := d.schema
	if err := Run(mustOptions(t, args(s, `ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;`)), &Flags{}); err != nil {
		t.Fatal(err)
	}

	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT user_id, username, purchased FROM books ORDER BY 1"),
		[]string{"1|jamesbond2|f", "2|jamesbond3|f", "3|jamesbond4|f"})
	// old table kept (no --drop), helper tables gone
	eq(t, d.pgoscTables(), []string{"pgosc_op_table_books"})
	// FKs from other tables point to the new table and are validated
	eq(t, d.values("SELECT conrelid::regclass, confrelid::regclass, convalidated FROM pg_constraint WHERE contype = 'f' AND confrelid = 'books'::regclass ORDER BY 1"),
		[]string{"book_audits|books|t", "chapters|books|t"})
	// own FK, trigger, view and storage params moved over
	eq(t, d.values("SELECT confrelid::regclass FROM pg_constraint WHERE contype = 'f' AND conrelid = 'books'::regclass"), []string{"sellers"})
	eq(t, d.values("SELECT tgname FROM pg_trigger WHERE tgrelid = 'books'::regclass AND NOT tgisinternal"), []string{"email_changes"})
	eq(t, d.values("SELECT count(*) FROM books_view"), []string{"3"})
	eq(t, d.values("SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = 'books'::regclass"),
		[]string{"autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000"})
	// the sequence continues after the copied ids
	d.exec(`INSERT INTO books(seller_id, username, password, email, "createdOn") VALUES (1, 'new', 'x', 'new@b.c', now())`)
	eq(t, d.values("SELECT max(user_id) FROM books"), []string{"4"})
}

func TestPerformWithDropRemovesOldTable(t *testing.T) {
	d := newTestDB(t, "drop_old")
	a := args(d.schema, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
	a.Drop = true
	if err := Run(mustOptions(t, a), &Flags{}); err != nil {
		t.Fatal(err)
	}
	eq(t, d.pgoscTables(), []string{})
}

func TestReplayWithRenameAndDropKeepsNulls(t *testing.T) {
	d := newTestDB(t, "rename_drop")
	s := d.schema
	// books_view is SELECT *, CREATE OR REPLACE VIEW can't drop its password column
	d.exec(fmt.Sprintf(`DROP VIEW "%s".books_view`, s))
	a := args(s, "ALTER TABLE books RENAME COLUMN email TO new_email; ALTER TABLE books DROP COLUMN password;")
	a.WaitForSwap = true
	_, done := spawnPerform(t, a)
	waitForSync(t, d, 3)

	d.exec(fmt.Sprintf(`SET search_path TO "%s";
		INSERT INTO books(seller_id, username, password, email, "createdOn", last_login) VALUES (1, 'it''s me', 'p', 'q''uote@b.c', now(), NULL);
		UPDATE books SET last_login = NULL, email = 'changed@b.c' WHERE user_id = 1;
		DELETE FROM chapters; DELETE FROM book_audits; DELETE FROM books WHERE user_id = 2;`, s))
	if err := RequestSwap(conn(s), s, "books"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}

	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT user_id, username, new_email, last_login FROM books ORDER BY 1"), []string{
		"1|jamesbond2|changed@b.c|NULL",
		"3|jamesbond4|james3@bond.com|" + d.values("SELECT last_login FROM pgosc_op_table_books WHERE user_id = 3")[0],
		"4|it's me|q'uote@b.c|NULL",
	})
	eq(t, d.values("SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema AND table_name = 'books' AND column_name = 'password'"), []string{})
}

func TestFilterCopiesAndReplaysMatchingRowsOnly(t *testing.T) {
	d := newTestDB(t, "filter")
	s := d.schema
	d.exec(fmt.Sprintf(`SET search_path TO "%s"; UPDATE books SET last_login = NULL WHERE user_id = 2`, s))

	a := args(s, "ALTER TABLE books ALTER COLUMN last_login SET NOT NULL;")
	a.Filter = "WHERE last_login IS NOT NULL"
	a.WaitForSwap = true
	// chapters/book_audits reference rows that are filtered out
	a.SkipForeignKeyValidation = true
	_, done := spawnPerform(t, a)
	waitForSync(t, d, 2)

	shadow := d.find("pgosc_st_")
	eq(t, d.values(fmt.Sprintf(`SELECT user_id FROM "%s".%s ORDER BY 1`, s, shadow)), []string{"1", "3"})

	d.exec(fmt.Sprintf(`SET search_path TO "%s";
		INSERT INTO books (user_id, seller_id, username, password, email, "createdOn", last_login)
		  VALUES (10, 1, 'with_login', 'x', 'a@b.c', now(), now()),
		         (11, 1, 'without_login', 'x', 'b@b.c', now(), NULL);
		UPDATE books SET last_login = now() WHERE user_id = 2;    -- moves in
		DELETE FROM chapters; UPDATE books SET last_login = NULL WHERE user_id = 1; -- moves out
		UPDATE books SET password = 'changed' WHERE user_id = 3;  -- stays, changed
		UPDATE books SET password = 'changed' WHERE user_id = 11; -- stays out
		DELETE FROM books WHERE user_id = 10;`, s))
	if err := RequestSwap(conn(s), s, "books"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}

	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT user_id, password FROM books ORDER BY 1"), []string{"2|008", "3|changed"})
	eq(t, d.values("SELECT attnotnull FROM pg_attribute WHERE attrelid = 'books'::regclass AND attname = 'last_login'"), []string{"t"})
	eq(t, d.values("SELECT user_id FROM pgosc_op_table_books ORDER BY 1"), []string{"1", "2", "3", "11"})
	// the old table stays writable: no trigger pointing to the dropped audit table
	d.exec("UPDATE pgosc_op_table_books SET password = 'later' WHERE user_id = 11")
}

func TestSwapWaitsForRequestAndSurvivesLockFailure(t *testing.T) {
	d := newTestDB(t, "lock_failure")
	s := d.schema
	a := args(s, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
	a.WaitForSwap = true
	a.WaitTimeForLock = 1
	flags, done := spawnPerform(t, a)
	waitForSync(t, d, 3)

	// no request -> no swap
	time.Sleep(500 * time.Millisecond)
	if finished(done) {
		t.Fatal(<-done)
	}

	// hold a lock so the swap can't get ACCESS EXCLUSIVE (3 attempts x 1s)
	blocker, err := Connect(conn(s))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.Run(fmt.Sprintf(`LOCK TABLE "%s".books IN ACCESS SHARE MODE`, s), true); err != nil {
		t.Fatal(err)
	}
	flags.SwapRequested.Store(true) // what SIGUSR1 does

	waitUntil(t, "failed swap clears the request", func() bool { return !flags.SwapRequested.Load() })
	if _, err := blocker.Run("COMMIT", false); err != nil {
		t.Fatal(err)
	}
	if finished(done) {
		t.Fatal(<-done)
	}
	for _, n := range d.pgoscTables() {
		if n == "pgosc_op_table_books" {
			t.Fatal("swapped while the lock was held")
		}
	}

	d.exec(fmt.Sprintf(`INSERT INTO "%s".books(seller_id, username, password, email, "createdOn") VALUES (1, 'late', 'x', 'late@b.c', now())`, s))
	if err := RequestSwap(conn(s), s, "books"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	eq(t, d.values(fmt.Sprintf(`SELECT count(*), count(purchased) FROM "%s".books`, s)), []string{"4|4"})
}

func TestTerminateCleansUpAndKeepsTheTable(t *testing.T) {
	d := newTestDB(t, "terminate")
	s := d.schema
	a := args(s, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
	a.WaitForSwap = true
	flags, done := spawnPerform(t, a)
	waitForSync(t, d, 3)

	flags.Terminate.Store(15)
	err := wait(t, done)
	var term Terminated
	if !errors.As(err, &term) || term.Signal != 15 {
		t.Fatalf("got %v", err)
	}

	eq(t, d.pgoscTables(), []string{})
	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT count(*) FROM books"), []string{"3"})
	eq(t, d.values("SELECT 1 FROM pg_trigger WHERE tgrelid = 'books'::regclass AND tgname = 'primary_to_audit_table_trigger'"), []string{})
}

func TestFailureAfterSwapNeverDropsTheOldTable(t *testing.T) {
	d := newTestDB(t, "keep_old")
	s := d.schema
	d.exec(fmt.Sprintf(`SET search_path TO "%s"; UPDATE books SET last_login = NULL WHERE user_id = 1`, s))

	// chapters references book 1, which is filtered out -> VALIDATE CONSTRAINT fails after the swap
	a := args(s, "ALTER TABLE books ALTER COLUMN last_login SET NOT NULL;")
	a.Filter = "last_login IS NOT NULL"
	a.Drop = true
	errContains(t, Run(mustOptions(t, a), &Flags{}), "violates foreign key")

	eq(t, d.pgoscTables(), []string{"pgosc_op_table_books"})
	eq(t, d.values("SELECT count(*) FROM pgosc_op_table_books"), []string{"3"})
}

func TestCopyStatementIsUsed(t *testing.T) {
	d := newTestDB(t, "copy_statement")
	s := d.schema
	file := filepath.Join(t.TempDir(), "copy.sql")
	err := os.WriteFile(file, []byte(`INSERT INTO %{shadow_table}(user_id, seller_id, username, password, email, "createdOn", last_login, purchased)
SELECT user_id, seller_id, username, password, email, "createdOn", last_login, true FROM ONLY books WHERE username LIKE 'james%'`), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	a := args(s, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
	a.CopyStatement = file
	if err := Run(mustOptions(t, a), &Flags{}); err != nil {
		t.Fatal(err)
	}
	eq(t, d.values(fmt.Sprintf(`SELECT count(*) FROM "%s".books WHERE purchased`, s)), []string{"3"})
}

func TestSwapRequestErrors(t *testing.T) {
	d := newTestDB(t, "swap_errors")
	errContains(t, RequestSwap(conn(d.schema), d.schema, "books"), "No running pg-osc sync")
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pg-online-schema-change")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/dedloki/pg-osc-go/cmd/pg-online-schema-change").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func TestBinaryReportsSetupErrors(t *testing.T) {
	t.Parallel()
	bin := buildBinary(t)
	cmd := exec.Command(bin, "perform",
		"-a", "ALTER TABLE b ALTER COLUMN stock_id SET NOT NULL",
		"-d", "no_such_db_pgosc", "-h", "127.0.0.1", "-u", "postgres", "-p", "1",
		"--filter", "WHERE stock_id IS NOT NULL", "--wait-for-swap")
	out, _ := cmd.Output()
	stdout := string(out)
	eq(t, cmd.ProcessState.ExitCode(), 1)
	if !strings.Contains(stdout, `"msg":"Setup failed: `) || !strings.Contains(stdout, `"dbname":"no_such_db_pgosc"`) {
		t.Fatal(stdout)
	}
}

func TestBinaryCLI(t *testing.T) {
	t.Parallel()
	bin := buildBinary(t)
	out, err := exec.Command(bin, "version").Output()
	if err != nil || strings.TrimSpace(string(out)) != Version {
		t.Fatalf("%q %v", out, err)
	}
	// -h is the host, --help prints help
	out, err = exec.Command(bin, "perform", "--help").Output()
	if err != nil || !strings.Contains(string(out), "--copy-batch-pages") || !strings.Contains(string(out), "-h, --host") {
		t.Fatalf("%s %v", out, err)
	}
	cmd := exec.Command(bin, "perform", "-a", "ALTER TABLE b ADD x int", "-d", "d", "-h", "h", "-u", "u", "--copy-batch-pages", "0")
	_ = cmd.Run()
	eq(t, cmd.ProcessState.ExitCode(), 2, "--copy-batch-pages 0")
}

// copyUnderLoad uses a table like the production `barcode`: uuid primary key,
// unique column, nullable stock_id, plain index. Writes keep going during the
// copy, the sync and up to the swap; afterwards the new table must equal the
// old one (filtered).
func copyUnderLoad(t *testing.T, name string, batchPages uint64, deferIdx, filter bool) {
	d := newTestDB(t, name)
	s := d.schema
	d.exec(fmt.Sprintf(`SET search_path TO "%s";
		CREATE TABLE barcode (
		  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
		  stock_id int,
		  barcode text NOT NULL UNIQUE,
		  quantity int,
		  marked boolean,
		  is_child boolean
		);
		CREATE INDEX barcode_stock_id_idx ON barcode (stock_id);
		INSERT INTO barcode (stock_id, barcode, quantity, marked, is_child)
		  SELECT CASE WHEN g %% 4 = 0 THEN NULL ELSE g %% 1000 END, 'bc' || g, g %% 7, g %% 2 = 0, false
		  FROM generate_series(1, 40000) g;`, s))

	var stop atomic.Bool
	var writes atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		db, err := Connect(conn(s))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()
		_, _ = db.Run(fmt.Sprintf(`SET search_path TO "%s"`, s), false)
		const pick = "SELECT id FROM barcode TABLESAMPLE SYSTEM (2) LIMIT 5"
		for n := 1; !stop.Load(); n++ {
			var sql string
			switch n % 6 {
			case 0: // non-HOT: unique column changes, row moves to another page
				sql = fmt.Sprintf("UPDATE barcode SET barcode = barcode || '-%d', quantity = quantity + 1 WHERE id IN (%s)", n, pick)
			case 1: // moves rows in and out of the filter
				sql = fmt.Sprintf("UPDATE barcode SET stock_id = NULL WHERE id IN (%s)", pick)
			case 2:
				sql = fmt.Sprintf("UPDATE barcode SET stock_id = %d WHERE id IN (%s)", n%1000, pick)
			case 3:
				sql = fmt.Sprintf("INSERT INTO barcode (stock_id, barcode, quantity) VALUES (%d, 'new%d', 1), (NULL, 'newnull%d', 1)", n%1000, n, n)
			case 4:
				sql = fmt.Sprintf("DELETE FROM barcode WHERE id IN (%s)", pick)
			default:
				sql = fmt.Sprintf("UPDATE barcode SET marked = NOT coalesce(marked, false) WHERE id IN (%s)", pick)
			}
			if _, err := db.Run(sql, false); err == nil {
				writes.Add(1)
			}
		}
	}()

	a := args(s, "ALTER TABLE barcode ALTER COLUMN stock_id SET NOT NULL;")
	a.CopyBatchPages = batchPages
	a.DeferIndexes = deferIdx
	if deferIdx {
		a.MaintenanceWorkMem = "64MB"
	}
	a.WaitForSwap = true
	if filter {
		a.Filter = "stock_id IS NOT NULL"
	} else {
		a.AlterStatement = "ALTER TABLE barcode ADD COLUMN note text;"
	}
	_, done := spawnPerform(t, a)

	// keep writing during the sync for a while, then swap under load
	waitUntil(t, "sync loop", func() bool {
		return len(d.values(fmt.Sprintf(`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = '%s' AND relname LIKE 'pgosc_at_%%' AND reloptions @> ARRAY['autovacuum_enabled=true']`, s))) > 0
	})
	time.Sleep(500 * time.Millisecond)
	// after the swap `barcode` is the new table and further writes would go
	// there only, so stop writing first (whatever is left in the audit table
	// is replayed by the swap itself)
	stop.Store(true)
	<-writerDone
	if err := RequestSwap(conn(s), s, "barcode"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	if writes.Load() <= 50 {
		t.Fatalf("the writer did too little: %d", writes.Load())
	}

	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	cols := "id, stock_id, barcode, quantity, marked, is_child"
	old := "pgosc_op_table_barcode"
	if filter {
		old += " WHERE stock_id IS NOT NULL"
	}
	// no writes since before the swap: both tables must be equal
	diff := d.values(fmt.Sprintf(`SELECT (SELECT count(*) FROM (SELECT %[1]s FROM barcode EXCEPT SELECT %[1]s FROM %[2]s) d),
		(SELECT count(*) FROM (SELECT %[1]s FROM %[2]s EXCEPT SELECT %[1]s FROM barcode) d),
		(SELECT count(*) FROM barcode), (SELECT count(*) FROM %[2]s)`, cols, old))
	parts := strings.Split(diff[0], "|")
	eq(t, parts[:2], []string{"0", "0"}, "new vs old", diff)
	eq(t, parts[2], parts[3], "row counts")
	if n, _ := strconv.Atoi(parts[2]); n <= 20000 {
		t.Fatalf("too few rows: %v", diff)
	}

	// indexes and constraints are back, primary key included
	eq(t, d.values("SELECT string_agg(contype::text, ',' ORDER BY contype) FROM pg_constraint WHERE conrelid = 'barcode'::regclass AND contype IN ('p','u')"), []string{"p,u"})
	eq(t, d.values("SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema AND tablename = 'barcode'"), []string{"3"})
	if filter {
		eq(t, d.values("SELECT attnotnull FROM pg_attribute WHERE attrelid = 'barcode'::regclass AND attname = 'stock_id'"), []string{"t"})
	}
}

func TestBatchedCopyDeferredIndexesFilterUnderLoad(t *testing.T) {
	copyUnderLoad(t, "batched_defer_filter", 1, true, true)
}

func TestBatchedCopyWithIndexesUnderLoad(t *testing.T) {
	copyUnderLoad(t, "batched_indexes", 3, false, false)
}

func TestDeferredIndexesSingleCopyUnderLoad(t *testing.T) {
	copyUnderLoad(t, "defer_single", 0, true, true)
}

func TestVacuumRunsByDefaultAndCanBeSkipped(t *testing.T) {
	for _, tc := range []struct {
		name     string
		skip     bool
		expected string
	}{{"vacuum_default", false, "1"}, {"vacuum_skipped", true, "0"}} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDB(t, tc.name)
			a := args(d.schema, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
			a.SkipVacuum = tc.skip
			if err := Run(mustOptions(t, a), &Flags{}); err != nil {
				t.Fatal(err)
			}
			// the stats belong to the relation, which is the old table after the swap
			eq(t, d.values(fmt.Sprintf("SELECT vacuum_count FROM pg_stat_user_tables WHERE schemaname = '%s' AND relname = 'pgosc_op_table_books'", d.schema)),
				[]string{tc.expected})
		})
	}
}

func TestCleanupWaitsForTheTriggerLockWithoutBlockingWrites(t *testing.T) {
	d := newTestDB(t, "cleanup_lock")
	s := d.schema
	a := args(s, "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;")
	a.WaitForSwap = true
	a.WaitTimeForLock = 1
	flags, done := spawnPerform(t, a)
	waitForSync(t, d, 3)
	if !strings.Contains(flags.CleanupSQL(), "DROP TRIGGER IF EXISTS") {
		t.Fatal(flags.CleanupSQL())
	}

	// a long transaction on the table: DROP TRIGGER can't get its lock
	blocker, err := Connect(conn(s))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.Run(fmt.Sprintf(`SELECT count(*) FROM "%s".books`, s), true); err != nil {
		t.Fatal(err)
	}
	flags.Terminate.Store(15)
	time.Sleep(2500 * time.Millisecond)
	if finished(done) {
		t.Fatal("cleanup must wait for the lock:", <-done)
	}
	// the trigger and its audit table are still there, writes keep working
	// (each wait for the lock is limited to lock_timeout); the writer's
	// search_path doesn't include the schema: the trigger must not depend on it
	eq(t, len(d.pgoscTables()), 2)
	writer, err := Connect(conn(s))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Run(fmt.Sprintf(`SET lock_timeout = '3s'; INSERT INTO "%s".books(seller_id, username, password, email, "createdOn") VALUES (1, 'during', 'x', 'during@b.c', now())`, s), false); err != nil {
		t.Fatal(err)
	}

	if _, err := blocker.Run("COMMIT", false); err != nil {
		t.Fatal(err)
	}
	var term Terminated
	if err := wait(t, done); !errors.As(err, &term) || term.Signal != 15 {
		t.Fatalf("got %v", err)
	}
	eq(t, d.pgoscTables(), []string{})
	eq(t, flags.CleanupSQL(), "")
	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT 1 FROM pg_trigger WHERE tgrelid = 'books'::regclass AND tgname = 'primary_to_audit_table_trigger'"), []string{})
	eq(t, d.values("SELECT count(*) FROM books"), []string{"4"})
}

func startBinary(t *testing.T, d *testDB, stdout *os.File) *exec.Cmd {
	t.Helper()
	c := connArgs(d.schema)
	cmd := exec.Command(buildBinary(t), "perform", "-a", "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE",
		"-d", c.DBName, "-h", c.Host, "-u", c.Username, "-p", strconv.Itoa(int(c.Port)), "-s", d.schema,
		"--sslmode", "disable", "--wait-for-swap", "--sync-interval", "0.05")
	cmd.Stdout = stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func assertCleanedUp(t *testing.T, d *testDB) {
	t.Helper()
	eq(t, d.pgoscTables(), []string{})
	eq(t, d.values(fmt.Sprintf(`SELECT 1 FROM pg_trigger WHERE tgrelid = '"%s".books'::regclass AND tgname = 'primary_to_audit_table_trigger'`, d.schema)), []string{})
}

// A closed ssh session / terminal sends HUP: it used to kill the process
// without cleanup, leaving the trigger on the table.
func TestBinaryCleansUpOnSighup(t *testing.T) {
	d := newTestDB(t, "sighup")
	out, err := os.Create(filepath.Join(t.TempDir(), "out.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := startBinary(t, d, out)
	waitForSync(t, d, 3)
	_ = cmd.Process.Signal(syscall.SIGHUP)
	_ = cmd.Wait()
	log, _ := os.ReadFile(out.Name())
	eq(t, cmd.ProcessState.ExitCode(), 129, string(log))
	if !strings.Contains(string(log), "Audit trigger dropped") {
		t.Fatal(string(log))
	}
	assertCleanedUp(t, d)
}

// `perform ... | tee log` and Ctrl-C: tee dies too, the next log line hits a
// broken pipe. Go kills the process on SIGPIPE for stdout by default.
func TestBinaryCleansUpWhenStdoutIsGone(t *testing.T) {
	d := newTestDB(t, "sigpipe")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := startBinary(t, d, w)
	_ = w.Close()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	waitForSync(t, d, 3)
	_ = r.Close() // the reader is gone
	_ = cmd.Process.Signal(syscall.SIGINT)
	_ = cmd.Wait()
	eq(t, cmd.ProcessState.ExitCode(), 130)
	assertCleanedUp(t, d)
}

// rewriteDuringCopy: VACUUM FULL in the middle of a batched copy moves every
// row to a new ctid. fail stops with an error, restart copies again and ends
// up correct.
func rewriteDuringCopy(t *testing.T, name string, action RewriteAction) {
	d := newTestDB(t, name)
	s := d.schema
	// the filter calls a slow function so the copy takes a few seconds
	d.exec(fmt.Sprintf(`SET search_path TO "%s";
		CREATE TABLE barcode (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), stock_id int, barcode text NOT NULL UNIQUE);
		INSERT INTO barcode (stock_id, barcode)
		  SELECT CASE WHEN g %% 4 = 0 THEN NULL ELSE g END, 'bc' || g FROM generate_series(1, 20000) g;
		DELETE FROM barcode WHERE stock_id %% 3 = 0; -- holes, so VACUUM FULL really moves rows
		CREATE FUNCTION slow(v int) RETURNS boolean LANGUAGE plpgsql AS $$
		BEGIN IF v %% 50 = 0 THEN PERFORM pg_sleep(0.01); END IF; RETURN true; END $$;`, s))
	a := args(s, "ALTER TABLE barcode ALTER COLUMN stock_id SET NOT NULL;")
	a.Filter = "stock_id IS NOT NULL AND slow(stock_id)"
	a.CopyBatchPages = 1
	a.WaitForSwap = true
	a.OnTableRewrite = action
	_, done := spawnPerform(t, a)

	waitUntil(t, "copy in progress", func() bool {
		shadow := d.find("pgosc_st_")
		return shadow != "" && d.values(fmt.Sprintf(`SELECT count(*) > 0 FROM "%s".%s`, s, shadow))[0] == "t"
	})
	if err := d.db.ExecRaw(fmt.Sprintf(`VACUUM FULL "%s".barcode`, s)); err != nil {
		t.Fatal(err)
	}
	d.exec(fmt.Sprintf(`UPDATE "%s".barcode SET barcode = barcode || '-x' WHERE stock_id %% 7 = 0`, s))

	if action == RewriteFail {
		errContains(t, wait(t, done), "rewritten during the batched copy")
		eq(t, d.pgoscTables(), []string{})
		return
	}
	waitForSync(t, d, 1)
	if err := RequestSwap(conn(s), s, "barcode"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	old := "pgosc_op_table_barcode WHERE stock_id IS NOT NULL"
	eq(t, d.values(fmt.Sprintf(`SELECT (SELECT count(*) FROM (SELECT * FROM barcode EXCEPT SELECT * FROM %[1]s) d),
		(SELECT count(*) FROM (SELECT * FROM %[1]s EXCEPT SELECT * FROM barcode) d),
		(SELECT count(*) FROM barcode) = (SELECT count(*) FROM %[1]s)`, old)), []string{"0|0|t"})
}

func TestTableRewriteDuringBatchedCopyFails(t *testing.T) {
	rewriteDuringCopy(t, "rewrite_fail", RewriteFail)
}

func TestTableRewriteDuringBatchedCopyRestarts(t *testing.T) {
	rewriteDuringCopy(t, "rewrite_restart", RewriteRestart)
}

func ensureRole(d *testDB, role string) {
	// roles are cluster-wide and the tests run in parallel
	d.exec(fmt.Sprintf("DO $$ BEGIN CREATE ROLE %s; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$", role))
}

// CREATE TABLE ... LIKE ... INCLUDING ALL doesn't copy owner, grants, RLS,
// policies, replica identity, publications, the table comment and column
// statistics, and names indexes after the shadow table: the swap carries them
// over (upstream issues 197 and 20). A foreign key with a mixed case name
// used to break the swap (upstream issue 196).
func TestSwapCarriesPropertiesNamesAndQuotedConstraints(t *testing.T) {
	d := newTestDB(t, "carry")
	s := d.schema
	role, owner := "pgosc_test_reader", "pgosc_test_owner"
	ensureRole(d, role)
	ensureRole(d, owner)
	publication := s + "_pub"
	d.exec(fmt.Sprintf(`SET search_path TO "%[1]s";
		DROP PUBLICATION IF EXISTS %[4]s;
		GRANT USAGE ON SCHEMA "%[1]s" TO %[2]s;
		GRANT SELECT ON books TO %[2]s;
		GRANT UPDATE (last_login) ON books TO %[2]s;
		ALTER TABLE books OWNER TO %[3]s;
		ALTER TABLE books ENABLE ROW LEVEL SECURITY;
		ALTER TABLE books FORCE ROW LEVEL SECURITY;
		CREATE POLICY seller_one ON books FOR SELECT TO %[2]s USING (seller_id = 1);
		ALTER TABLE books REPLICA IDENTITY USING INDEX books_username_key;
		ALTER TABLE books ALTER COLUMN email SET STATISTICS 300;
		ALTER TABLE books ALTER COLUMN username SET (n_distinct = 100);
		COMMENT ON TABLE books IS 'книги';
		CREATE PUBLICATION %[4]s FOR TABLE books;
		ALTER TABLE chapters RENAME CONSTRAINT chapters_book_id_fkey TO "FK_Chapters_Book";
		DROP VIEW books_view;`, s, role, owner, publication))
	const indexNames = "SELECT string_agg(indexrelid::regclass::text, ',' ORDER BY indexrelid::regclass::text) FROM pg_index WHERE indrelid = 'books'::regclass"
	before := d.values(indexNames)

	a := args(s, "ALTER TABLE books RENAME COLUMN email TO new_email; ALTER TABLE books ADD COLUMN purchased boolean;")
	err := Run(mustOptions(t, a), &Flags{})
	d.exec("DROP PUBLICATION IF EXISTS " + publication)
	if err != nil {
		t.Fatal(err)
	}

	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values(indexNames), before, "index names restored")
	eq(t, d.values("SELECT relowner::regrole, relrowsecurity, relforcerowsecurity, relreplident, obj_description(oid, 'pg_class') FROM pg_class WHERE oid = 'books'::regclass"),
		[]string{owner + "|t|t|i|книги"})
	eq(t, d.values("SELECT indexrelid::regclass FROM pg_index WHERE indrelid = 'books'::regclass AND indisreplident"), []string{"books_username_key"})
	eq(t, d.values(fmt.Sprintf("SELECT has_table_privilege('%[1]s', 'books', 'SELECT'), has_table_privilege('%[1]s', 'books', 'INSERT'), has_column_privilege('%[1]s', 'books', 'last_login', 'UPDATE'), has_column_privilege('%[1]s', 'books', 'username', 'UPDATE')", role)),
		[]string{"t|f|t|f"})
	eq(t, d.values("SELECT polname, polcmd, pg_get_expr(polqual, polrelid) FROM pg_policy WHERE polrelid = 'books'::regclass"),
		[]string{"seller_one|r|(seller_id = 1)"})
	defaultTarget := "-1"
	if v, _ := strconv.Atoi(d.values("SHOW server_version_num")[0]); v >= 170000 {
		defaultTarget = "NULL" // PostgreSQL 17 shows the default statistics target as NULL
	}
	eq(t, d.values("SELECT attname, attstattarget, array_to_string(attoptions, ',') FROM pg_attribute WHERE attrelid = 'books'::regclass AND attname IN ('new_email', 'username') ORDER BY 1"),
		[]string{"new_email|300|NULL", "username|" + defaultTarget + "|n_distinct=100"})
	eq(t, d.values("SELECT conname, convalidated, confrelid::regclass FROM pg_constraint WHERE conrelid = 'chapters'::regclass AND contype = 'f'"),
		[]string{"FK_Chapters_Book|t|books"})
	// the old table's indexes got out of the way
	for _, n := range d.values("SELECT indexrelid::regclass FROM pg_index WHERE indrelid = 'pgosc_op_table_books'::regclass") {
		if !strings.Contains(n, "_pgosc_op_") {
			t.Fatal("old index not renamed:", n)
		}
	}
}

func TestSwapMovesTheTableInItsPublications(t *testing.T) {
	d := newTestDB(t, "publication")
	s := d.schema
	publication := s + "_pub"
	d.exec(fmt.Sprintf(`DROP PUBLICATION IF EXISTS %[1]s; CREATE PUBLICATION %[1]s FOR TABLE "%[2]s".books;`, publication, s))
	err := Run(mustOptions(t, args(s, "ALTER TABLE books ADD COLUMN purchased boolean;")), &Flags{})
	members := d.values(fmt.Sprintf("SELECT tablename FROM pg_publication_tables WHERE pubname = '%s' ORDER BY 1", publication))
	d.exec("DROP PUBLICATION IF EXISTS " + publication)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, members, []string{"books"})
}

// While the trigger feeds the audit table, ALTER TABLE on the table would
// break every write to it and TRUNCATE wouldn't reach the new table: both are
// refused until the swap (upstream issue 15).
func TestAlterAndTruncateAreRefusedDuringTheRun(t *testing.T) {
	d := newTestDB(t, "ddl_guard")
	s := d.schema
	a := args(s, "ALTER TABLE books ADD COLUMN purchased boolean;")
	a.WaitForSwap = true
	_, done := spawnPerform(t, a)
	waitForSync(t, d, 3)

	other, err := Connect(conn(s))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_, err = other.Run(fmt.Sprintf(`ALTER TABLE "%s".books ADD COLUMN extra int`, s), false)
	errContains(t, err, "ALTER TABLE on it is refused")
	_, err = other.Run(fmt.Sprintf(`TRUNCATE "%[1]s".chapters, "%[1]s".book_audits, "%[1]s".books`, s), false)
	errContains(t, err, "TRUNCATE is refused")
	// other tables are not affected, writes keep working
	for _, sql := range []string{
		fmt.Sprintf(`ALTER TABLE "%s".sellers ADD COLUMN extra int`, s),
		fmt.Sprintf(`INSERT INTO "%s".books(seller_id, username, password, email, "createdOn") VALUES (1, 'g', 'x', 'g@b.c', now())`, s),
	} {
		if _, err := other.Run(sql, false); err != nil {
			t.Fatal(err)
		}
	}

	if err := RequestSwap(conn(s), s, "books"); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	// gone with the swap
	for _, sql := range []string{
		fmt.Sprintf(`ALTER TABLE "%s".books ADD COLUMN extra int`, s),
		fmt.Sprintf(`TRUNCATE "%s".pgosc_op_table_books CASCADE`, s),
	} {
		if _, err := other.Run(sql, false); err != nil {
			t.Fatal(err)
		}
	}
	d.exec(fmt.Sprintf(`SET search_path TO "%s"`, s))
	eq(t, d.values("SELECT evtname FROM pg_event_trigger e JOIN pg_proc p ON p.oid = e.evtfoid WHERE p.pronamespace = current_schema::regnamespace"), []string{})
	eq(t, d.values("SELECT proname FROM pg_proc WHERE pronamespace = current_schema::regnamespace AND proname LIKE 'pgosc_%guard%'"), []string{})
}

func TestPreflightRefusesLeftoversAndPartitionedTables(t *testing.T) {
	d := newTestDB(t, "preflight")
	s := d.schema
	d.exec(fmt.Sprintf(`SET search_path TO "%s";
		CREATE TABLE pgosc_op_table_books (id int);
		CREATE TABLE parted (id int PRIMARY KEY) PARTITION BY RANGE (id);
		CREATE TABLE parted_1 PARTITION OF parted FOR VALUES FROM (0) TO (100);`, s))
	errContains(t, Run(mustOptions(t, args(s, "ALTER TABLE books ADD COLUMN purchased boolean;")), &Flags{}), "pgosc_op_table_books already exists")
	eq(t, d.pgoscTables(), []string{"pgosc_op_table_books"})
	for _, table := range []string{"parted", "parted_1"} {
		errContains(t, Run(mustOptions(t, args(s, fmt.Sprintf("ALTER TABLE %s ADD COLUMN x int;", table))), &Flags{}), "Partitioned tables")
	}
}
