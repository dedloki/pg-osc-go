package pgosc

import (
	"reflect"
	"strings"
	"testing"
)

func TestAlterStatements(t *testing.T) {
	for q, want := range map[string]bool{
		"ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;": true,
		"ALTER TABLE books RENAME COLUMN email TO new_email;":           true,
		"CREATE DATABASE foo": false,
		"ALTER TABLE books ADD COLUMN x int; DROP TABLE books": false,
		"ALTER TABLE": false,
		"":            false,
	} {
		if got := IsAlterStatement(q); got != want {
			t.Errorf("IsAlterStatement(%q) = %v", q, got)
		}
	}
}

func TestSameTable(t *testing.T) {
	if !SameTable("ALTER TABLE books ADD COLUMN a int; ALTER TABLE books RENAME COLUMN b TO c;") {
		t.Error("same table")
	}
	if SameTable("ALTER TABLE books ADD COLUMN a int; ALTER TABLE cards ADD COLUMN a int;") {
		t.Error("different tables")
	}
}

func TestTableNames(t *testing.T) {
	for q, want := range map[string]string{
		"ALTER TABLE books ADD COLUMN a int":     "books",
		"ALTER TABLE books RENAME COLUMN a TO b": "books",
		`ALTER TABLE "Books" ADD COLUMN a int`:   "Books",
	} {
		if got, err := Table(q); err != nil || got != want {
			t.Errorf("Table(%q) = %q, %v", q, got, err)
		}
	}
	if got := TableName(`ALTER TABLE "Books" ADD COLUMN a int`, "Books"); got != `"Books"` {
		t.Error(got)
	}
	if got := TableName("ALTER TABLE books ADD a int", "books"); got != "books" {
		t.Error(got)
	}
}

func TestRewriteToShadow(t *testing.T) {
	q := "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE; ALTER TABLE books RENAME COLUMN email TO new_email;"
	got, err := AlterStatementFor(q, "pgosc_st_books_abc")
	want := "ALTER TABLE pgosc_st_books_abc ADD COLUMN purchased boolean DEFAULT false; ALTER TABLE pgosc_st_books_abc RENAME COLUMN email TO new_email"
	if err != nil || got != want {
		t.Errorf("got %q, %v", got, err)
	}
}

func TestDroppedAndRenamed(t *testing.T) {
	q := `ALTER TABLE books DROP COLUMN email; ALTER TABLE books DROP "Name"; ALTER TABLE books RENAME COLUMN a TO b;`
	if got := DroppedColumns(q); !reflect.DeepEqual(got, []string{"email", "Name"}) {
		t.Error(got)
	}
	if got := RenamedColumns(q); !reflect.DeepEqual(got, []Rename{{"a", "b"}}) {
		t.Error(got)
	}
}

func TestFilters(t *testing.T) {
	for f, want := range map[string]bool{
		"email IS NOT NULL":                   true,
		"a > 5 AND (b = 'x' OR c IS NULL)":    true,
		"email IS NOT NULL; DROP TABLE books": false,
		"true) UNION (SELECT 1":               false,
		"true LIMIT 1":                        false,
		"":                                    false,
	} {
		if got := ValidFilter(f); got != want {
			t.Errorf("ValidFilter(%q) = %v", f, got)
		}
	}
	for in, want := range map[string]string{
		"  WHERE x IS NOT NULL ": "x IS NOT NULL",
		"wherever IS NULL":       "wherever IS NULL",
		"  ":                     "",
		"":                       "",
	} {
		if got := NormalizeFilter(in); got != want {
			t.Errorf("NormalizeFilter(%q) = %q", in, got)
		}
	}
}

func TestQuoting(t *testing.T) {
	if QuoteIdent(`a"b`) != `"a""b"` || QuoteLiteral("it's") != "'it''s'" {
		t.Error("quoting")
	}
}

func TestLongQueriesAreShortenedInLogs(t *testing.T) {
	if shorten("SELECT 1") != "SELECT 1" {
		t.Error("short")
	}
	s := shorten(strings.Repeat("я", 3000)) // 2 bytes per char, the cut must stay on a char boundary
	if !strings.HasSuffix(s, "... (6000 bytes total)") || len(s) > 2100 || !strings.HasPrefix(s, "яя") {
		t.Error(s)
	}
	if strings.ContainsRune(s, '�') {
		t.Error("broken utf-8")
	}
}

func TestConnStringQuoting(t *testing.T) {
	cs, err := ConnOpts{Host: "h", Port: 5432, DBName: "d b", User: "u", Password: `p'w\`, SSLMode: "prefer"}.connString()
	if err != nil || !strings.Contains(cs, `password='p\'w\\'`) || !strings.Contains(cs, "dbname='d b'") {
		t.Error(cs, err)
	}
	if _, err := (ConnOpts{SSLMode: "verify-full"}).connString(); err == nil {
		t.Error("unsupported sslmode accepted")
	}
}
