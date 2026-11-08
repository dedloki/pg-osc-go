package pgosc

// Everything that needs the Postgres parser (libpg_query): validating and
// rewriting the ALTER statement, validating --filter. Pure functions, no DB.

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func parse(sql string) *pg_query.ParseResult {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil
	}
	return tree
}

// relnames returns the relation names of every ALTER TABLE / RENAME statement, in order.
func relnames(tree *pg_query.ParseResult) []string {
	var out []string
	for _, raw := range tree.Stmts {
		n := raw.GetStmt()
		if s := n.GetAlterTableStmt(); s != nil && s.Relation != nil {
			out = append(out, s.Relation.Relname)
		} else if s := n.GetRenameStmt(); s != nil && s.Relation != nil {
			out = append(out, s.Relation.Relname)
		}
	}
	return out
}

// IsAlterStatement: every statement is an ALTER TABLE or a RENAME.
func IsAlterStatement(query string) bool {
	tree := parse(query)
	if tree == nil || len(tree.Stmts) == 0 {
		return false
	}
	for _, raw := range tree.Stmts {
		n := raw.GetStmt()
		if n.GetAlterTableStmt() == nil && n.GetRenameStmt() == nil {
			return false
		}
	}
	return true
}

// SameTable: all statements touch the same table.
func SameTable(query string) bool {
	tree := parse(query)
	if tree == nil {
		return false
	}
	names := relnames(tree)
	sort.Strings(names)
	uniq := 0
	for i, n := range names {
		if i == 0 || n != names[i-1] {
			uniq++
		}
	}
	return uniq == 1
}

// Table the ALTER statement works on (unquoted, as Postgres folded it).
func Table(query string) (string, error) {
	tree := parse(query)
	if tree == nil {
		return "", fmt.Errorf("Not a valid ALTER statement: %s", query)
	}
	names := relnames(tree)
	if len(names) == 0 {
		return "", fmt.Errorf("Can't find the table in: %s", query)
	}
	return names[0], nil
}

// TableName is the table name to use in SQL: quoted if the user quoted a mixed case name.
func TableName(query, table string) string {
	quoted := `"` + table + `"`
	hasUpper := strings.IndexFunc(table, func(r rune) bool { return r < unicode.MaxASCII && unicode.IsUpper(r) }) >= 0
	if hasUpper && strings.Contains(query, quoted) && !strings.HasPrefix(table, `"`) {
		return quoted
	}
	return table
}

// AlterStatementFor returns the ALTER statement re-pointed at the shadow table.
func AlterStatementFor(query, shadowTable string) (string, error) {
	tree, err := pg_query.Parse(query)
	if err != nil {
		return "", err
	}
	for _, raw := range tree.Stmts {
		n := raw.GetStmt()
		if s := n.GetAlterTableStmt(); s != nil && s.Relation != nil {
			s.Relation.Relname = shadowTable
		} else if s := n.GetRenameStmt(); s != nil && s.Relation != nil {
			s.Relation.Relname = shadowTable
		}
	}
	return pg_query.Deparse(tree)
}

func DroppedColumns(query string) []string {
	tree := parse(query)
	if tree == nil {
		return nil
	}
	var out []string
	for _, raw := range tree.Stmts {
		s := raw.GetStmt().GetAlterTableStmt()
		if s == nil {
			continue
		}
		for _, cmd := range s.Cmds {
			c := cmd.GetAlterTableCmd()
			if c != nil && c.Subtype == pg_query.AlterTableType_AT_DropColumn {
				out = append(out, c.Name)
			}
		}
	}
	return out
}

type Rename struct{ Old, New string }

// RenamedColumns returns (old_name, new_name) for every RENAME COLUMN.
func RenamedColumns(query string) []Rename {
	tree := parse(query)
	if tree == nil {
		return nil
	}
	var out []Rename
	for _, raw := range tree.Stmts {
		if s := raw.GetStmt().GetRenameStmt(); s != nil {
			out = append(out, Rename{s.Subname, s.Newname})
		}
	}
	return out
}

// ValidFilter: the filter is an SQL boolean expression over the columns of
// the primary table (original names, before the ALTER), e.g. "email IS NOT NULL".
func ValidFilter(filter string) bool {
	tree := parse(fmt.Sprintf("SELECT 1 FROM t WHERE (%s)", filter))
	if tree == nil || len(tree.Stmts) != 1 {
		return false
	}
	s := tree.Stmts[0].GetStmt().GetSelectStmt()
	return s != nil &&
		s.Op == pg_query.SetOperation_SETOP_NONE &&
		s.LimitCount == nil &&
		len(s.SortClause) == 0 &&
		s.WhereClause != nil
}

// NormalizeFilter strips whitespace and an optional leading WHERE; "" means no filter.
func NormalizeFilter(filter string) string {
	f := strings.TrimSpace(filter)
	if len(f) > 5 && strings.EqualFold(f[:5], "where") && unicode.IsSpace(rune(f[5])) {
		f = strings.TrimSpace(f[5:])
	}
	return f
}

// QuoteIdent always quotes, like PG::Connection#quote_ident.
func QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// EscapeString: contents of a '...' literal (standard_conforming_strings = on).
func EscapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func QuoteLiteral(s string) string {
	return "'" + EscapeString(s) + "'"
}
