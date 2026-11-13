package pgosc

// Catalog queries and SQL builders (the Ruby Query class).

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const lockAttempt = 4

const (
	sqlStateLockNotAvailable       = "55P03"
	sqlStateInFailedSQLTransaction = "25P02"
	sqlStateQueryCanceled          = "57014"
)

var errNoPrimaryKey = errors.New("no primary key")

type Constraint struct {
	TableOn        string
	TableFrom      *string
	ConstraintType string
	ConstraintName string
	Definition     string
}

// ShadowIndex is an index of the shadow table, with the constraint it backs (p, u or x).
type ShadowIndex struct {
	// as regclass prints it (quoted if needed)
	Name      string
	IndexDef  string
	IsPrimary bool
	ConName   *string
	ConType   *string
	ConDef    *string
}

// AuditTablesFor returns the audit tables of running (or crashed) pg-osc
// operations on the table, see newCtx for the naming.
func AuditTablesFor(db *DB, schema, table string) ([]string, error) {
	prefix := QuoteLiteral(fmt.Sprintf("pgosc_at_%s_", strings.ToLower(table)))
	query := fmt.Sprintf(`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r' AND n.nspname = %s
           AND left(c.relname, length(%s)) = %s
           AND substr(c.relname, length(%s) + 1) ~ '^[0-9a-f]{6}$'
         ORDER BY 1`, QuoteLiteral(schema), prefix, prefix, prefix)
	rows, err := db.Run(query, false)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Val("relname"))
	}
	return out, nil
}

func getTableSize(db *DB, schema, tableName string) int64 {
	rel := QuoteLiteral(QuoteIdent(schema) + "." + tableName)
	v, err := db.FirstValue(fmt.Sprintf("SELECT pg_table_size(%s);", rel), false)
	if err != nil {
		Error("Error getting table size: " + err.Error())
		return 0
	}
	return parseInt(v)
}

func parseInt(v *string) int64 {
	if v == nil {
		return 0
	}
	n, _ := strconv.ParseInt(*v, 10, 64)
	return n
}

func (c *Ctx) serverVersionNum() (int64, error) {
	v, err := c.db.FirstValue("SHOW server_version_num;", false)
	return parseInt(v), err
}

// relationPages: heap pages of the table right now.
func (c *Ctx) relationPages(table string) (uint64, error) {
	sql := fmt.Sprintf("SELECT pg_relation_size(%s::regclass) / current_setting('block_size')::bigint;",
		QuoteLiteral(c.qualified(table)))
	v, err := c.db.FirstValue(sql, false)
	return uint64(parseInt(v)), err
}

// relationFilenode changes on every rewrite of the table (VACUUM FULL,
// CLUSTER, TRUNCATE, rewriting ALTER TABLE, pg_repack, pg_squeeze), so ctids
// may have moved. reuse = inside the open transaction.
func (c *Ctx) relationFilenode(table string, reuse bool) (string, error) {
	v, err := c.db.FirstValue(fmt.Sprintf("SELECT pg_relation_filenode(%s::regclass);", QuoteLiteral(c.qualified(table))), reuse)
	if err != nil || v == nil {
		return "", err
	}
	return *v, nil
}

// shadowIndexes: indexes of the shadow table, primary key first.
func (c *Ctx) shadowIndexes(reuse bool) ([]ShadowIndex, error) {
	sql := fmt.Sprintf(`SELECT i.indexrelid::regclass::text AS name, pg_get_indexdef(i.indexrelid) AS indexdef,
                    i.indisprimary AS is_primary, c.conname, c.contype, pg_get_constraintdef(c.oid) AS condef
             FROM pg_index i
             LEFT JOIN pg_constraint c ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid
                                      AND c.contype IN ('p', 'u', 'x')
             WHERE i.indrelid = %s::regclass
             ORDER BY i.indisprimary DESC, i.indexrelid;`, QuoteLiteral(c.qualified(c.shadowTable)))
	rows, err := c.db.Run(sql, reuse)
	if err != nil {
		return nil, err
	}
	out := make([]ShadowIndex, 0, len(rows))
	for _, r := range rows {
		out = append(out, ShadowIndex{
			Name:      r.Val("name"),
			IndexDef:  r.Val("indexdef"),
			IsPrimary: r.Val("is_primary") == "t",
			ConName:   r.Ptr("conname"),
			ConType:   r.Ptr("contype"),
			ConDef:    r.Ptr("condef"),
		})
	}
	return out, nil
}

// shadowPrimaryKey: primary key column of the shadow table (renamed by the ALTER, if so), quoted.
func (c *Ctx) shadowPrimaryKey() (string, error) {
	pk, err := c.requirePrimaryKey()
	if err != nil {
		return "", err
	}
	for _, r := range c.renamedColumns {
		if r.Old == pk {
			return QuoteIdent(r.New), nil
		}
	}
	return QuoteIdent(pk), nil
}

// qualified: schema-qualified, for regclass casts
func (c *Ctx) qualified(table string) string {
	return QuoteIdent(c.opts.Schema) + "." + table
}

// tableColumns returns the column names of the primary table, in order.
func (c *Ctx) tableColumns(reuse bool) ([]string, error) {
	sql := fmt.Sprintf(`SELECT attname as column_name, format_type(atttypid, atttypmod) as type, attnum as column_position FROM pg_attribute
             WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
             ORDER BY attnum;`, QuoteLiteral(c.opts.TableName))
	rows, err := c.db.Run(sql, reuse)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Val("column_name"))
	}
	return out, nil
}

func (c *Ctx) getSequenceName(table, column string) (*string, error) {
	sql := fmt.Sprintf("SELECT pg_get_serial_sequence(%s, %s);", QuoteLiteral(table), QuoteLiteral(column))
	return c.db.FirstValue(sql, false)
}

func (c *Ctx) getTriggersFor(table string) (string, error) {
	sql := fmt.Sprintf(`SELECT pg_get_triggerdef(oid) as tdef FROM pg_trigger
             WHERE tgrelid = %s::regclass AND tgisinternal = FALSE;`, QuoteLiteral(c.qualified(table)))
	rows, err := c.db.Run(sql, false)
	if err != nil {
		return "", err
	}
	triggers := make([]string, 0, len(rows))
	for _, r := range rows {
		triggers = append(triggers, r.Val("tdef")+";")
	}
	return strings.Join(triggers, ";"), nil
}

func (c *Ctx) getAllConstraints() ([]Constraint, error) {
	const sql = `SELECT conrelid::regclass AS table_on,
                          confrelid::regclass AS table_from,
                          contype as constraint_type,
                          conname AS constraint_name,
                          convalidated AS constraint_validated,
                          pg_get_constraintdef(oid) AS definition
                   FROM pg_constraint
                   WHERE contype IN ('f', 'p')`
	rows, err := c.db.Run(sql, false)
	if err != nil {
		return nil, err
	}
	out := make([]Constraint, 0, len(rows))
	for _, r := range rows {
		from := r.Ptr("table_from")
		if from != nil && *from == "-" { // confrelid = 0 for primary keys
			from = nil
		}
		out = append(out, Constraint{
			TableOn:        r.Val("table_on"),
			TableFrom:      from,
			ConstraintType: r.Val("constraint_type"),
			ConstraintName: r.Val("constraint_name"),
			Definition:     r.Val("definition"),
		})
	}
	return out, nil
}

func (k Constraint) from(table string) bool { return k.TableFrom != nil && *k.TableFrom == table }

func addConstraintStatement(k Constraint) string {
	if strings.HasSuffix(k.Definition, "NOT VALID") {
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;", k.TableOn, QuoteIdent(k.ConstraintName), k.Definition)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID;", k.TableOn, QuoteIdent(k.ConstraintName), k.Definition)
}

func (c *Ctx) referentialForeignKeysToRefresh(table string) (string, error) {
	all, err := c.getAllConstraints()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, k := range all {
		if k.from(table) && k.ConstraintType == "f" {
			fmt.Fprintf(&b, "ALTER TABLE %s DROP CONSTRAINT %s; %s", k.TableOn, QuoteIdent(k.ConstraintName), addConstraintStatement(k))
		}
	}
	return b.String(), nil
}

// selfForeignKeysToRefresh runs in the swap after the renames: the table's
// own foreign keys move to the new table. The old table loses them, otherwise
// it stays a child of the referenced tables (every ON DELETE CASCADE would
// also go through the frozen old table, and a delete there needs its locks).
func (c *Ctx) selfForeignKeysToRefresh(table string) (string, error) {
	all, err := c.getAllConstraints()
	if err != nil {
		return "", err
	}
	old := c.qualified(c.oldPrimaryTable)
	var b strings.Builder
	for _, k := range all {
		if k.TableOn == table && k.ConstraintType == "f" {
			b.WriteString(addConstraintStatement(k))
			fmt.Fprintf(&b, " ALTER TABLE %s DROP CONSTRAINT %s;", old, QuoteIdent(k.ConstraintName))
		}
	}
	return b.String(), nil
}

func (c *Ctx) getForeignKeysToValidate(table string) ([]string, error) {
	all, err := c.getAllConstraints()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range all {
		if k.from(table) && k.ConstraintType == "f" {
			out = append(out, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", k.TableOn, QuoteIdent(k.ConstraintName)))
		}
	}
	for _, k := range all {
		if k.TableOn == table && k.ConstraintType == "f" {
			out = append(out, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", k.TableOn, QuoteIdent(k.ConstraintName)))
		}
	}
	return out, nil
}

func (c *Ctx) primaryKeyFor(table string) (*string, error) {
	sql := fmt.Sprintf(`SELECT pg_attribute.attname as column_name
             FROM pg_index, pg_class, pg_attribute, pg_namespace
             WHERE pg_class.oid = %s::regclass AND
                   indrelid = pg_class.oid AND
                   nspname = %s AND
                   pg_class.relnamespace = pg_namespace.oid AND
                   pg_attribute.attrelid = pg_class.oid AND
                   pg_attribute.attnum = any(pg_index.indkey)
               AND indisprimary`, QuoteLiteral(table), QuoteLiteral(c.opts.Schema))
	rows, err := c.db.Run(sql, false)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	pk := rows[0].Val("column_name")
	return &pk, nil
}

// primaryKeyOpt returns the cached primary key column (unquoted), nil if none.
func (c *Ctx) primaryKeyOpt() (*string, error) {
	if c.primaryKey == nil {
		pk, err := c.primaryKeyFor(c.opts.TableName)
		if err != nil {
			return nil, err
		}
		c.primaryKey = pk
	}
	return c.primaryKey, nil
}

func (c *Ctx) requirePrimaryKey() (string, error) {
	pk, err := c.primaryKeyOpt()
	if err != nil {
		return "", err
	}
	if pk == nil {
		return "", errNoPrimaryKey
	}
	return *pk, nil
}

func (c *Ctx) storageParametersFor(table string, reuse bool) (string, error) {
	sql := fmt.Sprintf(`SELECT array_to_string(c.reloptions, ',') as params FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = %s AND n.nspname = %s;`,
		QuoteLiteral(strings.Trim(table, `"`)), QuoteLiteral(c.opts.Schema))
	v, err := c.db.FirstValue(sql, reuse)
	if err != nil || v == nil {
		return "", err
	}
	return *v, nil
}

type viewDef struct{ Name, Definition string }

// viewDefinitionsFor returns the views over the table (by oid, schema-qualified names).
func (c *Ctx) viewDefinitionsFor(table string) ([]viewDef, error) {
	// the Ruby version matched the name in every schema and could re-point
	// views of another schema's table
	sql := fmt.Sprintf(`SELECT DISTINCT
               dependent_view.relname AS view_name,
               pg_get_viewdef(dependent_view.oid) AS view_definition,
               view_ns.nspname AS schema_name
             FROM pg_class AS source_table
             JOIN pg_depend ON pg_depend.refobjid = source_table.oid
             JOIN pg_rewrite ON pg_rewrite.oid = pg_depend.objid
             JOIN pg_class AS dependent_view ON dependent_view.oid = pg_rewrite.ev_class
             JOIN pg_namespace AS view_ns ON dependent_view.relnamespace = view_ns.oid
             AND dependent_view.relkind = 'v'
             AND source_table.oid = %s::regclass;`, QuoteLiteral(c.qualified(table)))
	rows, err := c.db.Run(sql, false)
	if err != nil {
		return nil, err
	}
	out := make([]viewDef, 0, len(rows))
	for _, r := range rows {
		out = append(out, viewDef{
			Name:       QuoteIdent(r.Val("schema_name")) + "." + QuoteIdent(r.Val("view_name")),
			Definition: strings.TrimSpace(r.Val("view_definition")),
		})
	}
	return out, nil
}

// openLockExclusive acquires the lock and keeps the transaction open: on
// success the caller must COMMIT. On failure retries (killing backends if
// asked) and returns false.
func (c *Ctx) openLockExclusive() (bool, error) {
	attempts := 1
	for {
		if err := c.checkTerminate(); err != nil {
			return false, err
		}
		sql := fmt.Sprintf("SET lock_timeout = '%ds';\nLOCK TABLE %s IN ACCESS EXCLUSIVE MODE;",
			c.opts.WaitTimeForLock, c.opts.TableName)
		_, err := c.db.Run(sql, true)
		if err == nil {
			return true, nil
		}
		state := SQLState(err)
		_, terminating := c.flags.Terminating()
		if (state != sqlStateLockNotAvailable && state != sqlStateInFailedSQLTransaction) || terminating {
			return false, err
		}
		attempts++
		if attempts < lockAttempt {
			Info(fmt.Sprintf("Couldn't acquire lock, attempt: %d", attempts))
			if _, err := c.db.Run("RESET lock_timeout;", false); err != nil {
				return false, err
			}
			if err := c.killBackends(); err != nil {
				return false, err
			}
			continue
		}
		Info("Lock acquire failed")
		if _, err := c.db.Run("RESET lock_timeout;", false); err != nil {
			return false, err
		}
		return false, nil
	}
}

func (c *Ctx) killBackends() error {
	if !c.opts.KillBackends {
		return nil
	}
	Info("Terminating other backends")
	sql := fmt.Sprintf("SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'relation' AND relation = %s::regclass::oid AND pid <> pg_backend_pid()",
		QuoteLiteral(c.opts.TableName))
	_, err := c.db.Run(sql, false)
	return err
}

// copyColumns returns (insert into columns, select columns), both quoted:
// maps the columns of the primary table onto the shadow table (dropped
// columns are skipped, renamed columns get their new name on the insert side).
func (c *Ctx) copyColumns(reuse bool) ([]string, []string, error) {
	cols, err := c.tableColumns(reuse)
	if err != nil {
		return nil, nil, err
	}
	var insert, sel []string
	for _, col := range cols {
		if contains(c.droppedColumns, col) {
			continue
		}
		target := col
		for _, r := range c.renamedColumns {
			if r.Old == col {
				target = r.New
				break
			}
		}
		insert = append(insert, QuoteIdent(target))
		sel = append(sel, QuoteIdent(col))
	}
	return insert, sel, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (c *Ctx) copyDataStatement(reuse bool) (string, error) {
	insert, sel, err := c.copyColumns(reuse)
	if err != nil {
		return "", err
	}
	sql := fmt.Sprintf("INSERT INTO %s(%s)\nSELECT %s\nFROM ONLY %s\n",
		c.shadowTable, strings.Join(insert, ", "), strings.Join(sel, ", "), c.opts.TableName)
	if c.opts.Filter != "" {
		sql += fmt.Sprintf("WHERE (%s)\n", c.opts.Filter)
	}
	return sql, nil
}

func (c *Ctx) queryForPrimaryKeyRefresh(primaryKey string, reuse bool) (string, error) {
	sql := fmt.Sprintf("SELECT pg_get_serial_sequence(%s, %s) as sequence_name",
		QuoteLiteral(c.shadowTable), QuoteLiteral(primaryKey))
	v, err := c.db.FirstValue(sql, reuse)
	if err != nil || v == nil {
		return "", err
	}
	return fmt.Sprintf("SELECT setval((select pg_get_serial_sequence('%s', '%s')), (SELECT max(%s) FROM %s));",
		EscapeString(c.shadowTable), EscapeString(primaryKey), primaryKey, c.opts.TableName), nil
}
