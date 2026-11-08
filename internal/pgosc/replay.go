package pgosc

// Replays the audit table onto the shadow table.
//
// Unlike the Ruby version, values are never sent back as text literals: each
// audit row is copied with INSERT ... SELECT from the audit table itself (it
// has the same columns and types as the primary table). That keeps NULLs
// (the Ruby replay dropped NULL values, so `UPDATE ... SET x = NULL` was
// lost), and lets Postgres evaluate --filter on the row:
//   INSERT -> insert if the new row matches the filter
//   UPDATE -> delete by primary key, insert again if the new row matches
//   DELETE -> delete by primary key
// Without --filter the filter is simply TRUE.

import (
	"fmt"
	"strings"
)

func (c *Ctx) rowsToPlay(reuse bool) ([]Row, error) {
	pk, err := c.requirePrimaryKey()
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("SELECT %s, %s, %s FROM %s ORDER BY %s LIMIT %d;",
		c.auditTablePK, c.operationTypeColumn, QuoteIdent(pk), c.auditTable, c.auditTablePK, c.opts.PullBatchCount)
	return c.db.Run(sql, reuse)
}

func (c *Ctx) replayColumns(reuse bool) ([]string, []string, error) {
	if c.copyColumnsCache == nil {
		insert, sel, err := c.copyColumns(reuse)
		if err != nil {
			return nil, nil, err
		}
		c.copyColumnsCache = &[2][]string{insert, sel}
	}
	return c.copyColumnsCache[0], c.copyColumnsCache[1], nil
}

func (c *Ctx) play(rows []Row, reuse bool) error {
	filtered := ""
	if c.opts.Filter != "" {
		filtered = " (filtered)"
	}
	Info(fmt.Sprintf("Replaying rows%s, count: %d", filtered, len(rows)))
	if len(rows) == 0 {
		return nil
	}

	insertCols, selectCols, err := c.replayColumns(reuse)
	if err != nil {
		return err
	}
	primaryKey, err := c.requirePrimaryKey()
	if err != nil {
		return err
	}
	filter := c.opts.Filter
	if filter == "" {
		filter = "TRUE"
	}
	// the primary key may be renamed by the ALTER
	shadowPK, err := c.shadowPrimaryKey()
	if err != nil {
		return err
	}

	var replayed strings.Builder
	auditIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		auditID := QuoteLiteral(row.Val(c.auditTablePK))
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s = %s AND (%s);\n",
			c.shadowTable, strings.Join(insertCols, ","), strings.Join(selectCols, ","),
			c.auditTable, c.auditTablePK, auditID, filter)
		deleteSQL := ""
		if v, ok := row.Get(primaryKey); ok {
			deleteSQL = fmt.Sprintf("DELETE FROM %s WHERE %s=%s;\n", c.shadowTable, shadowPK, QuoteLiteral(v))
		}
		switch row.Val(c.operationTypeColumn) {
		case "INSERT":
			replayed.WriteString(insertSQL)
		case "UPDATE":
			replayed.WriteString(deleteSQL)
			replayed.WriteString(insertSQL)
		case "DELETE":
			replayed.WriteString(deleteSQL)
		}
		auditIDs = append(auditIDs, auditID)
	}

	if replayed.Len() > 0 {
		if _, err := c.db.Run(replayed.String(), reuse); err != nil {
			return err
		}
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", c.auditTable, c.auditTablePK, strings.Join(auditIDs, ","))
	_, err = c.db.Run(deleteQuery, reuse)
	return err
}

// replayUntilBelowDelta replays batches until fewer than delta_count rows are left.
func (c *Ctx) replayUntilBelowDelta() error {
	for {
		if err := c.checkTerminate(); err != nil {
			return err
		}
		rows, err := c.rowsToPlay(false)
		if err != nil {
			return err
		}
		if int64(len(rows)) <= c.opts.DeltaCount {
			return nil
		}
		if err := c.play(rows, false); err != nil {
			return err
		}
	}
}
