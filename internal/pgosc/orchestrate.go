package pgosc

// The whole operation (the Ruby Orchestrate class).

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const (
	swapStatementTimeout  = "5s"
	trackProgressInterval = 60 * time.Second
	SwapRequestMarker     = "pgosc:swap-requested"
	minVersionBatchedCopy = 140000
)

// errors.Join keeps both the step error and a failed COMMIT
func joinErr(errs ...error) error { return errors.Join(errs...) }

// setup connects, installs the helper functions and collects everything the
// operation needs to know about the table.
func setup(opts *Options, flags *Flags) (*Ctx, error) {
	db, err := Connect(opts.Conn)
	if err != nil {
		return nil, err
	}
	flags.setDB(db)
	c := newCtx(opts, db, flags, randomID())

	sql := fmt.Sprintf("SET statement_timeout = 0;\nSET client_min_messages = warning;\nSET search_path TO %s;", QuoteIdent(opts.Schema))
	for _, q := range []string{sql, funcFixSerialSequence, funcCreateTableAll} {
		if _, err := db.Run(q, false); err != nil {
			db.Close()
			return nil, err
		}
	}
	tn := opts.TableName
	if c.primaryTableStorageParameters, err = c.storageParametersFor(tn, false); err != nil {
		db.Close()
		return nil, err
	}
	if c.referentialForeignKeyStatements, err = c.referentialForeignKeysToRefresh(tn); err != nil {
		db.Close()
		return nil, err
	}
	if c.selfForeignKeyStatements, err = c.selfForeignKeysToRefresh(tn); err != nil {
		db.Close()
		return nil, err
	}
	if c.triggerStatements, err = c.getTriggersFor(tn); err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}

type target struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	DBName   string `json:"dbname"`
	Username string `json:"username"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
}

// Run is `perform`: on failure the trigger, audit and shadow tables are
// removed, the old table never (with --filter it holds the filtered-out rows).
func Run(opts *Options, flags *Flags) error {
	tgt := target{opts.Conn.Host, opts.Conn.Port, opts.Conn.DBName, opts.Conn.User, opts.Schema, opts.TableName}
	c, err := setup(opts, flags)
	if err != nil {
		// nothing to clean up yet, but the error must not go unnoticed
		Fatal("Setup failed: "+err.Error(), "target", tgt)
		return err
	}
	defer func() {
		flags.setDB(nil)
		c.db.Close()
	}()

	flags.SetCleanupSQL(c.manualCleanupSQL())
	err = c.runSteps()
	if err == nil {
		Info("All tasks successfully completed")
		return nil
	}
	var term Terminated
	if errors.As(err, &term) {
		Info(fmt.Sprintf("Signal %d received, cleaning up", term.Signal))
	} else {
		Fatal("Something went wrong: "+err.Error(), "e", fmt.Sprintf("%+v", err))
	}
	if cerr := c.dropAndCleanup(false); cerr != nil {
		Error("Cleanup failed: "+cerr.Error()+". Clean up by hand (drop the trigger first, it needs the lock; until then writes go to the audit table)",
			"cleanup_sql", c.manualCleanupSQL())
	}
	return err
}

func (c *Ctx) runSteps() error {
	pk, err := c.primaryKeyOpt()
	if err != nil {
		return err
	}
	if pk == nil {
		return errors.New("Parent table has no primary key, exiting...")
	}
	if c.batched() {
		v, err := c.serverVersionNum()
		if err != nil {
			return err
		}
		if v < minVersionBatchedCopy {
			return fmt.Errorf("--copy-batch-pages needs PostgreSQL 14+ (TID range scans), the server is %d", v)
		}
	}

	Info("Performing some house keeping....")
	if err := c.runAnalyze(); err != nil {
		return err
	}
	if c.opts.SkipVacuum {
		Info("Skipping VACUUM (--skip-vacuum)")
	} else if err := c.runVacuum(); err != nil {
		return err
	}

	steps := []func() error{
		c.setupAuditTable,
		c.setupTrigger,
		c.setupShadowTable,  // starts the serializable transaction (not with --copy-batch-pages)
		c.runAlterStatement, // same transaction
		c.copyData,          // same transaction, commits it
		c.finishCopy,        // dedup after batches, deferred indexes
		c.runAnalyze,
		c.replayAndSwap,
		c.runAnalyze,
		c.validateConstraints,
		c.replaceViews,
		func() error { return c.dropAndCleanup(c.opts.Drop) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// batched: --copy-batch-pages, copy in short transactions instead of one serializable one
func (c *Ctx) batched() bool { return c.opts.CopyBatchPages > 0 }

func (c *Ctx) setupAuditTable() error {
	Info("Setting up audit table", "audit_table", c.auditTable)
	sql := fmt.Sprintf("CREATE TABLE %s (%s SERIAL PRIMARY KEY, %s text, %s timestamp, LIKE %s) WITH (autovacuum_enabled = false);",
		c.auditTable, c.auditTablePK, c.operationTypeColumn, c.triggerTimeColumn, c.opts.TableName)
	if _, err := c.db.Run(sql, false); err != nil {
		return err
	}
	seq, err := c.getSequenceName(c.auditTable, c.auditTablePK)
	c.auditTablePKSequence = seq
	return err
}

// setupTrigger takes an ACCESS EXCLUSIVE lock (killing backends if asked) to
// add the trigger that feeds the audit table.
func (c *Ctx) setupTrigger() error {
	err := func() error {
		ok, err := c.openLockExclusive()
		if err != nil {
			return err
		}
		if !ok {
			return ErrLockNotAcquired
		}
		Info("Setting up triggers")
		if c.auditTablePKSequence == nil {
			return errors.New("audit table sequence not found")
		}
		// schema-qualified: the trigger runs with the search_path of whoever writes to the table
		seq, audit, table := *c.auditTablePKSequence, QuoteIdent(c.opts.Schema)+"."+QuoteIdent(c.auditTable), c.opts.TableName
		sql := fmt.Sprintf(`DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON %[3]s;

CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
RETURNS TRIGGER AS
$$
BEGIN
  IF ( TG_OP = 'INSERT') THEN
    INSERT INTO %[2]s select nextval('%[1]s'), 'INSERT', clock_timestamp(), NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'UPDATE') THEN
    INSERT INTO %[2]s select nextval('%[1]s'), 'UPDATE', clock_timestamp(),  NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'DELETE') THEN
    INSERT INTO %[2]s select nextval('%[1]s'), 'DELETE', clock_timestamp(), OLD.* ;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE TRIGGER primary_to_audit_table_trigger
AFTER INSERT OR UPDATE OR DELETE ON %[3]s
FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
`, seq, audit, table)
		_, err = c.db.Run(sql, true)
		return err
	}()
	_, cerr := c.db.Run("COMMIT;", false)
	return joinErr(err, cerr)
}

// setupShadowTable: from here till the end of copyData everything runs in one
// serializable transaction: the audit table is emptied and the data copied in
// the same snapshot, so the replay afterwards has no duplicates.
func (c *Ctx) setupShadowTable() error {
	Info("Setting up shadow table", "shadow_table", c.shadowTable)
	sql := fmt.Sprintf("SELECT create_table_all(%s, %s);", QuoteLiteral(c.opts.TableName), QuoteLiteral(c.shadowTable))
	if _, err := c.db.Run(sql, false); err != nil {
		return err
	}
	serial := !c.batched()
	if serial {
		if _, err := c.db.Run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true); err != nil {
			return err
		}
	}
	sql = fmt.Sprintf("SELECT fix_serial_sequence(%s, %s);", QuoteLiteral(c.opts.TableName), QuoteLiteral(c.shadowTable))
	_, err := c.db.Run(sql, serial)
	return err
}

func (c *Ctx) runAlterStatement() error {
	statement, err := AlterStatementFor(c.opts.AlterStatement, c.shadowTable)
	if err != nil {
		return err
	}
	Info("Running alter statement on shadow table", "shadow_table", c.shadowTable, "parent_table", c.opts.TableName)
	serial := !c.batched()
	if _, err := c.db.Run(statement, serial); err != nil {
		return err
	}
	c.droppedColumns = DroppedColumns(c.opts.AlterStatement)
	c.renamedColumns = RenamedColumns(c.opts.AlterStatement)
	if c.opts.DeferIndexes {
		return c.dropShadowIndexes(serial)
	}
	return nil
}

// dropShadowIndexes (--defer-indexes): remember the shadow table's indexes
// (as they are after the ALTER) and drop them, finishCopy builds them again.
func (c *Ctx) dropShadowIndexes(reuse bool) error {
	indexes, err := c.shadowIndexes(reuse)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		var sql string
		if idx.ConName != nil {
			sql = fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", c.shadowTable, QuoteIdent(*idx.ConName))
		} else {
			sql = fmt.Sprintf("DROP INDEX %s;", idx.Name)
		}
		if _, err := c.db.Run(sql, reuse); err != nil {
			return err
		}
		names = append(names, idx.Name)
	}
	Info("Indexes of the shadow table deferred till after the copy", "indexes", names)
	c.deferredIndexes = indexes
	return nil
}

func (c *Ctx) copyData() error {
	if c.batched() {
		return c.copyDataBatched()
	}
	var finished atomic.Bool
	err := func() error {
		Info("Clearing contents of audit table before copy..", "shadow_table", c.shadowTable, "parent_table", c.opts.TableName)
		if _, err := c.db.Run("DELETE FROM "+c.auditTable, true); err != nil {
			return err
		}
		if c.opts.CopyStatement != "" {
			query := strings.ReplaceAll(c.opts.CopyStatement, "%{shadow_table}", c.shadowTable)
			_, err := c.db.Run(query, true)
			return err
		}
		Info("Copying contents..", "shadow_table", c.shadowTable, "parent_table", c.opts.TableName)
		c.logProgress(&finished)
		sql, err := c.copyDataStatement(true)
		if err != nil {
			return err
		}
		_, err = c.db.Run(sql, true)
		return err
	}()
	finished.Store(true)
	_, cerr := c.db.Run("COMMIT;", false) // commits the serializable transaction
	return joinErr(err, cerr)
}

// copyDataBatched (--copy-batch-pages): INSERT ... SELECT by ctid ranges, each
// batch in its own transaction. Without one snapshot for the whole copy a row
// changed meanwhile can be copied twice (old and new version in different
// batches) or missed (new version in an already copied range). Every such row
// was changed after the trigger was set up, so its primary key is in the
// audit table: finishCopy removes those rows from the shadow table and the
// replay of the full audit table brings them back in their final state.
// That's why the audit table is not emptied here.
func (c *Ctx) copyDataBatched() error {
	pagesPerBatch := c.opts.CopyBatchPages
	table := c.opts.TableName
	// before the page count: a rewrite in between is caught by the first batch
	filenode, err := c.relationFilenode(table, false)
	if err != nil {
		return err
	}
	totalPages, err := c.relationPages(table)
	if err != nil {
		return err
	}
	insert, sel, err := c.copyColumns(false)
	if err != nil {
		return err
	}
	filter := ""
	if c.opts.Filter != "" {
		filter = fmt.Sprintf(" AND (%s)", c.opts.Filter)
	}
	// with the indexes in place a second version of a row would violate the
	// primary key: skip it, finishCopy removes the row anyway
	onConflict := " ON CONFLICT DO NOTHING"
	if c.opts.DeferIndexes {
		onConflict = ""
	}

	Info("Copying contents in batches..", "shadow_table", c.shadowTable, "parent_table", table,
		"pages", totalPages, "pages_per_batch", pagesPerBatch)

	started := time.Now()
	lastLog := time.Now()
	var copied int64
	var start uint64
	restarts := 0
	for {
		if err := c.checkTerminate(); err != nil {
			return err
		}
		end := start + pagesPerBatch
		// the last batch is open ended: pages added during the copy hold only
		// rows written after the trigger, copying them is harmless
		last := end >= totalPages
		rng := fmt.Sprintf("ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid", start, end)
		if last {
			rng = fmt.Sprintf("ctid >= '(%d,0)'::tid", start)
		}
		sql := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM ONLY %s WHERE %s%s%s;",
			c.shadowTable, strings.Join(insert, ", "), strings.Join(sel, ", "), table, rng, filter, onConflict)
		// the INSERT locks the table till the end of the batch, a rewrite
		// can't happen after it: check in the same transaction
		n, err := c.db.Execute(sql, true)
		if err != nil {
			return err
		}
		now, err := c.relationFilenode(table, true)
		if err != nil {
			_, _ = c.db.Run("ROLLBACK;", false)
			return err
		}
		if now != filenode {
			if _, err := c.db.Run("ROLLBACK;", false); err != nil {
				return err
			}
			msg := fmt.Sprintf("The table was rewritten during the batched copy (VACUUM FULL, CLUSTER, pg_repack, pg_squeeze, TRUNCATE or a rewriting ALTER TABLE): row addresses changed, the pages copied so far don't line up with the rest. Relfilenode %s -> %s", filenode, now)
			if c.opts.OnTableRewrite != RewriteRestart {
				return errors.New(msg + ". Don't run those on the table during the copy, or use --on-table-rewrite restart")
			}
			Warn(msg + ". Restarting the copy from the first page (--on-table-rewrite restart)")
			// the audit table is kept: it has every change since the trigger
			if _, err := c.db.Run(fmt.Sprintf("TRUNCATE %s;", c.shadowTable), false); err != nil {
				return err
			}
			if filenode, err = c.relationFilenode(table, false); err != nil {
				return err
			}
			if totalPages, err = c.relationPages(table); err != nil {
				return err
			}
			copied, start = 0, 0
			restarts++
			Info("Copying contents in batches again..", "pages", totalPages, "restarts", restarts)
			continue
		}
		if _, err := c.db.Run("COMMIT;", false); err != nil {
			return err
		}
		copied += n

		done := min(end, totalPages)
		if last || time.Since(lastLog) >= trackProgressInterval {
			pct := 100.0
			if totalPages > 0 {
				pct = float64(done) / float64(totalPages) * 100
			}
			elapsed := int64(time.Since(started).Seconds())
			var eta int64
			if done > 0 && !last {
				eta = int64(float64(elapsed) * float64(totalPages-done) / float64(done))
			}
			Info(fmt.Sprintf("Copy progress: %.2f%% of pages", pct),
				"pages_done", done, "pages_total", totalPages, "rows_copied", copied,
				"elapsed_s", elapsed, "eta_s", eta)
			lastLog = time.Now()
		}
		if last {
			break
		}
		start = end
	}
	Info("Batched copy done", "rows_copied", copied, "elapsed_s", int64(time.Since(started).Seconds()))
	return nil
}

// finishCopy runs after the copy: drops rows changed during a batched copy
// (the replay rebuilds them), builds the deferred indexes, ANALYZEs the shadow table.
func (c *Ctx) finishCopy() error {
	if c.batched() {
		if err := c.checkTerminate(); err != nil {
			return err
		}
		pk, err := c.requirePrimaryKey()
		if err != nil {
			return err
		}
		shadowPK, err := c.shadowPrimaryKey()
		if err != nil {
			return err
		}
		started := time.Now()
		sql := fmt.Sprintf("DELETE FROM %s s USING (SELECT DISTINCT %s AS k FROM %s) a WHERE s.%s = a.k;",
			c.shadowTable, QuoteIdent(pk), c.auditTable, shadowPK)
		removed, err := c.db.Execute(sql, false)
		if err != nil {
			return err
		}
		Info("Removed rows changed during the copy, the replay brings them back",
			"rows", removed, "elapsed_s", int64(time.Since(started).Seconds()))
	}

	if c.opts.DeferIndexes && len(c.deferredIndexes) > 0 {
		if err := c.buildDeferredIndexes(); err != nil {
			return err
		}
	}

	if c.batched() || c.opts.DeferIndexes {
		Info("Performing ANALYZE on the shadow table!")
		return c.db.ExecRaw(fmt.Sprintf("ANALYZE %s;", c.shadowTable))
	}
	return nil
}

func (c *Ctx) buildDeferredIndexes() error {
	if c.opts.MaintenanceWorkMem != "" {
		if _, err := c.db.Run(fmt.Sprintf("SET maintenance_work_mem = %s;", QuoteLiteral(c.opts.MaintenanceWorkMem)), false); err != nil {
			return err
		}
	}
	indexes := c.deferredIndexes
	c.deferredIndexes = nil
	total := len(indexes)
	for n, idx := range indexes {
		if err := c.checkTerminate(); err != nil {
			return err
		}
		Info(fmt.Sprintf("Building index %d/%d", n+1, total), "index", idx.Name, "constraint", idx.ConName)
		started := time.Now()
		contype := ""
		if idx.ConType != nil {
			contype = *idx.ConType
		}
		condef := ""
		if idx.ConDef != nil {
			condef = *idx.ConDef
		}
		switch {
		case (contype == "p" || contype == "u") && idx.ConName != nil:
			if _, err := c.db.Run(idx.IndexDef, false); err != nil {
				return err
			}
			deferrable := ""
			if strings.Contains(condef, "INITIALLY DEFERRED") {
				deferrable = " DEFERRABLE INITIALLY DEFERRED"
			} else if strings.Contains(condef, "DEFERRABLE") && !strings.Contains(condef, "NOT DEFERRABLE") {
				deferrable = " DEFERRABLE"
			}
			kind := "UNIQUE"
			if contype == "p" {
				kind = "PRIMARY KEY"
			}
			sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s USING INDEX %s%s;",
				c.shadowTable, QuoteIdent(*idx.ConName), kind, idx.Name, deferrable)
			if _, err := c.db.Run(sql, false); err != nil {
				return err
			}
		case contype == "x" && idx.ConName != nil:
			sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;", c.shadowTable, QuoteIdent(*idx.ConName), condef)
			if _, err := c.db.Run(sql, false); err != nil {
				return err
			}
		default:
			if _, err := c.db.Run(idx.IndexDef, false); err != nil {
				return err
			}
		}
		Info(fmt.Sprintf("Index %d/%d built", n+1, total), "index", idx.Name, "elapsed_s", int64(time.Since(started).Seconds()))
	}
	if c.opts.MaintenanceWorkMem != "" {
		if _, err := c.db.Run("RESET maintenance_work_mem;", false); err != nil {
			return err
		}
	}
	return nil
}

// logProgress logs the estimated copy progress every minute from a second connection.
func (c *Ctx) logProgress(finished *atomic.Bool) {
	conn, schema, tableName, shadow := c.opts.Conn, c.opts.Schema, c.opts.TableName, c.shadowTable
	go func() {
		db, err := Connect(conn)
		if err != nil {
			Info("Reporting progress failed: " + err.Error())
			return
		}
		defer db.Close()
		source := max(getTableSize(db, schema, tableName), 1)
		for !finished.Load() {
			size := getTableSize(db, schema, shadow)
			progress := float64(size) / float64(source) * 100
			Info(fmt.Sprintf("Estimated copy progress: %.2f%% complete", progress))
			if progress >= 100 {
				return
			}
			start := time.Now()
			for time.Since(start) < trackProgressInterval && !finished.Load() {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()
}

func (c *Ctx) replayAndSwap() error {
	if c.opts.WaitForSwap {
		return c.syncUntilSwapRequested()
	}
	if err := c.replayUntilBelowDelta(); err != nil {
		return err
	}
	Info("Remaining rows below delta count, proceeding towards swap")
	return c.swap()
}

// syncUntilSwapRequested (--wait-for-swap): keep replaying the audit table
// into the shadow table until a swap is requested, either by
// `pg-online-schema-change swap` (marks the audit table with a comment) or by
// SIGUSR1 to this process. If the swap fails (e.g. can't get its lock) keep
// syncing and wait for a new request.
func (c *Ctx) syncUntilSwapRequested() error {
	if err := c.enableAutovacuumForSync(); err != nil {
		return err
	}
	Info("Initial copy done, keeping shadow table in sync until a swap is requested",
		"shadow_table", c.shadowTable,
		"audit_table", c.auditTable,
		"how_to_swap", fmt.Sprintf("pg-online-schema-change swap -t %s -s %s ... or kill -USR1 %d", c.opts.Table, c.opts.Schema, os.Getpid()))

	var lastStatus time.Time
	for {
		if err := c.checkTerminate(); err != nil {
			return err
		}
		rows, err := c.rowsToPlay(false)
		if err != nil {
			return err
		}

		if int64(len(rows)) > c.opts.DeltaCount {
			if err := c.play(rows, false); err != nil {
				return err
			}
			continue
		}

		requested, err := c.swapRequested()
		if err != nil {
			return err
		}
		if requested {
			Info("Swap requested, remaining rows below delta count, proceeding towards swap")
			err := c.swap()
			if err == nil {
				return nil
			}
			_, terminating := c.flags.Terminating()
			if errors.Is(err, ErrLockNotAcquired) || (IsPgError(err) && !terminating) {
				// the swap runs in one transaction, on failure it is rolled
				// back and the trigger keeps feeding the audit table
				Error("Swap failed, keeping in sync. Request the swap again.", "error", err.Error())
				if err := c.clearSwapRequest(); err != nil {
					return err
				}
				continue
			}
			return err
		}

		if len(rows) > 0 {
			if err := c.play(rows, false); err != nil {
				return err
			}
		}

		if lastStatus.IsZero() || time.Since(lastStatus) >= trackProgressInterval {
			Info("In sync, waiting for swap request", "pending_rows", len(rows), "pid", os.Getpid())
			lastStatus = time.Now()
		}

		c.sleepInterruptible(time.Duration(c.opts.SyncInterval * float64(time.Second)))
	}
}

func (c *Ctx) sleepInterruptible(total time.Duration) {
	start := time.Now()
	for {
		left := total - time.Since(start)
		_, terminating := c.flags.Terminating()
		if left <= 0 || terminating || c.flags.SwapRequested.Load() {
			return
		}
		time.Sleep(min(50*time.Millisecond, left))
	}
}

func (c *Ctx) swapRequested() (bool, error) {
	if c.flags.SwapRequested.Load() {
		return true, nil
	}
	sql := fmt.Sprintf("SELECT obj_description(%s::regclass, 'pg_class') AS comment;", QuoteLiteral(c.auditTable))
	v, err := c.db.FirstValue(sql, false)
	return v != nil && *v == SwapRequestMarker, err
}

func (c *Ctx) clearSwapRequest() error {
	c.flags.SwapRequested.Store(false)
	_, err := c.db.Run(fmt.Sprintf("COMMENT ON TABLE %s IS NULL;", c.auditTable), false)
	return err
}

// enableAutovacuumForSync: the sync may run for days with constant
// deletes/updates on the audit and shadow tables, let autovacuum take care of
// them meanwhile. storageParamsReset restores the original parameters on swap.
func (c *Ctx) enableAutovacuumForSync() error {
	sql := fmt.Sprintf("ALTER TABLE %s SET (autovacuum_enabled = true); ALTER TABLE %s SET (autovacuum_enabled = true);",
		c.auditTable, c.shadowTable)
	_, err := c.db.Run(sql, false)
	return err
}

// swap does everything in one transaction under an ACCESS EXCLUSIVE lock:
// replay the rest, rename the tables, refresh FKs, triggers and storage params.
func (c *Ctx) swap() error {
	Info("Performing swap!")
	err := func() error {
		ok, err := c.openLockExclusive()
		if err != nil {
			return err
		}
		if !ok {
			return ErrLockNotAcquired
		}
		if _, err := c.db.Run(fmt.Sprintf("SET statement_timeout to '%s';", swapStatementTimeout), true); err != nil {
			return err
		}
		rows, err := c.rowsToPlay(true)
		if err != nil {
			return err
		}
		if err := c.play(rows, true); err != nil {
			return err
		}
		primaryKey, err := c.requirePrimaryKey()
		if err != nil {
			return err
		}
		pkRefresh, err := c.queryForPrimaryKeyRefresh(primaryKey, true)
		if err != nil {
			return err
		}
		t := c.opts.TableName
		sql := fmt.Sprintf(`%s;
ALTER TABLE %s RENAME to %s;
ALTER TABLE %s RENAME to %s;
%s
%s
%s
%s
DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON %s;
DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON %s;
`, pkRefresh, t, c.oldPrimaryTable, c.shadowTable, t,
			c.referentialForeignKeyStatements, c.selfForeignKeyStatements, c.triggerStatements,
			c.storageParamsReset(), t, c.oldPrimaryTable)
		_, err = c.db.Run(sql, true)
		return err
	}()
	_, cerr := c.db.Run("COMMIT;", false)
	_, rerr := c.db.Run("SET statement_timeout = 0;", false)
	return joinErr(err, cerr, rerr)
}

func (c *Ctx) runAnalyze() error {
	Info("Performing ANALYZE!")
	return c.db.ExecRaw(fmt.Sprintf("ANALYZE VERBOSE %s.%s;", QuoteIdent(c.opts.Schema), c.opts.TableName))
}

func (c *Ctx) runVacuum() error {
	Info("Performing VACUUM!")
	return c.db.ExecRaw(fmt.Sprintf("VACUUM VERBOSE %s.%s;", QuoteIdent(c.opts.Schema), c.opts.TableName))
}

func (c *Ctx) validateConstraints() error {
	if c.opts.SkipForeignKeyValidation {
		return nil
	}
	statements, err := c.getForeignKeysToValidate(c.opts.TableName)
	if err != nil {
		return err
	}
	for _, s := range statements {
		Info("Validating constraints!", "statement", s)
		if _, err := c.db.Run(s, false); err != nil {
			return err
		}
	}
	return nil
}

func (c *Ctx) replaceViews() error {
	views, err := c.viewDefinitionsFor(c.oldPrimaryTable)
	if err != nil {
		return err
	}
	for _, v := range views {
		definition := strings.ReplaceAll(v.Definition, c.oldPrimaryTable, c.opts.Table)
		Info("Replacing view " + v.Name)
		if _, err := c.db.Run(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", v.Name, definition), false); err != nil {
			return err
		}
	}
	return nil
}

// dropAndCleanup drops the trigger first (only when it is there, under
// lock_timeout, retried), then the audit and shadow tables: dropping the
// audit table while the trigger still writes into it would fail every write
// to the table.
func (c *Ctx) dropAndCleanup(dropPrimary bool) error {
	Info("Cleaning up", "audit_table", c.auditTable, "shadow_table", c.shadowTable)
	if err := c.dropAuditTrigger(); err != nil {
		return err
	}
	primaryDrop := ""
	if dropPrimary {
		primaryDrop = fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.oldPrimaryTable)
	}
	sql := fmt.Sprintf(`DROP TABLE IF EXISTS %s;
DROP TABLE IF EXISTS %s;
%s
RESET statement_timeout;
RESET client_min_messages;
RESET lock_timeout;`, c.auditTable, c.shadowTable, primaryDrop)
	if _, err := c.db.Run(sql, false); err != nil {
		return err
	}
	c.flags.SetCleanupSQL("")
	return nil
}

func (c *Ctx) auditTriggerExists() (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass(%s) AND tgname = 'primary_to_audit_table_trigger';",
		QuoteLiteral(c.qualified(c.opts.TableName)))
	rows, err := c.db.Run(sql, false)
	return len(rows) > 0, err
}

// dropAuditTrigger: DROP TRIGGER takes ACCESS EXCLUSIVE on the table, even
// with IF EXISTS and no trigger: without a lock_timeout it would wait behind
// any long query and stall every query on the table queued behind it. So:
// only when the trigger exists, with lock_timeout, until it succeeds (a
// second signal exits and logs the SQL to finish by hand).
func (c *Ctx) dropAuditTrigger() error {
	for attempt := 1; ; attempt++ {
		exists, err := c.auditTriggerExists()
		if err != nil || !exists {
			return err
		}
		sql := fmt.Sprintf("SET LOCAL lock_timeout = '%ds';\nDROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON %s;",
			max(c.opts.WaitTimeForLock, 1), c.opts.TableName)
		_, err = c.db.Run(sql, false)
		if err == nil {
			Info("Audit trigger dropped")
			return nil
		}
		if SQLState(err) != sqlStateLockNotAvailable {
			return err
		}
		Warn(fmt.Sprintf("Couldn't get the lock to drop the audit trigger, attempt %d, retrying (long transactions on the table block it)", attempt))
	}
}

// manualCleanupSQL is what dropAndCleanup does, to run by hand.
func (c *Ctx) manualCleanupSQL() string {
	s := QuoteIdent(c.opts.Schema)
	return fmt.Sprintf("SET lock_timeout = '5s'; DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON %[1]s.%[2]s; RESET lock_timeout; DROP TABLE IF EXISTS %[1]s.%[3]s; DROP TABLE IF EXISTS %[1]s.%[4]s;",
		s, c.opts.TableName, c.auditTable, c.shadowTable)
}

func (c *Ctx) storageParamsReset() string {
	t := c.opts.TableName
	s := fmt.Sprintf("ALTER TABLE %s RESET (autovacuum_enabled, toast.autovacuum_enabled);", t)
	if c.primaryTableStorageParameters != "" {
		s += fmt.Sprintf("ALTER TABLE %s SET (%s);", t, c.primaryTableStorageParameters)
	}
	return s
}

// RequestSwap is the `swap` command: marks the audit table of the running sync.
func RequestSwap(conn ConnOpts, schema, table string) error {
	db, err := Connect(conn)
	if err != nil {
		return err
	}
	defer db.Close()
	tables, err := AuditTablesFor(db, schema, table)
	if err != nil {
		return err
	}
	switch len(tables) {
	case 0:
		return fmt.Errorf("No running pg-osc sync found for %s.%s", schema, table)
	case 1:
		sql := fmt.Sprintf("COMMENT ON TABLE %s.%s IS %s", QuoteIdent(schema), QuoteIdent(tables[0]), QuoteLiteral(SwapRequestMarker))
		if _, err := db.Run(sql, false); err != nil {
			return err
		}
		Info("Swap requested", "audit_table", tables[0])
		return nil
	default:
		return fmt.Errorf("Several pg-osc audit tables found for %s.%s: %s. Drop the stale ones or send SIGUSR1 to the right process.",
			schema, table, strings.Join(tables, ", "))
	}
}
