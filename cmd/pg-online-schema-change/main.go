package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dedloki/pg-osc-go/internal/pgosc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func main() {
	os.Exit(run())
}

func connFlags(fs *pflag.FlagSet, c *pgosc.ConnArgs) {
	fs.StringVarP(&c.Schema, "schema", "s", "public", "The schema in which the table is")
	fs.StringVarP(&c.DBName, "dbname", "d", "", "Name of the database")
	fs.StringVarP(&c.Host, "host", "h", "", "Server host where the Database is located")
	fs.StringVarP(&c.Username, "username", "u", "", "Username for the Database")
	fs.Uint16VarP(&c.Port, "port", "p", 5432, "Port for the Database")
	fs.StringVar(&c.Password, "password", "", "Password for the Database, prefer the PGPASSWORD environment variable")
	sslmode := os.Getenv("PGSSLMODE")
	if sslmode == "" {
		sslmode = "prefer"
	}
	fs.StringVar(&c.SSLMode, "sslmode", sslmode, "disable, prefer or require (the certificate is not verified, as in libpq) [env: PGSSLMODE]")
	// -h is the host: --help has no short form
	fs.Bool("help", false, "Print help")
}

func applyEnv(c *pgosc.ConnArgs) {
	if c.Password == "" {
		c.Password = os.Getenv("PGPASSWORD")
	}
}

func required(cmd *cobra.Command, names ...string) {
	for _, n := range names {
		_ = cmd.MarkFlagRequired(n)
	}
}

func run() int {
	code := 0
	var perform pgosc.PerformArgs
	var swapArgs pgosc.ConnArgs
	var swapTable string

	root := &cobra.Command{
		Use:           "pg-online-schema-change",
		Short:         "Safely apply schema changes with minimal locks",
		Version:       pgosc.Version,
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetHelpCommand(&cobra.Command{Hidden: true})

	performCmd := &cobra.Command{
		Use:   "perform",
		Short: "Safely apply schema changes with minimal locks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			applyEnv(&perform.Conn)
			pgosc.SetVerbose(perform.Verbose)
			opts, err := pgosc.OptionsFromArgs(perform)
			if err != nil {
				pgosc.Fatal(err.Error())
				code = 1
				return nil
			}
			flags := &pgosc.Flags{}
			handleSignals(flags)
			if err := pgosc.Run(opts, flags); err != nil {
				if t, ok := err.(pgosc.Terminated); ok {
					code = 128 + t.Signal
				} else if sig, ok := flags.Terminating(); ok {
					code = 128 + sig
				} else {
					code = 1
				}
			}
			return nil
		},
	}
	f := performCmd.Flags()
	f.SortFlags = false
	f.StringVarP(&perform.AlterStatement, "alter-statement", "a", "", "The ALTER statement to perform the schema change")
	connFlags(f, &perform.Conn)
	f.BoolVarP(&perform.Verbose, "verbose", "v", false, "Emit logs in debug mode")
	f.BoolVarP(&perform.Drop, "drop", "f", false, "Drop the original table in the end after the swap")
	f.BoolVarP(&perform.KillBackends, "kill-backends", "k", false, "Kill other competing queries/backends when trying to acquire lock for the shadow table creation and swap. It will wait for --wait-time-for-lock duration before killing backends and try upto 3 times.")
	f.Uint64VarP(&perform.WaitTimeForLock, "wait-time-for-lock", "w", 10, "Time to wait before killing backends to acquire lock and/or retrying upto 3 times. It will kill backends if --kill-backends is true, otherwise try upto 3 times and exit if it cannot acquire a lock.")
	f.StringVarP(&perform.CopyStatement, "copy-statement", "c", "", "Takes a .sql file location where you can provide a custom query to be played (ex: backfills) when pgosc copies data from the primary to the shadow table. %{shadow_table} is replaced with the shadow table name.")
	f.Int64VarP(&perform.PullBatchCount, "pull-batch-count", "b", pgosc.PullBatchCount, "Number of rows to be replayed on each iteration after copy. This can be tuned for faster catch up and swap. Best used with delta-count.")
	f.Int64VarP(&perform.DeltaCount, "delta-count", "e", pgosc.DeltaCount, "Indicates how many rows should be remaining before a swap should be performed. This can be tuned for faster catch up and swap, especially on highly volume tables. Best used with pull-batch-count.")
	f.BoolVarP(&perform.SkipForeignKeyValidation, "skip-foreign-key-validation", "o", false, "Skip foreign key validation after swap. You shouldn't need this unless you have a very specific use case, like manually validating foreign key constraints after swap.")
	f.StringVarP(&perform.Filter, "filter", "r", "", "Copy only rows matching this SQL condition, e.g. \"email IS NOT NULL\" (a leading WHERE is optional). Uses the original column names. Applied to the initial copy and to the replay: rows that stop matching after an UPDATE are removed from the shadow table, rows that start matching are added. Rows not matching stay only in the old table after the swap. Can't be combined with --copy-statement.")
	f.BoolVarP(&perform.WaitForSwap, "wait-for-swap", "W", false, "Don't swap automatically. After the copy, keep the shadow table in sync until a swap is requested with `pg-online-schema-change swap` (or SIGUSR1 to this process).")
	f.Float64Var(&perform.SyncInterval, "sync-interval", pgosc.SyncInterval, "With --wait-for-swap: seconds to sleep between replay iterations once caught up.")
	f.Uint64Var(&perform.CopyBatchPages, "copy-batch-pages", 0, "Copy in separate short transactions of N heap pages each, by ctid ranges (PostgreSQL 14+), instead of one INSERT ... SELECT in one long transaction. No long-lived snapshot holding back vacuum; rows changed during the copy are taken from the audit table. 8kB pages: 10000 pages = ~80MB per transaction.")
	f.BoolVar(&perform.DeferIndexes, "defer-indexes", false, "Build the shadow table's indexes, primary key and unique constraints after the copy instead of maintaining them row by row during it. Much faster for big tables.")
	f.StringVar(&perform.MaintenanceWorkMem, "maintenance-work-mem", "", "maintenance_work_mem for building the deferred indexes, e.g. 4GB")
	f.BoolVar(&perform.SkipVacuum, "skip-vacuum", false, "Don't VACUUM the table before starting (the copy doesn't need it; on a big table it can take long)")
	required(performCmd, "alter-statement", "dbname", "host", "username")
	performCmd.PreRunE = func(cmd *cobra.Command, _ []string) error {
		if cmd.Flags().Changed("copy-batch-pages") && perform.CopyBatchPages == 0 {
			return fmt.Errorf("--copy-batch-pages should be > 0")
		}
		return nil
	}

	swapCmd := &cobra.Command{
		Use:   "swap",
		Short: "Request the swap for a running `perform --wait-for-swap` on the given table",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			applyEnv(&swapArgs)
			if err := pgosc.RequestSwap(swapArgs.ConnOpts(), swapArgs.Schema, swapTable); err != nil {
				pgosc.Error(err.Error())
				code = 1
			}
			return nil
		},
	}
	sf := swapCmd.Flags()
	sf.SortFlags = false
	sf.StringVarP(&swapTable, "table", "t", "", "Table the running perform works on")
	connFlags(sf, &swapArgs)
	required(swapCmd, "table", "dbname", "host", "username")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Args:  cobra.NoArgs,
		Run:   func(*cobra.Command, []string) { fmt.Println(pgosc.Version) },
	}

	root.AddCommand(performCmd, swapCmd, versionCmd)
	if err := root.Execute(); err != nil {
		return 2
	}
	return code
}

// handleSignals: USR1 requests the swap. TERM/INT/QUIT set the terminate flag
// and cancel the running query on the main connection; the main goroutine
// then cleans up (trigger, audit and shadow tables) and exits with 128+signal.
func handleSignals(flags *pgosc.Flags) {
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGUSR1)
	go func() {
		for s := range ch {
			sig := int(s.(syscall.Signal))
			if s == syscall.SIGUSR1 {
				pgosc.Info("Signal USR1 received, swap requested")
				flags.SwapRequested.Store(true)
				continue
			}
			if flags.Terminate.Swap(int32(sig)) != 0 {
				pgosc.Warn("Second signal received, exiting without cleanup")
				os.Exit(128 + sig)
			}
			flags.CancelQuery()
		}
	}()
}
