package meta

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/soumi/dfs/db"
)

// Migrate brings the schema up to date.
//
// It runs at coordinator startup, before the gRPC server accepts traffic, so
// the process either serves a schema it understands or fails to start. Goose
// takes an advisory lock, so two coordinators starting at once is safe.
func (s *Store) Migrate(ctx context.Context, log *slog.Logger) error {
	goose.SetBaseFS(db.Migrations)
	goose.SetLogger(gooseLogger{log})

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	// goose needs a database/sql handle; stdlib wraps the pgx pool without
	// opening a second set of connections to PostgreSQL.
	sqlDB := stdlib.OpenDBFromPool(s.pool)
	defer func() { _ = sqlDB.Close() }()

	before, err := goose.GetDBVersionContext(ctx, sqlDB)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	if err := goose.UpContext(ctx, sqlDB, "migrations"); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	after, err := goose.GetDBVersionContext(ctx, sqlDB)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	if before == after {
		log.Info("schema is up to date", slog.Int64("version", after))
	} else {
		log.Info("schema migrated",
			slog.Int64("from", before), slog.Int64("to", after))
	}
	return nil
}

// gooseLogger routes goose's output into the structured log rather than
// letting it print to stdout unstructured.
type gooseLogger struct{ log *slog.Logger }

func (g gooseLogger) Printf(format string, v ...any) {
	g.log.Debug(fmt.Sprintf(format, v...))
}

func (g gooseLogger) Fatalf(format string, v ...any) {
	g.log.Error(fmt.Sprintf(format, v...))
}
