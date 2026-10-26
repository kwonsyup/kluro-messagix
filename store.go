// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// waDB is whatsmeow's device store on a unique shared-cache SQLite database
// that exists only in this process. Its durable form is a compressed SQLite
// serialization carried in the connector-sealed state frame.
type waDB struct {
	db        *sql.DB
	container *sqlstore.Container
	pin       driver.Conn
}

var memDBSeq atomic.Int64

func openWADB(ctx context.Context, snapshot []byte, onWrite func()) (*waDB, error) {
	dsn := fmt.Sprintf("file:kluro-wa-%d?mode=memory&cache=shared&_foreign_keys=on&_busy_timeout=10000", memDBSeq.Add(1))
	base := &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		_, err := conn.Exec("PRAGMA temp_store=MEMORY", nil)
		if err == nil {
			_, err = conn.Exec("PRAGMA secure_delete=ON", nil)
		}
		return err
	}}
	pin, err := base.Open(dsn)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(&dirtyConnector{dsn: dsn, base: base, onWrite: onWrite})
	// A single connection keeps the named memory database alive for the
	// process lifetime and keeps Serialize/Deserialize on the same database.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	w := &waDB{db: db, container: sqlstore.NewWithDB(db, "sqlite3", waLog.Noop), pin: pin}
	if len(snapshot) > 0 {
		raw, err := gunzip(snapshot)
		if err != nil {
			_ = db.Close()
			_ = pin.Close()
			return nil, fmt.Errorf("device store snapshot: %w", err)
		}
		conn, err := db.Conn(ctx)
		if err == nil {
			err = restoreSerialized(ctx, conn, base, raw)
			_ = conn.Close()
		}
		if err != nil {
			_ = db.Close()
			_ = pin.Close()
			return nil, fmt.Errorf("device store snapshot: %w", err)
		}
	}
	if err := w.container.Upgrade(ctx); err != nil {
		_ = db.Close()
		_ = pin.Close()
		return nil, err
	}
	return w, nil
}

func restoreSerialized(ctx context.Context, targetConn *sql.Conn, driver *sqlite3.SQLiteDriver, raw []byte) error {
	// SQLite Deserialize owns a fixed-size copy of the input when the driver
	// uses FREEONCLOSE alone. Load it into a temporary memory connection, then
	// SQLite's backup API copies it into the live, normally growable store.
	sourceDSN := fmt.Sprintf("file:kluro-wa-restore-%d?mode=memory&cache=shared", memDBSeq.Add(1))
	source, err := driver.Open(sourceDSN)
	if err != nil {
		return err
	}
	defer source.Close()
	src, ok := source.(*sqlite3.SQLiteConn)
	if !ok {
		return fmt.Errorf("unexpected SQLite source connection %T", source)
	}
	if err := src.Deserialize(raw, "main"); err != nil {
		return err
	}
	return targetConn.Raw(func(dc any) error {
		dst, err := unwrapSQLiteConn(dc)
		if err != nil {
			return err
		}
		backup, err := dst.Backup("main", src, "main")
		if err != nil {
			return err
		}
		_, stepErr := backup.Step(-1)
		finishErr := backup.Finish()
		if stepErr != nil {
			return stepErr
		}
		return finishErr
	})
}

type dirtyConnector struct {
	dsn     string
	base    driver.Driver
	onWrite func()
}

func (c *dirtyConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return dirtyConn{Conn: conn, onWrite: c.onWrite}, nil
}

func (c *dirtyConnector) Driver() driver.Driver { return dirtyDriver{c.base} }

type dirtyDriver struct{ d driver.Driver }

func (d dirtyDriver) Open(name string) (driver.Conn, error) { return d.d.Open(name) }

// dirtyConn marks every successful execution, including direct context calls
// and prepared statements used inside transactions. Failed/rolled-back writes
// can cause an extra snapshot attempt, but never leave changed state unmarked.
type dirtyConn struct {
	driver.Conn
	onWrite func()
}

func (c dirtyConn) wrote(err error) {
	if err == nil && c.onWrite != nil {
		c.onWrite()
	}
}

func (c dirtyConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ex, ok := c.Conn.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := ex.Exec(query, args)
	c.wrote(err)
	return result, err
}

func (c dirtyConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ex, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := ex.ExecContext(ctx, query, args)
	c.wrote(err)
	return result, err
}

func (c dirtyConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return dirtyStmt{Stmt: stmt, onWrite: c.onWrite}, nil
}

func (c dirtyConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var stmt driver.Stmt
	var err error
	if pc, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = pc.PrepareContext(ctx, query)
	} else {
		stmt, err = c.Conn.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return dirtyStmt{Stmt: stmt, onWrite: c.onWrite}, nil
}

type dirtyStmt struct {
	driver.Stmt
	onWrite func()
}

func (s dirtyStmt) wrote(err error) {
	if err == nil && s.onWrite != nil {
		s.onWrite()
	}
}

func (s dirtyStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.Stmt.Exec(args)
	s.wrote(err)
	return result, err
}

func (s dirtyStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	ex, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := ex.ExecContext(ctx, args)
	s.wrote(err)
	return result, err
}

// snapshot serializes the live in-memory SQLite database. Serialize is called
// on its one pooled connection so no file or temporary copy is created.
func (w *waDB) snapshot(ctx context.Context) ([]byte, error) {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var freePages, totalPages int64
	if err := conn.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&freePages); err != nil {
		return nil, err
	}
	if err := conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&totalPages); err != nil {
		return nil, err
	}
	var raw []byte
	err = conn.Raw(func(dc any) error {
		sqliteConn, err := unwrapSQLiteConn(dc)
		if err != nil {
			return err
		}
		if _, err = sqliteConn.Exec("PRAGMA secure_delete=ON", nil); err != nil {
			return err
		}
		if freePages*4 > totalPages {
			if _, err = sqliteConn.Exec("VACUUM", nil); err != nil {
				return err
			}
		}
		raw, err = sqliteConn.Serialize("main")
		return err
	})
	if err != nil {
		return nil, err
	}
	return gzipBytes(raw)
}

func unwrapSQLiteConn(dc any) (*sqlite3.SQLiteConn, error) {
	conn, ok := dc.(dirtyConn)
	if !ok {
		return nil, fmt.Errorf("unexpected SQLite connection wrapper %T", dc)
	}
	sqliteConn, ok := conn.Conn.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("unexpected SQLite driver connection %T", conn.Conn)
	}
	return sqliteConn, nil
}

func (w *waDB) close() {
	_ = w.db.Close()
	_ = w.pin.Close()
}

func gzipBytes(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzip(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
