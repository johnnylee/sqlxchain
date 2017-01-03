package sqlxchain

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type ErrorConvertFunc func(err error) error
type ErrorLogFunc func(err error, format string, v ...interface{})

// SqlxChain is a thin wrapper around a sqlx.DB.
type SqlxChain struct {
	Db           *sqlx.DB
	errConverter ErrorConvertFunc
	errLogger    ErrorLogFunc
}

func New(driver, dns string) (*SqlxChain, error) {
	var err error

	sc := new(SqlxChain)
	if sc.Db, err = sqlx.Open(driver, dns); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *SqlxChain) ErrorConverter(f ErrorConvertFunc) {
	sc.errConverter = f
}

func (sc *SqlxChain) ErrorLogger(f ErrorLogFunc) {
	sc.errLogger = f
}

func (sc *SqlxChain) Context() *DbContext {
	ctx := new(DbContext)
	ctx.db = sc.Db
	ctx.errConverter = sc.errConverter
	ctx.errLogger = sc.errLogger
	return ctx
}

type DbContext struct {
	db           *sqlx.DB
	tx           *sqlx.Tx
	err          error
	errConverter ErrorConvertFunc
	errLogger    ErrorLogFunc
	result       sql.Result
}

func (d *DbContext) Begin() *DbContext {
	if d.err != nil {
		return d
	}
	d.tx, d.err = d.db.Beginx()
	return d
}

func (d *DbContext) Exec(query string, args ...interface{}) *DbContext {
	if d.err != nil {
		return d
	}
	if d.tx != nil {
		d.result, d.err = d.tx.Exec(query, args...)
	} else {
		d.result, d.err = d.db.Exec(query, args...)
	}
	return d
}

func (d *DbContext) Get(
	dest interface{}, query string, args ...interface{}) *DbContext {
	if d.err != nil {
		return d
	}
	if d.tx != nil {
		d.err = d.tx.Get(dest, query, args...)
	} else {
		d.err = d.db.Get(dest, query, args...)
	}
	return d
}

func (d *DbContext) Select(
	dest interface{}, query string, args ...interface{}) *DbContext {
	if d.err != nil {
		return d
	}
	if d.tx != nil {
		d.err = d.tx.Select(dest, query, args...)
	} else {
		d.err = d.db.Select(dest, query, args...)
	}
	return d
}

func (d *DbContext) LastInsertId(id *int64) *DbContext {
	if d.err != nil {
		return d
	}
	*id, d.err = d.result.LastInsertId()
	return d
}

func (d *DbContext) RowsAffected(n *int64) *DbContext {
	if d.err != nil {
		return d
	}
	*n, d.err = d.result.RowsAffected()
	return d
}

func (d *DbContext) Commit() *DbContext {
	if d.tx == nil {
		return d
	}

	defer func() {
		d.tx = nil
	}()

	if d.err != nil {
		if err := d.tx.Rollback(); err != nil {
			d.logErr(d.convertErr(err), "When rolling back transaction")
		}
		return d
	}

	d.err = d.tx.Commit()
	return d
}

func (d *DbContext) convertErr(err error) error {
	if err != nil && d.errConverter != nil {
		err = d.errConverter(err)
	}
	return err
}

func (d *DbContext) Err() error {
	return d.convertErr(d.err)
}

func (d *DbContext) logErr(err error, msg string, args ...interface{}) {
	if err != nil && d.errLogger != nil {
		d.errLogger(err, msg, args...)
	}
}

func (d *DbContext) LogErr(msg string, args ...interface{}) *DbContext {
	d.logErr(d.err, msg, args...)
	return d
}
