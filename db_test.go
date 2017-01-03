package sqlxchain

import (
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func setupChain(t *testing.T) (SqlxChain, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	chain := SqlxChain{Db: sqlx.NewDb(db, "mock")}
	return chain, mock
}

// a successful case
func TestShouldCommit(t *testing.T) {
	chain, mock := setupChain(t)
	defer chain.Db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE products").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err := chain.Context().Begin().
		Exec("UPDATE products").
		Exec("INSERT INTO product_viewers", 2, 3).
		Commit().
		Err(); err != nil {
		t.Errorf("error was not expected while updating: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

// a failing test case
func TestShouldRollbackOnFailure(t *testing.T) {
	chain, mock := setupChain(t)
	defer chain.Db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE products").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO product_viewers").
		WithArgs(2, 3).
		WillReturnError(fmt.Errorf("some error"))
	mock.ExpectRollback()

	// now we execute our method
	if err := chain.Context().Begin().
		Exec("UPDATE products").
		Exec("INSERT INTO product_viewers", 2, 3).
		Commit().
		Err(); err == nil {
		t.Errorf("error was expected while updating")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
