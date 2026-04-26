package sqly

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
)

const schema = `CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
)`

const mysqlSchema = `CREATE PROCEDURE IF NOT EXISTS return_rows_then_fail()
BEGIN
    SELECT 1 AS id, 'Yury' AS name
    	UNION ALL
    SELECT 2, 'John';

    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Intentional failure after returning rows';
END`

func createDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("Failed to create SQLite DB at %s: %v", path, err)
	}

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	return db
}

func mysqlDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", "/test")
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}

	if _, err := db.Exec(mysqlSchema); err != nil {
		t.Fatalf("Failed to init MySQL: %v", err)
	}

	return db
}

func addUser(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO users(name) VALUES($1)`, name)
	if err != nil {
		t.Fatalf("Failed to create user %q: %v", name, err)
	}
}

type user struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

var wantUsers = []*user{
	{ID: 1, Name: "Yury"},
	{ID: 2, Name: "John"},
}

func TestSimple(t *testing.T) {
	db := createDB(t, filepath.Join(t.TempDir(), "test.sqlite3"))

	addUser(t, db, "Yury")
	addUser(t, db, "John")

	var users []*user

	iter := NewIterator[user](db)

	for u, err := range iter.Query(`SELECT * FROM users ORDER BY id`) {
		if err != nil {
			t.Fatalf("Failed during iteration: %v", err)
		}

		users = append(users, u)
	}

	if diff := cmp.Diff(users, wantUsers); diff != "" {
		t.Fatalf("Unexpected diff for users list: %s", diff)
	}
}

func TestSimpleBreakIteration(t *testing.T) {
	db := createDB(t, filepath.Join(t.TempDir(), "test.sqlite3"))

	addUser(t, db, "Yury")
	addUser(t, db, "John")

	var users []*user

	iter := NewIterator[user](db)

	for u, err := range iter.Query(`SELECT * FROM users ORDER BY id`) {
		if err != nil {
			t.Fatalf("Failed during iteration: %v", err)
		}

		users = append(users, u)
		break
	}

	if diff := cmp.Diff(users, wantUsers[0:1]); diff != "" {
		t.Fatalf("Unexpected diff for users list: %s", diff)
	}
}

func TestSyntaxError(t *testing.T) {
	db := createDB(t, filepath.Join(t.TempDir(), "test.sqlite3"))

	addUser(t, db, "Yury")

	var users []*user

	iter := NewIterator[user](db)

	haveError := false

	for u, err := range iter.Query(`ELECT * FROM users ORDER BY id`) {
		if err != nil {
			haveError = true
			continue
		}

		users = append(users, u)
	}

	if len(users) > 0 {
		t.Errorf("Returned %d users for a query with a syntax error", len(users))
	}

	if !haveError {
		t.Error("A query with a syntax error returned no errors")
	}
}

type order struct {
	ID          int64  `db:"id"`
	ProductName string `db:"product_name"`
}

func TestScanError(t *testing.T) {
	db := createDB(t, filepath.Join(t.TempDir(), "test.sqlite3"))

	addUser(t, db, "Yury")

	var orders []*order

	iter := NewIterator[order](db).IgnoreUnknownFields(false)

	haveError := false

	for o, err := range iter.Query(`SELECT * FROM users ORDER BY id`) {
		if err != nil {
			haveError = true
			continue
		}

		orders = append(orders, o)
	}

	if len(orders) > 0 {
		t.Errorf("Returned %d orders for a query that selects users", len(orders))
	}

	if !haveError {
		t.Error("A query should have failed to scan rows")
	}
}

func TestRowsCloseFails(t *testing.T) {
	db := mysqlDB(t)

	var users []*user

	iter := NewIterator[user](db)
	haveError := false

	for u, err := range iter.Query(`CALL return_rows_then_fail()`) {
		if err != nil {
			haveError = true
			break
		}

		users = append(users, u)
	}

	if diff := cmp.Diff(users, wantUsers); diff != "" {
		t.Errorf("Unexpected diff for users list: %s", diff)
	}

	if !haveError {
		t.Fatalf("The stored procedure didn't fail after returning the rows")
	}
}
