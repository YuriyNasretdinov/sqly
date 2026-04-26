## Package SQLy

SQLy provides an iterator API for querying rows from `database/sql`. It's name is a nod towards https://github.com/jmoiron/sqlx. Iterators allow to get a type-safe API for iterating the query results and don't require buffering the result upfront compared to `sqlx`.

## Usage example

The main goal of the iterator API is to allow to more easily iterate over the query results, and, importantly, handle the corner cases (like `rows.Close()` returning errors) too.

```go
package main

// The database field tags are the same as in `sqlx`, prefixed with "db:".
// In fact you can continue using `sqlx` alongside with `sqly`.
type User struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func main() {
    db, _ := sql.Open(...)

    // Create an iterator object that can be used to send queries to db
    // and iterate over the results decoding them into (*User, error).
    iter := NewIterator[User](db)

    // Sending a query is as simple as calling `Query()` as with regular `database/sql`,
    // with the main difference being that `iter.Query()` returns an iterator over
    // the results instead of a *sql.Rows object.
    //
    // The iterator will return an error for these 3 cases:
    //   - Query failed.
    //   - Scanning the results into struct failed.
    //   - When rows.Close() failed
	for u, err := range iter.Query(`SELECT * FROM users ORDER BY id`) {
		if err != nil {
			log.Fatalf("Failed during iteration: %v", err)
		}

		log.Printf("User ID=%d   Name=%s", u.ID, u.Name)
	}
}
```