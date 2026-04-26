package sqly

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/jmoiron/sqlx/reflectx"
)

var mapper = reflectx.NewMapperFunc("db", strings.ToLower)

func NewIterator[T any](db *sql.DB) *Iterator[T] {
	return &Iterator[T]{
		db:     db,
		unsafe: true,
	}
}

type Iterator[T any] struct {
	db     *sql.DB
	unsafe bool
}

// IgnoreUnknownFields controls whether or not columns that cannot be mapped
// to any field in the provided struct are ignored. The default is to allow
// unknown fields, similar to how `encoding/json` package works. It allows
// for smooth SQL schema migrations (adding new fields doesn't break `SELECT *`
// queries before the code is updated).
func (i *Iterator[T]) IgnoreUnknownFields(v bool) *Iterator[T] {
	i.unsafe = v
	return i
}

// Query is a wrapper around QueryContext with context.Background().
func (i *Iterator[T]) Query(query string, args ...any) iter.Seq2[*T, error] {
	return i.QueryContext(context.Background(), query, args...)
}

// QueryContext returns an iterator that executes the query and then starts returning rows of
// the specific type one by one. When an error is returned iteration stops.
//
// Example:
//
//	iter := sqly.NewIterator[User](db)
//	for u, err := range iter.Query(`SELECT * FROM users ORDER BY id`) {
//	    if err != nil { ... }
//	    ...
//	}
func (i *Iterator[T]) QueryContext(ctx context.Context, query string, args ...any) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		rowsRaw, err := i.db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(nil, err)
			return
		}
		// We cannot use yield() inside defer, so we Close() the rows manually.

		rows := rowsx{Rows: rowsRaw, unsafe: i.unsafe}

		for rows.Next() {
			var row T
			if err := rows.structScan(&row); err != nil {
				yield(nil, err)

				// We've already returned a single error, so ignoring Close()
				// error here.
				_ = rows.Close()
				return
			}

			if !yield(&row, nil) {
				// Cannot yield again after the iteration has stopped, so ignoring
				// Close() error here.
				_ = rows.Close()
				return
			}
		}

		// It's a common mistake to ignore rows.Close() error value, however
		// it can still happen, so we need to handle it.
		if err := rows.Close(); err != nil {
			yield(nil, err)
		}
	}
}

// TemplateQuery is wrapper around TemplateQueryContext() with context.Background().
func (i *Iterator[T]) TemplateQuery(query string, args ...any) (iter.Seq[*T], func() error) {
	return i.TemplateQueryContext(context.Background(), query, args...)
}

// TemplateQueryContext is a wrapper around QueryContext() to allow it to be used in text/template or html/template.
// It returns an iterator with a single value instead of two and then returns a function to get an error
// in case it happened. In case of any errors the iterator returns a nil pointer as the last entry so that
// the template rendering fails, making the error obvious.
//
// TemplateQueryContext() allows you to do streaming rendering of query results instead of the usual
// buffering into a temporary slice first.
//
// Usage example:
//
//	const tplText = `{{ range .users }}<div><b>{{ .ID }}</b> {{ .Name }}</div>{{ end }}`
//	tpl := template.Must(template.New("test").Parse(tplText))
//
//	users, getErr := iter.TemplateQueryContext(ctx, `SELECT * FROM users`)
//	if err := tpl.Execute(w, map[string]any{"users": users}); err != nil {
//		log.Printf("Failed to render template: %v. SELECT error: %v", err, getErr())
//	}
func (i *Iterator[T]) TemplateQueryContext(ctx context.Context, query string, args ...any) (iter.Seq[*T], func() error) {
	var errPtr atomic.Pointer[error]
	getErr := func() error {
		err := errPtr.Load()
		if err == nil {
			return errors.New("results iteration hasn't finished before calling getErr()")
		}
		return *err
	}

	return func(yield func(*T) bool) {
		var nilErr error

		for res, err := range i.QueryContext(ctx, query, args...) {
			if err != nil {
				errPtr.Store(&err)

				// yield nil pointer so that template rendering fails, making failure harder to miss
				yield(nil)
				return
			}

			if !yield(res) {
				errPtr.Store(&nilErr)
				return
			}
		}

		errPtr.Store(&nilErr)
	}, getErr
}

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type rowsx struct {
	*sql.Rows
	unsafe bool

	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []any
}

// structScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
//
// Copied from https://github.com/jmoiron/sqlx/blob/master/sqlx.go.
func (r *rowsx) structScan(dest any) error {
	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Pointer {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	v = v.Elem()

	if !r.started {
		columns, err := r.Columns()
		if err != nil {
			return err
		}
		m := mapper

		r.fields = m.TraversalsByName(v.Type(), columns)
		// if we are not unsafe and are missing fields, return an error
		if f, err := missingFields(r.fields); err != nil && !r.unsafe {
			return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
		}
		r.values = make([]any, len(columns))
		r.started = true
	}

	err := fieldsByTraversal(v, r.fields, r.values, true)
	if err != nil {
		return err
	}
	// scan into the struct field pointers and append to our results
	err = r.Scan(r.values...)
	if err != nil {
		return err
	}
	return r.Err()
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
//
// Copied from https://github.com/jmoiron/sqlx/blob/master/sqlx.go.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []any, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(any)
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

// missingFields is copied from https://github.com/jmoiron/sqlx/blob/master/sqlx.go.
func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
