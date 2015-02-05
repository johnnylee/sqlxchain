# sqlxchain

A thin wrapper around sqlx for command chaining, as in the following simple example:

```go
db, _ = sqlxchain.New("mysql", dns)

var now int64
var id int64

err := db.Context().Begin().
    Get(&now, "SELECT UNIX_TIMESTAMP()").
    Get(&id, "SELECT id FROM employee WHERE should_fire=1 LIMIT 1).
    Exec("UPDATE employee SET terminated=? WHERE id=?", now, id).
    Commit().
    Err()
```