package mysqldriver

import (
	"exp/sql"
	"fmt"
	"testing"
	"time"
)

// test insert/update/select/delete

// test types
//   datetime
//   date
//   timestamp
//   time
//   year
//   blob
//   text

func TestCrud(t *testing.T) {
	var id int
	db := connect()

	rows := query(db, "select 1")
	rows.Next()
	rows.Scan(&id)
	if id != 1 {
		t.Errorf("%d != 1", id)
	}
}

func TestInts(t *testing.T) {
	db := connect()

	types := [5]string{"tinyint", "smallint", "mediumint", "int", "bigint"}
	for _, v := range types {
		table := "testtable"
		exec(db, ("drop table if exists " + table))
		exec(db, "create table "+table+" (x "+v+")")
		defer exec(db, "drop table if exists "+table)

		sql := fmt.Sprintf("insert into %s values (?)", table)
		i := int64(3)
		exec(db, sql, i)

		var val int64
		rows := query(db, fmt.Sprintf("select * from %s", table))
		rows.Next()
		rows.Scan(&val)
		if val != i {
			t.Errorf("%d != %d", i, val)
			return
		}
	}
}

func TestFloats(t *testing.T) {
	db := connect()

	types := [2]string{"float", "double"}
	for _, v := range types {
		table := "testtable"
		exec(db, ("drop table if exists " + table))
		exec(db, "create table "+table+" (x "+v+")")
		defer exec(db, "drop table if exists "+table)

		sql := fmt.Sprintf("insert into %s values (?)", table)
		i := float64(93.21)
		exec(db, sql, i)

		var val float64
		rows := query(db, fmt.Sprintf("select * from %s", table))
		rows.Next()
		rows.Scan(&val)
		if fmt.Sprintf("%.3f", i) != fmt.Sprintf("%.3f", val) {
			t.Errorf("%d != %d", i, val)
			return
		}
	}
}

func TestStringTypes(t *testing.T) {
	testStringQuery(t, "varchar(255)", "this is a string")
	testStringQuery(t, "enum ('val1', 'val2')", "val2")

	text := ""
	for i := 0; i < 1000; i++ {
		text += "12345678901234567890 était prêt à "
	}
	testStringQuery(t, "text", text)

	text = ""
	for i := 0; i < 10000; i++ {
		text += "12345678901234567890 était prêt à "
	}
	testStringQuery(t, "mediumtext", text)

}

func testStringQuery(t *testing.T, col string, val string) {
	db := connect()

	table := "testtable"
	exec(db, ("drop table if exists " + table))
	exec(db, "create table "+table+" (x "+col+") charset utf8")
	defer exec(db, "drop table if exists "+table)

	sql := fmt.Sprintf("insert into %s values (?)", table)
	exec(db, sql, val)

	var newval string
	rows := query(db, fmt.Sprintf("select * from %s", table))
	rows.Next()
	rows.Scan(&newval)
	if val != newval {
		t.Errorf("%d != %d", val, newval)
		return
	}
}

func TestInsertPerf(t *testing.T) {
	db := connect()
	exec(db, "drop table if exists testtable")
	exec(db, "create table testtable (a int, b varchar(255))")
	defer exec(db, "drop table if exists testtable")

	num := 1000
	start := time.Nanoseconds()
	tx, err := db.Begin()
	stmt, err := tx.Prepare("insert into testtable values (?,?)")
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < num; i++ {
		b := fmt.Sprintf("this is a string %d", i)
		stmt.Exec(i, b)
	}
	tx.Commit()

	elapsed := (time.Nanoseconds() - start) / 1000000
	fmt.Printf("%d inserts in %dms\n", num, elapsed)
}

//////////////////////////////////////

func connect() *sql.DB {
	db, err := sql.Open("mysql", "gomysql_test:abc123@localhost:3306/gomysql_test")
	if err != nil {
		panic("Error connecting to db: " + err.Error())
	}
	return db
}

func query(db *sql.DB, query string, bindvars ...interface{}) *sql.Rows {
	var rows *sql.Rows
	var err error
	if len(bindvars) > 0 {
		rows, err = db.Query(query, bindvars...)
	} else {
		rows, err = db.Query(query)
	}
	if err != nil {
		panic(fmt.Sprintf("Error running sql: %s %v", query, err))
	}
	return rows
}

func exec(db *sql.DB, query string, bindvars ...interface{}) sql.Result {
	var res sql.Result
	var err error
	if len(bindvars) > 0 {
		res, err = db.Exec(query, bindvars...)
	} else {
		res, err = db.Exec(query)
	}
	if err != nil {
		panic(fmt.Sprintf("Error running sql: %s %v", query, err))
	}
	return res
}
