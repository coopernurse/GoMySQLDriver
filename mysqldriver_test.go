package mysqldriver

import (
	"testing"
	"exp/sql"
)

// test insert/update/select/delete

// test types
//   tinyint
//   smallint
//   mediumint
//   int
//   bigint
//   float
//   double
//   datetime
//   date
//   timestamp
//   time
//   year
//   blob
//   text
//   enum

func connect(t *testing.T) (*sql.DB, error) {
	db, err := sql.Open("mysql", "gomysql_test:abc123@localhost:3306/gomysql_test")
	if err != nil {
		t.Error(err)
		return nil, err
	}
	return db, nil
}

func TestCrud(t *testing.T) {
	var id int
	db, err := connect(t)
	if err != nil { return }

    rows, err := db.Query("select 1")
	if err != nil {
		t.Error(err)
		return
	} else {
		for rows.Next() {
			err = rows.Scan(&id)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	if (id != 1) {
		t.Error("id != 1")
	}
}