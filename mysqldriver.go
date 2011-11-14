package mysqldriver

import (
	"exp/sql"
	"exp/sql/driver"
	"fmt"
	"io"
	"mysql"
	"strings"
)

func init() {
	sql.Register("mysql", &MySQLDriver{})
}

func parseDSN(dsn string) (hostport, user, passwd, dbname string, err error) {
	pos := strings.Index(dsn, "@")
	if pos < 1 {
		err = fmt.Errorf("mysqldriver: Invalid dsn: %s - Should be: user:password@host:port/dbname", dsn)
	} else {
		user = dsn[:pos]
		hostport = dsn[pos+1:]

		pos = strings.Index(user, ":")
		if pos > -1 {
			passwd = user[pos+1:]
			user = user[:pos]
		}

		pos = strings.Index(hostport, "/")
		if pos > -1 {
			dbname = hostport[pos+1:]
			hostport = hostport[:pos]
		}
	}

	return
}

////////////////

type MySQLDriver struct{}

func (d *MySQLDriver) Open(dsn string) (driver.Conn, error) {
	hostport, user, passwd, dbname, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	//fmt.Printf("host=%s user=%s pass=%s db=%s\n", hostport, user, passwd, dbname)

	client, err := mysql.DialTCP(hostport, user, passwd, dbname)
	if err != nil {
		return nil, err
	}

	return &MySQLConn{client}, nil
}

////////////////

type MySQLConn struct {
	client *mysql.Client
}

func (c *MySQLConn) Begin() (driver.Tx, error) {
	if err := c.client.SetAutoCommit(false); err != nil {
		return nil, err
	}
	if err := c.client.Start(); err != nil {
		return nil, err
	}
	return &MySQLTx{c.client}, nil
}

func (c *MySQLConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.client.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &MySQLStmt{stmt}, nil
}

func (c *MySQLConn) Close() error {
	if err := c.client.Close(); err != nil {
		return err
	}
	return nil
}

////////////////

type MySQLTx struct {
	client *mysql.Client
}

func (t *MySQLTx) Commit() error {
	if err := t.client.Commit(); err != nil {
		return err
	}
	return nil
}

func (t *MySQLTx) Rollback() error {
	if err := t.client.Rollback(); err != nil {
		return err
	}
	return nil
}

////////////////

func bindAndExec(stmt *mysql.Statement, args []interface{}) error {
	if err := stmt.BindParams(args...); err != nil {
		return err
	}
	if err := stmt.Execute(); err != nil {
		return err
	}

	return nil
}

type MySQLStmt struct {
	stmt *mysql.Statement
}

func (s *MySQLStmt) Exec(args []interface{}) (driver.Result, error) {
	if err := bindAndExec(s.stmt, args); err != nil {
		return nil, err
	}
	return &MySQLResult{s.stmt}, nil
}

func (s *MySQLStmt) Query(args []interface{}) (driver.Rows, error) {
	if err := bindAndExec(s.stmt, args); err != nil {
		return nil, err
	}
	return &MySQLRows{s.stmt, nil, nil}, nil
}

func (s *MySQLStmt) NumInput() int {
	return int(s.stmt.ParamCount())
}

func (s *MySQLStmt) Close() error {
	if err := s.stmt.Close(); err != nil {
		return err
	}
	return nil
}

////////////////

type MySQLResult struct {
	stmt *mysql.Statement
}

func (r *MySQLResult) LastInsertId() (int64, error) {
	return int64(r.stmt.LastInsertId), nil
}

func (r *MySQLResult) RowsAffected() (int64, error) {
	return int64(r.stmt.AffectedRows), nil
}

////////////////

type MySQLRows struct {
	stmt  *mysql.Statement
	cols  []string      // column names in result
	bindr []interface{} // placeholder vars to bind data into
}

func (r *MySQLRows) initCols() {
	if r.cols == nil {
		fields := r.stmt.FetchColumns()
		if fields == nil {
			return
		}

		count := len(fields)
		cols := make([]string, count)
		bindr := make([]interface{}, count)
		for i := 0; i < count; i++ {
			cols[i] = fields[i].Name
			switch fields[i].Type {
			case mysql.FIELD_TYPE_DECIMAL:
				bindr[i] = new(float64)
			case mysql.FIELD_TYPE_TINY:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_SHORT:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_LONG:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_FLOAT:
				bindr[i] = new(float64)
			case mysql.FIELD_TYPE_DOUBLE:
				bindr[i] = new(float64)
			case mysql.FIELD_TYPE_NULL:
				bindr[i] = nil
			case mysql.FIELD_TYPE_TIMESTAMP:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_LONGLONG:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_INT24:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_DATE:
				bindr[i] = new(string)
			case mysql.FIELD_TYPE_TIME:
				bindr[i] = new(string)
			case mysql.FIELD_TYPE_DATETIME:
				bindr[i] = new(string)
			case mysql.FIELD_TYPE_YEAR:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_NEWDATE:
				bindr[i] = new(int64)
			case mysql.FIELD_TYPE_VARCHAR:
				bindr[i] = new(string)
			case mysql.FIELD_TYPE_BIT:
				bindr[i] = new(bool)
			default:
				bindr[i] = new([]byte)
			}
		}
		r.cols = cols
		r.bindr = bindr
	}
}

func (r *MySQLRows) Columns() []string {
	r.initCols()
	return r.cols
}

func (r *MySQLRows) Next(dest []interface{}) error {
	r.initCols()

	if err := r.stmt.BindResult(r.bindr...); err != nil {
		return err
	}

	eof, err := r.stmt.Fetch()
	if err != nil {
		return err
	}
	if eof {
		return io.EOF
	}

	for k, v := range r.bindr {
		switch t := v.(type) {
		case *string:
			dest[k] = string(*t)
		case *int64:
			dest[k] = int64(*t)
		case *float64:
			dest[k] = float64(*t)
		case *bool:
			dest[k] = bool(*t)
		case *[]byte:
			dest[k] = []byte(*t)
		case nil:
			dest[k] = nil
		}
	}

	return nil
}

func (r *MySQLRows) Close() error {
	if err := r.stmt.FreeResult(); err != nil {
		return err
	}
	return nil
}
