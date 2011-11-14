## GoMySQLDriver ##

MySQL driver for the `exp/sql`, the generic database API for Go.  This package is still in active development and the API is subject to change.  I'm tracking the weekly Go releases at the moment.

## Installation ##

    # My fork of: github.com/Philio/GoMySQL - updated to compile on latest Go weekly
    goinstall github.com/coopernurse/GoMySQL
    
    # This package
    goinstall github.com/coopernurse/GoMySQLDriver

## Usage ##

See the `mysqldriver_test.go` file for example code.  But the important part is the `sql.Open()` command, which looks like this:

    db, err := sql.Open("mysql", "myuser:mypassword@localhost:3306/dbname")
    
The rest of the examples demonstrate usage of `exp/sql` itself.  Run: `godoc exp/sql` for more details on usage of the generic db API.

    
