package mgodbc

/*
#cgo darwin LDFLAGS: -lodbc
#cgo freebsd LDFLAGS: -lodbc
#cgo linux LDFLAGS: -lodbc
#cgo windows LDFLAGS: -lodbc32

#ifdef __MINGW32__
	#include <windef.h>
#endif

#include <sql.h>
#include <sqlext.h>


// The 32-bit and 64-bit versions of Microsoft's headers are different. Also, the
// unixodbc header appears to follow the 64-bit Microsoft header. The best way to
// resolve these difference seems to be to wrap them in C, rather than trying to
// create separate platform- or architecture-specific go files.
//
// These wrappers around the real ODBC functions provide a common interface for
// the go side to work with.
SQLRETURN mSQLColAttribute
(
	SQLHSTMT     hstmt,
	SQLUSMALLINT iCol,
	SQLUSMALLINT iField,
	SQLPOINTER   pCharAttr,
	SQLSMALLINT  cbDescMax,
	SQLSMALLINT  *pcbCharAttr,
	SQLLEN       *pNumAttr
)
{
	return SQLColAttributeW(
		hstmt,
		iCol,
		iField,
		pCharAttr,
		cbDescMax,
		pcbCharAttr,
		pNumAttr
	);
}

SQLRETURN mSQLAllocEnv(SQLHENV* OutputHandlePtr)
{
	return SQLAllocHandle(SQL_HANDLE_ENV, 0, OutputHandlePtr);
}

SQLRETURN mSQLAllocConnect(SQLHENV InputHandle, SQLHDBC* OutputHandlePtr)
{
	return SQLAllocHandle(SQL_HANDLE_DBC, InputHandle, OutputHandlePtr);
}

SQLRETURN mSQLAllocStmt(SQLHDBC InputHandle, SQLHSTMT* OutputHandlePtr)
{
	return SQLAllocHandle(SQL_HANDLE_STMT, InputHandle, OutputHandlePtr);
}

*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unsafe"
)

// init //
func init() {
	d, err := NewDriver()
	if err == nil && d != nil {
		sql.Register("mgodbc", d)
	}

	var wchar C.SQLWCHAR
	wcharSize = int(unsafe.Sizeof(wchar))
}

var (
	nullString [1]uint16
	nullByte   [1]byte
	wcharSize  int
)

// utility //
func success(ret C.SQLRETURN) bool {
	return ret == C.SQL_SUCCESS || ret == C.SQL_SUCCESS_WITH_INFO
}

func utf16ToString(utf16Data []uint16) string {
	return string(utf16.Decode(utf16Data))
}

func stringToUTF16(data string) []uint16 {
	return utf16.Encode([]rune(data))
}

// ODBC Error //
type StatusRecord struct {
	State       string
	NativeError int
	Message     string
}

func (sr *StatusRecord) toString() string {
	return fmt.Sprintf("{%s} %s", sr.State, sr.Message)
}

type ODBCError struct {
	StatusRecords []StatusRecord
}

func (e *ODBCError) Error() string {
	statusStrings := make([]string, len(e.StatusRecords))
	for i, sr := range e.StatusRecords {
		statusStrings[i] = sr.toString()
	}

	return strings.Join(statusStrings, "\n")
}

func newError(sqlHandle interface{}) error {
	// Figure out the handle type
	var handleType C.SQLSMALLINT
	var handle C.SQLHANDLE
	switch sqlHandle.(type) {
	case C.SQLHENV:
		handleType = C.SQL_HANDLE_ENV
		handle = C.SQLHANDLE(sqlHandle.(C.SQLHENV))

	case C.SQLHDBC:
		handleType = C.SQL_HANDLE_DBC
		handle = C.SQLHANDLE(sqlHandle.(C.SQLHDBC))

	case C.SQLHSTMT:
		handleType = C.SQL_HANDLE_STMT
		handle = C.SQLHANDLE(sqlHandle.(C.SQLHSTMT))

	default:
		return errors.New("unknown odbc handle type")
	}

	// Get the number of diagnostic records
	var diagRecordCount C.SQLULEN
	ret := C.SQLGetDiagFieldW(
		handleType,
		handle,
		0,
		C.SQL_DIAG_NUMBER,
		C.SQLPOINTER(&diagRecordCount),
		4,
		nil,
	)
	if !success(ret) {
		return errors.New("failed to retrieve diagnostic header information")
	}

	// Query each of the diagnostic records
	recordCount := int(diagRecordCount)
	statusRecords := make([]StatusRecord, 0)
	for i := 0; i < recordCount; i++ {
		// Find the needed size for the message buffer
		var messageLen C.SQLSMALLINT
		ret = C.SQLGetDiagRecW(
			handleType,
			handle,
			C.SQLSMALLINT(i+1),
			nil,
			nil,
			nil,
			0,
			&messageLen,
		)
		if !success(ret) {
			continue
		}

		// Get the diagnostic record values
		var state [6]uint16
		var nativeError C.SQLINTEGER
		messageBuf := make([]uint16, int(messageLen)+1)
		ret = C.SQLGetDiagRecW(
			handleType,
			handle,
			C.SQLSMALLINT(i+1),
			(*C.SQLWCHAR)(&state[0]),
			&nativeError,
			(*C.SQLWCHAR)(&messageBuf[0]),
			messageLen+1,
			nil,
		)
		if !success(ret) {
			continue
		}

		sr := StatusRecord{}
		sr.State = utf16ToString(state[:5])
		sr.NativeError = int(nativeError)
		sr.Message = utf16ToString(messageBuf[:messageLen])

		statusRecords = append(statusRecords, sr)
	}

	return &ODBCError{StatusRecords: statusRecords}
}

// Driver //
type Driver struct {
	envHandle C.SQLHENV
}

func NewDriver() (driver.Driver, error) {
	d := &Driver{}

	// Allocate the environment handle for the driver
	ret := C.mSQLAllocEnv(&d.envHandle)
	if !success(ret) {
		return nil, errors.New("failed to allocate environment handle")
	}

	// Set the environment handle to use ODBCv3
	ret = C.SQLSetEnvAttr(
		C.SQLHENV(d.envHandle),
		C.SQL_ATTR_ODBC_VERSION,
		C.SQLPOINTER(uintptr(C.SQL_OV_ODBC3)),
		0,
	)
	if !success(ret) {
		err := newError(d.envHandle)
		C.SQLFreeHandle(C.SQL_HANDLE_ENV, C.SQLHANDLE(d.envHandle))
		return nil, err
	}

	runtime.SetFinalizer(d, (*Driver).Close)
	return d, nil
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	if d.envHandle == nil {
		return nil, errors.New("driver has been closed")
	}

	c := &conn{
		d: d,
	}

	// Allocate the connection handle
	ret := C.mSQLAllocConnect(d.envHandle, &c.dbc)
	if !success(ret) {
		return nil, newError(d.envHandle)
	}

	// Perform the driver connect
	var outLen C.SQLSMALLINT
	utf16Name := stringToUTF16(name)
	ret = C.SQLDriverConnectW(
		c.dbc,
		nil,
		(*C.SQLWCHAR)(&utf16Name[0]),
		C.SQLSMALLINT(len(utf16Name)),
		nil,
		0,
		&outLen,
		C.SQL_DRIVER_NOPROMPT,
	)
	if !success(ret) {
		err := newError(c.dbc)
		C.SQLFreeHandle(C.SQL_HANDLE_DBC, C.SQLHANDLE(c.dbc))
		return nil, err
	}

	runtime.SetFinalizer(c, (*conn).Close)
	return c, nil
}

func (d *Driver) Close() error {
	if d.envHandle == nil {
		return errors.New("environment handle already freed")
	}

	runtime.SetFinalizer(d, nil)

	// Free the environment handle
	ret := C.SQLFreeHandle(C.SQL_HANDLE_ENV, C.SQLHANDLE(d.envHandle))
	d.envHandle = nil
	if !success(ret) {
		return errors.New("failed to free environment handle")
	}

	return nil
}

// Conn //
type conn struct {
	d   *Driver
	dbc C.SQLHDBC
	t   *tx
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.dbc == nil {
		return nil, errors.New("connection has been closed")
	}

	s := &stmt{
		c:     c,
		query: query,
	}

	// Allocate the statement handle
	ret := C.mSQLAllocStmt(c.dbc, &s.hstmt)
	if !success(ret) {
		return nil, newError(c.dbc)
	}

	// Prepare the statement
	utf16Query := stringToUTF16(query)
	ret = C.SQLPrepareW(
		s.hstmt,
		(*C.SQLWCHAR)(&utf16Query[0]),
		C.SQLINTEGER(len(utf16Query)),
	)
	if !success(ret) {
		err := newError(s.hstmt)
		C.SQLFreeHandle(C.SQL_HANDLE_STMT, C.SQLHANDLE(s.hstmt))
		return nil, err
	}

	// Get the input count
	var paramCount C.SQLSMALLINT
	ret = C.SQLNumParams(s.hstmt, &paramCount)
	if success(ret) {
		s.numInput = int(paramCount)
	}

	runtime.SetFinalizer(s, (*stmt).Close)
	return s, nil
}

func (c *conn) Close() error {
	if c.dbc == nil {
		return errors.New("connection already closed")
	}

	runtime.SetFinalizer(c, nil)

	// Disconnect the connection
	ret := C.SQLDisconnect(c.dbc)
	if !success(ret) {
		err := newError(c.dbc)
		C.SQLFreeHandle(C.SQL_HANDLE_DBC, C.SQLHANDLE(c.dbc))
		c.dbc = nil
		return err
	}

	// Free the connection handle
	ret = C.SQLFreeHandle(C.SQL_HANDLE_DBC, C.SQLHANDLE(c.dbc))
	if !success(ret) {
		err := newError(c.dbc)
		c.dbc = nil
		return err
	}

	c.dbc = nil
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.t != nil {
		return nil, errors.New("a transaction has already been started")
	}

	// Check for transaction support
	var txnCapable C.SQLUSMALLINT
	ret := C.SQLGetInfoW(
		c.dbc,
		C.SQLUSMALLINT(C.SQL_TXN_CAPABLE),
		C.SQLPOINTER(&txnCapable),
		2,
		nil,
	)
	if !success(ret) {
		return nil, newError(c.dbc)
	}

	if txnCapable == C.SQL_TC_NONE {
		return nil, errors.New("transactions are not supported by this ODBC driver")
	}

	// Turn autocommit off
	ret = C.SQLSetConnectAttrW(
		c.dbc,
		C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(uintptr(C.SQL_AUTOCOMMIT_OFF)),
		C.SQL_IS_UINTEGER,
	)
	if !success(ret) {
		return nil, newError(c.dbc)
	}

	return &tx{c: c}, nil
}

// Stmt //
type stmt struct {
	c        *conn
	query    string
	numInput int

	hstmt C.SQLHSTMT

	// mu protects closed and inUse
	mu sync.Mutex

	// closed specifies whether the stmt has been closed
	closed bool

	// inUse specifies whether the hstmt is currently in use
	inUse bool
}

func (s *stmt) startUsing() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("statement has been closed")
	}

	if s.inUse {
		return errors.New("statement is currently in use")
	}

	s.inUse = true
	return nil
}

func (s *stmt) endUsing() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.inUse {
		return errors.New("internal error: statement is not currently in use")
	}

	// If the stmt has already been closed, free the handle
	if s.closed {
		ret := C.SQLFreeHandle(C.SQL_HANDLE_STMT, C.SQLHANDLE(s.hstmt))
		if !success(ret) {
			return newError(s.hstmt)
		}
	}

	s.inUse = false
	return nil
}

func bindStmt(s C.SQLHSTMT, args []driver.Value) error {
	for i, arg := range args {
		var valueType C.SQLSMALLINT
		var parameterType C.SQLSMALLINT
		var columnSize C.SQLULEN
		var decimalDigits C.SQLSMALLINT
		var parameterValuePtr C.SQLPOINTER
		var bufferLength C.SQLLEN
		var intPtr C.SQLLEN

		switch arg.(type) {
		case nil:
			valueType = C.SQL_C_DEFAULT
			parameterType = C.SQL_WCHAR
			columnSize = 1
			intPtr = C.SQL_NULL_DATA

		case bool:
			var bValue [1]byte
			if arg.(bool) {
				bValue[0] = 1
			} else {
				bValue[0] = 0
			}

			valueType = C.SQL_C_BIT
			parameterType = C.SQL_BIT
			parameterValuePtr = C.SQLPOINTER(&bValue[0])
			bufferLength = 1

		case int64:
			llValue := C.longlong(arg.(int64))

			valueType = C.SQL_C_SBIGINT
			parameterType = C.SQL_BIGINT
			parameterValuePtr = C.SQLPOINTER(&llValue)
			bufferLength = 8

		case float64:
			dValue := C.double(arg.(float64))

			valueType = C.SQL_C_DOUBLE
			parameterType = C.SQL_DOUBLE
			parameterValuePtr = C.SQLPOINTER(&dValue)
			bufferLength = 8

		case string:
			stringValue := arg.(string)
			var utf16StringValue []uint16
			var ucs4StringValue []rune

			if len(stringValue) == 0 {
				parameterValuePtr = C.SQLPOINTER(&nullString[0])
				columnSize = 1
				bufferLength = 0

			} else if wcharSize == 2 {
				utf16StringValue = stringToUTF16(stringValue)
				length := len(utf16StringValue) * wcharSize

				parameterValuePtr = C.SQLPOINTER(&utf16StringValue[0])
				columnSize = C.SQLULEN(length)
				bufferLength = C.SQLLEN(length)

			} else if wcharSize == 4 {
				ucs4StringValue = []rune(stringValue)
				length := len(ucs4StringValue) * wcharSize

				parameterValuePtr = C.SQLPOINTER(&ucs4StringValue[0])
				columnSize = C.SQLULEN(length)
				bufferLength = C.SQLLEN(length)

			} else {
				return errors.New("invalid/unknown wchar size")
			}

			valueType = C.SQL_C_WCHAR
			parameterType = C.SQL_WVARCHAR
			intPtr = bufferLength

		case []byte:
			byteValue := arg.([]byte)
			if len(byteValue) == 0 {
				parameterValuePtr = C.SQLPOINTER(&nullByte[0])
				columnSize = 1
			} else {
				parameterValuePtr = C.SQLPOINTER(&byteValue[0])
				columnSize = C.SQLULEN(len(byteValue))
			}

			valueType = C.SQL_C_BINARY
			parameterType = C.SQL_BINARY
			bufferLength = C.SQLLEN(len(byteValue))
			intPtr = C.SQLLEN(len(byteValue))

		case time.Time:
			timeValue := arg.(time.Time)

			var timestampValue C.SQL_TIMESTAMP_STRUCT
			timestampValue.year = C.SQLSMALLINT(timeValue.Year())
			timestampValue.month = C.SQLUSMALLINT(timeValue.Month())
			timestampValue.day = C.SQLUSMALLINT(timeValue.Day())
			timestampValue.hour = C.SQLUSMALLINT(timeValue.Hour())
			timestampValue.minute = C.SQLUSMALLINT(timeValue.Minute())
			timestampValue.second = C.SQLUSMALLINT(timeValue.Second())
			timestampValue.fraction = C.SQLUINTEGER(timeValue.Nanosecond())

			valueType = C.SQL_C_TYPE_TIMESTAMP
			parameterType = C.SQL_TYPE_TIMESTAMP
			parameterValuePtr = C.SQLPOINTER(&timestampValue)
			bufferLength = C.SQLLEN(unsafe.Sizeof(timestampValue))

		default:
			return errors.New("invalid parameter type to bind to")
		}

		ret := C.SQLBindParameter(
			s,
			C.SQLUSMALLINT(i+1),
			C.SQL_PARAM_INPUT,
			valueType,
			parameterType,
			columnSize,
			decimalDigits,
			parameterValuePtr,
			bufferLength,
			&intPtr,
		)
		if !success(ret) {
			return newError(s)
		}
	}

	return nil
}

func (s *stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("statement has already been closed")
	}

	runtime.SetFinalizer(s, nil)

	s.closed = true
	if !s.inUse {
		ret := C.SQLFreeHandle(C.SQL_HANDLE_STMT, C.SQLHANDLE(s.hstmt))
		if !success(ret) {
			return newError(s.hstmt)
		}
	}

	return nil
}

func (s *stmt) NumInput() int {
	return s.numInput
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	err := s.startUsing()
	if err != nil {
		return nil, err
	}
	defer s.endUsing()

	// Bind the statement
	err = bindStmt(s.hstmt, args)
	if err != nil {
		return nil, err
	}

	// Execute the statement
	ret := C.SQLExecute(s.hstmt)
	if !success(ret) && ret != C.SQL_NO_DATA {
		return nil, newError(s.hstmt)
	}

	// Get the number of rows affected
	r := &result{}
	var rowCount C.SQLLEN
	ret = C.SQLRowCount(s.hstmt, &rowCount)
	if !success(ret) {
		return nil, newError(s.hstmt)
	}

	r.rowsAffected = int64(rowCount)
	return r, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	err := s.startUsing()
	if err != nil {
		return nil, err
	}

	// Bind the statement
	err = bindStmt(s.hstmt, args)
	if err != nil {
		s.endUsing()
		return nil, err
	}

	// Execute the statement
	ret := C.SQLExecute(s.hstmt)
	if !success(ret) && ret != C.SQL_NO_DATA {
		s.endUsing()
		return nil, newError(s.hstmt)
	}

	// Get the column names
	var numResultCols C.SQLSMALLINT
	ret = C.SQLNumResultCols(s.hstmt, &numResultCols)
	if !success(ret) {
		s.endUsing()
		return nil, newError(s.hstmt)
	}

	columns := make([]string, int(numResultCols))
	for i, _ := range columns {
		// Get the length of the column name
		var nameLen C.SQLSMALLINT
		ret = C.mSQLColAttribute(
			s.hstmt,
			C.SQLUSMALLINT(i+1),
			C.SQL_DESC_NAME,
			nil,
			0,
			&nameLen,
			nil,
		)
		if !success(ret) {
			s.endUsing()
			return nil, newError(s.hstmt)
		}

		// If the name length is 0, skip getting the name (the default is empty anyway)
		columnNameLen := int(nameLen) / 2
		if columnNameLen == 0 {
			continue
		}

		// Get the column name
		columnName := make([]uint16, columnNameLen+1)
		ret = C.mSQLColAttribute(
			s.hstmt,
			C.SQLUSMALLINT(i+1),
			C.SQL_DESC_NAME,
			C.SQLPOINTER(&columnName[0]),
			C.SQLSMALLINT((columnNameLen+1)*2),
			nil,
			nil,
		)
		if !success(ret) {
			s.endUsing()
			return nil, newError(s.hstmt)
		}

		columns[i] = utf16ToString(columnName[:columnNameLen])
	}

	r := &rows{
		s:       s,
		columns: columns,
	}

	runtime.SetFinalizer(r, (*rows).Close)
	return r, nil
}

// Result //
type result struct {
	rowsAffected int64
}

func (*result) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Rows //
type rows struct {
	s       *stmt
	columns []string
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	if r.s == nil {
		return errors.New("statement has already been closed")
	}

	runtime.SetFinalizer(r, nil)

	var err error
	ret := C.SQLCloseCursor(r.s.hstmt)
	if !success(ret) {
		err = newError(r.s.hstmt)
	}

	r.s.endUsing()
	r.s = nil
	return err
}

func (r *rows) Next(dest []driver.Value) error {
	if r.s == nil {
		return errors.New("statement has been closed")
	}

	// Special check in case there was no result set generated
	var columnCount C.SQLSMALLINT
	ret := C.SQLNumResultCols(r.s.hstmt, &columnCount)
	if !success(ret) {
		return newError(r.s.hstmt)
	}

	if columnCount == 0 {
		return io.EOF
	}

	// Fetch the next row
	ret = C.SQLFetch(r.s.hstmt)
	if ret == C.SQL_NO_DATA {
		return io.EOF
	} else if !success(ret) {
		return newError(r.s.hstmt)
	}

	for i, _ := range dest {
		// Get the type of the column
		var colType C.SQLLEN
		ret = C.mSQLColAttribute(
			r.s.hstmt,
			C.SQLUSMALLINT(i+1),
			C.SQL_DESC_CONCISE_TYPE,
			nil,
			0,
			nil,
			&colType,
		)
		if !success(ret) {
			return newError(r.s.hstmt)
		}

		// Query the data from the column
		var dummy [1]byte
		var indPtr C.SQLLEN
		switch int(colType) {
		case C.SQL_BIT:
			var bitValue byte
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_BIT,
				C.SQLPOINTER(&bitValue),
				0,
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				if bitValue == 0 {
					dest[i] = false
				} else {
					dest[i] = true
				}
			}

		case C.SQL_TINYINT, C.SQL_SMALLINT, C.SQL_INTEGER, C.SQL_BIGINT:
			var intValue C.longlong
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_SBIGINT,
				C.SQLPOINTER(&intValue),
				0,
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				dest[i] = int64(intValue)
			}

		case C.SQL_REAL, C.SQL_FLOAT, C.SQL_DOUBLE:
			var doubleValue C.double
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_DOUBLE,
				C.SQLPOINTER(&doubleValue),
				0,
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				dest[i] = float64(doubleValue)
			}

		case C.SQL_CHAR, C.SQL_VARCHAR, C.SQL_LONGVARCHAR, C.SQL_WCHAR, C.SQL_WVARCHAR, C.SQL_WLONGVARCHAR:
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_WCHAR,
				C.SQLPOINTER(&dummy[0]),
				0,
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				strLen := int(indPtr) / 2
				strBuf := make([]uint16, strLen+1)
				ret = C.SQLGetData(
					r.s.hstmt,
					C.SQLUSMALLINT(i+1),
					C.SQL_C_WCHAR,
					C.SQLPOINTER(&strBuf[0]),
					C.SQLLEN((strLen+1)*2),
					&indPtr,
				)
				if !success(ret) {
					return newError(r.s.hstmt)
				}

				dest[i] = []byte(utf16ToString(strBuf[:strLen]))
			}

		case C.SQL_GUID:
			strLen := 36
			strBuf := make([]uint16, strLen+1)
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_WCHAR,
				C.SQLPOINTER(&strBuf[0]),
				C.SQLLEN((strLen+1)*2),
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				dest[i] = []byte(utf16ToString(strBuf[:strLen]))
			}

		case C.SQL_BINARY, C.SQL_VARBINARY, C.SQL_LONGVARBINARY:
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_BINARY,
				C.SQLPOINTER(&dummy[0]),
				0,
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				binaryLen := int(indPtr)
				binaryBuf := make([]byte, binaryLen)
				ret = C.SQLGetData(
					r.s.hstmt,
					C.SQLUSMALLINT(i+1),
					C.SQL_C_BINARY,
					C.SQLPOINTER(&binaryBuf[0]),
					C.SQLLEN(binaryLen+1),
					&indPtr,
				)
				if !success(ret) {
					return newError(r.s.hstmt)
				}

				dest[i] = binaryBuf
			}

		case C.SQL_TYPE_TIMESTAMP:
			var timestampValue C.SQL_TIMESTAMP_STRUCT
			ret = C.SQLGetData(
				r.s.hstmt,
				C.SQLUSMALLINT(i+1),
				C.SQL_C_TYPE_TIMESTAMP,
				C.SQLPOINTER(&timestampValue),
				C.SQLLEN(unsafe.Sizeof(timestampValue)),
				&indPtr,
			)
			if !success(ret) {
				return newError(r.s.hstmt)
			}

			if indPtr == C.SQL_NULL_DATA {
				dest[i] = nil
			} else {
				dest[i] = time.Date(
					int(timestampValue.year),
					time.Month(timestampValue.month),
					int(timestampValue.day),
					int(timestampValue.hour),
					int(timestampValue.minute),
					int(timestampValue.second),
					int(timestampValue.fraction),
					time.UTC,
				)
			}

		default:
			dest[i] = nil
		}
	}

	return nil
}

func (*rows) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

func (r *rows) RowsAffected() (int64, error) {
	if r.s == nil {
		return 0, errors.New("statement has been closed")
	}

	// Get the number of rows affected
	var rowCount C.SQLLEN
	ret := C.SQLRowCount(r.s.hstmt, &rowCount)
	if !success(ret) {
		return 0, newError(r.s.hstmt)
	}

	return int64(rowCount), nil
}

// Tx //
type tx struct {
	c *conn
}

func (t *tx) Commit() error {
	if t.c == nil {
		return errors.New("transaction has already ended")
	}

	// Commit the transaction
	ret := C.SQLEndTran(
		C.SQL_HANDLE_DBC,
		C.SQLHANDLE(t.c.dbc),
		C.SQL_COMMIT,
	)
	if !success(ret) {
		return newError(t.c.dbc)
	}

	// Turn autocommit back on
	ret = C.SQLSetConnectAttrW(
		t.c.dbc,
		C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(uintptr(C.SQL_AUTOCOMMIT_ON)),
		0,
	)
	if !success(ret) {
		err := newError(t.c.dbc)
		t.c.t = nil
		t.c = nil
		return err
	}

	t.c.t = nil
	t.c = nil
	return nil
}

func (t *tx) Rollback() error {
	if t.c == nil {
		return errors.New("transaction has already ended")
	}

	// Rollback the transaction
	ret := C.SQLEndTran(
		C.SQL_HANDLE_DBC,
		C.SQLHANDLE(t.c.dbc),
		C.SQL_ROLLBACK,
	)
	if !success(ret) {
		return newError(t.c.dbc)
	}

	// Turn autocommit back on
	ret = C.SQLSetConnectAttrW(
		t.c.dbc,
		C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(uintptr(C.SQL_AUTOCOMMIT_ON)),
		0,
	)
	if !success(ret) {
		err := newError(t.c.dbc)
		t.c.t = nil
		t.c = nil
		return err
	}

	t.c.t = nil
	t.c = nil
	return nil
}
