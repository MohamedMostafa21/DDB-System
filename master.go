package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type Operation struct {
	Type      string
	Database  string
	Table     string
	Data      map[string]interface{}
	Condition map[string]interface{}
}

type Master struct {
	mutex    sync.RWMutex
	slaves   []string
	listener net.Listener
	db       *sql.DB
}

func NewMaster(slaveAddrs []string) *Master {
	return &Master{
		slaves: slaveAddrs,
	}
}

func (m *Master) connectToSQLServer() error {
	connString := "server=localhost;database=master;trusted_connection=true"
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return fmt.Errorf("failed to connect to SQL Server: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping SQL Server: %v", err)
	}
	m.db = db
	log.Println("Connected to SQL Server successfully")
	return nil
}

func (m *Master) startTCPServer(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %v", err)
	}
	m.listener = listener
	log.Printf("Master listening on :%s", port)
	go m.acceptConnections()
	return nil
}

func (m *Master) acceptConnections() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go m.handleClient(conn)
	}
}

func (m *Master) handleClient(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	var op Operation
	if err := decoder.Decode(&op); err != nil {
		log.Printf("Failed to decode operation: %v", err)
		conn.Write([]byte(fmt.Sprintf("Error: Failed to decode operation: %v", err)))
		return
	}
	log.Printf("Received operation: %s, Database: %s, Table: %s, Data: %v, Condition: %v",
		op.Type, op.Database, op.Table, op.Data, op.Condition)
	result, err := m.processOperation(op)
	if err != nil {
		log.Printf("Operation %s failed: %v", op.Type, err)
		conn.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	if op.Type == "SEARCH" {
		data, err := json.Marshal(result)
		if err != nil {
			log.Printf("Failed to marshal SEARCH result: %v", err)
			conn.Write([]byte(fmt.Sprintf("Error: Failed to marshal SEARCH result: %v", err)))
			return
		}
		_, err = conn.Write(data)
		if err != nil {
			log.Printf("Failed to send SEARCH result: %v", err)
		} else {
			log.Printf("Sent SEARCH result with %d records", len(result))
		}
	} else {
		conn.Write([]byte("Operation completed successfully"))
	}
	log.Printf("Processed operation: %s successfully", op.Type)
	m.broadcastOperation(op)
}

func (m *Master) processOperation(op Operation) ([]map[string]interface{}, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if op.Database == "" && op.Type != "CREATE_DB" {
		return nil, fmt.Errorf("database name is required for %s", op.Type)
	}

	switch op.Type {
	case "CREATE_DB":
		if op.Database == "" {
			return nil, fmt.Errorf("database name is required for CREATE_DB")
		}
		_, err := m.db.Exec(fmt.Sprintf("CREATE DATABASE [%s]", op.Database))
		if err != nil {
			return nil, fmt.Errorf("failed to create database in SQL Server: %v", err)
		}
		log.Printf("Executed SQL: CREATE DATABASE [%s]", op.Database)

	case "CREATE_TABLE":
		if op.Table == "" {
			return nil, fmt.Errorf("table name is required for CREATE_TABLE")
		}
		columns := make([]string, 0)
		colDefs := make([]string, 0)
		for col, typ := range op.Data {
			columns = append(columns, col)
			sqlType := "NVARCHAR(255)"
			if typ == "int" {
				sqlType = "INT"
			}
			colDefs = append(colDefs, fmt.Sprintf("[%s] %s", col, sqlType))
		}
		query := fmt.Sprintf("CREATE TABLE [%s].[dbo].[%s] (%s)", op.Database, op.Table, strings.Join(colDefs, ", "))
		_, err := m.db.Exec(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create table in SQL Server: %v", err)
		}
		log.Printf("Executed SQL: %s", query)

	case "INSERT":
		if op.Table == "" {
			return nil, fmt.Errorf("table name is required for INSERT")
		}
		if len(op.Data) == 0 {
			return nil, fmt.Errorf("data is required for INSERT")
		}
		cols := make([]string, 0)
		vals := make([]interface{}, 0)
		placeholders := make([]string, 0)
		for i, col := range sortedKeys(op.Data) {
			cols = append(cols, fmt.Sprintf("[%s]", col))
			vals = append(vals, op.Data[col])
			placeholders = append(placeholders, fmt.Sprintf("@p%d", i+1))
		}
		query := fmt.Sprintf("INSERT INTO [%s].[dbo].[%s] (%s) VALUES (%s)",
			op.Database, op.Table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		_, err := m.db.Exec(query, vals...)
		if err != nil {
			return nil, fmt.Errorf("failed to insert into SQL Server: %v", err)
		}
		log.Printf("Executed SQL: %s with values %v", query, vals)

	case "UPDATE":
		if op.Table == "" {
			return nil, fmt.Errorf("table name is required for UPDATE")
		}
		if len(op.Data) == 0 || len(op.Condition) == 0 {
			return nil, fmt.Errorf("data and condition are required for UPDATE")
		}
		setParts := make([]string, 0)
		vals := make([]interface{}, 0)
		paramCount := 1
		for col := range op.Data {
			setParts = append(setParts, fmt.Sprintf("[%s] = @p%d", col, paramCount))
			vals = append(vals, op.Data[col])
			paramCount++
		}
		condParts := make([]string, 0)
		for col := range op.Condition {
			condParts = append(condParts, fmt.Sprintf("[%s] = @p%d", col, paramCount))
			vals = append(vals, op.Condition[col])
			paramCount++
		}
		query := fmt.Sprintf("UPDATE [%s].[dbo].[%s] SET %s WHERE %s",
			op.Database, op.Table, strings.Join(setParts, ", "), strings.Join(condParts, " AND "))
		_, err := m.db.Exec(query, vals...)
		if err != nil {
			return nil, fmt.Errorf("failed to update SQL Server: %v", err)
		}
		log.Printf("Executed SQL: %s with values %v", query, vals)

	case "DELETE":
		if op.Table == "" {
			return nil, fmt.Errorf("table name is required for DELETE")
		}
		if len(op.Condition) == 0 {
			return nil, fmt.Errorf("condition is required for DELETE")
		}
		condParts := make([]string, 0)
		vals := make([]interface{}, 0)
		for i, col := range sortedKeys(op.Condition) {
			condParts = append(condParts, fmt.Sprintf("[%s] = @p%d", col, i+1))
			vals = append(vals, op.Condition[col])
		}
		query := fmt.Sprintf("DELETE FROM [%s].[dbo].[%s] WHERE %s",
			op.Database, op.Table, strings.Join(condParts, " AND "))
		_, err := m.db.Exec(query, vals...)
		if err != nil {
			return nil, fmt.Errorf("failed to delete from SQL Server: %v", err)
		}
		log.Printf("Executed SQL: %s with values %v", query, vals)

	case "SEARCH":
		if op.Table == "" {
			return nil, fmt.Errorf("table name is required for SEARCH")
		}
		columns, err := m.getTableColumns(op.Database, op.Table)
		if err != nil {
			return nil, err
		}
		query := fmt.Sprintf("SELECT %s FROM [%s].[dbo].[%s]", strings.Join(columns, ", "), op.Database, op.Table)
		var condParts []string
		var vals []interface{}
		if len(op.Condition) > 0 {
			for i, col := range sortedKeys(op.Condition) {
				condParts = append(condParts, fmt.Sprintf("[%s] = @p%d", col, i+1))
				vals = append(vals, op.Condition[col])
			}
			query += " WHERE " + strings.Join(condParts, " AND ")
		}
		log.Printf("Executing SEARCH query: %s with values %v", query, vals)
		rows, err := m.db.Query(query, vals...)
		if err != nil {
			return nil, fmt.Errorf("failed to search SQL Server: %v", err)
		}
		defer rows.Close()
		results := []map[string]interface{}{}
		for rows.Next() {
			record := make(map[string]interface{})
			scanVals := make([]interface{}, len(columns))
			for i := range scanVals {
				var val interface{}
				scanVals[i] = &val
			}
			if err := rows.Scan(scanVals...); err != nil {
				log.Printf("Failed to scan row: %v", err)
				continue
			}
			for i, col := range columns {
				record[col] = *(scanVals[i].(*interface{}))
			}
			results = append(results, record)
		}
		log.Printf("SEARCH returned %d records", len(results))
		return results, nil

	case "DROP_DB":
		if op.Database == "" {
			return nil, fmt.Errorf("database name is required for DROP_DB")
		}
		_, err := m.db.Exec(fmt.Sprintf("IF EXISTS (SELECT * FROM sys.databases WHERE name = '%s') DROP DATABASE [%s]", op.Database, op.Database))
		if err != nil {
			return nil, fmt.Errorf("failed to drop database in SQL Server: %v", err)
		}
		log.Printf("Executed SQL: DROP DATABASE [%s]", op.Database)
	default:
		return nil, fmt.Errorf("unknown operation type %s", op.Type)
	}
	return nil, nil
}

func (m *Master) getTableColumns(dbName, tableName string) ([]string, error) {
	// Check if database exists
	var dbCount int
	err := m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM sys.databases WHERE name = '%s'", dbName)).Scan(&dbCount)
	if err != nil {
		return nil, fmt.Errorf("failed to check database existence: %v", err)
	}
	if dbCount == 0 {
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	// Check table existence in dbo schema
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM [%s].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '%s'", dbName, tableName)
	log.Printf("Executing table columns query: %s", query)
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %v", err)
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			log.Printf("Failed to scan column: %v", err)
			continue
		}
		columns = append(columns, colName)
	}
	if len(columns) == 0 {
		// Fallback: Check sys.tables
		var tableCount int
		fallbackQuery := fmt.Sprintf("SELECT COUNT(*) FROM [%s].sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('dbo')", dbName, tableName)
		log.Printf("Executing fallback table check: %s", fallbackQuery)
		err = m.db.QueryRow(fallbackQuery).Scan(&tableCount)
		if err != nil {
			return nil, fmt.Errorf("failed to check table existence: %v", err)
		}
		if tableCount == 0 {
			return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
		}
		return nil, fmt.Errorf("no columns found for table %s in database %s", tableName, dbName)
	}
	log.Printf("Found columns for table %s: %v", tableName, columns)
	return columns, nil
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (m *Master) broadcastOperation(op Operation) {
	data, err := json.Marshal(op)
	if err != nil {
		log.Printf("Failed to marshal operation: %v", err)
		return
	}
	for _, slaveAddr := range m.slaves {
		conn, err := net.DialTimeout("tcp", slaveAddr, 5*time.Second)
		if err != nil {
			log.Printf("Failed to connect to slave %s: %v", slaveAddr, err)
			continue
		}
		_, err = conn.Write(data)
		if err != nil {
			log.Printf("Failed to send operation %s to slave %s: %v", op.Type, slaveAddr, err)
		} else {
			log.Printf("Sent operation %s to slave %s", op.Type, slaveAddr)
		}
		conn.Close()
	}
}

func main() {
	// For single laptop testing: master and two slaves on 192.168.149.137
	// For multi-laptop: update with colleague IPs (e.g., "192.168.149.138:8001", "192.168.149.139:8002")
	slaves := []string{"192.168.149.137:8001", "192.168.149.137:8002"}
	master := NewMaster(slaves)
	if err := master.connectToSQLServer(); err != nil {
		log.Fatal(err)
	}
	if err := master.startTCPServer("8000"); err != nil {
		log.Fatal(err)
	}
	select {}
}
