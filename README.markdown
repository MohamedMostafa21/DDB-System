# Distributed Database System

## Overview

This project implements a distributed database system with a master-slave architecture, using Go for the backend (master and slave nodes) and Python (Tkinter) for the GUI. The system supports operations like `CREATE_DB`, `CREATE_TABLE`, `INSERT`, `UPDATE`, `DELETE`, `SEARCH`, and `DROP_DB`, executed on SQL Server instances. The master node (`192.168.149.137:8000`) coordinates operations and broadcasts to slaves (`:8001`, `:8002`), which can operate independently when the master is down. The GUI provides node selection, dynamic input fields, tooltips, and a status panel for node connectivity.

### Structure

```
+---------------------+---------------------+
|                     |                     |
|  Slave Node 1       |  Slave Node 2       |
|  (`slave.go`)       |  (`slave.go`)       |
|  - Read-only DB     |  - Read-only DB     |
|  - Listen to MQ     |  - Listen to MQ     |
|                     |                     |
+---------------------+---------------------+
              ↓             ↓
              |             |
              |             |
+---------------------------------+
|             Master Node         |
|             (`master.go`)       |
|  - DB Write Access             |
|  - Broadcast to Slaves         |
+---------------------------------+
```

- **Master Node (`master.go`)**: Coordinates operations, executes them on SQL Server, and broadcasts to slaves via TCP.
- **Slave Nodes (`slave.go`)**: Process operations independently on their SQL Server instances, supporting availability when the master is down.
- **GUI (`gui.py`)**: Tkinter-based interface for user interaction, with dynamic fields, tooltips, and node status indicators.
- **SQL Server**: Persistent storage for each node, using Windows Authentication for local connections.

## Prerequisites

- **Go**: Version 1.16+ (`go install`)
- **Python**: Version 3.8+ with `tkinter` (`pip install tk`)
- **SQL Server**: Express edition, running on `localhost` with Windows Authentication
- **OS**: Windows (for SQL Server compatibility)
- **Network**: Single laptop (`192.168.149.137`) or multiple laptops on a phone hotspot

## Setup (Single Laptop)

1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**:
   - Go: `go mod init distributed-db && go get github.com/denisenkom/go-mssqldb`
   - Python: Ensure `tkinter` is available (`python -m tkinter`)

3. **Start SQL Server**:
   ```bash
   net start MSSQLSERVER
   ```
   - Drop existing `mydb` in SSMS: `DROP DATABASE mydb`

4. **Open Firewall Ports**:
   ```bash
   netsh advfirewall firewall add rule name="Allow Port 8000" dir=in action=allow protocol=TCP localport=8000
   netsh advfirewall firewall add rule name="Allow Port 8001" dir=in action=allow protocol=TCP localport=8001
   netsh advfirewall firewall add rule name="Allow Port 8002" dir=in action=allow protocol=TCP localport=8002
   ```

5. **Run Nodes**:
   - **Master** (Terminal 1):
     ```bash
     go run master.go
     ```
   - **Slave 1** (Terminal 2):
     ```bash
     go run slave.go 8001
     ```
   - **Slave 2** (Terminal 3):
     ```bash
     go run slave.go 8002
     ```
   - **GUI** (Terminal 4):
     ```bash
     python gui.py
     ```

## Setup (Multi-Laptop)

1. **Update IPs**:
   - In `master.go`:
     ```go
     slaves := []string{"192.168.149.138:8001", "192.168.149.139:8002"}
     ```
   - In `gui.py`:
     ```python
     self.node_configs = {
         "Master": "192.168.149.137:8000",
         "Slave 1": "192.168.149.138:8001",
         "Slave 2": "192.168.149.139:8002"
     }
     ```
   - Find IPs: `ipconfig` on each laptop.

2. **Network**:
   - Connect to a phone hotspot.
   - Test: `ping 192.168.149.137` from slaves.
   - Open firewall ports (as above).

3. **Run Nodes**:
   - Master (`192.168.149.137`): `go run master.go`
   - Slave 1 (`192.168.149.138`): `go run slave.go 8001`
   - Slave 2 (`192.168.149.139`): `go run slave.go 8002`
   - GUI: Run on any laptop (`python gui.py`).

## Usage

1. **GUI Interface**:
   - **Node Type**: Select “Master”, “Slave 1”, or “Slave 2”.
   - **Operation**: Choose from `CREATE_DB`, `CREATE_TABLE`, etc.
   - **Fields**: Enter database name, table name, data (JSON), condition (JSON). Hover for tooltips.
   - **Execute**: Click “Execute” to send operation.
   - **Status Panel**: Shows node connectivity (green: connected, red: disconnected).
   - **Results**: Displays operation output (e.g., `SEARCH` results in JSON).

2. **Example Workflow**:
   - **Create Database**: Node: Master, Operation: `CREATE_DB`, Database: `mydb`
   - **Create Table**: Operation: `CREATE_TABLE`, Database: `mydb`, Table: `users`, Data: `{"id": "int", "name": "string"}`
   - **Insert**: Operation: `INSERT`, Table: `users`, Data: `{"id": 1, "name": "Alice"}`
   - **Search**: Operation: `SEARCH`, Table: `users`, Condition: `{"id": 1}` (Result: `[{"id": 1, "name": "Alice"}]`)
   - **Test Master Down**: Stop master, run `INSERT` or `SEARCH` on Slave 1.

## Troubleshooting

- **SEARCH Fails**: “table users does not exist”
  - Check SSMS: `SELECT * FROM [mydb].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'users'`
  - Fix: Run `CREATE_TABLE` again or drop/recreate `mydb`.
- **Node Disconnected**: Verify nodes are running (`netstat -an | findstr 8000`). Check firewall.
- **SQL Server Errors**: Ensure `MSSQLSERVER` is running. Check logs for detailed errors.
- **GUI Issues**: Restart GUI if fields are incorrect. Verify Python version.

## Notes

- Slaves operate independently when the master is down, but data may diverge.
- For syncing slave data when the master reconnects, re-run operations on the master.
- Logs are written to stdout for debugging (e.g., SQL queries, operation results).