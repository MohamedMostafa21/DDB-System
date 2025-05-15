import tkinter as tk
from tkinter import ttk, messagebox
import json
import socket
import threading
import time

# ToolTip provides hover-over help text for GUI widgets
class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event=None):
        # Display tooltip at widget's position
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tooltip = tk.Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{x}+{y}")
        label = tk.Label(self.tooltip, text=self.text, background="#FFFFDD", relief="solid", borderwidth=1, font=("Arial", 10))
        label.pack()

    def hide_tooltip(self, event=None):
        # Remove tooltip
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

# DatabaseGUI manages the Tkinter interface
class DatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed Database System")
        self.root.geometry("900x700")
        self.root.configure(bg="#2E2E2E")

        # GUI state variables
        self.node_type = tk.StringVar(value="Master")
        self.node_ip_port = tk.StringVar(value="192.168.149.137:8000")
        self.operation = tk.StringVar(value="CREATE_DB")
        self.database = tk.StringVar()
        self.table = tk.StringVar()
        self.data = tk.StringVar()
        self.condition = tk.StringVar()

        # Node configurations
        self.node_configs = {
            "Master": "192.168.149.137:8000",
            "Slave 1": "192.168.149.137:8001",
            "Slave 2": "192.168.149.137:8002"
            # For multi-laptop: update with colleague IPs
        }

        # Status indicators for node connectivity
        self.status_labels = {}
        self.status_colors = {}

        # Configure Tkinter styles
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TButton", padding=8, relief="flat", background="#2196F3", foreground="white", font=("Arial", 10, "bold"))
        style.map("TButton", background=[("active", "#1976D2")])
        style.configure("TLabel", background="#2E2E2E", foreground="#FFFFFF", font=("Arial", 11))
        style.configure("TEntry", padding=6, fieldbackground="#424242", foreground="#FFFFFF")
        style.configure("TCombobox", padding=6, fieldbackground="#424242", foreground="#FFFFFF")
        style.configure("TRadiobutton", background="#2E2E2E", foreground="#FFFFFF", font=("Arial", 10))

        # Header
        header = tk.Label(root, text="Distributed Database System", bg="#2E2E2E", fg="#2196F3", font=("Arial", 16, "bold"))
        header.pack(pady=10)

        # Status panel for node connectivity
        status_frame = tk.Frame(root, bg="#2E2E2E")
        status_frame.pack(pady=5, fill="x", padx=20)
        for node in self.node_configs:
            tk.Label(status_frame, text=f"{node}:", bg="#2E2E2E", fg="#FFFFFF", font=("Arial", 10)).pack(side="left", padx=5)
            status_label = tk.Label(status_frame, text="Checking...", bg="#2E2E2E", fg="#FF9800", font=("Arial", 10))
            status_label.pack(side="left", padx=5)
            self.status_labels[node] = status_label
            self.status_colors[node] = "#FF9800"

        # Node selection dropdown
        node_frame = tk.Frame(root, bg="#2E2E2E")
        node_frame.pack(pady=10, fill="x", padx=20)
        tk.Label(node_frame, text="Node Type:", bg="#2E2E2E", fg="#FFFFFF").pack(side="left")
        node_combo = ttk.Combobox(node_frame, textvariable=self.node_type, values=["Master", "Slave 1", "Slave 2"], state="readonly")
        node_combo.pack(side="left", padx=5)
        node_combo.bind("<<ComboboxSelected>>", self.update_node_ip)
        tk.Label(node_frame, text="IP:Port:", bg="#2E2E2E", fg="#FFFFFF").pack(side="left", padx=5)
        ttk.Entry(node_frame, textvariable=self.node_ip_port, width=20, state="readonly").pack(side="left", padx=5)

        # Operation selection radio buttons
        op_frame = tk.Frame(root, bg="#2E2E2E")
        op_frame.pack(pady=10, fill="x", padx=20)
        tk.Label(op_frame, text="Operation:", bg="#2E2E2E", fg="#FFFFFF").pack(anchor="w")
        operations = ["CREATE_DB", "CREATE_TABLE", "INSERT", "UPDATE", "DELETE", "SEARCH"]
        if self.node_type.get() == "Master":
            operations.append("DROP_DB")
        for op in operations:
            ttk.Radiobutton(op_frame, text=op.replace("_", " "), value=op, variable=self.operation, command=self.update_fields).pack(anchor="w")

        # Input fields frame
        self.input_frame = tk.Frame(root, bg="#2E2E2E")
        self.input_frame.pack(pady=10, fill="x", padx=20)

        # Input fields with tooltips
        self.db_label = tk.Label(self.input_frame, text="Database Name:", bg="#2E2E2E", fg="#FFFFFF")
        self.db_entry = ttk.Entry(self.input_frame, textvariable=self.database)
        ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
        self.table_label = tk.Label(self.input_frame, text="Table Name:", bg="#2E2E2E", fg="#FFFFFF")
        self.table_entry = ttk.Entry(self.input_frame, textvariable=self.table)
        ToolTip(self.table_entry, "Name of the table, e.g., 'users'")
        self.data_label = tk.Label(self.input_frame, text="Data (JSON):", bg="#2E2E2E", fg="#FFFFFF")
        self.data_entry = ttk.Entry(self.input_frame, textvariable=self.data)
        self.cond_label = tk.Label(self.input_frame, text="Condition (JSON):", bg="#2E2E2E", fg="#FFFFFF")
        self.cond_entry = ttk.Entry(self.input_frame, textvariable=self.condition)

        # Initialize fields
        self.update_fields()

        # Execute button
        ttk.Button(root, text="Execute", command=self.execute).pack(pady=20)

        # Status label for operation feedback
        self.status = tk.Label(root, text="", bg="#2E2E2E", fg="#4CAF50", font=("Arial", 10))
        self.status.pack()

        # Result display
        self.result = tk.Text(root, height=12, width=80, bg="#424242", fg="#FFFFFF", insertbackground="white")
        self.result.pack(pady=10, padx=20)

        # Start node status checker
        self.running = True
        threading.Thread(target=self.check_node_status, daemon=True).start()

    def check_node_status(self):
        # Periodically check node connectivity
        while self.running:
            for node, addr in self.node_configs.items():
                host, port = addr.split(":")
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(2)
                        s.connect((host, int(port)))
                        self.status_colors[node] = "#4CAF50"  # Green
                        self.root.after(0, lambda n=node: self.status_labels[n].config(text="Connected", fg="#4CAF50"))
                except:
                    self.status_colors[node] = "#F44336"  # Red
                    self.root.after(0, lambda n=node: self.status_labels[n].config(text="Disconnected", fg="#F44336"))
            time.sleep(5)

    def update_node_ip(self, event=None):
        # Update IP:Port based on node selection
        self.node_ip_port.set(self.node_configs[self.node_type.get()])
        self.update_fields()  # Update operations based on node type

    def update_fields(self):
        # Dynamically show/hide fields based on operation
        for widget in [self.db_label, self.db_entry, self.table_label, self.table_entry,
                      self.data_label, self.data_entry, self.cond_label, self.cond_entry]:
            widget.pack_forget()

        self.db_entry.delete(0, tk.END)
        self.table_entry.delete(0, tk.END)
        self.data_entry.delete(0, tk.END)
        self.cond_entry.delete(0, tk.END)

        op = self.operation.get()
        if op == "CREATE_DB":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database to create, e.g., 'mydb'")
        elif op == "DROP_DB":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database to drop, e.g., 'mydb'")
        elif op == "CREATE_TABLE":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
            self.table_label.pack(anchor="w")
            self.table_entry.pack(fill="x", pady=2)
            self.table_entry.insert(0, "users")
            ToolTip(self.table_entry, "Name of the table to create, e.g., 'users'")
            self.data_label.pack(anchor="w")
            self.data_entry.pack(fill="x", pady=2)
            self.data_entry.insert(0, '{"id": "int", "name": "string"}')
            ToolTip(self.data_entry, 'Column definitions as JSON, e.g., {"id": "int", "name": "string"}')
        elif op == "INSERT":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
            self.table_label.pack(anchor="w")
            self.table_entry.pack(fill="x", pady=2)
            self.table_entry.insert(0, "users")
            ToolTip(self.table_entry, "Name of the table, e.g., 'users'")
            self.data_label.pack(anchor="w")
            self.data_entry.pack(fill="x", pady=2)
            self.data_entry.insert(0, '{"id": 1, "name": "Alice"}')
            ToolTip(self.data_entry, 'Row data as JSON, e.g., {"id": 1, "name": "Alice"}')
        elif op == "UPDATE":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
            self.table_label.pack(anchor="w")
            self.table_entry.pack(fill="x", pady=2)
            self.table_entry.insert(0, "users")
            ToolTip(self.table_entry, "Name of the table, e.g., 'users'")
            self.data_label.pack(anchor="w")
            self.data_entry.pack(fill="x", pady=2)
            self.data_entry.insert(0, '{"name": "Alicia"}')
            ToolTip(self.data_entry, 'Values to update as JSON, e.g., {"name": "Alicia"}')
            self.cond_label.pack(anchor="w")
            self.cond_entry.pack(fill="x", pady=2)
            self.cond_entry.insert(0, '{"id": 1}')
            ToolTip(self.cond_entry, 'Condition to select rows, e.g., {"id": 1}')
        elif op == "DELETE":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
            self.table_label.pack(anchor="w")
            self.table_entry.pack(fill="x", pady=2)
            self.table_entry.insert(0, "users")
            ToolTip(self.table_entry, "Name of the table, e.g., 'users'")
            self.cond_label.pack(anchor="w")
            self.cond_entry.pack(fill="x", pady=2)
            self.cond_entry.insert(0, '{"id": 1}')
            ToolTip(self.cond_entry, 'Condition to select rows to delete, e.g., {"id": 1}')
        elif op == "SEARCH":
            self.db_label.pack(anchor="w")
            self.db_entry.pack(fill="x", pady=2)
            self.db_entry.insert(0, "mydb")
            ToolTip(self.db_entry, "Name of the database, e.g., 'mydb'")
            self.table_label.pack(anchor="w")
            self.table_entry.pack(fill="x", pady=2)
            self.table_entry.insert(0, "users")
            ToolTip(self.table_entry, "Name of the table, e.g., 'users'")
            self.cond_label.pack(anchor="w")
            self.cond_entry.pack(fill="x", pady=2)
            self.cond_entry.insert(0, '{"id": 1}')
            ToolTip(self.cond_entry, 'Filter rows, e.g., {"id": 1} for specific ID, or {} for all rows')

    def execute(self):
        # Trigger operation execution in a separate thread
        self.status.config(text="Processing...", fg="#FF9800")
        threading.Thread(target=self._execute_operation, daemon=True).start()

    def _execute_operation(self):
        # Execute operation by sending to selected node
        addr = self.node_ip_port.get()
        op = {
            "Type": self.operation.get(),
            "Database": self.database.get(),
            "Table": self.table.get(),
            "Data": {},
            "Condition": {}
        }
        try:
            data_str = self.data.get() or "{}"
            op["Data"] = json.loads(data_str)
            cond_str = self.condition.get() or "{}"
            op["Condition"] = json.loads(cond_str)
        except json.JSONDecodeError as e:
            self.root.after(0, lambda: messagebox.showerror("Error", f"Invalid JSON: {e}"))
            self.root.after(0, lambda: self.status.config(text="Error: Invalid JSON", fg="#F44336"))
            return

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                host, port = addr.split(":")
                s.settimeout(10)
                s.connect((host, int(port)))
                s.sendall(json.dumps(op).encode())
                data = s.recv(4096).decode()
                self.root.after(0, lambda: self.result.delete(1.0, tk.END))
                if op["Type"] == "SEARCH":
                    try:
                        results = json.loads(data)
                        self.root.after(0, lambda: self.result.insert(tk.END, json.dumps(results, indent=2)))
                        self.root.after(0, lambda: self.status.config(text="SEARCH completed", fg="#4CAF50"))
                    except json.JSONDecodeError:
                        self.root.after(0, lambda: self.result.insert(tk.END, data))
                        self.root.after(0, lambda: self.status.config(text="Error: Invalid SEARCH response", fg="#F44336"))
                else:
                    self.root.after(0, lambda: self.result.insert(tk.END, data))
                    if data.startswith("Error"):
                        self.root.after(0, lambda: self.status.config(text="Operation failed", fg="#F44336"))
                    else:
                        self.root.after(0, lambda: self.status.config(text=f"Operation {op['Type']} completed", fg="#4CAF50"))
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to execute operation: {e}"))
            self.root.after(0, lambda: self.status.config(text=f"Error: {e}", fg="#F44336"))
            self.root.after(0, lambda: self.result.delete(1.0, tk.END))
            self.root.after(0, lambda: self.result.insert(tk.END, f"Error: {e}"))

    def cleanup(self):
        # Stop status checker on window close
        self.running = False

if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseGUI(root)
    root.protocol("WM_DELETE_WINDOW", lambda: [app.cleanup(), root.destroy()])
    root.mainloop()