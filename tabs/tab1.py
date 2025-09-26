import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Any, Dict, List

from config.config_manager import DesktopConfig
from services.odoo_client import OdooConnectionDetails, OdooRPCClient, OdooRPCError


class Tab1(ttk.Frame):
    """Settings tab: manage directories and Odoo connection details."""

    PATH_FIELDS = [
        ("path1", "Select Source Path 1"),
        ("path2", "Select Source Path 2"),
        ("move_path1", "Select Archive Path 1"),
        ("move_path2", "Select Archive Path 2"),
        ("source_pdf", "Select Source PDF Path"),
        ("move_pdf", "Select PDF Archive Path"),
    ]

    def __init__(self, parent: ttk.Notebook, config: DesktopConfig):
        super().__init__(parent)
        self.config_store = config

        self.path_vars: dict[str, tk.StringVar] = {}
        self.odoo_vars: dict[str, tk.StringVar] = {}
        self.monitor_vars: dict[str, tk.Variable] = {}

        self._build_ui()
        self._load_values()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        title = ttk.Label(self, text="Path and Odoo Settings", font=("Tahoma", 12, "bold"))
        title.grid(row=0, column=0, columnspan=3, pady=(10, 5))

        self._build_path_section()
        self._build_odoo_section()
        self._build_monitor_section()
        self._build_buttons()

    def _build_path_section(self) -> None:
        ttk.Label(self, text="Paths", font=("Tahoma", 10, "bold")).grid(row=1, column=0, columnspan=3, pady=(10, 5))

        for idx, (key, label) in enumerate(self.PATH_FIELDS, start=2):
            ttk.Label(self, text=f"{idx-1}. {label}:").grid(row=idx, column=0, sticky=tk.E, padx=5, pady=2)
            var = tk.StringVar()
            self.path_vars[key] = var
            ttk.Entry(self, textvariable=var, width=45).grid(row=idx, column=1, padx=5, pady=2)
            ttk.Button(
                self,
                text="Browse",
                command=lambda k=key: self._browse_directory(self.path_vars[k]),
            ).grid(row=idx, column=2, padx=5, pady=2)

    def _build_odoo_section(self) -> None:
        base_row = len(self.PATH_FIELDS) + 3
        ttk.Label(self, text="Odoo Connection", font=("Tahoma", 10, "bold")).grid(
            row=base_row,
            column=0,
            columnspan=3,
            pady=(15, 5),
        )

        for offset, (key, label) in enumerate(
            [
                ("url", "Odoo URL"),
                ("database", "Database"),
                ("username", "Username"),
                ("password", "Password"),
            ],
            start=base_row + 1,
        ):
            ttk.Label(self, text=label + ":").grid(row=offset, column=0, sticky=tk.E, padx=5, pady=2)
            var = tk.StringVar()
            self.odoo_vars[key] = var
            show = "*" if key == "password" else None
            ttk.Entry(self, textvariable=var, width=35, show=show).grid(row=offset, column=1, padx=5, pady=2)

    def _build_monitor_section(self) -> None:
        base = len(self.PATH_FIELDS) + 8
        ttk.Label(self, text="Monitoring", font=("Tahoma", 10, "bold")).grid(
            row=base,
            column=0,
            columnspan=3,
            pady=(15, 5),
        )

        interval_var = tk.IntVar()
        delete_var = tk.BooleanVar()
        auto_upload_var = tk.BooleanVar()
        self.monitor_vars = {
            "interval": interval_var,
            "delete_source": delete_var,
            "auto_upload": auto_upload_var,
        }

        ttk.Label(self, text="Polling interval (seconds):").grid(row=base + 1, column=0, sticky=tk.E, padx=5, pady=2)
        ttk.Spinbox(self, from_=5, to=3600, increment=5, textvariable=interval_var, width=10).grid(
            row=base + 1,
            column=1,
            sticky=tk.W,
            padx=5,
            pady=2,
        )

        ttk.Checkbutton(self, text="Delete source file", variable=delete_var).grid(
            row=base + 2,
            column=1,
            sticky=tk.W,
            padx=5,
        )
        ttk.Checkbutton(self, text="Auto upload rows", variable=auto_upload_var).grid(
            row=base + 3,
            column=1,
            sticky=tk.W,
            padx=5,
        )

    def _build_buttons(self) -> None:
        row = len(self.PATH_FIELDS) + 12
        button_frame = ttk.Frame(self)
        button_frame.grid(row=row, column=0, columnspan=3, pady=15)

        ttk.Button(button_frame, text="Save", command=self._save_settings).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Reset", command=self._reset_settings).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT, padx=5)

    # ------------------------------------------------------------------ actions
    def _browse_directory(self, var: tk.StringVar) -> None:
        selected = filedialog.askdirectory()
        if selected:
            var.set(selected)

    def _load_values(self) -> None:
        paths = self.config_store.get_section("paths")
        for key, var in self.path_vars.items():
            var.set(paths.get(key, ""))

        odoo = self.config_store.get_section("odoo")
        for key, var in self.odoo_vars.items():
            var.set(odoo.get(key, ""))

        monitoring = self.config_store.get_section("monitoring")
        self.monitor_vars["interval"].set(monitoring.get("interval", 30))
        self.monitor_vars["delete_source"].set(bool(monitoring.get("delete_source", False)))
        self.monitor_vars["auto_upload"].set(bool(monitoring.get("auto_upload", True)))

    def _collect_path_values(self) -> Dict[str, str]:
        return {key: var.get().strip() for key, var in self.path_vars.items()}

    def _collect_odoo_values(self) -> Dict[str, str]:
        return {key: var.get().strip() for key, var in self.odoo_vars.items()}

    def _collect_monitor_values(self) -> Dict[str, Any]:
        return {
            "interval": int(self.monitor_vars["interval"].get() or 30),
            "delete_source": bool(self.monitor_vars["delete_source"].get()),
            "auto_upload": bool(self.monitor_vars["auto_upload"].get()),
        }

    def _save_settings(self) -> None:
        self.config_store.set_section("paths", self._collect_path_values())
        self.config_store.set_section("odoo", self._collect_odoo_values())
        self.config_store.set_section("monitoring", self._collect_monitor_values())
        messagebox.showinfo("Settings", "Settings saved successfully")

    def _reset_settings(self) -> None:
        if not messagebox.askyesno("Reset", "Reset settings to defaults?"):
            return
        self.config_store = DesktopConfig()
        self._load_values()

    def _test_connection(self) -> None:
        values = self._collect_odoo_values()
        if not all(values.values()):
            messagebox.showwarning("Connection", "Please fill all Odoo connection fields")
            return
        try:
            details = OdooConnectionDetails(
                url=values["url"],
                database=values["database"],
                username=values["username"],
                password=values["password"],
            )
            client = OdooRPCClient(details)
            uid = client.authenticate()
            messagebox.showinfo("Connection", f"Connection successful (uid {uid})")
        except OdooRPCError as exc:
            messagebox.showerror("Connection", f"Failed: {exc}")

    def refresh(self) -> None:
        self._load_values()
