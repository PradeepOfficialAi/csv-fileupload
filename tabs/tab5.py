import tkinter as tk
from tkinter import ttk, messagebox
from typing import Dict, List

from config.config_manager import DesktopConfig
from services.odoo_client import OdooConnectionDetails, OdooRPCClient, OdooRPCError
from services.uploader import OdooCsvUploader


class Tab5(ttk.Frame):
    """Database helper tab for inspecting uploads in Odoo."""

    def __init__(self, parent: ttk.Notebook, config: DesktopConfig):
        super().__init__(parent)
        self.config_store = config
        self.profiles: List[Dict[str, object]] = []
        self._build_ui()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        ttk.Label(self, text="Database Tools", font=("Tahoma", 12, "bold")).pack(pady=10)

        frame = ttk.Frame(self)
        frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame, text="Upload Type:").pack(side=tk.LEFT, padx=5)
        self.profile_var = tk.StringVar()
        self.profile_combo = ttk.Combobox(frame, textvariable=self.profile_var, state="readonly", width=40)
        self.profile_combo.pack(side=tk.LEFT, padx=5)
        self.profile_combo.bind("<<ComboboxSelected>>", lambda _e: self._load_uploads())

        ttk.Button(frame, text="Refresh Types", command=self._load_profiles).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Refresh Records", command=self._load_uploads).pack(side=tk.LEFT, padx=5)

        columns = ("ID", "Name", "State", "Rows", "Duplicates", "Errors", "Created")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=15)
        for col in columns:
            width = 80 if col in {"ID", "Rows", "Duplicates", "Errors"} else 150
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # ------------------------------------------------------------------ data helpers
    def _get_client(self) -> OdooRPCClient:
        odoo = self.config_store.get_section("odoo")
        missing = [key for key in ("url", "database", "username", "password") if not odoo.get(key)]
        if missing:
            raise ValueError("Missing Odoo configuration: " + ", ".join(missing))
        details = OdooConnectionDetails(
            url=odoo["url"],
            database=odoo["database"],
            username=odoo["username"],
            password=odoo["password"],
        )
        client = OdooRPCClient(details)
        client.ensure_authenticated()
        return client

    def _load_profiles(self) -> None:
        try:
            client = self._get_client()
            uploader = OdooCsvUploader(client)
            self.profiles = uploader.list_profiles()
            display = [f"{item['name']} ({item['code']})" for item in self.profiles]
            self.profile_combo.configure(values=display)
            if display:
                self.profile_combo.set(display[0])
                self._load_uploads()
        except (ValueError, OdooRPCError) as exc:
            messagebox.showerror("Profiles", f"Failed to load profiles: {exc}")

    def _selected_profile(self) -> Dict[str, object] | None:
        selection = self.profile_combo.get()
        for profile in self.profiles:
            label = f"{profile['name']} ({profile['code']})"
            if selection == label:
                return profile
        return None

    def _load_uploads(self) -> None:
        self.tree.delete(*self.tree.get_children())
        try:
            client = self._get_client()
        except (ValueError, OdooRPCError) as exc:
            messagebox.showerror("Uploads", f"Failed to connect: {exc}")
            return

        domain = []
        profile = self._selected_profile()
        if profile:
            domain = [["type_id", "=", profile["id"]]]

        try:
            records = client.search_read(
                "csv.upload",
                domain=domain,
                fields=["name", "state", "row_count", "duplicate_count", "error_count", "create_date"],
                limit=100,
            )
            for record in records:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        record.get("id"),
                        record.get("name"),
                        record.get("state"),
                        record.get("row_count"),
                        record.get("duplicate_count"),
                        record.get("error_count"),
                        record.get("create_date"),
                    ),
                )
        except OdooRPCError as exc:
            messagebox.showerror("Uploads", f"Failed to load records: {exc}")

    def refresh(self) -> None:
        # Reload profiles to pick up new types
        self._load_profiles()
