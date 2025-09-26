import tkinter as tk
from tkinter import ttk, messagebox

from config.config_manager import DesktopConfig
from services.folder_monitor import FolderMonitor
from services.logger import Logger
from services.odoo_client import OdooConnectionDetails, OdooRPCClient, OdooRPCError
from services.uploader import OdooCsvUploader


class Tab4(ttk.Frame):
    """Folder monitoring tab."""

    def __init__(self, parent: ttk.Notebook, config: DesktopConfig):
        super().__init__(parent)
        self.config_store = config
        self.logger = Logger("FolderMonitor")
        self.monitor: FolderMonitor | None = None

        self.status_var = tk.StringVar(value="Status: Stopped")
        self._build_ui()

    def _build_ui(self) -> None:
        ttk.Label(self, text="Automatic Folder Monitoring", font=("Tahoma", 12, "bold")).pack(pady=10)
        ttk.Label(self, textvariable=self.status_var).pack(pady=5)

        button_frame = ttk.Frame(self)
        button_frame.pack(pady=10)

        self.start_btn = ttk.Button(button_frame, text="Start", command=self._start)
        self.stop_btn = ttk.Button(button_frame, text="Stop", command=self._stop, state=tk.DISABLED)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(self, text="Activity")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.log_text = tk.Text(log_frame, height=12, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    # ------------------------------------------------------------------ actions
    def _start(self) -> None:
        try:
            uploader = self._build_uploader()
        except Exception as exc:
            messagebox.showerror("Monitoring", f"Failed to initialize uploader:\n{exc}")
            return

        if not self.monitor:
            self.monitor = FolderMonitor(self.config_store, uploader, self.logger)
        else:
            self.monitor.uploader = uploader

        self.monitor.start()
        self.start_btn.configure(state=tk.DISABLED)
        self.stop_btn.configure(state=tk.NORMAL)
        self._append_log("Monitoring started")
        self.status_var.set("Status: Running")

    def _stop(self) -> None:
        if self.monitor:
            self.monitor.stop()
        self.start_btn.configure(state=tk.NORMAL)
        self.stop_btn.configure(state=tk.DISABLED)
        self._append_log("Monitoring stopped")
        self.status_var.set("Status: Stopped")

    def _build_uploader(self) -> OdooCsvUploader:
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
        try:
            client.ensure_authenticated()
        except OdooRPCError as exc:
            raise ValueError(f"Odoo authentication failed: {exc}")
        return OdooCsvUploader(client)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.configure(state=tk.DISABLED)
        self.log_text.see(tk.END)

    def refresh(self) -> None:
        # Nothing dynamic to refresh; placeholder for API parity
        pass
