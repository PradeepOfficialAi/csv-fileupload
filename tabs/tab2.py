import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
import datetime
import re
from typing import Optional, Tuple

from services.logger import Logger


class Tab2(ttk.Frame):
    """Log viewer for the desktop client."""

    def __init__(self, parent: ttk.Notebook, log_dir: Path | str = "logs"):
        super().__init__(parent)
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_log_file: Optional[Path] = None
        self.logger = Logger("Tab2")

        self._build_ui()
        self._load_log_files()

    # ------------------------------------------------------------------ UI setup
    def _build_ui(self) -> None:
        ttk.Label(self, text="Log Viewer", font=("Tahoma", 12, "bold")).pack(pady=10)
        self._build_controls()
        self._build_search()
        self._build_tree()
        self._build_status()

    def _build_controls(self) -> None:
        frame = ttk.Frame(self)
        frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame, text="Select Date:").pack(side=tk.LEFT, padx=5)
        self.date_var = tk.StringVar()
        self.date_combo = ttk.Combobox(frame, textvariable=self.date_var, state="readonly", width=18)
        self.date_combo.pack(side=tk.LEFT, padx=5)
        self.date_combo.bind("<<ComboboxSelected>>", self._on_date_selected)

        ttk.Button(frame, text="Refresh", command=self._load_log_files).pack(side=tk.RIGHT, padx=5)
        ttk.Button(frame, text="Export", command=self._export_csv).pack(side=tk.RIGHT, padx=5)

    def _build_search(self) -> None:
        frame = ttk.Frame(self)
        frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame, text="Search:").pack(side=tk.LEFT, padx=5)
        self.search_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=self.search_var, width=40)
        entry.pack(side=tk.LEFT, padx=5)
        entry.bind("<KeyRelease>", lambda _: self._apply_search())

    def _build_tree(self) -> None:
        columns = ("Timestamp", "Module", "Level", "Message")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=20)
        for col in columns:
            self.tree.heading(col, text=col, anchor=tk.W)
            self.tree.column(col, width=180 if col == "Message" else 120, anchor=tk.W)

        vsb = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0), pady=5)
        vsb.pack(side=tk.LEFT, fill=tk.Y, pady=5)
        hsb.pack(side=tk.TOP, fill=tk.X, padx=10)

        self.tree.tag_configure("ERROR", foreground="red")
        self.tree.tag_configure("WARNING", foreground="orange")
        self.tree.tag_configure("INFO", foreground="green")

    def _build_status(self) -> None:
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.status_var, anchor=tk.W).pack(fill=tk.X, padx=10, pady=5)

    # ------------------------------------------------------------------ data loading
    def _load_log_files(self) -> None:
        self.tree.delete(*self.tree.get_children())
        log_files = sorted(self.log_dir.glob("*.log"), reverse=True)
        choices = [file.stem for file in log_files]
        self.date_combo.configure(values=choices)
        if choices:
            self.date_combo.set(choices[0])
            self._load_log_file(self.log_dir / f"{choices[0]}.log")
        else:
            self.status_var.set("No log files found")

    def _load_log_file(self, path: Path) -> None:
        if not path.exists():
            self.status_var.set("Selected log file not found")
            return

        self.tree.delete(*self.tree.get_children())
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                parsed = self._parse_line(line)
                if parsed:
                    timestamp, module, level, message = parsed
                    self.tree.insert("", tk.END, values=parsed, tags=(level,))
        self.current_log_file = path
        self.status_var.set(f"Loaded {path.name}")

    def _parse_line(self, line: str) -> Optional[Tuple[str, str, str, str]]:
        pattern = r"^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) - ([^-]+) - (\\w+) - (.*)$"
        match = re.match(pattern, line.strip())
        if match:
            return match.groups()
        return None

    # ------------------------------------------------------------------ events
    def _on_date_selected(self, _event=None) -> None:
        selection = self.date_var.get()
        if selection:
            self._load_log_file(self.log_dir / f"{selection}.log")

    def _apply_search(self) -> None:
        term = self.search_var.get().lower()
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            if term and not any(term in str(value).lower() for value in values):
                self.tree.detach(item)
            else:
                self.tree.reattach(item, "", tk.END)

    def _export_csv(self) -> None:
        if not self.current_log_file:
            messagebox.showwarning("Export", "No log file selected")
            return
        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"{self.current_log_file.stem}.csv",
        )
        if not save_path:
            return

        with open(save_path, "w", encoding="utf-8") as handle:
            handle.write("Timestamp,Module,Level,Message\n")
            for item in self.tree.get_children():
                values = self.tree.item(item, "values")
                message = str(values[3]).replace('"', '""')
                handle.write(f'{values[0]},"{values[1]}","{values[2]}","{message}"\n')
        messagebox.showinfo("Export", f"Saved to {save_path}")

    def refresh(self) -> None:
        self._load_log_files()
