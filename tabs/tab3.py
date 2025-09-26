import json
import tkinter as tk
from tkinter import ttk, simpledialog, messagebox
from typing import Dict, List

from config.config_manager import DesktopConfig


TABLE_OPTIONS = [
    "glass",
    "frame",
    "rush",
    "casingcutting",
    "optlabel",
    "casing",
    "extention",
    "urbancutting",
    "wrapping",
]


class Tab3(ttk.Frame):
    """Manage email recipients for Odoo notifications (stored locally)."""

    def __init__(self, parent: ttk.Notebook, config: DesktopConfig):
        super().__init__(parent)
        self.config_store = config
        self.recipients: List[Dict[str, object]] = []

        self._build_ui()
        self._load_recipients()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        ttk.Label(self, text="Email Management", font=("Tahoma", 12, "bold")).pack(pady=10)

        button_frame = ttk.Frame(self)
        button_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Button(button_frame, text="Add", command=self._add_recipient).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Edit", command=self._edit_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Delete", command=self._delete_selected).pack(side=tk.LEFT, padx=5)

        columns = ("Email", "Subscriptions")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=12)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=250 if col == "Email" else 400)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

    # ------------------------------------------------------------------ data
    def _load_recipients(self) -> None:
        raw = self.config_store.get("emails", "recipients", "[]")
        try:
            data = json.loads(raw) if raw else []
            self.recipients = [rec for rec in data if isinstance(rec, dict) and rec.get("email")]
        except json.JSONDecodeError:
            self.recipients = []
        self._refresh_tree()

    def _save_recipients(self) -> None:
        serialized = json.dumps(self.recipients, indent=2)
        self.config_store.set("emails", "recipients", serialized)
        self._refresh_tree()

    def _refresh_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for recipient in self.recipients:
            enabled = [name for name in TABLE_OPTIONS if recipient.get(name)]
            self.tree.insert(
                "",
                tk.END,
                values=(recipient.get("email", ""), ", ".join(enabled)),
            )

    # ------------------------------------------------------------------ actions
    def _add_recipient(self) -> None:
        dialog = RecipientDialog(self, title="Add Recipient")
        if dialog.result:
            self.recipients.append(dialog.result)
            self._save_recipients()

    def _edit_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        index = self.tree.index(selection[0])
        dialog = RecipientDialog(self, title="Edit Recipient", initial=self.recipients[index])
        if dialog.result:
            self.recipients[index] = dialog.result
            self._save_recipients()

    def _delete_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        index = self.tree.index(selection[0])
        if messagebox.askyesno("Delete", "Remove selected recipient?"):
            self.recipients.pop(index)
            self._save_recipients()

    def refresh(self) -> None:
        self._load_recipients()


class RecipientDialog(simpledialog.Dialog):
    def __init__(self, parent, title: str, initial: Dict[str, object] | None = None):
        self.initial = initial or {}
        super().__init__(parent, title)

    def body(self, master):  # type: ignore[override]
        ttk.Label(master, text="Email:" ).grid(row=0, column=0, sticky=tk.W, pady=5)
        self.email_var = tk.StringVar(value=self.initial.get("email", ""))
        ttk.Entry(master, textvariable=self.email_var, width=40).grid(row=0, column=1, pady=5)

        ttk.Label(master, text="Subscriptions:").grid(row=1, column=0, sticky=tk.NW)
        self.vars: Dict[str, tk.BooleanVar] = {}
        box = ttk.Frame(master)
        box.grid(row=1, column=1, sticky=tk.W)
        for idx, option in enumerate(TABLE_OPTIONS):
            var = tk.BooleanVar(value=bool(self.initial.get(option)))
            self.vars[option] = var
            ttk.Checkbutton(box, text=option.title(), variable=var).grid(row=idx // 3, column=idx % 3, sticky=tk.W, padx=5, pady=2)
        return master

    def apply(self) -> None:  # type: ignore[override]
        email = self.email_var.get().strip().lower()
        if not email:
            messagebox.showerror("Error", "Email is required")
            self.result = None
            return
        result = {"email": email}
        result.update({option: bool(var.get()) for option, var in self.vars.items()})
        self.result = result
