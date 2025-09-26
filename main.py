import os
import sys
import tkinter as tk
from tkinter import ttk

try:
    from PIL import Image, ImageTk
except ImportError:  # optional dependency
    Image = ImageTk = None

try:
    from .config.config_manager import DesktopConfig
    from .services.logger import Logger
    from .tabs.tab1 import Tab1
    from .tabs.tab2 import Tab2
    from .tabs.tab3 import Tab3
    from .tabs.tab4 import Tab4
    from .tabs.tab5 import Tab5
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from config.config_manager import DesktopConfig
    from services.logger import Logger
    from tabs.tab1 import Tab1
    from tabs.tab2 import Tab2
    from tabs.tab3 import Tab3
    from tabs.tab4 import Tab4
    from tabs.tab5 import Tab5


class MainApplication(tk.Tk):
    def __init__(
        self,
        config_store: DesktopConfig | None = None,
        *,
        logger_name: str = "Main",
    ) -> None:
        super().__init__()
        self.title("CSV File Uploader (Odoo)")
        self.geometry("1000x700")
        self.minsize(900, 700)

        self.config_store = config_store or DesktopConfig()
        self.logger = Logger(logger_name)

        self._set_icon()
        self._build_ui()
        self._build_menu()

    # ------------------------------------------------------------------ UI
    def _set_icon(self) -> None:
        icon_path = os.path.join(os.path.dirname(__file__), "csv.png")
        if Image and ImageTk and os.path.exists(icon_path):
            try:
                icon = ImageTk.PhotoImage(Image.open(icon_path))
                self.iconphoto(True, icon)
                self._icon_ref = icon
            except Exception:
                pass

    def _build_ui(self) -> None:
        notebook = ttk.Notebook(self)
        notebook.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)
        self.notebook = notebook

        logs_dir = os.path.join(os.path.dirname(__file__), "logs")
        self.tabs = {
            "settings": Tab1(notebook, self.config_store),
            "logs": Tab2(notebook, log_dir=logs_dir),
            "emails": Tab3(notebook, self.config_store),
            "monitor": Tab4(notebook, self.config_store),
            "database": Tab5(notebook, self.config_store),
        }

        notebook.add(self.tabs["settings"], text="Settings")
        notebook.add(self.tabs["logs"], text="Log Viewer")
        notebook.add(self.tabs["emails"], text="Email Settings")
        notebook.add(self.tabs["monitor"], text="Monitoring")
        notebook.add(self.tabs["database"], text="Database Tools")

    def _build_menu(self) -> None:
        menubar = tk.Menu(self)

        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Refresh", command=self._refresh_app)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about)
        menubar.add_cascade(label="Help", menu=help_menu)

        self.config(menu=menubar)

    # ------------------------------------------------------------------ actions
    def _refresh_app(self) -> None:
        self.config_store.load()
        for tab in self.tabs.values():
            if hasattr(tab, "refresh"):
                tab.refresh()

    def _show_about(self) -> None:
        about = tk.Toplevel(self)
        about.title("About")
        about.geometry("320x180")
        about.resizable(False, False)

        tk.Label(
            about,
            text="CSV File Uploader\nOdoo Edition",
            font=("Tahoma", 12, "bold"),
            justify=tk.CENTER,
        ).pack(pady=20)
        tk.Button(about, text="OK", command=about.destroy).pack(pady=10)


def run(
    config_store: DesktopConfig | None = None,
    *,
    logger_name: str = "Main",
) -> None:
    app = MainApplication(config_store=config_store, logger_name=logger_name)
    app.mainloop()


if __name__ == "__main__":
    run()


