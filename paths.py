import os
import sys
from typing import Optional


def resource_path(relative_path: str) -> str:
    """Resolve bundled resources for PyInstaller and source runs."""
    base_path = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base_path, relative_path)


def get_exe_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def ensure_parent_dir(path: str) -> str:
    output_dir = os.path.dirname(os.path.abspath(path)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def temp_excel_path(filename: str, suffix: Optional[str] = None) -> str:
    output_dir = ensure_parent_dir(filename)
    marker = suffix or "tmp"
    return os.path.join(output_dir, f".~{os.path.basename(filename)}.{marker}.xlsx")
