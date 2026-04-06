"""
Chrome Profile Manager - PySide6 GUI
Quản lý Chrome profiles cho reCAPTCHA token trên VPS.
- Tạo/xóa profiles
- Mở Chrome với profile để đăng nhập labs.google
- Xem trạng thái profile (đã login chưa)
"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from PySide6.QtCore import Qt, QThread, Signal, QTimer
from PySide6.QtGui import QFont, QColor, QIcon
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QLabel, QLineEdit, QMessageBox, QStatusBar, QFrame,
)

# ── Config ────────────────────────────────────────────────────────────────────
PROFILES_DIR = os.environ.get("PROFILES_DIR", r"C:\BananaPro\chrome_profiles")
CHROME_PATH = None

def find_chrome():
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return "chrome.exe"

CHROME_PATH = find_chrome()

def get_profiles():
    """List all profiles in PROFILES_DIR."""
    d = Path(PROFILES_DIR)
    d.mkdir(parents=True, exist_ok=True)
    profiles = []
    for p in sorted(d.iterdir()):
        if p.is_dir() and not p.name.startswith("."):
            # Check if has cookies (logged in)
            cookies_db = p / "Default" / "Network" / "Cookies"
            has_cookies = cookies_db.exists() and cookies_db.stat().st_size > 0
            # Check login state from saved info
            info_file = p / ".profile_info.json"
            info = {}
            if info_file.exists():
                try:
                    info = json.loads(info_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
            profiles.append({
                "name": p.name,
                "path": str(p),
                "has_cookies": has_cookies,
                "email": info.get("email", ""),
                "status": info.get("status", "unknown"),
            })
    return profiles


def save_profile_info(profile_path, email="", status="active"):
    info_file = Path(profile_path) / ".profile_info.json"
    info_file.write_text(json.dumps({"email": email, "status": status}, ensure_ascii=False), encoding="utf-8")


# ── Chrome Launcher Thread ────────────────────────────────────────────────────
class ChromeThread(QThread):
    finished = Signal(str, int)  # profile_name, return_code

    def __init__(self, profile_path, profile_name):
        super().__init__()
        self.profile_path = profile_path
        self.profile_name = profile_name

    def run(self):
        cmd = [
            CHROME_PATH,
            f"--user-data-dir={self.profile_path}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-sync",
            "--disable-signin-promo",
            "--password-store=basic",
            "--restore-last-session",
            "https://labs.google/fx/tools/flow",
        ]
        try:
            proc = subprocess.Popen(cmd)
            proc.wait()
            self.finished.emit(self.profile_name, proc.returncode)
        except Exception as e:
            self.finished.emit(self.profile_name, -1)


# ── Main Window ───────────────────────────────────────────────────────────────
class ProfileManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🍌 Banana Pro - Chrome Profile Manager")
        self.setMinimumSize(750, 500)
        self.chrome_threads = {}

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        # Header
        header = QLabel("🍌 Chrome Profile Manager")
        header.setFont(QFont("Segoe UI", 18, QFont.Bold))
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        sub = QLabel(f"Profiles: {PROFILES_DIR}")
        sub.setStyleSheet("color: #6b7280; font-size: 11px;")
        sub.setAlignment(Qt.AlignCenter)
        layout.addWidget(sub)

        # Toolbar
        toolbar = QHBoxLayout()
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Tên profile mới (vd: account_1)")
        self.name_input.setFixedHeight(36)
        toolbar.addWidget(self.name_input, 1)

        btn_create = QPushButton("➕ Tạo Profile")
        btn_create.setFixedHeight(36)
        btn_create.clicked.connect(self.create_profile)
        btn_create.setStyleSheet("QPushButton{background:#16a34a;color:#fff;border:none;border-radius:8px;padding:0 16px;font-weight:bold;} QPushButton:hover{background:#15803d;}")
        toolbar.addWidget(btn_create)

        btn_refresh = QPushButton("🔄 Làm mới")
        btn_refresh.setFixedHeight(36)
        btn_refresh.clicked.connect(self.refresh_table)
        btn_refresh.setStyleSheet("QPushButton{background:#0369a1;color:#fff;border:none;border-radius:8px;padding:0 16px;font-weight:bold;} QPushButton:hover{background:#075985;}")
        toolbar.addWidget(btn_refresh)

        layout.addLayout(toolbar)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Profile", "Email", "Trạng thái", "Cookies", "Hành động"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Fixed)
        self.table.setColumnWidth(4, 220)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.setStyleSheet("""
            QTableWidget { border: 1px solid #dee2e6; border-radius: 8px; font-size: 13px; }
            QTableWidget::item { padding: 6px; }
            QHeaderView::section { background: #f1f5f9; font-weight: bold; padding: 8px; border: none; border-bottom: 2px solid #dee2e6; }
        """)
        layout.addWidget(self.table)

        # Status bar
        self.statusBar().showMessage("Sẵn sàng")

        self.refresh_table()

    def refresh_table(self):
        profiles = get_profiles()
        self.table.setRowCount(len(profiles))
        for i, p in enumerate(profiles):
            # Name
            name_item = QTableWidgetItem(p["name"])
            name_item.setFont(QFont("Segoe UI", 11, QFont.Bold))
            self.table.setItem(i, 0, name_item)

            # Email
            self.table.setItem(i, 1, QTableWidgetItem(p["email"] or "—"))

            # Status
            running = p["name"] in self.chrome_threads
            if running:
                status_text, color = "🟢 Đang mở", "#16a34a"
            elif p["has_cookies"]:
                status_text, color = "✅ Đã login", "#0369a1"
            else:
                status_text, color = "⚪ Chưa login", "#6b7280"
            status_item = QTableWidgetItem(status_text)
            status_item.setForeground(QColor(color))
            self.table.setItem(i, 2, status_item)

            # Cookies
            cookie_text = "✅ Có" if p["has_cookies"] else "❌ Không"
            self.table.setItem(i, 3, QTableWidgetItem(cookie_text))

            # Actions
            actions = QWidget()
            actions_layout = QHBoxLayout(actions)
            actions_layout.setContentsMargins(4, 2, 4, 2)
            actions_layout.setSpacing(6)

            btn_open = QPushButton("🌐 Mở Chrome")
            btn_open.setFixedHeight(30)
            btn_open.setEnabled(p["name"] not in self.chrome_threads)
            btn_open.setStyleSheet("QPushButton{background:#0369a1;color:#fff;border:none;border-radius:6px;padding:0 10px;font-size:12px;} QPushButton:hover{background:#075985;} QPushButton:disabled{background:#94a3b8;}")
            btn_open.clicked.connect(lambda _, name=p["name"], path=p["path"]: self.open_chrome(name, path))
            actions_layout.addWidget(btn_open)

            btn_del = QPushButton("🗑")
            btn_del.setFixedSize(30, 30)
            btn_del.setStyleSheet("QPushButton{background:#dc2626;color:#fff;border:none;border-radius:6px;font-size:13px;} QPushButton:hover{background:#b91c1c;}")
            btn_del.clicked.connect(lambda _, name=p["name"], path=p["path"]: self.delete_profile(name, path))
            actions_layout.addWidget(btn_del)

            self.table.setCellWidget(i, 4, actions)
            self.table.setRowHeight(i, 44)

        self.statusBar().showMessage(f"{len(profiles)} profile(s) | Chrome: {CHROME_PATH}")

    def create_profile(self):
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "Lỗi", "Nhập tên profile")
            return
        if any(c in name for c in r'\/:*?"<>|'):
            QMessageBox.warning(self, "Lỗi", "Tên không được chứa ký tự đặc biệt")
            return
        path = Path(PROFILES_DIR) / name
        if path.exists():
            QMessageBox.warning(self, "Lỗi", f"Profile '{name}' đã tồn tại")
            return
        path.mkdir(parents=True)
        save_profile_info(str(path), status="new")
        self.name_input.clear()
        self.refresh_table()
        self.statusBar().showMessage(f"Đã tạo profile: {name}")

    def open_chrome(self, name, path):
        if name in self.chrome_threads:
            return
        thread = ChromeThread(path, name)
        thread.finished.connect(self.on_chrome_closed)
        self.chrome_threads[name] = thread
        thread.start()
        self.refresh_table()
        self.statusBar().showMessage(f"Đang mở Chrome: {name} → Đăng nhập labs.google rồi đóng Chrome")

    def on_chrome_closed(self, name, code):
        if name in self.chrome_threads:
            del self.chrome_threads[name]
        # Check if cookies exist now
        path = Path(PROFILES_DIR) / name
        cookies_db = path / "Default" / "Network" / "Cookies"
        if cookies_db.exists() and cookies_db.stat().st_size > 0:
            save_profile_info(str(path), status="active")
            self.statusBar().showMessage(f"✅ {name}: Chrome đã đóng, cookies đã lưu")
        else:
            self.statusBar().showMessage(f"⚠️ {name}: Chrome đã đóng, chưa có cookies")
        self.refresh_table()

    def delete_profile(self, name, path):
        if name in self.chrome_threads:
            QMessageBox.warning(self, "Lỗi", "Đóng Chrome trước khi xóa profile")
            return
        reply = QMessageBox.question(self, "Xác nhận", f"Xóa profile '{name}'?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            try:
                shutil.rmtree(path, ignore_errors=True)
                self.refresh_table()
                self.statusBar().showMessage(f"Đã xóa: {name}")
            except Exception as e:
                QMessageBox.critical(self, "Lỗi", str(e))


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    # Dark-ish palette
    app.setStyleSheet("""
        QMainWindow { background: #f8fafc; }
        QLineEdit { border: 1px solid #dee2e6; border-radius: 8px; padding: 6px 12px; font-size: 13px; }
        QLineEdit:focus { border-color: #16a34a; }
    """)
    win = ProfileManager()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
