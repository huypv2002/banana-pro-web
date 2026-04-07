"""
Chrome Profile Manager - PySide6 GUI
Quản lý Chrome profiles cho reCAPTCHA token trên VPS.
- Tạo/xóa profiles
- Mở Chrome để đăng nhập labs.google
- Mở Chrome để cài extension (VPN, v.v.)
- Path đồng bộ với main.py (PROFILES_DIR env)
"""
import json, os, shutil, subprocess, sys
from pathlib import Path

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QFont, QColor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QLabel, QLineEdit, QMessageBox,
)

# ── Config — PHẢI khớp với main.py ───────────────────────────────────────────
PROFILES_DIR = os.environ.get("PROFILES_DIR", r"C:\BananaPro\chrome_profiles").strip()

def find_chrome():
    for p in [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]:
        if os.path.isfile(p): return p
    return "chrome.exe"

CHROME_PATH = find_chrome()

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_profiles():
    d = Path(PROFILES_DIR)
    d.mkdir(parents=True, exist_ok=True)
    profiles = []
    for p in sorted(d.iterdir()):
        if not p.is_dir() or p.name.startswith("."): continue
        cookies_db = p / "Default" / "Network" / "Cookies"
        has_cookies = cookies_db.exists() and cookies_db.stat().st_size > 0
        info = {}
        try: info = json.loads((p / ".profile_info.json").read_text(encoding="utf-8"))
        except: pass
        profiles.append({
            "name": p.name, "path": str(p),
            "has_cookies": has_cookies,
            "email": info.get("email", ""),
            "status": info.get("status", "unknown"),
        })
    return profiles

def save_profile_info(profile_path, email="", status="active"):
    (Path(profile_path) / ".profile_info.json").write_text(
        json.dumps({"email": email, "status": status}, ensure_ascii=False), encoding="utf-8")

# ── Chrome Thread ─────────────────────────────────────────────────────────────
class ChromeThread(QThread):
    finished = Signal(str)

    def __init__(self, profile_path, profile_name, mode="login"):
        super().__init__()
        self.profile_path = profile_path
        self.profile_name = profile_name
        self.mode = mode  # "login" | "extension"

    def run(self):
        base_args = [
            CHROME_PATH,
            f"--user-data-dir={self.profile_path}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-sync",
            "--password-store=basic",
        ]
        if self.mode == "login":
            args = base_args + [
                "--disable-extensions",
                "--disable-signin-promo",
                "https://labs.google/fx/tools/flow",
            ]
        else:
            # Extension mode: mở Chrome visible, sau đó mở tab chrome://extensions
            # Dùng 2 lần gọi: lần 1 khởi động, lần 2 mở tab mới
            args = base_args + ["--new-window", "--start-maximized"]
        try:
            proc = subprocess.Popen(args)
            if self.mode == "extension":
                import time
                time.sleep(2)  # Đợi Chrome khởi động
                # Mở chrome://extensions trong tab mới bằng cách gọi Chrome lần 2
                subprocess.Popen([CHROME_PATH, f"--user-data-dir={self.profile_path}", "chrome://extensions/"])
            proc.wait()
        except Exception:
            pass
        self.finished.emit(self.profile_name)

# ── Main Window ───────────────────────────────────────────────────────────────
BTN_STYLE = {
    "green":  "QPushButton{background:#16a34a;color:#fff;border:none;border-radius:6px;padding:0 12px;font-weight:bold;font-size:12px;} QPushButton:hover{background:#15803d;} QPushButton:disabled{background:#94a3b8;}",
    "blue":   "QPushButton{background:#0369a1;color:#fff;border:none;border-radius:6px;padding:0 12px;font-weight:bold;font-size:12px;} QPushButton:hover{background:#075985;} QPushButton:disabled{background:#94a3b8;}",
    "orange": "QPushButton{background:#d97706;color:#fff;border:none;border-radius:6px;padding:0 12px;font-weight:bold;font-size:12px;} QPushButton:hover{background:#b45309;} QPushButton:disabled{background:#94a3b8;}",
    "red":    "QPushButton{background:#dc2626;color:#fff;border:none;border-radius:6px;padding:0 10px;font-size:13px;} QPushButton:hover{background:#b91c1c;}",
}

class ProfileManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🍌 Banana Pro - Chrome Profile Manager")
        self.setMinimumSize(860, 520)
        self.threads = {}  # name → ChromeThread

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(10)
        layout.setContentsMargins(16, 16, 16, 16)

        # Header
        lbl = QLabel("🍌 Chrome Profile Manager")
        lbl.setFont(QFont("Segoe UI", 16, QFont.Bold))
        lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(lbl)

        path_lbl = QLabel(f"📁 Profiles dir: {PROFILES_DIR}   |   🌐 Chrome: {CHROME_PATH}")
        path_lbl.setStyleSheet("color:#6b7280;font-size:11px;")
        path_lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(path_lbl)

        # Toolbar
        tb = QHBoxLayout()
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Tên profile mới (vd: account1)")
        self.name_input.setFixedHeight(34)
        tb.addWidget(self.name_input, 1)

        for label, style, slot in [
            ("➕ Tạo Profile", "green", self.create_profile),
            ("🔄 Làm mới",    "blue",  self.refresh_table),
        ]:
            btn = QPushButton(label)
            btn.setFixedHeight(34)
            btn.setStyleSheet(BTN_STYLE[style])
            btn.clicked.connect(slot)
            tb.addWidget(btn)
        layout.addLayout(tb)

        # Info box
        info = QLabel(
            "💡 <b>Mở Chrome (Login)</b>: đăng nhập labs.google → đóng Chrome → backend tự dùng cookies\n"
            "💡 <b>Mở Chrome (Extension)</b>: cài VPN/extension → đóng Chrome → extension được lưu vào profile"
        )
        info.setStyleSheet("background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:8px 12px;font-size:12px;color:#1e40af;")
        info.setWordWrap(True)
        layout.addWidget(info)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["Profile", "Email", "Trạng thái", "Cookies", "Path", "Hành động"])
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.Stretch)
        hh.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.Stretch)
        hh.setSectionResizeMode(5, QHeaderView.Fixed)
        self.table.setColumnWidth(5, 300)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.setStyleSheet("""
            QTableWidget{border:1px solid #dee2e6;border-radius:8px;font-size:12px;}
            QTableWidget::item{padding:4px;}
            QHeaderView::section{background:#f1f5f9;font-weight:bold;padding:6px;border:none;border-bottom:2px solid #dee2e6;}
        """)
        layout.addWidget(self.table)

        self.status_lbl = QLabel("Sẵn sàng")
        self.status_lbl.setStyleSheet("color:#6b7280;font-size:11px;padding:4px;")
        layout.addWidget(self.status_lbl)

        self.refresh_table()

    def set_status(self, msg):
        self.status_lbl.setText(msg)

    def refresh_table(self):
        profiles = get_profiles()
        self.table.setRowCount(len(profiles))
        for i, p in enumerate(profiles):
            running = p["name"] in self.threads

            self.table.setItem(i, 0, QTableWidgetItem(p["name"]))
            self.table.setItem(i, 1, QTableWidgetItem(p["email"] or "—"))

            if running:
                st, col = "🟢 Đang mở", "#16a34a"
            elif p["has_cookies"]:
                st, col = "✅ Đã login", "#0369a1"
            else:
                st, col = "⚪ Chưa login", "#6b7280"
            si = QTableWidgetItem(st)
            si.setForeground(QColor(col))
            self.table.setItem(i, 2, si)

            self.table.setItem(i, 3, QTableWidgetItem("✅" if p["has_cookies"] else "❌"))
            self.table.setItem(i, 4, QTableWidgetItem(p["path"]))

            # Actions widget
            w = QWidget()
            hl = QHBoxLayout(w)
            hl.setContentsMargins(4, 2, 4, 2)
            hl.setSpacing(4)

            btn_login = QPushButton("🌐 Login")
            btn_login.setFixedHeight(28)
            btn_login.setEnabled(not running)
            btn_login.setStyleSheet(BTN_STYLE["blue"])
            btn_login.setToolTip("Mở Chrome để đăng nhập labs.google")
            btn_login.clicked.connect(lambda _, n=p["name"], path=p["path"]: self.open_chrome(n, path, "login"))
            hl.addWidget(btn_login)

            btn_ext = QPushButton("🧩 Extension")
            btn_ext.setFixedHeight(28)
            btn_ext.setEnabled(not running)
            btn_ext.setStyleSheet(BTN_STYLE["orange"])
            btn_ext.setToolTip("Mở Chrome để cài extension (VPN, v.v.)")
            btn_ext.clicked.connect(lambda _, n=p["name"], path=p["path"]: self.open_chrome(n, path, "extension"))
            hl.addWidget(btn_ext)

            btn_del = QPushButton("🗑")
            btn_del.setFixedSize(28, 28)
            btn_del.setStyleSheet(BTN_STYLE["red"])
            btn_del.setEnabled(not running)
            btn_del.clicked.connect(lambda _, n=p["name"], path=p["path"]: self.delete_profile(n, path))
            hl.addWidget(btn_del)

            self.table.setCellWidget(i, 5, w)
            self.table.setRowHeight(i, 40)

        self.set_status(f"{len(profiles)} profile(s) | PROFILES_DIR={PROFILES_DIR}")

    def create_profile(self):
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "Lỗi", "Nhập tên profile"); return
        if any(c in name for c in r'\/:*?"<>|'):
            QMessageBox.warning(self, "Lỗi", "Tên không được chứa ký tự đặc biệt"); return
        path = Path(PROFILES_DIR) / name
        if path.exists():
            QMessageBox.warning(self, "Lỗi", f"Profile '{name}' đã tồn tại"); return
        path.mkdir(parents=True)
        save_profile_info(str(path), status="new")
        self.name_input.clear()
        self.refresh_table()
        self.set_status(f"✅ Đã tạo profile: {name}  →  {path}")

    def open_chrome(self, name, path, mode):
        if name in self.threads: return
        t = ChromeThread(path, name, mode)
        t.finished.connect(self.on_chrome_closed)
        self.threads[name] = t
        t.start()
        self.refresh_table()
        if mode == "login":
            self.set_status(f"🌐 {name}: Chrome đang mở → Đăng nhập labs.google rồi đóng Chrome")
        else:
            self.set_status(f"🧩 {name}: Chrome đang mở → Cài extension VPN rồi đóng Chrome")

    def on_chrome_closed(self, name):
        self.threads.pop(name, None)
        path = Path(PROFILES_DIR) / name
        cookies_db = path / "Default" / "Network" / "Cookies"
        if cookies_db.exists() and cookies_db.stat().st_size > 0:
            save_profile_info(str(path), status="active")
            self.set_status(f"✅ {name}: Chrome đã đóng, cookies đã lưu → Backend sẵn sàng dùng profile này")
        else:
            self.set_status(f"⚠️ {name}: Chrome đã đóng, chưa có cookies")
        self.refresh_table()

    def delete_profile(self, name, path):
        if name in self.threads:
            QMessageBox.warning(self, "Lỗi", "Đóng Chrome trước khi xóa"); return
        if QMessageBox.question(self, "Xác nhận", f"Xóa profile '{name}'?",
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            shutil.rmtree(path, ignore_errors=True)
            self.refresh_table()
            self.set_status(f"🗑 Đã xóa: {name}")


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet("QMainWindow{background:#f8fafc;} QLineEdit{border:1px solid #dee2e6;border-radius:8px;padding:6px 12px;font-size:13px;} QLineEdit:focus{border-color:#16a34a;}")
    win = ProfileManager()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
