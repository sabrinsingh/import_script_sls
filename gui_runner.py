import os
import sys
import subprocess
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext
from dotenv import load_dotenv
import ttkbootstrap as ttk
from ttkbootstrap.constants import *

# -------------------------------
# Base and Environment Setup
# -------------------------------
def resource_path(relative_path):
    """Get absolute path to resource (works in both dev and PyInstaller bundle)."""
    try:
        base_path = sys._MEIPASS  # Folder created by PyInstaller at runtime
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = resource_path(".env")
MAIN_SCRIPT_PATH = resource_path("main_script.py")

if not os.path.exists(ENV_PATH):
    print(f"⚠️ .env file not found at: {ENV_PATH}")
else:
    print(f"✅ Loading environment from: {ENV_PATH}")

load_dotenv(dotenv_path=ENV_PATH)


# -------------------------------
# GUI Application
# -------------------------------
class RedshiftApp(ttk.Window):
    def __init__(self):
        super().__init__(themename="darkly")  # ✅ Default dark mode
        self.title("Stage1,3 Import GUI - SLS")
        self.geometry("850x750")

        self.create_widgets()

    # -------------------------------
    # UI Setup
    # -------------------------------
    def create_widgets(self):
        # Title
        title = ttk.Label(
            self,
            text="🚀 Stage1,3 Import Automation Tool",
            font=("Helvetica", 20, "bold"),
            anchor="center"
        )
        title.pack(pady=20)

        # Theme toggle
        theme_frame = ttk.Frame(self)
        theme_frame.pack(anchor="ne", padx=20, pady=(0, 10))
        self.theme_var = tk.StringVar(value="darkly")
        self.theme_btn = ttk.Checkbutton(
            theme_frame,
            text="🌙 Dark Mode",
            variable=self.theme_var,
            onvalue="darkly",
            offvalue="flatly",
            command=self.toggle_theme,
            bootstyle="round-toggle"
        )
        self.theme_btn.pack(side="right")

        # ✅ sync toggle state with dark mode
        self.after(200, lambda: [self.theme_btn.invoke(), self.theme_btn.invoke()])

        # Input Section
        self.form_frame = ttk.Labelframe(self, text="Connection Configuration", padding=20)
        self.form_frame.pack(fill="x", padx=20, pady=10)

        self.aws_entry = self.labeled_entry("AWS Profile:", os.getenv("AWS_PROFILE", ""))
        self.s3_entry = self.labeled_entry("S3 Location:", os.getenv("S3_LOCATION", ""))
        self.host_entry = self.labeled_entry("Redshift Host:", os.getenv("REDSHIFT_HOST", ""))
        self.port_entry = self.labeled_entry("Redshift Port:", os.getenv("REDSHIFT_PORT", "5439"))
        self.user_entry = self.labeled_entry("Redshift User:", os.getenv("REDSHIFT_USER", ""))
        self.pass_entry = self.labeled_entry("Redshift Password:", os.getenv("REDSHIFT_PASSWORD", ""), show="*")

        # Password toggle
        self.show_pass = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            self.form_frame,
            text="Show Password",
            variable=self.show_pass,
            command=self.toggle_password,
            bootstyle="round-toggle"
        ).pack(anchor="w", padx=10, pady=(5, 0))

        # Run button
        self.run_btn = ttk.Button(
            self,
            text="▶ Run Program",
            bootstyle="success-outline",
            command=self.run_script
        )
        self.run_btn.pack(pady=15)

        # Log section
        log_frame = ttk.Labelframe(self, text="Logs", padding=10)
        log_frame.pack(fill="both", expand=True, padx=20, pady=10)

        self.log_box = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            font=("Consolas", 10),
            height=20,
            background="#1e1e1e",
            foreground="#dcdde1",
            insertbackground="white",
            relief="flat"
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.insert(tk.END, "🪶 Ready. Verify credentials, then click 'Run Program'.\n")
        self.log_box.tag_configure("error", foreground="#ff6b6b")

        # Footer
        footer = ttk.Label(
            self,
            text="👨‍💻 Developed by 𝗦𝗮𝗯𝗿𝗶𝗻 𝗟𝗮𝗹 𝗦𝗶𝗻𝗴𝗵",
            font=("Helvetica", 11, "bold italic"),
            anchor="center",
            foreground="#00BFFF"
        )
        footer.pack(side="bottom", fill="x", pady=10)

    # -------------------------------
    # Helper for labeled entry
    # -------------------------------
    def labeled_entry(self, label, default="", show=None):
        frame = ttk.Frame(self.form_frame)
        frame.pack(fill="x", pady=5)
        ttk.Label(frame, text=label, width=18, anchor="w").pack(side="left", padx=(5, 10))
        entry = ttk.Entry(frame, width=50, show=show)
        entry.insert(0, default)
        entry.pack(side="left", fill="x", expand=True)
        return entry

    # -------------------------------
    # Password toggle
    # -------------------------------
    def toggle_password(self):
        self.pass_entry.config(show="" if self.show_pass.get() else "*")

    # -------------------------------
    # Theme toggle
    # -------------------------------
    def toggle_theme(self):
        new_theme = self.theme_var.get()
        self.style.theme_use(new_theme)

        if hasattr(self, "log_box"):
            if new_theme == "darkly":
                bg, fg = "#1e1e1e", "#dcdde1"
                label_text = "🌙 Dark Mode"
            else:
                bg, fg = "white", "black"
                label_text = "☀️ Light Mode"

            self.log_box.config(background=bg, foreground=fg, insertbackground=fg)
            self.theme_btn.config(text=label_text)

    # -------------------------------
    # Run Script Logic
    # -------------------------------
    def run_script(self):
        self.run_btn.config(state="disabled")
        self.log_box.delete("1.0", tk.END)

        # Collect values
        aws_profile = self.aws_entry.get().strip()
        s3_path = self.s3_entry.get().strip()
        host = self.host_entry.get().strip()
        port = self.port_entry.get().strip()
        user = self.user_entry.get().strip()
        password = self.pass_entry.get().strip()

        if not all([aws_profile, s3_path, host, port, user, password]):
            messagebox.showerror("Error", "Please fill all fields before running.")
            self.run_btn.config(state="normal")
            return

        # ✅ Update os.environ with current values
        os.environ["AWS_PROFILE"] = aws_profile
        os.environ["S3_LOCATION"] = s3_path
        os.environ["REDSHIFT_HOST"] = host
        os.environ["REDSHIFT_PORT"] = port
        os.environ["REDSHIFT_USER"] = user
        os.environ["REDSHIFT_PASSWORD"] = password

        # ✅ Persist to .env file
        try:
            with open(ENV_PATH, "w") as f:
                f.write(f"AWS_PROFILE={aws_profile}\n")
                f.write(f"S3_LOCATION={s3_path}\n")
                f.write(f"REDSHIFT_HOST={host}\n")
                f.write(f"REDSHIFT_PORT={port}\n")
                f.write(f"REDSHIFT_USER={user}\n")
                f.write(f"REDSHIFT_PASSWORD={password}\n")
        except Exception as e:
            print(f"⚠️ Could not save .env: {e}")

        self.log("🚀 Starting main_script logic directly...\n")

        def execute():
            try:
                # Wrapper for logging to GUI from the other thread
                # main_script calls logger(msg), we apppend newline for GUI text box if needed
                def gui_logger(msg):
                    # Schedule GUI update on main thread
                    self.after(0, lambda: self.log(str(msg) + "\n"))

                # Run the pipeline
                import main_script
                main_script.run_pipeline(logger=gui_logger)

                self.after(0, lambda: messagebox.showinfo("Success", "✅ Script executed finished!"))

            except Exception as e:
                err_msg = f"Unexpected error:\n{e}"
                self.after(0, lambda: self.log(err_msg, tag="error"))
                self.after(0, lambda: messagebox.showerror("Error", err_msg))
            finally:
                self.after(0, lambda: self.run_btn.config(state="normal"))

        threading.Thread(target=execute, daemon=True).start()

    # -------------------------------
    # Logging helper
    # -------------------------------
    def log(self, text, tag=None):
        self.log_box.insert(tk.END, text, tag)
        self.log_box.see(tk.END)


# -------------------------------
# Run Application
# -------------------------------
if __name__ == "__main__":
    app = RedshiftApp()
    app.mainloop()
