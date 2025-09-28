Student Information Management (Tkinter) — README

A minimal desktop app built with Tkinter that demonstrates a multi-page GUI and CSV-based persistence for student enrollment.
This version focuses on the Enroll flow (save to CSV). Query/Delete/Update pages are scaffolded for future work.

⸻

Features (current)
	•	Cross-platform Tkinter GUI (single Tk() root; pages switch via stacked Frame + tkraise()).
	•	Menu bar with page switching (Home / Enroll / Query / Delete / Update / Help).
	•	Enroll Page form with fields: ID, Name, Math, English.
	•	CSV persistence using a small repository class (append + read/list).
	•	Message boxes for validation and success/error feedback.

Note: Query/Delete/Update are placeholders in this version (UI only). Functionality will be added in the next release.

⸻

Project Structure (suggested)

1.Student_system_advance/
├─ Main.py                # main window, menu, page switching
├─ Login.py               # (optional) login window if used
├─ csv_repo.py            # CSV repository (CRUD helpers)
├─ students.csv           # data file (auto-created)
└─ README.md              # this file

	•	Main.py
	•	Creates the root window and a container frame.
	•	Registers pages: home_page, Enroll_Page, Query_Page, Delete_Page, Update_Page.
	•	Menu actions call showframe("ClassName") to raise the target page.
	•	Enroll_Page
	•	StringVars for form fields.
	•	Enroll button → validation → StudentCSVRepo.add(...) → messagebox feedback.
	•	csv_repo.py (CSV storage)
	•	Ensures students.csv exists with header: id,name,math,english.
	•	Minimal APIs: add, get, list, update, delete, upsert.
	•	Uses csv.DictReader/DictWriter and pathlib.Path.

⸻

Requirements
	•	Python 3.10+ (3.11 recommended)
	•	Tkinter available in your interpreter (macOS users: prefer the python.org installer or a conda env with tk)

Optional (recommended): create a virtual environment.

⸻

Setup
	1.	Clone/copy the project into a folder.
	2.	(Optional) Create and activate a venv.
	3.	Ensure Tkinter is available:

python -c "import tkinter; print('OK', tkinter.TkVersion)"

	4.	Make sure csv_repo.py is in the same folder as Main.py.

⸻

Run

python Main.py

On macOS, the Tk menu appears in the system menubar (next to ), not inside the window.

⸻

Usage
	1.	Launch the app.
	2.	Use the menu File → Enroll (or your “Update” menu items) to open the Enroll page.
	3.	Fill in:
	•	ID (required)
	•	Name (required)
	•	Math (optional; numeric)
	•	English (optional; numeric)
	4.	Click Enroll:
	•	On success: a message box confirms, inputs are cleared.
	•	On validation failure or duplicate ID: an error/warning dialog is shown.

Records are saved to students.csv in the project folder.

⸻

Data Storage (CSV)
	•	File: students.csv (auto-created with header)
	•	Columns: id,name,math,english
	•	Types are stored as strings; empty numeric fields are written as empty strings.

Example row:

20230101, Alice, 95, 88


⸻

Message Boxes

The app uses tkinter.messagebox:
	•	showinfo("Success", "...")
	•	showwarning("Validation error", "...")
	•	showerror("Error", "...")
	•	askyesno("Confirm", "...") (for future delete confirmation)

⸻

Known Limitations (this version)
	•	No full CRUD yet: Query/Delete/Update pages are UI placeholders.
	•	No concurrency: CSV is fine for single-user desktop usage; not safe for multi-process writes.
	•	No schema enforcement: CSV does not validate types beyond simple checks.
	•	No pagination/search UI yet.

⸻

Roadmap (next release)
	•	Replace CSV repo with SQLite (sqlite3 built-in): transactions, queries, indexes.
	•	Implement:
	•	Query_Page: search by ID, list all results (e.g., ttk.Treeview).
	•	Delete_Page: confirm dialog + delete by ID.
	•	Update_Page: load by ID → edit → save changes.
	•	Basic input sanitization and numeric validation widgets.
	•	Export/Import tools (CSV ↔ DB).

⸻

Troubleshooting
	•	Mac: menu not visible in window → It’s in the system menubar.
	•	_tkinter.TclError: cannot use geometry manager grid inside ... managed by pack
	•	Do not mix pack() and grid() in the same parent. Use one manager per container, or nest a child Frame and use a different manager inside it.
	•	ModuleNotFoundError: No module named '_tkinter'
	•	Your Python lacks Tk support. Use python.org installer or conda with tk, or brew-install tcl-tk and configure paths.

⸻

License

Personal/educational use. Adapt as you see fit.
