import tkinter as tk
from tkinter import ttk
from csv_repo import Student_Repository
from tkinter import messagebox

class Main_Page:
    def __init__(self, root=None):  
        self.root = root
        self.root.title("system homepage")
        self.root.geometry("600x400")
        self.create_page()
        self.create_menu()

    def create_page(self):
        # 1. set a container frame to contain all pages
        self.container_frame = tk.Frame(self.root, bg="white")
        self.container_frame.pack(fill="both", expand=True)
        self.container_frame.grid_rowconfigure(0, weight=1)
        self.container_frame.grid_columnconfigure(0, weight=1)

        # 2. create and cache all pages
        self.pages = {}
        for Page in (home_page, Enroll_Page, Query_Page, Delete_Page, Update_Page):
            page = Page(self.container_frame, self)
            self.pages[Page.__name__] = page
            page.grid(row=0, column=0, sticky="nsew")

        # set default page
        self.showframe("home_page")

    def create_menu(self):
        # set menu bar
        menu_bar = tk.Menu(self.root)

        # add file menu
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Enroll",
                              command=lambda: self.showframe("Enroll_Page"))
        menu_bar.add_cascade(label="File", menu=file_menu)

        # add update menu
        update_menu = tk.Menu(menu_bar, tearoff=0)
        update_menu.add_command(label="Query",
                                command=lambda: self.showframe("Query_Page"))
        update_menu.add_command(label="Delete",
                                command=lambda: self.showframe("Delete_Page"))
        update_menu.add_command(label="Update",
                                command=lambda: self.showframe("Update_Page"))
        menu_bar.add_cascade(label="Update", menu=update_menu)

        # add help menu
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="About",
                              command=lambda: self.showframe("home_page"))
        help_menu.add_command(label="Version",
                              command=lambda: print("version 1.1"))
        menu_bar.add_cascade(label="Help", menu=help_menu)

        # set menu bar to root
        self.root.config(menu=menu_bar)

    # show a frame by name
    def showframe(self, name: str):
        self.pages[name].tkraise()


# sub pages
class home_page(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        ttk.Label(self, text="Home Page", font=("Arial", 16)).pack(pady=10)

class Enroll_Page(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.repo = Student_Repository()
        ttk.Label(self, text="Enroll Page", font=("Arial", 16)).grid(
            row=0, column=0, columnspan=2, pady=(10, 5)
        )
        # add input fields
        self.name = tk.StringVar()
        self.id = tk.StringVar()
        self.math = tk.StringVar()
        self.english = tk.StringVar()
        self.create_widgets()
    
    def create_widgets(self):
        ttk.Label(self, text="").grid(row=0, column=0, padx=10, pady=10)
        ttk.Label(self, text="Name:", font=("Arial", 12)).grid(row=1, column=0, padx=10, pady=10)
        ttk.Entry(self, textvariable=self.name).grid(row=1, column=1, padx=10, pady=10)
        ttk.Label(self, text="ID:", font=("Arial", 12)).grid(row=2, column=0, padx=10, pady=10)
        ttk.Entry(self, textvariable=self.id).grid(row=2, column=1, padx=10, pady=10)
        ttk.Label(self, text="Math:", font=("Arial", 12)).grid(row=3, column=0, padx=10, pady=10)
        ttk.Entry(self, textvariable=self.math).grid(row=3, column=1, padx=10, pady=10)
        ttk.Label(self, text="English:", font=("Arial", 12)).grid(row=4, column=0, padx=10, pady=10)
        ttk.Entry(self, textvariable=self.english).grid(row=4, column=1, padx=10, pady=10)

        ttk.Button(self, text="Enroll", command=self.enroll).grid(row=5, column=0, padx=10, pady=10)
    
    def enroll(self):
        print("Enroll student:", self.name.get(), self.id.get(), self.math.get(), self.english.get())
        sid = self.id.get().strip()
        name = self.name.get().strip()
        math = self.math.get().strip()
        english = self.english.get().strip()

        # basic validation
        if not sid or not name:
            print("Invalid input")
            return
        # safe number validation
        math_Val = float(math) if math.isnumeric() else None
        english_Val = float(english) if english.isnumeric() else None
        
        try:
            self.repo.add(sid, name, math_Val, english_Val)
            print("saved: ", sid, name, math_Val, english_Val)
            messagebox.showinfo(f"Success", f"Enrolled student {name} with ID {sid}")
            self.id.set("")
            self.name.set("")
            self.math.set("")
            self.english.set("")
        except Exception as e:
            print("Error:", e)
            messagebox.showerror(f"Error", "Failed to enroll student {e}")

class Query_Page(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        ttk.Label(self, text="Query Page", font=("Arial", 16)).pack(pady=10)

class Delete_Page(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        ttk.Label(self, text="Delete Page", font=("Arial", 16)).pack(pady=10)

class Update_Page(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        ttk.Label(self, text="Update Page", font=("Arial", 16)).pack(pady=10)


# if __name__ == "__main__":
#     root = tk.Tk()
#     app = Main_Page(root)
#     root.mainloop()