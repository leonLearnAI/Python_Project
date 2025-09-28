import tkinter as  tk
from Main import Main_Page


class Login_Page:
    def __init__(self,root=None):  
        self.root = root
        # set title
        self.root.title("student information management system")
        self.root.geometry("300x180")
        # set page frame
        self.roottk_frame = tk.Frame(self.root)
        self.roottk_frame.pack(padx=20, pady=20,fill="both")
        # define entry
        self.id_StringVar = tk.StringVar()
        self.Password_StringVar = tk.StringVar()
        # set page
        self.creat_login_page()
        # show window
        self.root.mainloop()

    def creat_login_page(self):
        # set page Info
        tk.Label(self.roottk_frame,width=5).grid(row=0, columnspan=10)
        tk.Label(self.roottk_frame,width=1).grid(row=0, column=0)

        tk.Label(self.roottk_frame, text="Student ID:").grid(row=1, column=1)
        tk.Entry(self.roottk_frame, textvariable=self.id_StringVar).grid(row=1, column=2)
        tk.Label(self.roottk_frame,text="Password:").grid(row=2, column=1)
        tk.Entry(self.roottk_frame, textvariable=self.Password_StringVar, show="*").grid(row=2, column=2)
        tk.Button(self.roottk_frame, text="Login", command=self.check_login).grid(row=3, column=1, sticky="e", padx=(0,8), pady=10, ipadx=8)
        tk.Button(self.roottk_frame, text="Exit", command=self.root.quit).grid(row=3, column=2, sticky="w", pady=10, ipadx=8)

    # click button and login
    def check_login(self):
        print("check_login")
        if self.id_StringVar.get() == "admin" and self.Password_StringVar.get() == "123456":
            self.login()
        else:
            print("login failed")
    def login(self):
        print("login successfully")
        self.roottk_frame.destroy()
        # create main page
        main_page = Main_Page(self.root)

# define window object
root = tk.Tk()
login_page = Login_Page(root)
