import tkinter as tk
from tkinter import ttk
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import pandas.io.sql as psql
from procedure import add_export, delete_entry, clear_table, find_by_key_word, presence_export, add_reader, create_library_tabel, delete_library_table, filling_labrary_table, add_library_book, delete_by_key_word
from function import trigger_function, trigger
import traceback


class Main(tk.Frame):
    def __init__(self, root):
        super().__init__(root)
        self.init_main()
        self.db = db

    def init_main(self):
        toolbar = tk.Frame(bg='#d7d8e0', bd=2)
        toolbar.pack(side=tk.TOP, fill=tk.X)

        btn_create_db = tk.Button(toolbar, text='Create databse', command=self.create_db, bg='#d7d8e0', bd=3,
                                  compound=tk.TOP)
        btn_create_db.pack(side=tk.LEFT)

        btn_delete_db = tk.Button(toolbar, text='Delete databse', command=self.delete_db, bg='#d7d8e0', bd=3,
                                  compound=tk.TOP)
        btn_delete_db.pack(side=tk.LEFT)

        btn_connect = tk.Button(toolbar, text='Connect', command=self.connect, bg='#d7d8e0', bd=3,
                                compound=tk.TOP)
        btn_connect.pack(side=tk.LEFT)

        btn_clt = tk.Button(toolbar, text='Create library tables', command=self.create_tables, bg='#d7d8e0', bd=3,
                            compound=tk.TOP)
        btn_clt.pack(side=tk.LEFT)

        btn_dlt = tk.Button(toolbar, text='Delete library tables', command=self.delete_tables, bg='#d7d8e0', bd=3,
                            compound=tk.TOP)
        btn_dlt.pack(side=tk.LEFT)

        btn_fill = tk.Button(toolbar, text='Filling tables', command=self.filling_tables, bg='#d7d8e0', bd=3,
                             compound=tk.TOP)
        btn_fill.pack(side=tk.LEFT)

        btn_at = tk.Button(toolbar, text='Add table', command=self.add_table, bg='#d7d8e0',
                           bd=3,
                           compound=tk.TOP)
        btn_at.pack(side=tk.LEFT)

        btn_add_book = tk.Button(toolbar, text='Add book', command=self.add_book, bg='#d7d8e0', bd=3,
                                 compound=tk.TOP)
        btn_add_book.pack(side=tk.LEFT)

        btn_dbk = tk.Button(toolbar, text='Action by KW', command=self.delete_key_word, bg='#d7d8e0', bd=3,
                            compound=tk.TOP)
        btn_dbk.pack(side=tk.LEFT)

        btn_add_reader = tk.Button(toolbar, text='Add reader', command=self.add_reader, bg='#d7d8e0', bd=3,
                                   compound=tk.TOP)
        btn_add_reader.pack(side=tk.LEFT)

        btn_add_export = tk.Button(toolbar, text='Add export', command=self.add_export, bg='#d7d8e0', bd=3,
                                   compound=tk.TOP)
        btn_add_export.pack(side=tk.LEFT)

        btn_return_book = tk.Button(toolbar, text='Return book', command=self.return_book, bg='#d7d8e0', bd=3,
                                    compound=tk.TOP)
        btn_return_book.pack(side=tk.LEFT)

        btn_print_tb = tk.Button(toolbar, text='Print table', command=self.print_table, bg='#d7d8e0', bd=3,
                                 compound=tk.TOP)
        btn_print_tb.pack(side=tk.LEFT)

        btn_find_book = tk.Button(toolbar, text='Find book', command=self.find_book, bg='#d7d8e0', bd=3,
                                  compound=tk.TOP)
        btn_find_book.pack(side=tk.LEFT)

        btn_cleare_tables = tk.Button(toolbar, text='Clear tables', command=self.clear_tables, bg='#d7d8e0', bd=3,
                                      compound=tk.TOP)
        btn_cleare_tables.pack(side=tk.LEFT)

        btn_cleare_tables = tk.Button(toolbar, text='Delete entry', command=self.delete_entry, bg='#d7d8e0', bd=3,
                                      compound=tk.TOP)
        btn_cleare_tables.pack(side=tk.LEFT)

    def create_db(self):
        Create_db()

    def delete_db(self):
        Delete_db()

    def connect(self):
        Connect()

    def create_tables(self):
        self.db.procedure_create_table()

    def delete_tables(self):
        self.db.procedure_delete_table()

    def filling_tables(self):
        self.db.procedure_filling_tables()

    def add_table(self):
        Add_table()

    def add_book(self):
        Add_book()

    def delete_key_word(self):
        Delete_key_word()

    def add_reader(self):
        Add_reader()

    def add_export(self):
        Add_export()

    def return_book(self):
        Return_book()

    def print_table(self):
        Print_table()

    def find_book(self):
        Find_book()

    def clear_tables(self):
        Clear_tables()

    def delete_entry(self):
        Delete_entry()


class Template(tk.Toplevel):
    def __init__(self):
        super().__init__(root)
        self.init_template()
        self.db = db

    def init_template(self):
        self.geometry('400x220+400+300')
        self.resizable(False, False)

        btn_close = ttk.Button(self, text='Close', command=self.destroy)
        btn_close.place(x=300, y=170)

        self.grab_set()
        self.focus_set()


class Create_db(Template):
    def __init__(self):
        super().__init__()
        self.init_create_db()

    def init_create_db(self):
        self.title('Create database')

        label_name = tk.Label(self, text='Name:')
        label_name.place(x=50, y=50)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=50)

        btn_create = ttk.Button(self, text='Create')
        btn_create.place(x=220, y=170)
        btn_create.bind('<Button-1>', lambda event: self.add_db(
            self.entry_name.get()))

    def add_db(self, name):
        self.db.create_db(name)


class Delete_db(Template):
    def __init__(self):
        super().__init__()
        self.init_delete_db()

    def init_delete_db(self):
        self.title('Delete database')

        label_name = tk.Label(self, text='Name:')
        label_name.place(x=50, y=50)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=50)

        btn_delete = ttk.Button(self, text='Delete')
        btn_delete.place(x=220, y=170)
        btn_delete.bind('<Button-1>', lambda event: self.del_db(
            self.entry_name.get()))

    def del_db(self, name):
        self.db.delete_db(name)


class Connect(Template):
    def __init__(self):
        super().__init__()
        self.init_create_table()
        self.view = app

    def init_create_table(self):
        self.title('Create table')

        label_name = tk.Label(self, text='Name:')
        label_name.place(x=50, y=20)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=20)

        btn_connect = ttk.Button(self, text='Connect')
        btn_connect.place(x=220, y=170)
        btn_connect.bind('<Button-1>', lambda event: self.connect(
            self.entry_name.get()))

    def connect(self, name):
        self.db.connect(name)


class Add_table(Template):
    def __init__(self):
        super().__init__()
        self.init_add_table()

    def init_add_table(self):
        self.title('Add table')

        label_name = tk.Label(self, text='Name:')
        label_name.place(x=50, y=20)
        label_structure = tk.Label(self, text='Structure')
        label_structure.place(x=50, y=70)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=20)

        self.entry_structure = ttk.Entry(self)
        self.entry_structure.place(x=200, y=70)

        btn_ct = ttk.Button(self, text='Add')
        btn_ct.place(x=220, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.add_tb(
            self.entry_name.get(), self.entry_structure.get(), ))

    def add_tb(self, name, structure):
        self.db.add_table(name, structure)


class Add_book(Template):
    def __init__(self):
        super().__init__()
        self.init_add_book()
        self.view = app

    def init_add_book(self):
        self.title('Add book')
        label_title = tk.Label(self, text='Title:')
        label_title.place(x=50, y=20)

        label_writing_year = tk.Label(self, text='Writing year:')
        label_writing_year.place(x=50, y=45)

        label_release_year = tk.Label(self, text='Release year:')
        label_release_year.place(x=50, y=70)

        label_author_surname = tk.Label(self, text='Author surname:')
        label_author_surname.place(x=50, y=95)

        label_author_name = tk.Label(self, text='Author name:')
        label_author_name.place(x=50, y=120)

        label_num_book = tk.Label(self, text='Author patronymic')
        label_num_book.place(x=50, y=145)

        self.entry_title = ttk.Entry(self)
        self.entry_title.place(x=200, y=20)

        self.entry_writing_year = ttk.Entry(self)
        self.entry_writing_year.place(x=200, y=45)

        self.entry_release_year = ttk.Entry(self)
        self.entry_release_year.place(x=200, y=70)

        self.entry_author_surname = ttk.Entry(self)
        self.entry_author_surname.place(x=200, y=95)

        self.entry_author_name = ttk.Entry(self)
        self.entry_author_name.place(x=200, y=120)

        self.entry_author_patronymic = ttk.Entry(self)
        self.entry_author_patronymic.place(x=200, y=145)

        btn_ct = ttk.Button(self, text='Add book')
        btn_ct.place(x=220, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.add_b(
            self.entry_title.get(), self.entry_writing_year.get(),
            self.entry_release_year.get(), self.entry_author_surname.get(),
            self.entry_author_name.get(), self.entry_author_patronymic.get(), ))

    def add_b(self, title, writing_year, release_year, author_surname, author_name, author_patronymic):
        self.db.procedure_add_book(title, writing_year, release_year, author_surname, author_name, author_patronymic)


class Delete_key_word(Template):

    def __init__(self):
        super().__init__()
        self.init_delete_key_word()
        self.view = app

    def init_delete_key_word(self):
        self.title('Action by word')
        label_table_name = tk.Label(self, text='Table Name:')
        label_table_name.place(x=50, y=20)

        self.label_table_name = ttk.Entry(self)
        self.label_table_name.place(x=200, y=20)

        temp_button = tk.Button(self, text='Choose', command=self.choose_key_word, bg='#d7d8e0', bd=3)
        temp_button.place(x=280, y=50)

        btn_ct = ttk.Button(self, text='Delete')
        btn_ct.place(x=220, y=170)
        btn_ct2 = ttk.Button(self, text='Find')
        btn_ct2.place(x=140, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.delete_kw(
            self.label_table_name.get(), self.field, self.label.get(), ))
        btn_ct2.bind('<Button-1>', lambda event: self.find_kw(
            self.label_table_name.get(), self.label.get(), ))

    def choose_key_word(self):
        if self.label_table_name.get() == 'book':
            self.title('Choose book')
            label = tk.Label(self, text='Title book:')
            label.place(x=50, y=95)

            self.label = ttk.Entry(self)
            self.label.place(x=200, y=95)

            self.field = 'title'

        elif self.label_table_name.get() == 'author':
            self.title('Choose surname')
            label = tk.Label(self, text='Author surname:')
            label.place(x=50, y=95)

            self.label = ttk.Entry(self)
            self.label.place(x=200, y=95)

            self.field = 'surname'


        elif self.label_table_name.get() == 'reader':
            self.title('Choose name')
            label = tk.Label(self, text='Reader name:')
            label.place(x=50, y=95)

            self.label = ttk.Entry(self)
            self.label.place(x=200, y=95)

            self.field = 'name'

        elif self.label_table_name.get() == 'export':
            self.title('Choose loaning date')
            label = tk.Label(self, text='Loaning date:')
            label.place(x=50, y=95)

            self.label = ttk.Entry(self)
            self.label.place(x=200, y=95)

            self.field = 'loaning_date'

    def delete_kw(self, t_name, field, key_word):
        self.db.procedure_delete_kw(t_name, field, key_word)

    def find_kw(self, t_name, key_word):
        self.db.procedure_find_kw(t_name, key_word)


class Add_reader(Template):
    def __init__(self):
        super().__init__()
        self.init_add_reader()
        self.view = app

    def init_add_reader(self):
        self.title('Add reader')
        label_title = tk.Label(self, text='Surname:')
        label_title.place(x=50, y=20)

        label_writing_year = tk.Label(self, text='Name:')
        label_writing_year.place(x=50, y=45)

        label_author_name = tk.Label(self, text='Patronymic:')
        label_author_name.place(x=50, y=70)

        self.entry_surname = ttk.Entry(self)
        self.entry_surname.place(x=200, y=20)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=45)

        self.entry_patronymic = ttk.Entry(self)
        self.entry_patronymic.place(x=200, y=70)

        btn_ct = ttk.Button(self, text='Add reader')
        btn_ct.place(x=220, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.add_r(
            self.entry_surname.get(), self.entry_name.get(),
            self.entry_patronymic.get(), ))

    def add_r(self, surname, name, patronymic):
        self.db.query_add_reader(surname, name, patronymic)


class Add_export(Template):
    def __init__(self):
        super().__init__()
        self.init_add_export()
        self.view = app

    def init_add_export(self):
        self.title('Add export')
        label_loaning_date = tk.Label(self, text='Loaning Date:')
        label_loaning_date.place(x=50, y=20)

        label_writing_year = tk.Label(self, text='Reader ID:')
        label_writing_year.place(x=50, y=45)

        label_author_name = tk.Label(self, text='Book ID:')
        label_author_name.place(x=50, y=70)

        self.entry_date = ttk.Entry(self)
        self.entry_date.place(x=200, y=20)

        self.entry_reader_id = ttk.Entry(self)
        self.entry_reader_id.place(x=200, y=45)

        self.entry_book_id = ttk.Entry(self)
        self.entry_book_id.place(x=200, y=70)

        btn_ct = ttk.Button(self, text='Add export')
        btn_ct.place(x=220, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.add_e(
            self.entry_date.get(), self.entry_reader_id.get(),
            self.entry_book_id.get(), ))

    def add_e(self, date, reader_id, book_id):
        self.db.query_add_export(date, reader_id, book_id)


class Return_book(Template):
    def __init__(self):
        super().__init__()
        self.init_presence_export()
        self.view = app

    def init_presence_export(self):
        self.title('Return book')
        label_loaning_date = tk.Label(self, text='Return Date:')
        label_loaning_date.place(x=50, y=20)

        label_writing_year = tk.Label(self, text='Reader ID:')
        label_writing_year.place(x=50, y=45)

        label_author_name = tk.Label(self, text='Book ID:')
        label_author_name.place(x=50, y=70)

        self.entry_date = ttk.Entry(self)
        self.entry_date.place(x=200, y=20)

        self.entry_reader_id = ttk.Entry(self)
        self.entry_reader_id.place(x=200, y=45)

        self.entry_book_id = ttk.Entry(self)
        self.entry_book_id.place(x=200, y=70)

        btn_ct = ttk.Button(self, text='Return book')
        btn_ct.place(x=220, y=170)
        btn_ct.bind('<Button-1>', lambda event: self.add_e(
            self.entry_date.get(), self.entry_reader_id.get(),
            self.entry_book_id.get(), ))

    def add_e(self, date, reader_id, book_id):
        self.db.return_book(date, reader_id, book_id)


class Print_table(Template):
    def __init__(self):
        super().__init__()
        self.init_print_table()

    def init_print_table(self):
        self.title('Print table')

        label_name = tk.Label(self, text='Name:')
        label_name.place(x=50, y=20)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=100, y=20)

        btn_connect = ttk.Button(self, text='Print')
        btn_connect.place(x=220, y=170)
        btn_connect.bind('<Button-1>', lambda event: self.print_table(
            self.entry_name.get()))

    def print_table(self, name):
        self.db.print_table(name)


class Find_book(Template):
    def __init__(self):
        super().__init__()
        self.init_find_book()

    def init_find_book(self):
        self.title('Find book')

        label_name = tk.Label(self, text='Title:')
        label_name.place(x=50, y=20)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=100, y=20)

        btn_connect = ttk.Button(self, text='Find')
        btn_connect.place(x=220, y=170)
        btn_connect.bind('<Button-1>', lambda event: self.find_book(
            self.entry_name.get()))

    def find_book(self, name):
        self.db.query_find_book(name)


class Clear_tables(Template):
    def __init__(self):
        super().__init__()
        self.init_find_book()

    def init_find_book(self):
        self.title('Clear tables')

        label_name = tk.Label(self, text='Table:')
        label_name.place(x=50, y=20)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=100, y=20)

        btn_connect = ttk.Button(self, text='Cleare')
        btn_connect.place(x=220, y=170)
        btn_connect.bind('<Button-1>', lambda event: self.clear_tables(
            self.entry_name.get()))

    def clear_tables(self, name):
        self.db.procedure_clear_tables(name)


class Delete_entry(Template):
    def __init__(self):
        super().__init__()
        self.init_find_book()

    def init_find_book(self):
        self.title('Delete entry')

        label_name = tk.Label(self, text='Table:')
        label_name.place(x=50, y=20)
        label_id = tk.Label(self, text='ID:')
        label_id.place(x=50, y=45)

        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=100, y=20)
        self.entry_id = ttk.Entry(self)
        self.entry_id.place(x=100, y=45)

        btn_connect = ttk.Button(self, text='Delete')
        btn_connect.place(x=220, y=170)
        btn_connect.bind('<Button-1>', lambda event: self.delete_entry(
            self.entry_name.get(), self.entry_id.get()))

    def delete_entry(self, name, id):
        self.db.procedure_delete_entry(name, id)


class DB:
    def __init__(self):
        self.con = psycopg2.connect(
            user="postgres",
            password="22001",
            host="127.0.0.1",
            port="5432"
        )
        try:
            self.cur = self.con.cursor()
        except:
            print("It is impossible to connect to postgres")

    def create_db(self, name):
        try:
            self.con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = "create database " + name + ";"
            self.cur.execute(query)
            self.con.commit()
            print("Created")
            return "Created"
        except psycopg2.errors.DuplicateDatabase:
            print("Duplicate")
            return "Duplicate"
        except:
            traceback.print_exc()
            return "Not created"

    def delete_db(self, name):
        try:
            self.con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = "drop database " + name + ";"
            self.cur.execute(query)
            self.con.commit()
            print("Deleted")
            return "Deleted"
        except:
            traceback.print_exc()
            print("Not deleted")
            return "Not deleted"

    def connect(self, name):
        try:
            self.con = psycopg2.connect(
                user="postgres",
                database=name,
                password="22001",
                host="127.0.0.1",
                port="5432"
            )
            self.cur = self.con.cursor()
            # При подключении к БД(любой) автоматически создаются процедуры для создания и удаления наших таблиц
            self.cur.execute("{}".format(create_library_tabel()))
            self.cur.execute("{}".format(delete_library_table()))
            self.con.commit()
            print("Connected")
            return "Connected"
        except:
            traceback.print_exc()
            return "Not connected"

    def close(self):
        try:
            self.con.close()
            return "Close"
        except:
            traceback.print_exc()
            return "Not close"

    def procedure_create_table(self):
        try:
            self.cur.execute("CALL create_tables();")
            self.cur.execute("{}".format(trigger_function()))
            self.cur.execute("{}".format(trigger()))
            self.cur.execute("{}".format(add_library_book()))
            self.cur.execute("{}".format(delete_by_key_word()))
            self.cur.execute("{}".format(find_by_key_word()))
            self.cur.execute("{}".format(filling_labrary_table()))  # Создаем процедуру для заполнения таблиц
            self.cur.execute("{}".format(add_export()))
            self.cur.execute("{}".format(presence_export()))
            self.cur.execute("{}".format(clear_table()))
            self.cur.execute("{}".format(add_reader()))
            self.cur.execute("{}".format(delete_entry()))
            self.con.commit()
            print("Tables created")
            return "Tables created"
        except psycopg2.errors.DuplicateTable:
            print("Duplicate tables")
            return "Duplicate tables"
        except:
            traceback.print_exc()
            return "Not created"

    def procedure_delete_table(self):
        try:
            self.cur.execute("CALL delete_tables();")
            self.con.commit()
            print("Deleted")
            return "Deleted"
        except:
            traceback.print_exc()
            return "Not deleted"

    def procedure_filling_tables(self):
        try:
            self.cur.execute("CALL filling_tables();")
            self.con.commit()
            print("Filling")
            return "Filling"
        except:
            traceback.print_exc()
            return "Not filling"

    def add_table(self, name, structure):
        try:
            self.cur.execute('CALL add_table(%s, %s);', (name, structure))
            print("TABLE ADDED!")
            self.con.commit()
        except:
            print("TABLE IS CREATED YET!")

    def procedure_add_book(self, title, writing_year, release_year, author_surname, author_name, author_patronymic):
        try:
            self.cur.execute('CALL add_book(%s, %s, %s, %s, %s, %s);',
                             (title, writing_year, release_year, author_surname, author_name, author_patronymic))
            self.con.commit()
            print("Book added")
            return "Book added"
        except:
            traceback.print_exc()
            return "Book not added"

    def procedure_delete_kw(self, t_name, field, key_word):
        try:
            self.cur.execute('CALL delete_key_word(%s, %s, %s);',
                             (t_name, field, key_word))
            self.con.commit()
            print("Deleted")
            return "Deleted"
        except:
            traceback.print_exc()
            return "Not deleted"

    def procedure_find_kw(self, t_name, key_word):
        try:
            query = "SELECT * FROM find_key_word_from_" + t_name + "('" + key_word + "')"
            table = psql.read_sql(query, self.con)
            print(table)
            return table
        except:
            traceback.print_exc()
            print("There is no table " + t_name + " or key " + key_word)
            return "Not deleted"

    def print_table(self, name):
        try:
            if (name == 'author'):
                table = psql.read_sql("SELECT * FROM print_author()", self.con)
            elif (name == 'book'):
                table = psql.read_sql("SELECT * FROM print_book()", self.con)
            elif (name == 'reader'):
                table = psql.read_sql("SELECT * FROM print_reader()", self.con)
            elif (name == 'export'):
                table = psql.read_sql("SELECT * FROM print_export()", self.con)
            print(table)
        except:
            print("There is no table " + name)

    def query_add_reader(self, surname, name, patronymic):
        try:
            self.cur.execute('CALL add_reader(%s, %s, %s);',
                             (surname, name, patronymic))
            self.con.commit()
            print("Reader added")
            return "Reader added"
        except:
            traceback.print_exc()
            return "Not added"

    def query_add_export(self, date, reader_id, book_id):
        try:
            self.cur.execute('CALL add_export(%s, %s, %s);',
                             (date, reader_id, book_id))
            self.con.commit()
            print("Export added")
            return "Export added"
        except:
            traceback.print_exc()
            return "Not added"

    def return_book(self, date, reader_id, book_id):
        try:
            self.cur.execute('CALL presence_export(%s, %s, %s);',
                             (date, reader_id, book_id))
            self.con.commit()
            print("Book returned")
            return "Book returned"
        except:
            traceback.print_exc()
            return "Not returned"

    def query_find_book(self, name):
        table = psql.read_sql("SELECT * FROM show_book('{}')".format(name), self.con)
        print(table)
        return table

    def procedure_clear_tables(self, name):
        try:
            self.cur.execute('CALL clear_table(%s);',
                             (name,))
            if (name == "ALL"):
                print("ALL TABLES CLEAR!")
                return "All clear"
            else:
                print("TABLE " + name + " CLEAR!")
                return "Table " + name + " Clear!"
        except:
            print("There is no table " + name)
            traceback.print_exc()
            return "Not clear"

    def procedure_delete_entry(self, name, id):
        try:
            self.cur.execute('CALL delete_entry(%s, %s);',
                             (name, id))
            print("Entry " + id + " From " + name + " Delete!")
            return "Entry " + id + " From " + name + " Delete!"
        except:
            print("There is no " + id + " in " + name)
            traceback.print_exc()
            return "Not deleted"


if __name__ == "__main__":
    root = tk.Tk()
    db = DB()
    app = Main(root)
    app.pack()
    root.title("Library")
    root.geometry("1850x150+300+200")
    root.resizable(False, False)
    root.mainloop()
