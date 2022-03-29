import interface
import unittest
import pandas.io.sql as psql
import pandas
import numpy as np
from pandas.util.testing import assert_frame_equal
# Запуск тестов из терминала - python unit_tests.py -v
class TestDB(unittest.TestCase):
    def setUp(self):
        self.db = interface.DB()

    def test_create_db(self):
        db_name = psql.read_sql("SELECT datname FROM pg_database WHERE datistemplate = false;", self.db.con)
        if "library1" not in db_name.values:
            self.assertEqual(self.db.create_db("library1"), "Created")

    def test_create_db_duplicate(self):
        db_name = psql.read_sql("SELECT datname FROM pg_database WHERE datistemplate = false;", self.db.con)
        if "library90" in db_name:
            self.assertEqual(self.db.create_db("library90"), "Duplicate")

    def test_delete_db(self):
        db_name = psql.read_sql("SELECT datname FROM pg_database WHERE datistemplate = false;", self.db.con)
        if "library90" in db_name:
            self.assertEqual(self.db.delete_db("library90"), "Deleted")
        else:
            self.assertEqual(self.db.delete_db("library90"), "Not deleted")

    def test_connect(self):
        db_name = psql.read_sql("SELECT datname FROM pg_database WHERE datistemplate = false;", self.db.con)
        if "library1" in db_name.values:
            self.assertEqual(self.db.connect("library1"), "Connected")
        else:
            self.assertEqual(self.db.connect("library1"), "Not connected")

    def test_create_table(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables == tables:
            self.assertEqual(self.db.procedure_create_table(), "Duplicate tables")
        else:
            self.assertEqual(self.db.procedure_create_table(), "Tables created")

    def test_filling_tables(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_filling_tables(), "Filling")

    def test_delete_tables(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_delete_table(), "Deleted")

    def test_add_book(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                       WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_add_book("Парфюмер", 1985, 1985, "Зюскинд", "Патрик", ""), "Book added")

    def test_delete_kw(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_delete_kw("author", "name", "Михаил"), "Deleted")

    def test_find_kw(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                       WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        d = {'id': [1, 18], 'title': ["Вишневый сад", "Вишневый сад"], "writing_year": [1903, 1903], "author": [1, 1], "release_year": [1968, 1998], "presence": [True, True]}
        my_table = pandas.DataFrame(data=d)
        table = self.db.procedure_find_kw("book", "Вишневый сад")
        assert_frame_equal(my_table, table)

    def test_add_reader(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.query_add_reader("Полтавский", "Томир", "Томирович"), "Reader added")

    def test_add_export(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.query_add_export("12.01.2004", 5, 1), "Export added")

    def test_return_book(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                                       WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.return_book("12.01.2004", 5, 1), "Book returned")

    def test_find_book(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                       WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        d = {'title': ["Вишневый сад", "Вишневый сад"], "surname": ["Чехов", "Чехов"], "name": ["Антон", "Антон"]}
        my_table = pandas.DataFrame(data=d)
        table = self.db.query_find_book("Вишневый сад")
        assert_frame_equal(my_table, table)

    def test_clear_all_tables(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_clear_tables("ALL"), "All clear")

    def test_clear_table(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                               WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_clear_tables("book"), "Table book Clear!")

    def test_print_table(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                              WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()

        self.assertEqual(self.db.print_table("export"), "Printed")

    def test_delete_entry(self):
        self.db.connect("library1")
        self.db.cur.execute("""SELECT table_name FROM information_schema.tables
                                                      WHERE table_schema = 'public'""")
        tables = self.db.cur.fetchall()
        my_tables = [('author',), ('book',), ('export',), ('reader',)]
        tables.sort()
        my_tables.sort()
        if my_tables != tables:
            self.db.procedure_create_table()
        self.assertEqual(self.db.procedure_delete_entry("reader", "3"), "Entry 3 From reader Delete!")

if __name__ == "__main__":
  unittest.main()
