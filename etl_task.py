from abc import ABC, abstractmethod
import csv
import json
import sqlite3
import os
import xml.etree.cElementTree as ET
import re

files_in = ['csv_data_1.csv', 'csv_data_2.csv', 'json_data.json',
            'xml_data.xml']
valid_files = []
basic_result_path = './basic_results.tsv'
advance_result_path = './advanced_results.tsv'


class TableCreator(ABC):
    @abstractmethod
    def factory_method(self):
        pass

    def get_table(self, file):
        product = self.factory_method()
        table = product.get_table(file)
        return table


class CsvCreator(TableCreator):
    def factory_method(self):
        return CsvTable()


class XmlCreator(TableCreator):
    def factory_method(self):
        return XmlTable()


class JsonCreator(TableCreator):
    def factory_method(self):
        return JsonTable()


class Table(ABC):
    @abstractmethod
    def get_table(self, file):
        pass


class CsvTable(Table):
    def get_table(self, file):
        with open(file) as f:
            reader = csv.DictReader(f)
            csv_table = list(reader)
        return csv_table


class XmlTable(Table):
    def get_table(self, file):
        tree = ET.parse(file)
        root = tree.getroot()
        dict = {}
        xml_table = []
        for objects in root:
            for header in objects.findall('object'):
                key = header.get('name')
                value = header.find('value').text
                dict.update({key: value})
            xml_table.append(dict)
            dict = {}
        return xml_table


class JsonTable(Table):
    def get_table(self, file):
        with open(file) as f:
            templates = json.load(f)
            json_table = list(templates.values())[0]
        return json_table


def file_to_table(creator: TableCreator, file):
    table = creator.get_table(file)
    return table

class sql_table:
    default_n = 0

    def __init__(self, file, table):
        self.table = table
        self.headers = list(table[0].keys())
        self.tablename = file.split(".")[0]
        self.format_check()
        self.table_to_sql()
        self.insert_n()

    def format_check(self):
        self.check_headers()
        self.check_values()

    def check_headers(self):
        headers = self.headers
        header_d = re.compile(r'^D\d+$')
        header_m = re.compile(r'^M\d+$')
        for header in headers:
            if header_d.match(str(header)) is not None:
                self.default_n += 1
            elif header_m.match(str(header)) is not None:
                continue
            else:
                raise ValueError

    def check_values(self):
        table = self.table
        value_d = re.compile(r'^\w+$')
        value_m = re.compile(r'^\d+$')
        n = self.default_n
        for row in table:
            for i, value in enumerate(row.values()):
                if i < n:
                    if value_d.match(str(value)) is not None:
                        continue
                    else:
                        raise ValueError
                if i >= n:
                    if value_m.match(str(value)) is not None:
                        continue
                    else:
                        raise ValueError

    def table_to_sql(self):
        table_name = self.tablename
        table = self.table
        headers = self.headers
        headers = ', '.join(headers)
        sql_create_table = "CREATE TABLE {table_name} ({headers})".format(table_name=table_name, headers=headers)
        cur.execute(sql_create_table)
        for row in table:
            values = ", ".join(list(map(lambda s: "\'" + str(s) + "\'", row.values())))
            sql_insert = "INSERT INTO {table_name} ({headers}) VALUES ({data})".format(table_name=table_name,
                                                                                       headers=headers,
                                                                                       data=values)
            cur.execute(sql_insert)

    def insert_n(self):
        sql = "INSERT INTO n(n) VALUES(?)"
        cur.execute(sql, str(self.default_n))


class Result:
    def __init__(self):
        self.n = self.n_select()

    def n_select(self):
        cur.execute("SELECT n FROM n ORDER BY n DESC LIMIT 1")
        n = int(cur.fetchall()[0][0])
        return n

    def file_out(self, select, file_path):
        res = cur.execute(select)
        out_names_list = list(map(lambda x: x[0], res.description))
        with open(file_path, 'wt', newline="") as out_file:
            tsv_writer = csv.writer(out_file, delimiter='\t')
            tsv_writer.writerow(out_names_list)
            for row in res.fetchall():
                tsv_writer.writerow(row)

    def basic_result(self):
        select = self.basic_select()
        file_path = basic_result_path
        self.file_out(select, file_path)

    def basic_select(self):
        column_names = self.get_column_names()
        sql_select = ""
        for file in valid_files:
            table_name = file.split(".")[0]
            sql_select += "SELECT {column_names} FROM {table_name} UNION ALL " \
                          "".format(column_names=column_names, table_name=table_name)
        table_name = valid_files[-1].split(".")[0]
        sql_select += "SELECT {column_names} FROM {table_name} ORDER BY D1 ASC " \
                      "".format(column_names=column_names, table_name=table_name)
        return sql_select

    def get_column_names(self):
        n = self.n
        column_names = []
        for i in range(1, n + 1):
            column_names.append("D" + str(i))
            column_names.append("M" + str(i))
        column_names.sort()
        to_return = ', '.join(column_names)
        return to_return

    def advanced_result(self):
        select = self.advanced_select()
        file_path = advance_result_path
        self.file_out(select, file_path)

    def advanced_select(self):
        n = self.n
        basic_select = self.basic_select()
        d_list = []
        ms_list = []
        for i in range(1, n + 1):
            d_list.append("D" + str(i))
            ms_list.append("SUM(M" + str(i) + ") AS MS" + str(i))
        d = ', '.join(d_list)
        ms = ', '.join(ms_list)
        advanced_select = "SELECT {d}, {ms} FROM ({basic_select}) GROUP BY {d} ORDER BY D1 ASC " \
                          "".format(d=d, ms=ms, basic_select=basic_select)
        return advanced_select

if __name__ == "__main__":
    if len(files_in) > 0:
        try:
            con = sqlite3.connect("local_db")
            cur = con.cursor()
            cur.execute("CREATE TABLE n(n)")
            for file in files_in:
                try:
                    file_extension = file.split(".")[-1]

                    if file_extension == "csv":
                        table = file_to_table(CsvCreator(), file)
                    elif file_extension == "json":
                        table = file_to_table(JsonCreator(), file)
                    elif file_extension == "xml":
                        table = file_to_table(XmlCreator(), file)
                    else:
                        raise ValueError
                    sql_table(file, table)
                    valid_files += [file]
                except ValueError:
                    print("Неверный формат данных в файле:", file)
                except FileNotFoundError:
                    print("Данный файл не найден:", file)
            if len(valid_files) > 0:
                Result().basic_result()
                Result().advanced_result()

                print("Файлы", valid_files, "успешно обработаны!")
        finally:
            con.close()
            os.remove("./local_db")
    else:
        print("Не указанны именна входных фалов!")