import sqlite3
from typing import final
import yaml
import random
import rich
import os
import os.path as osp
import rich


class TableMngr:
    def __init__(self, table_def: dict, db_path, conn: sqlite3.Connection):
        self.table_def = table_def
        self.columns = table_def['columns']
        self.db_path = db_path
        self.conn = conn
        
    def create_table(self, conn: sqlite3.Connection = None):
        if conn is None:
            conn = self.conn
        create_sql = "CREATE TABLE IF NOT EXISTS log("
        create_sql += 'id                          INTEGER     PRIMARY KEY     AUTOINCREMENT, '
        for column in self.table_def['columns']:
            create_sql += "{name} {type} {not_null} {primary_key}, ".format(name=column['name'], type=column['type'], not_null="", primary_key="")
        create_sql += "UNIQUE (" + ', '.join([column['name'] for column in self.table_def['columns']]) + ")"
        create_sql += ")"
        conn.cursor().execute(create_sql)
        conn.commit()
    
    def check_table_def(self, autoupdate=False):
        table_info = self.get_table_info(self.conn.cursor())
        map_column2type_from_db = {}
        for column_info in table_info:
            if column_info[1] == 'id':
                continue
            map_column2type_from_db[column_info[1]] = column_info[2]
        map_column2type_from_table_def = {}
        for column in self.columns:
            map_column2type_from_table_def[column['name']] = column['type'].upper()
        if set(db_keys:=map_column2type_from_db.keys()) != set(table_def_keys:=map_column2type_from_table_def.keys()):
            rich.print(f'db({sorted(db_keys)})')
            rich.print(f'table_def({sorted(table_def_keys)})')
            rich.print(f'Found different column between db and table_def')
            if autoupdate:
                self._update_schema(additional_key_set=set(table_def_keys) - set(db_keys))
                return True
            return False
        if (db_type:=map_column2type_from_db[column['name']]) != (table_def_type:=column['type']):
            raise RuntimeError(f'Found different column type for `{column["name"]}` between db({db_type}) and table_def({table_def_type})')
        return True

    def get_column(self, name):
        for column in self.columns:
            if column['name'] == name:
                return column
        else:
            raise RuntimeError('column not found')

    def get_table_info(self, cursor: sqlite3.Cursor):
        cursor.execute('PRAGMA table_info(log)')
        return cursor.fetchall()

    def show_create_table(self, conn: sqlite3.Connection):
        rich.print(self.get_table_info(conn.cursor()))

    def _update_schema(self, additional_key_set: set, mapper=None):
        if len(additional_key_set) != 1:
            raise RuntimeError("unhandled #of additional_key != 1")
        additional_key = list(additional_key_set)[0]
        new_column = self.get_column(additional_key)
        if not mapper:
            def mapper(row):
                if 'default' not in new_column:
                    raise RuntimeError('default value should be provided in table_def when using default mapper')
                row[new_column['name']] = new_column['default']
                return row
        new_db_path = self.db_path + '.new.sqlite'
        os.system(f'rm {new_db_path}')
        new_conn = sqlite3.connect(new_db_path)
        # 1. create new table
        self.create_table(new_conn)
        old_cursor = self.conn.cursor()
        new_cursor = new_conn.cursor()
        # 2. add column to old table (will rollback later)
        try:
            self._copy_data(old_cursor, new_cursor, mapper=mapper, sample_column_list=['id', 'version'])
            old_cursor.execute
            self.conn.rollback()
            new_conn.commit()
        except:
            self.conn.rollback()
            new_conn.rollback()
            import traceback
            traceback.print_exc()
            exit(1)
        print(f'schema update done. new db in {new_db_path}. please re-run your program')
        if osp.exists(self.db_path) and osp.exists(new_db_path):
            os.system(f'mv {self.db_path} {self.db_path + ".old.sqlite"}')
            os.system(f'mv {new_db_path} {self.db_path}')
        exit(0)

    def _copy_data(self, old_cursor: sqlite3.Cursor, new_cursor: sqlite3.Cursor, mapper, sample_column_list=None):
        old_cursor.execute('SELECT * FROM log')
        data = old_cursor.fetchall()
        old_table_info = self.get_table_info(old_cursor)
        new_table_info = self.get_table_info(new_cursor)
        assert old_table_info[0][1] == 'id'
        assert new_table_info[0][1] == 'id'
        for datum in data:
            datum_dict = {old_table_info[i][1]: datum[i] for i in range(len(datum))}
            result_dict = mapper(datum_dict)
            if result_dict is None:
                continue
            new_cursor.execute(
                'insert into log ({column_name_list}) values ({value_list})'.format(
                    column_name_list=','.join([new_table_info[i][1] for i in range(len(result_dict))]),
                    value_list=','.join(['?'] * (len(result_dict)))
                ),
                [result_dict[new_table_info[i][1]] for i in range(len(result_dict))]
            )
            new_cursor.execute(
                'select id from log where ' + ' AND '.join([new_table_info[i][1] + "=?" for i in range(1, len(result_dict))]),
                [result_dict[new_table_info[i][1]] for i in range(1, len(result_dict))]
            )
            res = new_cursor.fetchall()
            if len(res) != 1:
                print('len(res) != 1', res)
            if res[0][0] != datum_dict['id'] or res[0][0] != result_dict['id']:
                print("res[0][0] != datum_dict['id'] or res[0][0] != result_dict['id']     :     ", res[0], datum_dict['id'], result_dict['id'])
        if sample_column_list is not None:
            new_cursor.execute('SELECT ' + ','.join(sample_column_list) + ' FROM log')
            result = new_cursor.fetchall()
            rich.print(random.sample(result, min(10, len(result))))
