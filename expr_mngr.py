import yaml
import os
import os.path as osp
import sqlite3
import sys
from db import TableMngr
import rich


CODE_DIR = osp.dirname(__file__)
if CODE_DIR not in sys.path:
    sys.path.append(CODE_DIR)


class ExprMngr:
    def __init__(self, table_def: dict, db_path: str, config: dict, autoupdate=False):
        ExprMngr.format_table_def(table_def)
        ExprMngr.format_config(config)
        self.table_def = table_def
        self.columns = table_def['columns']
        self.column_names = [column['name'] for column in self.columns]
        self.conn = sqlite3.connect(db_path)
        self.table_mngr = TableMngr(table_def, db_path, self.conn)
        self.table_mngr.create_table()
        is_table_update = self.table_mngr.check_table_def(autoupdate=autoupdate)
        if not is_table_update:
            print('db is out-of-date')
            exit(1)
        # config
        self.config = config

    @property
    def local_working_dirname(self):
        return self.config['paths']['local']['working_dirname']

    @property
    def local_exec_configs_dirname(self):
        return self.config['paths']['local']['exec_configs_dirname']

    @property
    def local_logs_dirname(self):
        return self.config['paths']['local']['logs_dirname']

    @property
    def remote_working_dirname(self):
        return self.config['paths']['remote']['working_dirname']

    @property
    def remote_exec_configs_dirname(self):
        return self.config['paths']['remote']['exec_configs_dirname']

    @property
    def remote_logs_dirname(self):
        return self.config['paths']['remote']['logs_dirname']

    @staticmethod
    def format_table_def(table_def):
        for i in range(len(table_def['columns'])):
            table_def['columns'][i]['type'] = table_def['columns'][i]['type'].upper()

    @staticmethod
    def format_config(config):
        for place in ['local', 'remote']:
            for key in config['paths'][place].keys():
                config['paths'][place][key] = osp.abspath(config['paths'][place][key])
            if 'logs_dirname' in config['paths'][place]:
                config['paths'][place]['logs_dirname'] = osp.join(
                    config['paths'][place]['working_dirname'], 
                    config['paths'][place]['logs_dirname']
                )
            else:
                config['paths'][place]['logs_dirname'] = osp.join(config['paths'][place]['working_dirname'], 'logs')
            if 'exec_configs_dirname' in config['paths'][place]:
                config['paths'][place]['exec_configs_dirname'] = osp.join(
                    config['paths'][place]['working_dirname'], 
                    config['paths'][place]['exec_configs_dirname']
                )
            else:
                config['paths'][place]['exec_configs_dirname'] = osp.join(config['paths'][place]['working_dirname'], 'exec_configs')
            for dirname in config['paths'][place].keys():
                config['paths'][place][dirname] = osp.join(
                    config['paths'][place]['working_dirname'], 
                    config['paths'][place][dirname]
                )
                config['paths'][place][dirname] = osp.abspath(config['paths'][place][dirname])

    def get_table_info(self, cursor: sqlite3.Cursor):
        return self.table_mngr.get_table_info(cursor)

    def get_id(self, **kwargs):
        if not (column_names_set:=set(self.column_names)).issubset(kwargs_keys:=set(kwargs.keys())):
            raise RuntimeError(f'kwargs_keys is not subset of column_names, differences are:\n'
                               f'{column_names_set - kwargs_keys}')
        conn = self.conn
        cursor = self.conn.cursor()
        try:
            cursor.execute(f'INSERT INTO log ({",".join([column["name"] for column in self.columns])}) VALUES ({",".join(["?"] * (len(self.columns)))})',
                           [kwargs[column["name"]] for column in self.columns])
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            
        # fetch id
        cursor.execute(f'SELECT id FROM log WHERE {" AND ".join([column["name"] + "=?" for column in self.columns])}',
                       [kwargs[column["name"]] for column in self.columns])
        id_list = cursor.fetchall()
        assert len(id_list) == 1 or print(f'{id_list}')
        conf_id = id_list[0][0]
        conn.commit()
        return conf_id

    def get_local_log_path(self, conf_id: int, rank: int = None, world_size: int = None):
        # TODO: let the format be configurable in etc, or remove this
        if rank is not None and world_size is not None:
            return osp.join(self.local_logs_dirname, f'log{conf_id}--RANK{world_size}_{rank}.csv')
        else:
            return osp.join(self.local_logs_dirname, f'{conf_id}.log')

    def generate_expr_config(self, **kwargs):
        assert 'num_nodes' in kwargs and 'num_process' in kwargs
        os.makedirs(self.local_exec_configs_dirname, exist_ok=True)

        conf_id = self.get_id(**kwargs)
        kwargs['conf_id'] = conf_id
        config = self.config
        col_cmd_name_mapper = config['col_cmd_name_mapper']
        for name, path in config['paths']['remote'].items():
            kwargs[name] = path
        kwargs['master_addr'] = config['master_addr']
        
        with open(osp.join(self.local_exec_configs_dirname, f'{conf_id}.sh'), 'wb') as f:
            for k, v in kwargs.items():
                if k in col_cmd_name_mapper:
                    cmd_k = col_cmd_name_mapper[k]
                else:
                    cmd_k = k
                f.write(bytes(f'export {cmd_k}="{v}"\n', encoding='utf-8'))
        return conf_id

    def __getattr__(self, name: str):
        if name.endswith('_dirname'):
            if name.startswith('remote_'):
                place = 'remote'
            elif name.startswith('local_'):
                place = 'local'
            else:
                raise AttributeError(f"{self} has no attribute '{name}'")
            dirname_name = name[len(place)+1:]
            return self.config['paths'][place][dirname_name]
        else:
            raise AttributeError(f"{self} has no attribute '{name}'")
