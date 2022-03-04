import logging
import lmdb
import pickle
import os
import datetime

try:
    from .logging_utils import EasyLoggerManager
    from .misc_utils import warning_prompt
except:
    # inner import
    from logging_utils import EasyLoggerManager
    from misc_utils import warning_prompt


def open_db(db_path, write=False, map_size=1099511627776 * 2):
    if not write:
        env = lmdb.open(db_path, subdir=os.path.isdir(db_path),
                    readonly=True, lock=False,
                    readahead=True, meminit=False)
    else:
        env = lmdb.open(db_path, subdir=os.path.isdir(db_path), 
                    map_size=map_size, readonly=False, meminit=False, map_async=True)
    return env


def db_get(env, sid, serialize=False, logger=None):
    if isinstance(sid, str):
        sid = sid.encode('utf-8')
    assert isinstance(sid, bytes)
    txn = env.begin()
    item = txn.get(sid)
    if item is None:
        error_info = f'Error found in `db_get`, sid is [{sid}] but not found.'
        if logger:
            logger.error(error_info)
        else:
            print(error_info)
        raise ValueError
    else:
        if serialize:
            try:
                item = pickle.loads(item)
            except Exception as e:
                error_info = f'Error found in `db_get`, sid is [{sid}] but not found.Traceback:\n{e}'
                if logger:
                    logger.error(error_info)
                else:
                    print(error_info)
                raise
    return item


class LMDB(object):
    def __init__(self, db_path, write=False, create_if_not_exist=False, create_if_not_exist_prompt=False, max_size=1099511627776 * 2, commit_inteval=1000, logger=None):
        self.db_path = db_path
        self.write = write
        self.create_if_not_exist = create_if_not_exist
        self.create_if_not_exist_prompt = create_if_not_exist_prompt
        self.max_size = max_size
        self.commit_inteval = commit_inteval

        if logger:
            self.logger = logger
        else:
            self.logger = EasyLoggerManager('LMDB_' + db_path).get_logger(log_to_console=True, stream_handler_color=True, formatter_template=None, handler_singleton=True)

        if not os.path.exists(self.db_path):
            if not self.create_if_not_exist:
                self.logger.error(f"db_path {db_path} not exists!")
                raise ValueError
            elif write:
                if self.create_if_not_exist_prompt:
                    warning_prompt(f"db_path {db_path} not exists, create one ?")
                try:
                    os.makedirs(db_path)
                except Exception as e:
                    self.logger.error(f"Failed to create dir {db_path}, error info:\n{e}")
                    raise ValueError
            else:
                self.logger.error(f"db_path {db_path} not exists and fail to create as `write` is set to False")
                raise ValueError

        self._read_env = None
        self._write_env = None
        self._write_cnt = 0
    
    @property
    def read_env(self):
        if not self._read_env:
            self._read_env = open_db(self.db_path, write=False)
        return self._read_env

    @property
    def write_env(self):
        if not self._write_env:
            self._write_env = open_db(self.db_path, write=True, map_size=self.max_size)
        return self._write_env

    def update_write_txn(self):
        if self._write_cnt == 0:
            self._write_txn = self.write_env.begin(write=True)
        elif self._write_cnt % self.commit_inteval == 0:
            self._write_txn.commit()
            self._write_txn = self.write_env.begin(write=True)
    
    def get(self, sid, serialize=True):
        # TODO: efficiency
        return db_get(self.read_env, sid, serialize, logger=self.logger)

    def __getitem__(self, sid, serialize=True):
        return self.get(sid, serialize)
    
    def put(self, sid, item, instant_commit=False):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, put() is not allowed.")
            raise ValueError
        if not isinstance(sid, str) and not isinstance(sid, bytes):
            self.logger.error(f"In db.put(), sid must be str or bytes, but get {type(sid)}")
            raise TypeError

        self.update_write_txn()

        if isinstance(sid, str):
            sid = sid.encode('utf-8')
        if not isinstance(item, bytes):
            item = pickle.dumps(item)
        self._write_txn.put(sid, item) 

        if instant_commit:
            self._write_txn.commit()
            self._write_txn = self.write_env.begin(write=True)

        self._write_cnt += 1

    def delete(self, sid, instant_commit=False):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, delete() is not allowed.")
            raise ValueError
        if not isinstance(sid, str) and not isinstance(sid, bytes):
            self.logger.error(f"In db.delete(), sid must be str or bytes, but get {type(sid)}")
            raise TypeError
        
        self.update_write_txn()

        if isinstance(sid, str):
            sid = sid.encode('utf-8')
        self._write_txn.delete(sid)

        if instant_commit:
            self._write_txn.commit()
            self._write_txn = self.write_env.begin(write=True)

        self._write_cnt += 1

    def cursor(self):
        txn = self.read_env.begin()
        cursor = txn.cursor()
        return cursor

    def sync(self):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, put() is not allowed.")
            raise ValueError
        self.logger.info('flushing database...')
        self._write_txn.commit()
        self.write_env.sync()
        self._write_txn = self.write_env.begin(write=True)  # all new write op
        self.logger.info(f'flushing database done.')
    
    def get_keys(self, remove_meta=False):
        keys = []
        for k, _ in self.cursor():
            if remove_meta and k.startswith(b'__'):
                continue
            keys.append(k.decode('utf-8'))
        return keys

    def close(self):
        self.read_env.close()
        self.write_env.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write:
            self.sync()
            self.close()
        else:
            self.close()
        if exc_tb is None:
            return True
        else:
            return False


if __name__ == "__main__":
    db_path = './test_lmdb'
    db = LMDB(db_path, write=False, create_if_not_exist=True, commit_inteval=10)

    for k, v in db.cursor():
        print(k, pickle.loads(v))

