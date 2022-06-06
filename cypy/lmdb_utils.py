import logging
import lmdb
import pickle
import os
import sys
import traceback
import datetime
import socket
import threading
import queue
import time

from cypy.cli_utils import warn_print
from cypy.logging_utils import EasyLoggerManager
from cypy.misc_utils import warning_prompt

#TODO: more serialization methods like quickle (https://github.com/jcrist/quickle)

def open_db(db_path, write=False, map_size=1099511627776 * 2, readahead=True):
    if not write:
        # The official doc says if setting readahead to False,
        # LMDB will disable the OS filesystem readahead mechanism, 
        # which may improve random read performance when a database is larger than RAM.
        # But the real practice may not behave as documented
        env = lmdb.open(db_path, subdir=os.path.isdir(db_path),
                    readonly=True, lock=False,
                    readahead=readahead, meminit=False)
    else:
        # for write, default setting: sync=True, map_async=True
        # sync = True means flushing data from system buffers to disk after txn.commit(), 
        # and moreover, setting map_async = True to enable asynchronous flushing for better performance
        env = lmdb.open(db_path, subdir=os.path.isdir(db_path), 
                    map_size=map_size, readonly=False, meminit=False, map_async=True)
    return env


def db_get(env, sid, serialize=False, logger=None, suppress_error=False):
    if isinstance(sid, str):
        sid = sid.encode('utf-8')
    assert isinstance(sid, bytes)
    # in each thread/process, we call db_get and create a separate transaction(txn)
    # reason: a read-only Transaction can move across threads, but it cannot be used concurrently from multiple threads.
    # actually the op of creating txn is extremely cheap
    txn = env.begin()
    item = txn.get(sid)
    if item is None:
        error_info = f'Error found in `db_get`, sid is [{sid}] but not found.'
        if suppress_error:
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
                if suppress_error:
                    if logger:
                        logger.error(error_info)
                    else:
                        print(error_info)
                raise
    return item


class LMDB(object):
    def __init__(self, 
                db_path, 
                write=False, 
                create_if_not_exist=False, 
                create_if_not_exist_prompt=False,
                max_size=1099511627776 * 2, 
                readahead=True,
                batch_size=10, 
                queue_len=10, 
                logger=None):
        self.db_path = db_path
        self.write = write
        self.create_if_not_exist = create_if_not_exist
        self.create_if_not_exist_prompt = create_if_not_exist_prompt
        self.max_size = max_size
        self.readahead = readahead
        self.batch_size = batch_size
        self.queue_len = queue_len

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
        self._write_txn = None
        self._delete_cnt = 0
        self._len_inaccurate_flag = False
        
        # multi_threading for bulk write
        self._init_bulk_write()

    
    def _init_bulk_write(self):
        if self.write:
            self._finish_event = threading.Event()
            self._closed = False
            self._queue = queue.Queue(self.queue_len)
            self._put_thread = threading.Thread(target=self._bulk_put_bg)
            self._put_thread.daemon = True
            self._put_thread.start()
    

    def _bulk_put_bg(self):
        try:
            broken_pipe_error = BrokenPipeError
        except NameError:
            broken_pipe_error = socket.error
        
        batch_data = []

        while True:
            try:
                if self._finish_event.is_set() and self._queue.empty():
                    break

                sid, item = self._queue.get(timeout=0.1)
                if isinstance(sid, str):
                    sid = sid.encode('utf-8')
                if not isinstance(item, bytes):
                    item = pickle.dumps(item)
                
                batch_data.append((sid, item))
                if len(batch_data) % self.batch_size == 0:
                    if self._write_txn is None:
                        self._write_txn = self.write_env.begin(write=True)
                    with self._write_txn.cursor() as cursor:
                        cursor.putmulti(batch_data)
                        if not self._len_inaccurate_flag:
                            self._len_inaccurate_flag = True
                    self._write_txn.commit()
                    self._write_txn = self.write_env.begin(write=True)
                    batch_data = []                
                
            except (KeyboardInterrupt, SystemExit):
                raise
            except (broken_pipe_error, EOFError):
                break
            except queue.Empty:
                pass  # This periodically checks if closed.
            except:
                traceback.print_exc(file=sys.stderr)
        
        # last batch
        if self._write_txn is None:
            self._write_txn = self.write_env.begin(write=True)
        with self._write_txn.cursor() as cursor:
            cursor.putmulti(batch_data)
        self._write_txn.commit()
        self._write_txn = self.write_env.begin(write=True)

        self._closed = True

    
    @property
    def read_env(self):
        if not self._read_env:
            self._read_env = open_db(self.db_path, write=False, readahead=self.readahead)
        return self._read_env


    @property
    def write_env(self):
        if not self._write_env:
            self._write_env = open_db(self.db_path, write=True, map_size=self.max_size)
        return self._write_env

    
    def get(self, sid, serialize=True, suppress_error=False):
        # if serialize, use pickle to unserialize data
        # otherwise keep the original data
        return db_get(self.read_env, sid, serialize, logger=self.logger, suppress_error=suppress_error)
    

    def put(self, sid, item):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, put() is not allowed.")
            raise ValueError
        if not isinstance(sid, str) and not isinstance(sid, bytes):
            self.logger.error(f"In db.put(), sid must be str or bytes, but get {type(sid)}")
            raise TypeError

        self._queue.put((sid, item))

    
    def delete(self, sid):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, delete() is not allowed.")
            raise ValueError
        if not isinstance(sid, str) and not isinstance(sid, bytes):
            self.logger.error(f"In db.delete(), sid must be str or bytes, but get {type(sid)}")
            raise TypeError
        
        if self._write_txn is None:
            self._write_txn = self.write_env.begin(write=True)

        if isinstance(sid, str):
            sid = sid.encode('utf-8')
        self._write_txn.delete(sid)

        self._delete_cnt += 1
        if self._delete_cnt % self.batch_size == 0:
            self._write_txn.commit()
            self._write_txn = self.write_env.begin(write=True)

    
    def cursor(self):
        txn = self.read_env.begin()
        cursor = txn.cursor()
        return cursor

    
    def __getitem__(self, sid):
        return self.get(sid)


    def __setitem__(self, sid, item):
        return self.put(sid, item)
    
    
    def __delitem__(self, sid):
        self.delete(sid)


    def sync(self):
        if not self.write:
            self.logger.error(f"Your LMDB is not writeable, put() is not allowed.")
            raise ValueError
        self._finish_event.set()
        while not self._closed:
            time.sleep(0.1)
        self._write_txn.commit()
        self.write_env.sync()
        self._len_inaccurate_flag = False  # accurate again
        self._write_txn = self.write_env.begin(write=True)  # all new write op

    
    def close(self):
        self.read_env.close()
        if self.write:
            self.write_env.close()
    

    # since the value may be raw bytes and not pickled
    # so we only implement keys() 
    def keys(self):
        with self.read_env.begin() as txn:
            for key in txn.cursor().iternext(keys=True, values=False):
                yield key.decode('utf-8')
    

    def __iter__(self):
        return self.keys()

    
    def __contains__(self, sid):
        try:
            _ = self.get(sid, suppress_error=True)
        except:
            return False
        return True
    

    def __len__(self):
        if self._len_inaccurate_flag:
            warn_print(f'The returned db length may be inaccurate, since the db was modified.')
        with self.read_env.begin() as txn:
            return txn.stat()['entries']


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
    db = LMDB(db_path, write=False, create_if_not_exist=True)

    for k, v in db.cursor():
        print(k, pickle.loads(v))

