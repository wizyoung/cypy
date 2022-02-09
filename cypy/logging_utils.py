import os
import logging
import multiprocessing
import threading
import socket
import queue
import sys
import traceback
import time
import datetime
from copy import deepcopy, copy
from logging.handlers import BaseRotatingHandler
import re

original_print = print

logging_color_set = {
    "underline_grey":  "\033[4m",
    "grey": "\x1b[38;21m",
    "yellow":  "\x1b[33;1m",
    "red": "\x1b[31;1m",
    "green": "\033[32m",
    "reset":  "\x1b[0m",
}

def stdout_write(s, flush=False):
    sys.stdout.write(s)
    if flush:
        sys.stdout.flush()


def stderr_write(s, flush=False):
    sys.stderr.write(s)
    if flush:
        sys.stderr.flush()


def debug_print(*args, sep=' ', end='\n', file=None, flush=True):
    args = (str(arg) for arg in args)  # convert to string as numbers cannot be joined
    if file == sys.stderr:
        stderr_write(sep.join(args), flush)
    elif file in [sys.stdout, None]:
        lineno = sys._getframe().f_back.f_lineno
        filename = sys._getframe(1).f_code.co_filename

        stdout = f'\033[31m{time.strftime("%H:%M:%S")}\x1b[0m  \033[32m{filename}:{lineno}\x1b[0m  {sep.join(args)} {end}'
        stdout_write(stdout, flush)
    else:
        # catch exceptions
        original_print(*args, sep=sep, end=end, file=file)


def patch_print():
    try:
        __builtins__.print = debug_print
    except AttributeError:
        __builtins__['print'] = debug_print


def remove_patch_print():
    try:
        __builtins__.print = original_print
    except AttributeError:
        __builtins__['print'] = original_print

class ConcurrentHandler(logging.Handler):
    '''
    multiprocessing logger handler, code from:
    https://github.com/jruere/multiprocessing-logging/blob/master/multiprocessing_logging.py

    '''

    def __init__(self, name, sub_handler):
        name = 'Concurrent_' + name
        super(ConcurrentHandler, self).__init__()

        assert sub_handler is not None
        self.sub_handler = sub_handler

        self.setLevel(self.sub_handler.level)
        self.setFormatter(self.sub_handler.formatter)
        self.filters = self.sub_handler.filters

        self.queue = multiprocessing.Queue(-1)
        self._is_closed = False
        # The thread handles receiving records asynchronously.
        self._receive_thread = threading.Thread(target=self._receive, name=name)
        self._receive_thread.daemon = True
        self._receive_thread.start()

    def setFormatter(self, fmt):
        super(ConcurrentHandler, self).setFormatter(fmt)
        self.sub_handler.setFormatter(fmt)

    def _receive(self):
        try:
            broken_pipe_error = BrokenPipeError
        except NameError:
            broken_pipe_error = socket.error
        
        while True:
            try:
                if self._is_closed and self.queue.empty():
                    break

                record = self.queue.get(timeout=0.2)
                self.sub_handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except (broken_pipe_error, EOFError):
                break
            except queue.Empty:
                pass  # This periodically checks if the logger is closed.
            except:
                traceback.print_exc(file=sys.stderr)

        self.queue.close()
        self.queue.join_thread()

    def _send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified. Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe.
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self._send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        if not self._is_closed:
            self._is_closed = True
            self._receive_thread.join(5.0)  # Waits for receive queue to empty.

            self.sub_handler.close()
            super(ConcurrentHandler, self).close()


class CustomFormatter(logging.Formatter):
    def __init__(self, color=False, formatter_template=0):
        self.color = color
        self.formatter_template = formatter_template

        preset_formatter_template_dict = {
            0: "%(asctime)s | %(message)s",
            1: "%(asctime)s - %(name)s - %(levelname)s | (%(filename)s:%(lineno)d) | %(message)s",
            2: "%(asctime)s - %(name)s - %(levelname)s | (%(filename)s:%(funcName)s:%(lineno)d) |  %(message)s",
        }

        if self.formatter_template is None:
            self.fmt = "%(message)s"
        elif isinstance(self.formatter_template, int):
            assert self.formatter_template in preset_formatter_template_dict, f'formatter_template {self.formatter_template} is not valid! valid formatter_templates(int) are {sorted(list(preset_formatter_template_dict.keys()))}.'
            self.fmt = preset_formatter_template_dict[self.formatter_template]
        elif isinstance(self.formatter_template, str):
            self.fmt = self.formatter_template
        else:
            raise ValueError(f'formatter_template {self.formatter_template} must be int or str or None!')

        self.color_formats = {
            logging.DEBUG: logging_color_set["underline_grey"] + self.fmt + logging_color_set["reset"],
            logging.INFO: logging_color_set["grey"] + self.fmt + logging_color_set["reset"],
            logging.WARNING: logging_color_set["yellow"] + self.fmt + logging_color_set["reset"],
            logging.ERROR: logging_color_set["red"] + self.fmt + logging_color_set["reset"],
            logging.CRITICAL: logging_color_set["red"] + self.fmt + logging_color_set["reset"]
        }
    
    def format(self, record):
        if self.color:
            self.fmt = self.color_formats.get(record.levelno)
        real_formatter = logging.Formatter(self.fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return real_formatter.format(record)


class RotatingFileSizeHandler(BaseRotatingHandler):
    def __init__(self, filename, mode='a', max_size=None, backup_count=None):
        """Rotate log file by size.
        For example, if filename is test.log, and max_size is 1M, the log files will be like:
            test.log,
            test.log.1,
            test.log.2,
            ...
        Smaller name suffix number means newer file. The lastes file is always test.log.
        Once the rotate size is reached, the log file will be rename with number suffix and a new empty log file named test.log will be created.

        There is a mechanism to resume from last log process. For example, if max_size is 1G, but the log process is killed with producing 1G log file,
        next time the log process will resume from the last 1G log file (test.log). In this condition, to prevent from logging file missing, I suggest
        set mode='a'.

        Args:
            filename (str):
                Log file path.
            mode (str, optional): 
                Defaults to 'a'.
            max_size (int || str || None, optional): 
                Rotate file size. 
                If set to int, means bytes.
                If set to str, means human readable size. Format like '10K', '10M', '10G'. Allowed units: K/KB, M/MB, G/GB. (case insensitive)
                If set to None, defaults to 10M.
                Defaults to None.
            backup_count (int || None, optional): 
                How many log files to keep. If None, default to 50. 
                Defaults to None.
        """
        super().__init__(filename, mode, 'utf-8')
        self.mode = mode
        if max_size is None:
            max_size = "10M"
        self.max_size = max_size
        self.convert_max_size()

        if backup_count is None:
            backup_count = 50
        assert isinstance(backup_count, int) and backup_count > 0, f'backup_count must be int and > 0, but got {backup_count}.'
        self.backup_count = backup_count
        #TODO: backup file clean up at start up.

        self.init_filled_bytes = os.path.getsize(self.baseFilename)
        self.first_rotate_flag = True

    
    def convert_max_size(self):
        if isinstance(self.max_size, int) or isinstance(self.max_size, float):
            self.max_byte_size = int(self.max_size)
        elif isinstance(self.max_size, str):
            max_size = self.max_size.upper()
            pattern = re.compile(r'(\d+||\.\d+||\d+\.||\d+\.\d+)\ *([A-Z]+)')
            # TODO: more exception handling
            res = pattern.findall(max_size)[0]
            digit, unit = float(res[0]), res[1].strip()

            if unit == "K" or "KB":
                self.max_byte_size = int(digit * 1024)
            elif unit == "M" or "MB":
                self.max_byte_size = int(digit * 1024 * 1024)
            elif unit == "G" or "GB":
                self.max_byte_size = int(digit * 1024 * 1024 * 1024)
        else:
            raise ValueError(f'Unsupported type of max_size ({type(self.max_size)}).')
    
    def shouldRollover(self, record):
        # record not used
        if self.max_byte_size > 0:
            try:
                self.stream.seek(0, 2)  # non-posix-compliant
                cur_file_size = self.stream.tell()
                if self.first_rotate_flag and "a" in self.mode:
                    cur_file_size += self.init_filled_bytes
                    self.first_rotate_flag = False
                if cur_file_size >= self.max_byte_size:
                    return True
            finally:
                self.close()
        return False
    
    def doRollover(self):
        # TODO: do we need to design a lock here to prevent sb uses it in multiprocessing context?
        self.close()

        rename_pairs = []
        for i in range(1, self.backup_count - 1):
            src_file = f"{self.baseFilename}.{i}"
            dst_file = f"{self.baseFilename}.{i + 1}"
            if os.path.exists(src_file):
                rename_pairs.append((src_file, dst_file))
            else:
                # eary break
                break
        
        for src_file, dst_file in reversed(rename_pairs):
            os.rename(src_file, dst_file)
        
        dst_file = f"{self.baseFilename}.1"
        os.rename(self.baseFilename, dst_file)

        self.stream = self._open()


class RotatingFileDateHandler(BaseRotatingHandler):
    def __init__(self, filename, mode='a', interval=None, backup_count=None):
        """Rotate log files based on a specified date interval.
        For example, if filename is test.log, and interval is 5s, the log files will be like:
           test.log,
           test.log.2022-02-09_15-01-54,
           test.log.2022-02-09_15-01-59,
           ...
        The lastest log file is always test.log. Once the rotate interval is reached, the log file will be rename with date
        suffix and a new empty log file named test.log will be created.

        There is a mechainsm to resume logging from last try. That is, if the interval is 1d, but the logging process is interrupted less than 1d,
        once restarted, the rotate will remember last rotate time and start from there (test.log). In this condition, to prevent from logging file missing, I suggest
        set mode='a'.

        Args:
            filename (str):
                 Log file path.
            mode (str, optional): 
                Defaults to 'a'.
            interval (str || None, optional): 
                Time interval to rotate the log file. If None, default to '1d'.
                This arg should be form like "1w2d3h4m4s". Allowed time units are:
                    w, week: week
                    d, day: day
                    h, hr, hour: hour
                    m, min, minute: minute
                    s, sec, second: second
                Defaults to None.
            backup_count (int || None, optional): 
                How many log files to keep. If None, default to 50. 
                Defaults to None.
        """
        super().__init__(filename, mode, 'utf-8')
        self.mode = mode
        if interval is None:
            interval = "1d"
        self.interval = self.parse_interval(interval)

        if backup_count is None:
            backup_count = 50
        assert isinstance(backup_count, int) and backup_count > 0, f'backup_count must be int and > 0, but got {backup_count}.'
        self.backup_count = backup_count

        self.base_name = os.path.basename(filename)
        self.log_dir = os.path.dirname(self.baseFilename)

        # at first retreive the last time
        # so that we can achieve continuous logging
        self.last_log_time = None
        all_file_names = self.find_and_sort_log_names()
        if all_file_names:
            # c.log.%Y-%m-%d_%H-%M-%S
            last_log_time_str = all_file_names[0].replace(self.base_name, "")
            if last_log_time_str.startswith("."):
                last_log_time_str = last_log_time_str[1:]

            try:
                self.last_log_time = datetime.datetime.strptime(last_log_time_str, "%Y-%m-%d_%H-%M-%S")
            except:
                self.last_log_time = None
        
        if self.last_log_time is None:
            self.last_log_time = datetime.datetime.now()

    def parse_interval(self, interval):
        # parse str like "1d2h3m4s" to seconds(int)
        x = interval.replace(' ', '').lower()
        paired_list = []
        tmp_list = []
        found_time_unit = False
        for v in x:
            if v.isnumeric() or v == ".":
                if found_time_unit:
                    paired_list.append(tmp_list)
                    tmp_list = []
                    found_time_unit = False
                if not len(tmp_list):
                    tmp_list.append('')
                tmp_list[0] += v
            else:
                if len(tmp_list) == 1:
                    tmp_list.append(v)
                else:
                    tmp_list[1] += v
                found_time_unit = True
        if tmp_list:
            if len(tmp_list) == 1:
                raise ValueError(f'Unclosed time {interval}')
            paired_list.append(tmp_list)
        
        total_seconds = 0.
        for val_str, val_unit in paired_list:
            val = float(val_str)
            if val_unit in ['w', 'week']:
                val *= 86400 * 7
            elif val_unit in ['d', 'day']:
                val *= 86400
            elif val_unit in ['h', 'hr', 'hour']:
                val *= 3600
            elif val_unit in ['m', 'min', 'miniute']:
                val *= 60
            elif val_unit in ['s', 'sec', 'second']:
                pass
            else:
                raise ValueError(f'Unknown time unit {val_unit}.')
                
            total_seconds += val
            assert total_seconds >= 1, f'Interval must be greater than 1 second, but got {interval} ({total_seconds} sec).'
            
        return int(total_seconds)

    def find_and_sort_log_names(self):
        all_file_names = os.listdir(self.log_dir)
        all_file_names = [f for f in all_file_names if f.startswith(self.base_name)]
        if self.base_name in all_file_names:
            all_file_names.remove(self.base_name)
        
        def parse_datetime_str(f):
            # c.log.%Y-%m-%d_%H:%M:%S -> datetime object
            # TODO: regex not found error handling
            pattern = re.compile(r'\.(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})')
            res = pattern.findall(f)[0]
            date_cmp = int(''.join(res))
            return date_cmp
        
        if all_file_names:
            all_file_names.sort(key=lambda x: parse_datetime_str(x), reverse=True)

        return all_file_names


    def shouldRollover(self, record):
        time_delta = datetime.datetime.now() - self.last_log_time
        if time_delta.total_seconds() >= self.interval:
            return True
        return False
    

    def doRollover(self):
        # a/b/c.log -> a/b/c.log.%Y-%m-%d_%H-%M-%S
        self.close()

        all_file_names = self.find_and_sort_log_names()
        all_file_names_to_remove = all_file_names[self.backup_count-1:]

        # remove old log files by date
        for name in all_file_names_to_remove:
            os.remove(os.path.join(self.log_dir, name))

        self.last_log_time = datetime.datetime.now()
        self.last_log_time_str = self.last_log_time.strftime("%Y-%m-%d_%H-%M-%S")

        dst_file = f"{self.baseFilename}.{self.last_log_time_str}"
        os.rename(self.baseFilename, dst_file)

        self.stream = self._open()
        

class EasyLoggerManager(object):
    """
    EasyLoggerManager is a class to manage multiple EasyLogger instances.

    (1) Create logger or bind new handler to already existed logger: Use `get_logger` func.
    e.g.:
    logger = EasyLoggerManager("test").get_logger(log_to_console=True, 
                                                  stream_handler_color=True,
                                                  log_file_path="test.log",
                                                  log_file_mode="a",
                                                  log_file_rotate_size="10KB",
                                                  formatter_template=0,
                                                  )
    Will create a logger named "test" if not existed, otherwise, just get that logger.
    Then a stream handler is added to the logger. stream_handler_color = True means that the logs will be printed in colored text in the console.
    Also, a file handler is added to the logger. The logs will also be redirected to file test.log and rotated by 10KB.
    formatter_template int value means the preset log format (0, 1, 2). String format is also supported to directly customize the log format.

    Multiprocessing support:
    logger = EasyLoggerManager("test").get_logger(log_to_console=True, 
                                                  stream_handler_color=True,
                                                  log_file_path="test.log",
                                                  log_file_mode="a",
                                                  log_file_rotate_size="10KB",
                                                  formatter_template=0,
                                                  log_file_multiprocessing=True)
    def job(i):
        time.sleep(1)
        logger.error(f'thread {i}')
    
    for i in range(4):
        p = Process(target=job, args=(i,))
        p.start()

    (2) Some class methods:
    get_logger_dict(),
    get_logger_names(),
    retrieve_logger(),
    get_logger_level()
    
    """

    def __init__(self, name, propagate=True):
        self.logger_name = name
        self.logger = logging.getLogger(name)
        # I find some libs will modify the root logger's default behavior or hook on the root level
        # in this condition, set propagate as False will prevent from this pollution.
        self.logger.propagate = propagate

    @classmethod
    def get_logger_dict(cls, filter_defined=True):
        # Python logging module defines a hierarchy of loggers
        # for example, if the logger name is "a.b.c", 
        # then loggers with name "a.b" and "a" will also be created but are type logging.PlaceHolder (undefined)
        # if filter_defined is True, these PlaceHolder loggers will be removed
        root_logger = logging.getLogger("root")
        all_logger_dict = root_logger.manager.loggerDict
        if filter_defined:
            new_logger_dict = {}
            for k, v in all_logger_dict.items():
                if not isinstance(v, logging.PlaceHolder):
                    new_logger_dict[k] = v
            return new_logger_dict
        return all_logger_dict

    @classmethod
    def get_logger_names(cls, filter_defined=True):
        return sorted(list(cls.get_logger_dict(filter_defined).keys()))

    @classmethod
    def hook_level(cls, logger_name, logger_level):
        # hook the level of a logger, may be loggers of other scope
        # this is useful when you want to change the level of a preset logger of some third-party library, e.g., urllib3
        logger_level = logging._checkLevel(logger_level)
        logger = cls.retrieve_logger(logger_name)
        logger.setLevel(level=logger_level)

    @classmethod
    def retrieve_logger(cls, logger_name):
        # retrieve a logger that has been defined by its name
        logger_dict = cls.get_logger_dict(False)
        if not logger_name in logger_dict:
            raise ValueError(f'logger with name {logger_name} has not be defined!')
        elif isinstance(logger_dict[logger_name], logging.PlaceHolder):
            raise ValueError(f'logger with name {logger_name} is type of logging.PlaceHolder and is not defined actually!')
        else:
            logger = logger_dict[logger_name]
            return logger
    
    @classmethod
    def get_logger_level(cls, logger_name):
        logger = cls.retrieve_logger(logger_name)
        return logger.level
    

    def get_logger(self,
                   level=logging.DEBUG,
                   log_to_console=True,
                   stream_handler_color=False,
                   log_file_path=None,
                   log_file_mode="a",
                   log_file_rotate_size=None,
                   log_file_backup_count=None,
                   log_file_rotate_interval=None,
                   log_file_multiprocessing=False,
                   formatter_template=0,
                   regex_filter=None):
        """
        Create/get a logger with given parameters.
        level: logging level, default is logging.DEBUG
        log_to_console: whether to log to console, default is True
        stream_handler_color: whether to colorize the stream handler, default is False
        formatter_template: the template of the formatter, default is 0

        log_file_path: the path of the log file, default is None
        log_file_mode: the mode of the log file, default is "a"
        log_file_rotate_size: the size of the log file to rotate, default is None
        log_file_backup_count: the number of backup log files to keep, default is None

        log_file_rotate_interval: the interval of the log file to rotate, default is None

        log_file_multiprocessing: whether to use multiprocessing to rotate the log file, default is False

        regex_filter: if set, only log messages that match the regex will be logged.
        """
        self.level = logging._checkLevel(level)
        self.log_to_console = log_to_console
        self.stream_handler_color = stream_handler_color
        self.log_file_path = log_file_path
        self.log_file_mode = log_file_mode
        self.log_file_rotate_size = log_file_rotate_size
        self.log_file_backup_count = log_file_backup_count
        self.log_file_rotate_interval = log_file_rotate_interval
        self.log_file_multiprocessing = log_file_multiprocessing
        self.formatter_template = formatter_template
        self.regex_filter = regex_filter

        self.logger.setLevel(self.level)

        self.add_handlers()

        self.add_filter_to_handlers()

        return self.logger

    def add_filter_to_handlers(self):
        if self.regex_filter is not None:
            pattern = re.compile(self.regex_filter)
            filter_func = lambda record: pattern.match(record.getMessage())
            for handler in self.logger.handlers:
                handler.addFilter(filter_func)
    
    def add_handlers(self):
        if self.log_to_console:
            self.add_stream_handler()
        if self.log_file_path:
            self.add_file_handler()
    
    def add_stream_handler(self):
        formater = CustomFormatter(self.stream_handler_color, self.formatter_template)
        handler = logging.StreamHandler()
        handler.setFormatter(formater)
        self.logger.addHandler(handler)
    
    def add_file_handler(self):
        # NOTE: disable log file color hightlighting
        formater = CustomFormatter(False, self.formatter_template)

        if (not self.log_file_rotate_size) and (not self.log_file_rotate_interval):
            handler = logging.FileHandler(self.log_file_path, mode=self.log_file_mode, encoding='utf-8')
        elif self.log_file_rotate_size and self.log_file_rotate_interval:
            raise ValueError('log_file_rotate_size and log_file_rotate_interval cannot be both set!')
        elif self.log_file_rotate_size:
            handler = RotatingFileSizeHandler(self.log_file_path, mode=self.log_file_mode, max_size=self.log_file_rotate_size, backup_count=self.log_file_backup_count)
        elif self.log_file_rotate_interval:
            handler = RotatingFileDateHandler(self.log_file_path, mode=self.log_file_mode, interval=self.log_file_rotate_interval, backup_count=self.log_file_backup_count)

        handler.setFormatter(formater)

        if self.log_file_multiprocessing:
            handler = ConcurrentHandler(self.logger_name, sub_handler=handler)
        
        self.logger.addHandler(handler)
            

if __name__ == "__main__":
    import time
    from tqdm import trange
    from multiprocessing import Process
    import time
    import random
    logger = EasyLoggerManager("test").get_logger(level=logging.DEBUG,
                                                  log_to_console=True, 
                                                  stream_handler_color=True,
                                                  formatter_template=2,
                                                  log_file_mode="a",
                                                #   log_file_path="test.log",
                                                  log_file_path=None,
                                                  log_file_rotate_size="20KB",
                                                #   log_file_rotate_interval="5s",
                                                  log_file_backup_count=50,
                                                  log_file_multiprocessing=True,
                                                  regex_filter=r'.*5.*')
    print('starting to logging')

    print(type(logger))
    print(logger.handlers)

    print('here')
    for hdler in logger.handlers:
        print(hdler)
        print(hasattr(hdler, 'filter'))
        f = hdler.filter
        print(hdler.filters)
        # print(f.__code__)

    for i in trange(10):
        logger.info(i)
        time.sleep(0.05)
