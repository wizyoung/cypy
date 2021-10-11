from calendar import monthrange
from datetime import datetime

class Duration(object):
    '''
    d1 = Duration(1234)
    d2 = Duration(2)
    print(f'before is {d1},after is {d1 + d2}')

    d1 += d2
    d1 += 1

    d3 = Duration(1234.54)
    '''
    def __init__(self, duration):
        assert isinstance(duration, int) or isinstance(duration, float)
        # assert duration < 86400, f'duration should be less than 1 day.'
        self.duration = duration

        self.reformat()

    def reformat(self):
        self.hour = int(self.duration // 3600)
        self.miniute = int((self.duration - self.hour * 3600) // 60)
        self.second = int(self.duration - self.hour * 3600 - self.miniute * 60)
        # keep two valid digits
        self.subsecond = round(self.duration - self.hour * 3600 - self.miniute * 60 - self.second, 2)

        if self.subsecond == 0:
            self.data_str = f'{self.hour:02d}:{self.miniute:02d}:{self.second:02d}'
        else:
            self.subsecond_digit_len = len(str(self.subsecond)) - 2
            self.subsecond_digit_num = int(self.subsecond * (10 ** self.subsecond_digit_len))
            self.subsecond_digit_str = str(self.subsecond_digit_num)[:2]
            if len(self.subsecond_digit_str) < 2:
                self.subsecond_digit_str = self.subsecond_digit_str + '0'
            self.data_str = f'{self.hour:02d}:{self.miniute:02d}:{self.second:02d}.{self.subsecond_digit_str}'
        return self.data_str

    def __repr__(self):
        return self.data_str

    def __add__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return Duration(self.duration + other)
        elif isinstance(other, Duration):
            return Duration(self.duration + other.duration)

    def __iadd__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            self.duration += other
            self.reformat()
        elif isinstance(other, Duration):
            self.duration += other.duration
            self.reformat()
        return self


def date_format_check(date_str):
    '''
    func:
        Check whether data_str is a valid data format. e.g. "20201201".

    input:
        date_str -> str: input date str.

    return:
        check_flag -> bool: Whether the input date_str is valid or not.
    '''
    if not isinstance(date_str, str):
        return False
    try:
        if date_str != datetime.strptime(date_str, "%Y%m%d").strftime('%Y%m%d'):
            return False
    except ValueError:
        return False
    return True


def get_target_date_range(start_str, end_str=None):
    '''
    func:
        Given a time range, from all the days in the given range

    input: 
        start_str -> str: start timestamp, in the format of "20201201"
        end_str -> None or str: if None, end_str will be the same as start_str

    return:
        range_days -> list: a list of all the days in the given time range, each item is a str.
    '''
    assert date_format_check(start_str), "start_str [{}] is not a valid time format!".format(start_str)
    if end_str:
        assert date_format_check(end_str), "end_str [{}] is not a valid time format!".format(end_str)
    else:
        end_str = start_str
        
    assert int(end_str) >= int(start_str), "end_str [{}] should be later than start_str [{}] in calendar!".format(end_str, start_str)
    
    start_year, start_month = int(start_str[:4]), int(start_str[4:6])
    end_year, end_month = int(end_str[:4]), int(end_str[4:6])
    
    range_days = []
    
    if start_year == end_year:
        range_days.extend(['{:04d}{:02d}{:02d}'.format(start_year, m, d) \
            for m in range(start_month, end_month + 1) \
            for d in range(1, monthrange(start_year, m)[1] + 1)])
    else:
        range_days.extend(['{:04d}{:02d}{:02d}'.format(start_year, m, d) \
            for m in range(start_month, 13) \
            for d in range(1, monthrange(start_year, m)[1] + 1)])
        range_days.extend(['{:04d}{:02d}{:02d}'.format(y, m, d) \
            for y in range(start_year + 1, end_year)\
            for m in range(1, 13) \
            for d in range(1, monthrange(y, m)[1] + 1)])
        range_days.extend(['{:04d}{:02d}{:02d}'.format(y, m, d) \
            for y in range(end_year, end_year + 1)\
            for m in range(1, end_month + 1) \
            for d in range(1, monthrange(y, m)[1] + 1)])
        
    start_idx = range_days.index(start_str)
    end_idx = len(range_days) - 1 - range_days[::-1].index(end_str)
    return range_days[start_idx: end_idx+1]