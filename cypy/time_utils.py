

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