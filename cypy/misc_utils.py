import os
import subprocess
import functools
import inspect
import warnings

from cypy.logging_utils import logging_color_set


class LazyImport:
    '''
    Python LazyImport, from https://zhuanlan.zhihu.com/p/274760625
    Sometimes sub-module A may conflict with module B, but when we call methods from B,
    A is not needed. So we can use LazyImport to avoid import A when we need B.

    e.g:
    > sub-module A:
    change:
        import decord
    to:
        decord = LazyImport('decord')
    can avoid import decord as the very beginning.

    Then, we can use decord.VideoReader to read video.

    '''
    def __init__(self, module_name):
        self.module_name = module_name
        self.module = None
    
    def __getattr__(self, name):
        if self.module is None:
            self.module = __import__(self.module_name)
        return getattr(self.module, name)


def get_cmd_output(cmd):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    return stdout.decode("utf-8").strip(), stderr.decode("utf-8").strip()


def color_print(info, color='grey'):
    available_colors = list(logging_color_set.keys())
    available_colors.remove('reset')
    assert color in available_colors, 'color must be one of {}'.format(available_colors)
    info = logging_color_set[color] + info + logging_color_set['reset']
    print(info)


def warning_prompt(warning_str, color='yellow'):
    assert color in ['yellow', 'green', 'red'], "Now only `yellow`, `green`, `red` colors are supported in warning_prompt!"
    print(logging_color_set[color] + warning_str + logging_color_set['reset'])  # yellow
    passed = False
    for _ in range(3):
        inp = input(logging_color_set[color] + 'Continue? [y/n]: ' + logging_color_set['reset']).strip()  # yellow
        if not inp.upper() in ['Y', 'N', 'YES', 'NO']:
            print(logging_color_set['red'] + 'Invalid input!' + logging_color_set['reset'])  # red
            continue
        elif inp.upper() in ['Y', 'YES']:
            passed = True
            break
        else:
            print(logging_color_set['red'] + 'Cancelled. ' + logging_color_set['reset'])  # red
            break
    if not passed:
        os._exit(0)


def warn_print(x):
    x = str(x)
    x = "\x1b[33;1m" + x + "\x1b[0m"
    print(x)


def verbose_print(s, verbose=True, color=True):
    if verbose:
        if color:
            warn_print(s)
        else:
            print(s)

# Ref: https://stackoverflow.com/a/40301488/6631854
string_types = (type(b''), type(u''))
def deprecated(reason):
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    """

    if isinstance(reason, string_types):

        # The @deprecated is used with a 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated("please, use another function")
        #    def old_function(x, y):
        #      pass

        def decorator(func1):

            if inspect.isclass(func1):
                fmt1 = "Call to deprecated class {name} ({reason})."
            else:
                fmt1 = "Call to deprecated function {name} ({reason})."

            @functools.wraps(func1)
            def new_func1(*args, **kwargs):
                warnings.simplefilter('always', DeprecationWarning)
                warnings.warn(
                    fmt1.format(name=func1.__name__, reason=reason),
                    category=DeprecationWarning,
                    stacklevel=2
                )
                warnings.simplefilter('default', DeprecationWarning)
                return func1(*args, **kwargs)

            return new_func1

        return decorator

    elif inspect.isclass(reason) or inspect.isfunction(reason):

        # The @deprecated is used without any 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated
        #    def old_function(x, y):
        #      pass

        func2 = reason

        if inspect.isclass(func2):
            fmt2 = "Call to deprecated class {name}."
        else:
            fmt2 = "Call to deprecated function {name}."

        @functools.wraps(func2)
        def new_func2(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                fmt2.format(name=func2.__name__),
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)
            return func2(*args, **kwargs)

        return new_func2

    else:
        raise TypeError(repr(type(reason)))


if __name__ == "__main__":
    warning_prompt('This is a test')
    print('here')