import os

try:
    from .logging_utils import logging_color_set
except:
    # inner import
    from logging_utils import logging_color_set


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


if __name__ == "__main__":
    warning_prompt('This is a test')
    print('here')