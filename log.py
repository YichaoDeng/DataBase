import time

from loguru import logger

logger.add(sink='log.txt', format="{time} {level} {message}", level="INFO")


def get_func_params(func, *args, **kwargs):
    dict_param = {}
    if len(args) > 0:
        var_names = func.__code__.co_varnames
        for index, name in enumerate(var_names):
            dict_param.update({name: func.__closure__[index].cell_contents})

    return dict_param


def log_wrapper(func):
    def inner(*args, **kwargs):
        timestamp = int(time.time() * 1000)
        res = func(*args, **kwargs)
        func_params = get_func_params(func, *args, **kwargs)
        logger.info(f"{timestamp}, {func_params.get('statement')}, {res}")
        return res

    return inner


@log_wrapper
def foo(a, b):
    return a + b


if __name__ == '__main__':
    foo(1, 2)
