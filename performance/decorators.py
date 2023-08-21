import functools
from time import perf_counter
from typing import Callable, Any
from logging import Logger, getLogger, StreamHandler, DEBUG


def execution_timer(n: int = 1, passed_logger: Logger | None = None) -> Callable[..., Any]:
    """
    Measures running time of decorated function.
    If n is provided, runs function n times, but returns result of the first run.
    Be aware of passing n argument on functions that alter mutable objects in your code.
    :param n: number of times function will be run, only result of the first run will be returned
    :param passed_logger: logger object you want to use. If not provided, the basic one will be created
    """
    if n < 1:
        raise ValueError('n should be greater than 0.')

    if passed_logger is None:
        passed_logger = getLogger(__name__)
        sh = StreamHandler()
        passed_logger.setLevel(DEBUG)
        passed_logger.addHandler(sh)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start = perf_counter()
            value = func(*args, **kwargs)
            for _ in range(n - 1):
                func(*args, **kwargs)
            end = perf_counter()
            plural_s = ['s', ''][n == 1]
            passed_logger.debug(f'{n} execution{plural_s} of {func.__name__} took {end - start} sec')
            return value
        return wrapper
    return decorator
