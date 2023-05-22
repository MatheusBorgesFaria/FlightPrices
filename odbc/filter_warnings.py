from functools import wraps
from warnings import filterwarnings


def filter_warnings(func):
    """Decorator to filter warning mensage"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        filterwarnings("ignore")
        result = func(*args, **kwargs)
        filterwarnings("default")
        return result

    return wrapper