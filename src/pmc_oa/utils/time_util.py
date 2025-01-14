import time


def timing(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        self = args[0]
        self.logger.info(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper