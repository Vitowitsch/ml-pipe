import logging
from functools import wraps


def log_result(func):
    """Log within function chaining."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logging.debug("%s,%s" % (func.__name__, result.columns))
        return result

    return wrapper


@log_result
def step(df, op):
    """Run pipeline step func."""
    return df.pipe(op)


def exec(df, ops):
    """Execute all pipeline ops."""
    df = df.fillna(0)
    if not ops:
        return df
    model = ops.pop(0)
    op = model.apply

    return exec(step(df, op), ops)
