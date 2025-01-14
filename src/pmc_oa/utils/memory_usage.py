import psutil
def get_memory_usage(msg:str):
    """
    Prints the memory usage of the current process in gigabytes.

    Parameters
    ----------
    msg : str
        A message to display alongside the memory usage information.

    Notes
    -----
    The function retrieves the memory usage of the current process
    using `psutil.Process().memory_info().rss`, which returns the
    Resident Set Size (RSS) — the portion of memory occupied by the process
    in RAM. The memory usage is then displayed in gigabytes.

    Examples
    --------
    >>> get_memory_usage("Current memory usage")
    Current memory usage: 0.12 GB
    """
    print(f"{msg}: {psutil.Process().memory_info().rss / (1024 * 1024 * 1024):.2f} GB")

