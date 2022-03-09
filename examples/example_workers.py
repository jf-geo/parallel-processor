from typing import Any
from osgeo import gdal
from time import sleep
from random import randint

gdal.UseExceptions()

########################################################################################


def gdal_translate(*args, **kwargs):
    """Wrapper of gdal.Translate.

    gdal.Translate returns a dataset object which must be brought out of scope for the file to be written to disc.

    Returns True if successful
    """

    dst = gdal.Translate(*args, **kwargs)

    dst = None

    return True


########################################################################################


def print_process_id(process_id: Any, sleep_min: int = 1, sleep_max: int = 10) -> Any:
    """Example function. Sleeps for a random time and then prints and returns the process_id.

    Args:
        process_id (Any): Anything that can be printed.
        sleep_min (int, optional): Minimum time to sleep for. Defaults to 1.
        sleep_max (int, optional): Maximum time to sleep for. Defaults to 10.

    Returns:
        Any: Returns the input process_id.
    """

    sleep(randint(sleep_min, sleep_max))

    print(f"Running process: {process_id}.")

    return process_id


########################################################################################
