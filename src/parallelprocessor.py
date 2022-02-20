# -*- coding: utf-8 -*-

"""ParallelProcessor

Requirements:
    - pyton 3.7+
    - tqdm (optional)

This script allows the user to parallize a process using multiprocessing.Pool.
For best results, define the function in a separate file with light dependancies
 and import it before using it. 
 
For more information and examples see ParallelProcessor.__doc__
"""

import collections
import time
import sys

from importlib.util import find_spec
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import AsyncResult
from typing import Callable, Hashable, Tuple


################################################################################

__author__ = "James Ford"
__copyright__ = "Copyright (c) 2022 James Ford"
__credits__ = ["James Ford"]
__license__ = """CC BY-NC-SA 4.0
https://creativecommons.org/licenses/by-nc-sa/4.0/
Copyright (c) 2022 James Ford
"""

__version__ = "1.0.0"
__maintainer__ = "James Ford"
__email__ = "irvine.ford@gmail.com"
__status__ = "Prod"

################################################################################


class ParallelProcessor:
    """A class to run processs in parallel.


    Designed for parallelizing a single function across numerous arguments.
    Utilizies multiprocessing.Pool and runs processes asynchronously.
    Optionally provides a progressbar using tqdm or a basic custom progressbar if tqdm is not available.


    Args:
        worker (func, optional): The python function to run in parallel. Can be set using ParallelProcessor.set_worker(func).
                Function must be imported into main script. Defaults to None.
        threads (int, optional): The number of CPU threads to use. Defaults to cpu_count().


    Examples:

    ############################################################################

    Example 1 - Simple use of ParallelProcessor:
    
    >>> from parallelprocessor import ParallelProcessor

    >>> def worker_func(process_id):
    >>>     print(f"Running process: {process_id}.")
    >>>     return process_id

    >>> parallel_processor = ParallelProcessor(worker_func)

    >>> for i in range(4):
    >>>     parallel_processor.add_argument(process_id=i, func_args=(i,))

    >>> parallel_processor.run()
    
    >>> results = parallel_processor.results

    ############################################################################

    Example 2 - Define a function, set multiple processes, run in parallel with a progressbar and a 1 minute timeout set:
    
    >>> from parallelprocessor import ParallelProcessor

    >>> # For best performance define this function in a separate file and import it
    >>> def worker_func(args, kwargs):
    >>>     result = do_something()
    >>>     return result

    >>> parallel_processor = ParallelProcessor(worker_func, threads=4)

    >>> parallel_processor.add_argument(process_id=1, func_args=(1, ), func_kwargs={"letter": "a"})
    >>> parallel_processor.add_argument(process_id=2, func_args=(2, ), func_kwargs={"letter": "b"})
    >>> parallel_processor.add_argument(process_id=3, func_args=(3, 4), func_kwargs={"letter": "c"})
    >>> parallel_processor.add_argument(process_id=4, func_args=(5, 6), func_kwargs={"letter": "d"})

    >>> parallel_processor.run(progressbar=True, timeout=60)
    
    >>> results = parallel_processor.results

    ############################################################################

    Example 3 - Convert a directory of ECWs to GeoTIFFs using gdal:
    
    >>> from parallelprocessor import ParallelProcessor
    >>> from osgeo import gdal
    >>> from pathib import Path

    >>> gdal.UseExceptions

    >>> kwargs = {
    >>>     "options": [
    >>>             "TILED=YES",
    >>>             "COMPRESS=DEFLATE",
    >>>             "PREDICTOR=2",
    >>>         ]
    >>> }

    >>> ecw_dir = "C:/Path to ECW files"
    >>> gtiff_dir = "C:/Output Folder"
    >>> ecw_ext = ".ecw"
    >>> gtiff_ext = ".tiff"

    >>> ecws = Path(ecw_dir).rglob(f"*{ecw_ext}")

    >>> parallel_processor = ParallelProcessor(gdal.Translate)

    >>> for ecw in ecws:
    >>>     id = ecw.name
    >>>     input_filename = ecw.as_posix()
    >>>     output_filename = Path(gtiff_dir).joinpath(f"{ecw.stem}.{gtiff_ext}").as_posix()
    >>>     args = (output_filename, input_filename)
    >>>     parallel_processor.add_argument(process_id=id, func_args=args, func_kwargs=kwargs)

    >>> parallel_processor.run(progressbar=True, timeout=60*10)
    
    >>> results = parallel_processor.results

    ############################################################################

    """

    def __init__(self, worker: Callable = None, threads: int = cpu_count(),) -> None:

        # Set attributes based on __init__ arguments
        self.threads = threads
        self.set_worker(worker) if worker else setattr(self, "worker", None)

        # Initialize empty attributes
        self.ids = set()
        self.args = {}
        self.kwargs = {}
        self.processes = {}
        self.results = {}

        # Initialize processing pool
        self._init_pool()

    ############################################################################

    def _init_pool(self):
        """Initialize multiprocessing.Pool as self.pool"""

        if self.threads > cpu_count():
            self.threads = cpu_count()

        self.pool = Pool(processes=self.threads)

    ############################################################################

    def _pool_apply_async(
        self, worker: Callable = None, args: Tuple = None, kwargs: dict = None
    ) -> AsyncResult:
        """Add a function with args/kwargs to the processing queue.

        
        Uses ParallelProcessor.worker as the worker function unless otherwise specified.
        Arguments args and/or kwargs must be passed.

        Args:
            worker (Callable, optional): Worker function to use. If 'None' defaults to ParallelProcessor.worker. Defaults to None.
            args (Tuple, optional): A tuple of args to be passed to the worker function. Defaults to None.
            kwargs (dict, optional): A dictionary of kwargs to be passed to the worker function. Defaults to None.

        Returns:
            AsyncResult: Result of Pool.apply_async().

        Raises:
            AttributeError: Raises an AttributeError if the worker function is not valid.
            ValueError: Raises a ValueError if neither args nor kwargs are passed.
        """

        if worker is None:
            worker = self.worker

        # Validate worker function is set
        if not worker or not callable(worker):
            raise AttributeError(
                "ParallelProcessor._pool_apply_async: 'worker' is not callable. Please set the worker function with ParallelProcessor.set_worker"
            )

        if args is None and kwargs is None:
            raise ValueError(
                f"ParallelProcessor._pool_apply_async: Arguments args= and/or kwargs= must be passed. Both are 'None' by default."
            )

        # Create async process
        if args and kwargs:
            async_result = self.pool.apply_async(worker, args=args, kwds=kwargs)
        elif args and not kwargs:
            async_result = self.pool.apply_async(worker, args=args)
        else:
            async_result = self.pool.apply_async(worker, kwds=kwargs)

        # Return async_result
        return async_result

    ############################################################################

    def set_worker(self, worker: Callable) -> None:
        """Set the function to be run in parallel."""

        self.worker = worker

    ############################################################################

    def add_argument(
        self, process_id: Hashable, func_args: Tuple = None, func_kwargs: dict = None
    ) -> None:
        """Add an argument and arguement id to the argument dictionary.


        Note: If only passing one func_arg it must be formatted (arg, ) otherwise it will produce an error.

        Args:
            process_id (Hashable): Process ID. Used for retrieving outputs from self.results.
            func_args (Tuple): A tuple of args to pass to the worker function.
            func_kwargs (dict): A dictionary of kwargs to pass to the worker function.

        Raises:
            ValueError: Raises ValueError if process_id is already in use
        """

        # Verify process_id is hashable
        if not isinstance(process_id, collections.Hashable):
            raise ValueError(
                f"ParallelProcessor.add_argument: Argument ID '{process_id}' is not hashable. Argument not added."
            )

        # Verify func_args &/or func_kwargs are passed
        if not func_args and not func_kwargs:
            raise ValueError(
                f"ParallelProcessor.add_argument: Neither func_args or func_kwargs passed. Argument not added."
            )

        # Add process_id, func_args, and fun_kwargs to class instance attributes
        if not process_id in self.ids:
            self.ids.add(process_id)
            self.args[process_id] = func_args
            self.kwargs[process_id] = func_kwargs

        # If process_if is already in use raise a ValueError
        else:
            raise ValueError(
                f"ParallelProcessor.add_argument: Argument ID '{process_id}' already in use. Argument not added."
            )

    ############################################################################

    def _create_processes(self):
        """Create processes from saved process_ids, func_args, and func_kwargs.
        
        
        After ParallelProcessor._create_processes is run the pool is closed, preventing new processes from being created.

        Raises:
            AttributeError: Raises an Attribute error if not arguments are stored
        
        """

        # Verify arguments exist
        if not self.ids or (not self.args and not self.kwargs):
            raise AttributeError(
                "ParallelProcessor._create_processes: Processes cannot be created as arguments have not been stored with ParallelProcessor.set_arguments."
            )

        # Create processes
        self.processes = {
            id: self._pool_apply_async(
                self.worker, args=self.args[id], kwargs=self.kwargs[id]
            )
            for id in self.ids
        }

        # Prevent pool from taking new tasks
        self.pool.close()

    ############################################################################

    def run(self, progressbar: bool = False, timeout: float = 60.0 * 10):
        f"""Run worker function {type(self.worker).__name__} in parallel using multiprocessing.Pool and arguments provided ({len(self.ids)} arguments stored).

        Args:
            progressbar (bool, optional): Whether to or not to display progress using tqdm. Defaults to False.
            timeout (float, optional): The timeout for each process in seconds. Defaults to 10 minutes.
        """

        # Create processes
        self._create_processes()

        # Ensure timeout is a float
        timeout = float(timeout)

        # Retrieve progressbar function if dependancies are met
        if progressbar:
            if find_spec("tqdm"):
                from tqdm import tqdm

                progressbar_func = tqdm
            else:
                print("Could not import tqdm. Basic progressbar will be used.")
                progressbar_func = BasicProgressBar
        else:
            progressbar_func = None

        # Run processes, retrieve results, print progress.
        if progressbar and progressbar_func:
            for item in progressbar_func(self.processes.items()):
                process_id, async_result = item
                self.results[process_id] = async_result.get(timeout)

        else:
            for item in self.processes.items():
                process_id, async_result = item
                self.results[process_id] = async_result.get(timeout)

        print(
            "Processing complete. Results can be accessed via ParallelProcessor.results"
        )


################################################################################


class BasicProgressBar:
    """Basic progressbar with no dependancies other than python 3.7+"""

    ############################################################################

    def __init__(self, iterator):
        self.len = len(iterator)
        self.i = 0
        self._iterator = iterator.__iter__()

    ############################################################################

    def __iter__(self):
        return self

    ############################################################################

    def __next__(self):

        if self.i == 0:
            self.start_time = time.time()

        msg = (
            f"\rCompleted {self.i}/{self.len} processes. {self._time_passed()} passed."
        )

        if self.i == self.len:
            msg += "\n"

        sys.stdout.write(msg)

        self.i += 1

        return next(self._iterator)

    ############################################################################

    def _time_passed(self):

        time_passed = time.time() - self.start_time

        h = int(time_passed // 60 ** 2)
        m = int((time_passed - h * 60 ** 2) // 60)
        s = time_passed - (h * 60 ** 2 + m * 60)

        return f"{h} hours {m} minutes {s:.2f} seconds"


################################################################################


def main():
    pass


################################################################################

if __name__ == "__main__":
    main()
