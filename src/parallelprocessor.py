import collections
import sys

from multiprocessing import Pool, cpu_count
from typing import Callable, Hashable, Tuple

################################################################################


class ParallelProcessor:
    """A class to run a process in parallel.

    Args:
        worker (func, optional): The python function to run in parallel. Can be set using ParallelProcessor.set_worker(func).
                Function must be imported into main script. Defaults to None.
        args (dict, optional): A dictionary of IDs & arguments to pass to the worker function.
                Can be set with ParallelProcessor.set_arguments() or ParallelProcessor.set_argument() Defaults to None.
        threads (int, optional): The number of CPU threads to use. Defaults to cpu_count().
    """

    def __init__(
        self,
        worker: Callable = None,
        processing_args: dict = None,
        threads: int = cpu_count(),
    ) -> None:

        self.threads = threads
        self.set_worker(worker) if worker else setattr(self, "worker", None)
        self.set_arguments(processing_args) if processing_args else setattr(
            self, "args", {}
        )
        self.results = {}

    ############################################################################

    def set_worker(self, worker: Callable) -> None:
        """Set the function to be run in parallel."""

        self.worker = worker

    ############################################################################

    def set_arguments(self, args: dict) -> None:
        """Set the arguments to be run by the worker function. Stored as a dict of id's and arguments."""

        if type(args) == dict:
            self.args = args

        else:
            self.args = {}
            raise ValueError(
                f"ParallelProcessor.set_arguments: Arguments must be of type 'dict' not '{type(args).__name__}'. Arguments not added."
            )

    ############################################################################

    def add_argument(self, arg_id: Hashable, arg: Tuple) -> None:
        """Add an argument and arguement id to the argument dictionary.

        Args:
            arg_id (Hashable): Argument ID. Used for retrieving outputs from self.results.
            arg (Tuple): A tuple of arguments to pass to the worker function.

        Raises:
            ValueError: Raises ValueError if arg_id is already in use
        """

        # Verify arg_id is hashable
        if not isinstance(arg_id, collections.Hashable):
            raise ValueError(
                f"ParallelProcessor.add_argument: Argument ID '{arg_id}' is not hashable. Argument not added."
            )

        # Add argument ID and argument as dictionary entry to self.args
        if not arg_id in self.args.keys():
            self.args[arg_id] = arg

        else:
            raise ValueError(
                f"ParallelProcessor.add_argument: Argument ID '{arg_id}' already in use. Argument not added."
            )

    ############################################################################

    def run(self, progressbar=True):
        f"""Run worker function {type(self.worker).__name__} in parallel using argument list ({len(self.args)} arguments stored)."""

        # Validate worker function is set
        if not self.worker or not callable(self.worker):
            raise AttributeError(
                "ParallelProcessor.run: Worker is not callable. Please set the worker function with ParallelProcessor.set_worker"
            )

        # Validate args exist
        if not self.args:
            raise AttributeError(
                "ParallelProcessor.run: Arguments are not set. Please set the arguments to be passed to the workfer function with ParallelProcessor.set_arguments"
            )

        # Validate thread arg.
        if self.threads > cpu_count():
            self.threads = cpu_count()

        # Initialize processing Pool.
        pool = Pool(processes=self.threads)

        # Dictionary of arg tuples.
        self.args = {k: v if type(v) == tuple else (v,) for k, v in self.args.items()}

        # Map args to Pool processes.
        processes = {
            k: pool.apply_async(self.worker, args=v) for k, v in self.args.items()
        }

        # Get number of processes for progress bar.
        num_processes = len(processes)

        # Run processes, retrieve results, print progress.
        for i, item in enumerate(processes.items()):
            i += 1
            k, v = item
            self.results[k] = v.get()
            if progressbar:
                progress_str = "\rdone {0:.2%}".format(i / num_processes)
                sys.stdout.write(progress_str)
                if i == num_processes:
                    print("")


################################################################################


def main():
    pass


################################################################################

if __name__ == "__main__":
    main()
