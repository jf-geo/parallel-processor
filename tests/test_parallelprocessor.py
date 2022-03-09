"""Tests for parallelprocessor.py"""

import os
import re
import sys
import multiprocessing

from contextlib import redirect_stdout
from importlib.util import find_spec
from io import StringIO

# pylint: disable=invalid-name
# pylint: disable=redefined-outer-name
# pylint: disable=import-outside-toplevel
# pylint: disable=unused-import
# pylint: disable=import-error
# pylint: disable=reimported
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

# Add src directory to sys.path
sys.path.insert(
    1, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

########################################################################################


def dummy_worker_1(x):
    """Dummy worker function for testing ParallelProcessor.run"""

    return x ** 2


########################################################################################


def dummy_worker_2(a, b):
    """Dummy worker function for testing ParallelProcessor.run"""

    return a * b


########################################################################################


def dummy_worker_3(a, b=1):
    """Dummy worker function for testing ParallelProcessor.run"""

    return a * b


########################################################################################


def test_parallelprocessor_import_1():
    """Test that parallelprocessor can be found by importlib."""

    assert find_spec("parallelprocessor")


########################################################################################


def test_parallelprocessor_import_2():
    """Test that BasicProgressBar can be imported without errors."""

    try:
        from parallelprocessor import BasicProgressBar

        result = True

    except ModuleNotFoundError:
        result = False

    assert result


########################################################################################


def test_parallelprocessor_import_3():
    """Test that ParallelProcessor can be imported without errors."""

    try:
        from parallelprocessor import ParallelProcessor

        result = True

    except ModuleNotFoundError:
        result = False

    assert result


########################################################################################

# Import classes to test
from parallelprocessor import ParallelProcessor, BasicProgressBar

########################################################################################


def test_basic_progress_bar():
    """Test BasicProgressBar works as expected."""

    # Define progress print out regex pattern
    progress_pattern = "".join(
        (
            "^Completed [0-9]+/[0-9]+ processes[.] [0-9]{1,2} hours",
            " [1-6]*[0-9] minutes [1-6]*[0-9][.][0-9]{2} seconds passed[.]$",
        )
    )

    # Number of itervals
    n = 10

    # Variable to capture stdout as a string
    stdout_str = StringIO()

    # Capture BasicProgressBar output from iterating through range(n)
    with redirect_stdout(stdout_str):
        _ = list(BasicProgressBar(range(n)))

    # Get stdout as string
    output = stdout_str.getvalue()

    # Strip string and split based on carriage return
    output_split = output.strip().split("\r")

    # Check that progressbar has printed the correct number of times
    assert len(output_split) == n + 1

    # Check that the last character printed is a newline character
    assert output.endswith("\n")

    # Check that all printed outputs match the correct pattern
    assert all(re.match(progress_pattern, e) for e in output_split)


########################################################################################


def test_parallelprocessor_init_1():
    """Test ParallelProcessor.__init__() works."""

    # Check that ParallelProcessor can be initialized without error.
    parallel_processor = ParallelProcessor()

    assert parallel_processor


########################################################################################


def test_parallelprocessor_init_2():
    """Test ParallelProcessor.__init__() works as expected."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # Check that attributes are set as expected

    assert getattr(parallel_processor, "threads") == os.cpu_count()

    assert getattr(parallel_processor, "worker") is None

    ids = getattr(parallel_processor, "ids")
    assert not ids and isinstance(ids, set)

    args = getattr(parallel_processor, "args")
    assert not args and isinstance(args, dict)

    kwargs = getattr(parallel_processor, "kwargs")
    assert not kwargs and isinstance(kwargs, dict)

    processes = getattr(parallel_processor, "processes")
    assert not processes and isinstance(processes, dict)

    results = getattr(parallel_processor, "results")
    assert not results and isinstance(results, dict)

    assert isinstance(getattr(parallel_processor, "_pool"), multiprocessing.pool.Pool)


########################################################################################


def test_parallelprocessor_init_3():
    """Test ParallelProcessor.__init__() works as expected."""

    # worker func
    _worker = dummy_worker_1

    # Threads arg
    _threads = 2

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor(worker=_worker, threads=_threads)

    # Check that threads attribute is set properly
    assert getattr(parallel_processor, "threads") == _threads

    # Check that worker function is set properly
    assert getattr(parallel_processor, "worker").__name__ == _worker.__name__


########################################################################################


def test__pool_apply_async_1():
    """Test ParallelProcessor._pool_apply_async with args only."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    # args to pass
    _args = ("Hello World",)

    # Set worker
    parallel_processor.set_worker(_worker)

    # Test ParallelProcessor._pool_apply_async
    parallel_processor._pool_apply_async(worker=_worker, args=_args)

    assert True


########################################################################################


def test__pool_apply_async_2():
    """Test ParallelProcessor._pool_apply_async with kwargs only."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    # kwargs to pass
    _kwargs = {"end": "\n"}

    # Test ParallelProcessor._pool_apply_async
    parallel_processor._pool_apply_async(worker=_worker, kwargs=_kwargs)

    assert True


########################################################################################


def test__pool_apply_async_3():
    """Test ParallelProcessor._pool_apply_async with args and kwargs."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    # args to pass
    _args = ("Hello World",)

    # kwargs to pass
    _kwargs = {"end": "\n"}

    # Test ParallelProcessor._pool_apply_async
    parallel_processor._pool_apply_async(worker=_worker, args=_args, kwargs=_kwargs)

    assert True


########################################################################################


def test__pool_apply_async_4():
    """Test ParallelProcessor._pool_apply_async without args or kwargs."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    try:
        # Test ParallelProcessor._pool_apply_async
        parallel_processor._pool_apply_async(worker=_worker)
        result = False

    # Expecting a ValueError to be raised
    except ValueError:
        result = True

    assert result


########################################################################################


def test__pool_apply_async_5():
    """Test ParallelProcessor._pool_apply_async without worker."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # args to pass
    _args = ("Hello World",)

    # kwargs to pass
    _kwargs = {"end": "\n"}

    try:
        # Test ParallelProcessor._pool_apply_async
        parallel_processor._pool_apply_async(args=_args, kwargs=_kwargs)
        result = False

    # Expecting an AttributeError to be raised
    except AttributeError:
        result = True

    assert result


########################################################################################


def test_set_worker():
    """Test ParallelProcessor.set_worker"""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    # Set worker
    parallel_processor.set_worker(_worker)

    # Check that worker func has been set
    assert parallel_processor.worker is _worker


########################################################################################


def test_add_argument_1():
    """Test ParallelProcessor.add_argument works as expected."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # process_id to pass
    _process_id = 1

    # args to pass
    _func_args = ("a",)

    # kwargs to pass
    _func_kwargs = {"foo": "bar"}

    # Add arguments to ParallelProcessor instance
    parallel_processor.add_argument(
        process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
    )

    # Get set ids, args, kwargs dictionaries from ParallelProcessor instance
    ids = getattr(parallel_processor, "ids")
    args = getattr(parallel_processor, "args")
    kwargs = getattr(parallel_processor, "kwargs")

    # Check that process_ids are in ids
    # Check that args & kwargs are as expected
    tests = [
        _process_id in ids,
        _process_id in args,
        _process_id in kwargs,
        args[_process_id] == _func_args,
        kwargs[_process_id] == _func_kwargs,
    ]

    assert all(tests)


########################################################################################


def test_add_argument_2():
    """Test ParallelProcessor.add_argument raises a ValueError if process_id is not
     hashable."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # process_id to pass
    _process_id = []

    # args to pass
    _func_args = ("a",)

    # kwargs to pass
    _func_kwargs = {"foo": "bar"}

    try:
        # Add arguments to ParallelProcessor instance
        parallel_processor.add_argument(
            process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
        )
        result = False

    # Expecting a ValueError to be raised
    except ValueError:
        result = True

    assert result


########################################################################################


def test_add_argument_3():
    """Test ParallelProcessor.add_argument raises a ValueError if process_id is already
     in use."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # process_id to pass
    _process_id = 1

    # args to pass
    _func_args = ("a",)

    # kwargs to pass
    _func_kwargs = {"foo": "bar"}

    # Add arguments to ParallelProcessor instance
    parallel_processor.add_argument(
        process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
    )

    try:
        # Add arguments to ParallelProcessor instance
        parallel_processor.add_argument(
            process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
        )
        result = False

    # Expecting a ValueError to be raised
    except ValueError:
        result = True

    assert result


########################################################################################


def test_add_argument_4():
    """Test ParallelProcessor.add_argument raises a ValueError func_kwargs is passed and
     is not a dictionary."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # process_id to pass
    _process_id = 1

    # kwargs to pass
    _func_kwargs = ["foo", "bar"]

    try:
        # Add arguments to ParallelProcessor instance
        parallel_processor.add_argument(
            process_id=_process_id, func_kwargs=_func_kwargs
        )
        result = False

    # Expecting a ValueError to be raised
    except ValueError:
        result = True

    assert result


########################################################################################


def test_add_argument_5():
    """Test ParallelProcessor.add_argument raises a ValueError if neither func_args nor
     func_kwargs are passed."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # process_id to pass
    _process_id = 1

    try:
        # Add arguments to ParallelProcessor instance
        parallel_processor.add_argument(process_id=_process_id)
        result = False

    # Expecting a ValueError to be raised
    except ValueError:
        result = True

    assert result


########################################################################################


def test_add_argument_6():
    """Test ParallelProcessor.add_argument converts non-tuple func_args to tuples
     without modifying it's value/values."""

    # worker func
    _worker = dummy_worker_1

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker)

    # dictionary of process_id: args pairs to pass
    args_in = {
        "str": "Hello World",
        "int": 27,
        "float": 3.14,
        "list": ["foo", "bar"],
        "set": {"foo", "bar"},
        "tuple": ("foo", "bar"),
    }

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in args_in.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    # Get args dictionary from ParallelProcessor instance
    args_out = getattr(parallel_processor, "args")

    # Check that all set args are tuples
    assert all(map(lambda x: isinstance(x, tuple), args_out.values()))

    # Check that int input arg is unmodified
    assert args_out["str"][0] == args_in["str"]

    # Check that int input arg is unmodified
    assert args_out["int"][0] == args_in["int"]

    # Check that float input arg is unmodified
    assert args_out["float"][0] == args_in["float"]

    # Check that list input values are unmodified
    assert all(args_out["list"][i] == e for i, e in enumerate(args_in["list"]))

    # Check that set input values are unmodified
    assert all(e in args_in["set"] for e in args_out["set"])

    # Check that tuple input values are unmodified
    assert args_in["tuple"] == args_out["tuple"]

    # Check that args can be consumed by ParallelProcessor._pool_apply_async
    for arg in args_out:
        parallel_processor._pool_apply_async(args=arg)


########################################################################################


def test__create_processes_1():
    """Test ParallelProcessor._create_processes runs as expected."""

    # Init ParallelProcessor
    parallel_processor = ParallelProcessor()

    # worker func
    _worker = print

    # process_id to pass
    _process_id = 1

    # args to pass
    _args = ("Hello World",)

    # kwargs to pass
    _kwargs = {"end": "\n"}

    # Set worker
    parallel_processor.set_worker(_worker)

    # Add arguments to ParallelProcessor instance
    parallel_processor.add_argument(
        process_id=_process_id, func_args=_args, func_kwargs=_kwargs
    )

    # Call ParallelProcessor._create_processes
    parallel_processor._create_processes()

    assert True


########################################################################################


def test__create_processes_2():
    """Test ParallelProcessor._create_processes raises an AttributeError if arguments
     haven't been set."""

    # worker to set
    _worker = dummy_worker_1

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker)

    try:
        # Call ParallelProcessor._create_processes()
        parallel_processor._create_processes()
        assert False

    # Expecting an AttributeError to be raised
    except AttributeError:
        assert True


########################################################################################


def test_run_1():
    """Test ParallelProcessor.run raises a AttributeError if worker is not set."""

    # args to pass
    _args = {i: (i,) for i in range(1, 6)}

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=None, threads=2)

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in _args.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    try:
        # Call ParallelProcessor.run()
        parallel_processor.run()
        result = False

    # Expecting a ValueError to be raised
    except AttributeError:
        result = True

    assert result


########################################################################################


def test_run_2():
    """Test ParallelProcessor.run raises a AttributeError if args are not set."""

    # worker func
    _worker = dummy_worker_1

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    try:
        # Call ParallelProcessor.run()
        parallel_processor.run()
        result = False

    # Expecting an AttributeError to be raised
    except AttributeError:
        result = True

    assert result


########################################################################################


def test_run_3():
    """Test ParallelProcessor.run works as expected when progressbar=True."""

    # worker func
    _worker = dummy_worker_1

    # args to pass
    _args = {_worker(i): (i,) for i in range(1, 6)}

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in _args.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    # Capture stdout as a string when calling ParallelProcessor.run(progressbar=True)
    stdout_str = StringIO()
    with redirect_stdout(stdout_str):
        parallel_processor.run(progressbar=True)

    # Check the stdout string is not empty
    assert stdout_str.getvalue() != ""

    # Get results
    results = parallel_processor.results

    # Check that results are not none
    assert results

    # Check that outputs are as expected
    tests = [k == v for k, v in results.items()]

    assert all(tests)


########################################################################################


def test_run_4():
    """Test ParallelProcessor.run works as expected when progressbar=False."""

    # worker func
    _worker = dummy_worker_1

    # args to pass
    _args = {_worker(i): (i,) for i in range(1, 6)}

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in _args.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    # Capture stdout as a string when calling ParallelProcessor.run(progressbar=False)
    stdout_str = StringIO()
    with redirect_stdout(stdout_str):
        parallel_processor.run(progressbar=False)

    # Check the stdout string is empty
    assert stdout_str.getvalue() != ""

    # Get results
    results = parallel_processor.results

    # Check that results are not none
    assert results

    # Check that outputs are as expected
    assert all(k == v for k, v in results.items())


########################################################################################


def test_run_5():
    """Test ParallelProcessor.run works as expected with multiple arguments per
     function."""

    # worker func
    _worker = dummy_worker_2

    # args to pass
    _args = {_worker(i, i): (i, i) for i in range(1, 6)}

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in _args.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    # Call ParallelProcessor.run()
    parallel_processor.run()

    # Get results
    results = parallel_processor.results

    # Check that results are not none
    assert results

    # Check that outputs are as expected
    assert all(k == v for k, v in results.items())


########################################################################################


def test_run_6():
    """Test ParallelProcessor.run works as expected passing kwargs."""

    # worker func
    _worker = dummy_worker_3

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    # Add arguments to ParallelProcessor instance
    for i in range(1, 6):
        parallel_processor.add_argument(
            process_id=_worker(i, b=i), func_args=(i,), func_kwargs={"b": i}
        )

    # Call ParallelProcessor.run()
    parallel_processor.run()

    # Get results
    results = parallel_processor.results

    # Check that results are not none
    assert results

    # Check that outputs are as expected
    assert all(k == v for k, v in results.items())


########################################################################################


def test_run_7():
    """Test ParallelProcessor.run works as expected passing positional args to kwargs."""

    # worker func
    _worker = dummy_worker_3

    # args to pass
    _args = {_worker(i, i): (i, i) for i in range(1, 6)}

    # Init ParallelProcessor instance
    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    # Add arguments to ParallelProcessor instance
    for _process_id, arg in _args.items():
        parallel_processor.add_argument(process_id=_process_id, func_args=arg)

    # Call ParallelProcessor.run()
    parallel_processor.run()

    # Get results
    results = parallel_processor.results

    # Check that results are not none
    assert results

    # Check that outputs are as expected
    assert all(k == v for k, v in results.items())


########################################################################################


def main():
    """Empty fuction"""


########################################################################################

if __name__ == "__main__":
    main()
