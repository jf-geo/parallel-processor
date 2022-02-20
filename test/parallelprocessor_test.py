import os
import re
import sys
import multiprocessing

from contextlib import redirect_stdout
from importlib.util import find_spec
from io import StringIO


# Add src directory to sys.path
sys.path.insert(
    1, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

################################################################################


def dummy_worker_1(x):
    return x ** 2


################################################################################


def dummy_worker_2(a, b):
    return a * b


################################################################################


def dummy_worker_3(a, b=1):
    return a * b


################################################################################


def test_parallelprocessor_import_1():
    """Test that parallelprocessor can be found by importlib."""

    assert find_spec("parallelprocessor")


################################################################################


def test_parallelprocessor_import_1():
    """Test that BasicProgressBar can be imported with errors."""

    try:
        from parallelprocessor import BasicProgressBar

        result = True

    except ModuleNotFoundError:
        result = False

    assert result


################################################################################


def test_parallelprocessor_import_3():
    """Test that ParallelProcessor can be imported with errors."""

    try:
        from parallelprocessor import ParallelProcessor

        result = True

    except ModuleNotFoundError:
        result = False

    assert result


################################################################################

from parallelprocessor import ParallelProcessor, BasicProgressBar

################################################################################


def test_basic_progress_bar():
    """Test BasicProgressBar works as expected."""

    n = 10

    stdout_str = StringIO()

    with redirect_stdout(stdout_str):
        _ = list(BasicProgressBar(range(n)))

    output = stdout_str.getvalue()

    output_split = output.strip().split("\r")

    assert len(output_split) == n + 1

    assert output.endswith("\n")

    assert all(
        [
            re.match(
                "^Completed [0-9]+/[0-9]+ processes[.] [0-9]{1,2} hours [1-6]*[0-9] minutes [1-6]*[0-9][.][0-9]{2} seconds passed[.]$",
                e,
            )
            for e in output_split
        ]
    )


################################################################################


def test_parallelprocessor_init_1():
    """Test ParallelProcessor.__init__() works."""

    parallel_processor = ParallelProcessor()

    assert True


################################################################################


def test_parallelprocessor_init_2():
    """Test ParallelProcessor.__init__() works as expected."""

    parallel_processor = ParallelProcessor()

    tests = [
        getattr(parallel_processor, "threads") == os.cpu_count(),
        getattr(parallel_processor, "worker") is None,
        getattr(parallel_processor, "ids") == set(),
        getattr(parallel_processor, "args") == {},
        getattr(parallel_processor, "kwargs") == {},
        getattr(parallel_processor, "processes") == {},
        getattr(parallel_processor, "results") == {},
        type(getattr(parallel_processor, "pool")) == multiprocessing.pool.Pool,
    ]

    assert all(tests)


################################################################################


def test__pool_apply_async_1():
    """Test ParallelProcessor._pool_apply_async with args only."""

    parallel_processor = ParallelProcessor()

    _worker = print

    _args = ("Hello World",)

    parallel_processor.set_worker(_worker)

    parallel_processor._pool_apply_async(worker=_worker, args=_args)

    assert True


################################################################################


def test__pool_apply_async_2():
    """Test ParallelProcessor._pool_apply_async with kwargs only."""

    parallel_processor = ParallelProcessor()

    _worker = print

    _kwargs = {"end": "\n"}

    parallel_processor._pool_apply_async(worker=_worker, kwargs=_kwargs)

    assert True


################################################################################


def test__pool_apply_async_3():
    """Test ParallelProcessor._pool_apply_async with args and kwargs."""

    parallel_processor = ParallelProcessor()

    _worker = print

    _args = ("Hello World",)

    _kwargs = {"end": "\n"}

    parallel_processor._pool_apply_async(worker=_worker, args=_args, kwargs=_kwargs)

    assert True


################################################################################


def test__pool_apply_async_4():
    """Test ParallelProcessor._pool_apply_async without args or kwargs."""

    parallel_processor = ParallelProcessor()

    _worker = print

    try:
        parallel_processor._pool_apply_async(worker=_worker)
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test__pool_apply_async_4():
    """Test ParallelProcessor._pool_apply_async without worker."""

    parallel_processor = ParallelProcessor()

    _args = ("Hello World",)

    _kwargs = {"end": "\n"}

    try:
        parallel_processor._pool_apply_async(args=_args, kwargs=_kwargs)
        result = False

    except AttributeError:
        result = True

    assert result


################################################################################


def test_set_worker():
    """Test ParallelProcessor.set_worker"""

    parallel_processor = ParallelProcessor()

    worker = print

    parallel_processor.set_worker(worker)

    assert parallel_processor.worker is worker


################################################################################


def test_add_argument_1():
    """Test ParallelProcessor.add_argument works as expected."""

    parallel_processor = ParallelProcessor()

    _process_id = 1

    _func_args = ("a",)

    _func_kwargs = {"foo": "bar"}

    parallel_processor.add_argument(
        process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
    )

    ids = getattr(parallel_processor, "ids")
    args = getattr(parallel_processor, "args")
    kwargs = getattr(parallel_processor, "kwargs")

    tests = [
        _process_id in ids,
        _process_id in args,
        _process_id in kwargs,
        args[_process_id] == _func_args,
        kwargs[_process_id] == _func_kwargs,
    ]

    assert all(tests)


################################################################################


def test_add_argument_2():
    """Test ParallelProcessor.add_argument raises a ValueError if process_id is not hashable."""

    parallel_processor = ParallelProcessor()

    _process_id = []

    _func_args = ("a",)

    _func_kwargs = {"foo": "bar"}

    try:
        parallel_processor.add_argument(
            process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
        )
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test_add_argument_3():
    """Test ParallelProcessor.add_argument raises a ValueError if process_id is already in use."""

    parallel_processor = ParallelProcessor()

    _process_id = 1

    _func_args = ("a",)

    _func_kwargs = {"foo": "bar"}

    parallel_processor.add_argument(
        process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
    )

    try:
        parallel_processor.add_argument(
            process_id=_process_id, func_args=_func_args, func_kwargs=_func_kwargs
        )
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test_add_argument_4():
    """Test ParallelProcessor.add_argument raises a ValueError if neither func_args nor func_kwargs are passed."""

    parallel_processor = ParallelProcessor()

    _process_id = 1

    try:
        parallel_processor.add_argument(process_id=_process_id)
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test__create_processes_1():
    """Test ParallelProcessor._create_processes runs as expected."""

    parallel_processor = ParallelProcessor()

    _worker = print

    _process_id = 1

    _args = ("Hello World",)

    _kwargs = {"end": "\n"}

    parallel_processor.set_worker(_worker)

    parallel_processor.add_argument(
        process_id=_process_id, func_args=_args, func_kwargs=_kwargs
    )

    parallel_processor._create_processes()

    assert True


################################################################################


def test__create_processes_1():
    """Test ParallelProcessor._create_processes raises an AttributeError if arguments haven't been set."""

    parallel_processor = ParallelProcessor(worker=dummy_worker_1)

    try:
        parallel_processor._create_processes()
        assert False

    except AttributeError:
        assert True


################################################################################


def test_run_1():
    """Test ParallelProcessor.run raises a AttributeError if worker is not set."""

    _args = {(i + 1) ** 2: (j) for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(worker=None, threads=2)

    for id, arg in _args.items():
        parallel_processor.add_argument(process_id=id, func_args=arg)

    try:
        parallel_processor.run()
        result = False

    except AttributeError:
        result = True

    assert result


################################################################################


def test_run_2():
    """Test ParallelProcessor.run raises a AttributeError if args are not set."""

    _worker = dummy_worker_1

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    try:
        parallel_processor.run()
        result = False

    except AttributeError:
        result = True

    assert result


################################################################################


def test_run_3():
    """Test ParallelProcessor.run works as expected when progressbar=True."""

    _worker = dummy_worker_1

    _args = {(i + 1) ** 2: (j,) for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    for id, arg in _args.items():
        parallel_processor.add_argument(process_id=id, func_args=arg)

    stdout_str = StringIO()
    with redirect_stdout(stdout_str):
        parallel_processor.run(progressbar=True)

    assert stdout_str.getvalue() != ""

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def test_run_4():
    """Test ParallelProcessor.run works as expected when progressbar=False."""

    _worker = dummy_worker_1

    _args = {(i + 1) ** 2: (j,) for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    for id, arg in _args.items():
        parallel_processor.add_argument(process_id=id, func_args=arg)

    stdout_str = StringIO()
    with redirect_stdout(stdout_str):
        parallel_processor.run(progressbar=False)

    assert stdout_str.getvalue() != ""

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def test_run_5():
    """Test ParallelProcessor.run works as expected with multiple arguments per function."""

    _worker = dummy_worker_2

    _args = {(i + 1) ** 2: (j, j) for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    for id, arg in _args.items():
        parallel_processor.add_argument(process_id=id, func_args=arg)

    parallel_processor.run()

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def test_run_6():
    """Test ParallelProcessor.run works as expected passing kwargs."""

    _worker = dummy_worker_3

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    for i in range(1, 6):
        parallel_processor.add_argument(
            process_id=(i) ** 2, func_args=(i,), func_kwargs={"b": i}
        )

    parallel_processor.run()

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def test_run_7():
    """Test ParallelProcessor.run works as expected passing positional args to kwargs."""

    _worker = dummy_worker_3

    _args = {(i + 1) ** 2: (j, j) for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(worker=_worker, threads=2)

    for id, arg in _args.items():
        parallel_processor.add_argument(process_id=id, func_args=arg)

    parallel_processor.run()

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def main():
    pass


################################################################################

if __name__ == "__main__":
    main()
