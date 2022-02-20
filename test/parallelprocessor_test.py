import os
import sys

from contextlib import redirect_stdout
from io import StringIO


# Add src directory to sys.path
sys.path.insert(
    1, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

################################################################################


def dummy_worker(x):
    return x ** 2


################################################################################


def test_parallelprocessor_import():
    """Test that ParallelProcessor can be imported with errors."""

    try:
        from parallelprocessor import ParallelProcessor as _import_test_1

        result = True

    except ModuleNotFoundError:
        result = False

    assert result


################################################################################

from parallelprocessor import ParallelProcessor

################################################################################


def test_parallelprocessor_init_1():
    """Test ParallelProcessor.__init__() works."""

    try:
        parallel_processor = ParallelProcessor()
        result = True

    except:
        result = False

    assert result


################################################################################


def test_parallelprocessor_init_2():
    """Test ParallelProcessor.__init__() works as expected."""

    parallel_processor = ParallelProcessor()

    tests = [
        getattr(parallel_processor, "threads") == os.cpu_count(),
        getattr(parallel_processor, "worker") is None,
        getattr(parallel_processor, "args") == {},
        type(getattr(parallel_processor, "results")) == dict,
    ]

    assert all(tests)


################################################################################


def test_set_worker():
    """Test ParallelProcessor.set_worker"""

    parallel_processor = ParallelProcessor()

    worker = print

    parallel_processor.set_worker(worker)

    assert parallel_processor.worker is worker


################################################################################


def test_set_arguments_1():
    """Test ParallelProcessor.set_arguments works as expected."""

    parallel_processor = ParallelProcessor()

    args = {1: "a", 2: "b"}

    parallel_processor.set_arguments(args)

    assert getattr(parallel_processor, "args") == args


################################################################################


def test_set_arguments_2():
    """Test ParallelProcessor.set_arguments raises a ValueError if an incorrect argument is used."""

    parallel_processor = ParallelProcessor()

    args = "foo"

    try:
        parallel_processor.set_arguments(args)
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test_add_argument_1():
    """Test ParallelProcessor.add_argument works as expected."""

    parallel_processor = ParallelProcessor()

    arg_id = 1

    arg = "a"

    parallel_processor.add_argument(arg_id, arg)

    args = getattr(parallel_processor, "args")

    tests = [arg_id in args, args[arg_id] == arg]

    assert all(tests)


################################################################################


def test_add_argument_2():
    """Test ParallelProcessor.add_argument raises a ValueError if arg_id is not hashable."""

    parallel_processor = ParallelProcessor()

    arg_id = []

    arg = "a"

    try:
        parallel_processor.add_argument(arg_id, arg)
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test_add_argument_3():
    """Test ParallelProcessor.add_argument raises a ValueError if arg_id is already in use."""

    parallel_processor = ParallelProcessor()

    arg_id = 1

    arg = "a"

    parallel_processor.add_argument(arg_id, arg)

    try:
        parallel_processor.add_argument(arg_id, arg)
        result = False

    except ValueError:
        result = True

    assert result


################################################################################


def test_run_1():
    """Test ParallelProcessor.run raises a AttributeError if worker is not set."""

    _args = {(i + 1) ** 2: j for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(
        worker=None, processing_args=_args, threads=2
    )

    try:
        parallel_processor.run()
        result = False

    except AttributeError:
        result = True

    assert result


################################################################################


def test_run_2():
    """Test ParallelProcessor.run raises a AttributeError if args is not set."""

    _worker = dummy_worker

    parallel_processor = ParallelProcessor(
        worker=_worker, processing_args=None, threads=2
    )

    try:
        parallel_processor.run()
        result = False

    except AttributeError:
        result = True

    assert result


################################################################################


def test_run_3():
    """Test ParallelProcessor.run works as expected when progressbar=True."""

    _worker = dummy_worker

    _args = {(i + 1) ** 2: j for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(
        worker=_worker, processing_args=_args, threads=2
    )

    parallel_processor.run(progressbar=True)

    results = parallel_processor.results

    assert results

    tests = [k == v for k, v in results.items()]

    assert all(tests)


################################################################################


def test_run_4():
    """Test ParallelProcessor.run works as expected when progressbar=False."""

    _worker = dummy_worker

    _args = {(i + 1) ** 2: j for i, j in enumerate(range(1, 6))}

    parallel_processor = ParallelProcessor(
        worker=_worker, processing_args=_args, threads=2
    )

    stdout_str = StringIO()
    with redirect_stdout(stdout_str):
        parallel_processor.run(progressbar=False)

    assert stdout_str.getvalue() == ""

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
