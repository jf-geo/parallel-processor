from setuptools import setup, find_packages

with open("README.md") as readme_file:
    README = readme_file.read()

PACKAGE_NAME = "parallel_processor"

PACKAGE_DESCRIPTION = "Wrapper of Multiprocessing pool.apply_async"

VERSION = "0.2.0"

LICENSE = """CC BY-NC-SA 4.0"""

COPYRIGHT = "Copyright (c) 2022 James Ford"

AUTHOR = "James Ford"
AUTHOR_EMAIL = "irvine.ford@gmail.com"

setup_args = dict(
    name=PACKAGE_NAME,
    version=VERSION,
    description=PACKAGE_DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=README,
    license=LICENSE,
    packages=find_packages("src"),
    package_dir={"": "src"},
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    include_package_data=True
)

if __name__ == "__main__":
    setup(**setup_args)
