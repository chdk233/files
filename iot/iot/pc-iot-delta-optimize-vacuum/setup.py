"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from pc_iot_delta_optimize_vacuum import __version__

PACKAGE_REQUIREMENTS = ["pyyaml==6.0",
                        "pydantic==1.10.7"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.3.2",
    "delta-spark==2.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
]

setup(
    name="pc_iot_delta_optimize_vacuum",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points = {
        "console_scripts": [
            "optimize = pc_iot_delta_optimize_vacuum.tasks.optimize:entrypoint",
            "vacuum = pc_iot_delta_optimize_vacuum.tasks.vacuum:entrypoint"
    ]},
    version=__version__,
    description="",
    author="",
)
