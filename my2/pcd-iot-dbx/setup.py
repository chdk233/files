"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from pcd_iot_dbx import __version__

PACKAGE_REQUIREMENTS = ["pyyaml"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.3.2",
    "delta-spark==2.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
    "dbldatagen"
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
]

setup(
    name="pcd_iot_dbx",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points = {
        "console_scripts": [
            "auto_loader = pcd_iot_dbx.tasks.auto_loader_ingest:entrypoint",
            "smarthome_device_summary = pcd_iot_dbx.tasks.smarthome.smarthome_device_summary:entrypoint",
            "smarthome_devcie_summary_view = pcd_iot_dbx.tasks.smarthome.smarthome_devcie_summary_view:entrypoint"
    ]},
    version=__version__,
    description="",
    author="",
)
