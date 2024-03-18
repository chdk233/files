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
            "dqi_real_time = pcd_iot_dbx.tasks.dqi_real_time:entrypoint",
            "dqi_ingest_check = pcd_iot_dbx.tasks.dqi_ingest_check:entrypoint",
            "kafka_ingest = pcd_iot_dbx.tasks.kafka_ingest:entrypoint",
            "rivian_dqi = pcd_iot_dbx.tasks.rivian.rivian_dqi:entrypoint",
            "rivian_reporting = pcd_iot_dbx.tasks.rivian.rivian_reporting:entrypoint",
    ]},
    version=__version__,
    description="",
    author="",
    package_data={'': ['*.jks']}
)
