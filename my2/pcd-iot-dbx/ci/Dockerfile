FROM ntr.nwie.net/library/python:3.11-bullseye

RUN pip3 install --retries 3 --upgrade \
     --no-cache-dir \
        dbx pyspark==3.3.2 delta-spark==2.1.0 scikit-learn pandas mlflow pytest coverage[toml] pytest-cov dbldatagen ipython&&