# pcd-iot-dbx

This is a sample project for Databricks, generated via cookiecutter.

## CI Status

[![chks](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/pytests-and-sonarqube/badge?title=chks)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/pytests-and-sonarqube) [![dev](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-dev/badge?title=dev)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-dev) [![test](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-test/badge?title=test)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-test) [![prod](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-prod/badge?title=prod)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-prod) 

## SonarQube Status

[![Quality Gate Status](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=alert_status&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Bugs](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=bugs&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Security Hotspots](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=security_hotspots&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Vulnerabilities](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=vulnerabilities&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Coverage](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=coverage&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx)

*update the token in the url once the project has been created

## Local environment setup

While using this project, you need Python 3.X and `pip` for package management.

1. Install project locally (this will also install dev requirements):
```bash
pip install -e ".[local,test]"
```

## Local environment setup in container (docker desktop required)

1. Install Dev Containers VS Code extension
2. Navigate to Remote Explorer section click on the + butten and select open current folder in container

## Running unit tests

For unit testing, please use `pytest`:
```
pytest tests/unit --cov
```

Please check the directory `tests/unit` for more details on how to use unit tests.
In the `tests/unit/conftest.py` you'll also find useful testing primitives, such as local Spark instance with Delta support, local MLflow and DBUtils fixture.

To view the htmlcoverage run the view htmlcov task.

## Running integration tests

There are two options for running integration tests:

- On an all-purpose cluster via `dbx execute`
- On a job cluster via `dbx launch`

For quicker startup of the job clusters we recommend using instance pools ([AWS](https://docs.databricks.com/clusters/instance-pools/index.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/), [GCP](https://docs.gcp.databricks.com/clusters/instance-pools/index.html)).

For an integration test on all-purpose cluster, use the following command:
```
export ENV="dev"
export VERSION="RC"
dbx execute --jinja-variables-file=conf/vars.yml  --deployment-file <deployment-file> <workflow-name> \
    --cluster-name="<some-cluster-name>"
```

To execute a task inside multitask job, use the following command:
```
export ENV="dev"
export VERSION="RC"
dbx execute --jinja-variables-file=conf/vars.yml --deployment-file <deployment-file> <workflow-name> \
    --cluster-name=<name of all-purpose cluster> \
    --task=<task-key-from-job-definition>
```

For a test on a job cluster, deploy the job assets and then launch a run from them:
```
export ENV="dev"
export VERSION="RC"
dbx deploy --jinja-variables-file=conf/vars.yml --deployment-file <deployment-file> <workflow-name> --assets-only
dbx launch --jinja-variables-file=conf/vars.yml --deployment-file <deployment-file> <workflow-name>  --from-assets --trace
```


## Interactive execution and development on Databricks clusters

1. `dbx` expects that cluster for interactive execution supports `%pip` and `%conda` magic [commands](https://docs.databricks.com/libraries/notebooks-python-libraries.html).
2. Please configure your workflow (and tasks inside it) in `conf/deployment.yml` file.
3. To execute the code interactively, provide either `--cluster-id` or `--cluster-name`.
```bash
export ENV="dev"
export VERSION="RC"
dbx execute --jinja-variables-file=conf/vars.yml --deployment-file <deployment-file> <workflow-name> \
    --cluster-name="<some-cluster-name>"
```

Multiple users also can use the same cluster for development. Libraries will be isolated per each user execution context.

## Working with notebooks and Repos

To start working with your notebooks from a Repos, do the following steps:

1. Add your git provider token to your user settings in Databricks
2. Add your repository to Repos. This could be done via UI, or via CLI command below:
```bash
databricks repos create --url <your repo URL> --provider <your-provider>
```
This command will create your personal repository under `/Repos/<username>/pcd-iot-dbx`.
3. Use `git_source` in your job definition as described [here](https://dbx.readthedocs.io/en/latest/guides/python/devops/notebook/?h=git_source#using-git_source-to-specify-the-remote-source)

## Testing and releasing via CI pipeline

- To trigger the CI pipeline, simply publish your code to a feature branch in the repository and submit a pull request.

## Running Interactive Shell in container

pyspark --repositories "https://art.nwie.net:443/artifactory/libs-release,https://art.nwie.net:443/artifactory/libs-snapshot" --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

## Spark-Submit Example container

spark-submit --repositories "https://art.nwie.net:443/artifactory/libs-release,https://art.nwie.net:443/artifactory/libs-snapshot" --packages io.delta:delta-core_2.12:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pcd_iot_dbx/tasks/kafka_ingest.py -- --conf-file conf/tasks/enrollment/dev/enrollment.yml