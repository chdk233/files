# CI Status

[![chks](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/pytests-and-sonarqube/badge?title=chks)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/pytests-and-sonarqube) [![dev](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-dev/badge?title=dev)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-dev) [![test](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-test/badge?title=test)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-test) [![prod](https://concourse.nwie.net/api/v1/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-prod/badge?title=prod)](https://concourse.nwie.net/teams/pcds-IoT/pipelines/pcd-iot-dbx/jobs/deploy-prod) 

# SonarQube Status

[![Quality Gate Status](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=alert_status&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Bugs](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=bugs&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Security Hotspots](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=security_hotspots&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Vulnerabilities](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=vulnerabilities&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx) [![Coverage](https://sonarqube.nwie.net/api/project_badges/measure?project=PNC.PCDS.6327.pcd-iot-dbx&metric=coverage&token=sqb_fe5deed151196a57ffc855d98730621e65654d28)](https://sonarqube.nwie.net/dashboard?id=PNC.PCDS.6327.pcd-iot-dbx)

*update the token in the url once the project has been created

# Environment setup

1. SSH `ssh <your_short_name>@pcdiotdbxdev.aws.e1.nwie.net`
2. Clone repo `git clone https://github.nwie.net/Nationwide/pcd-iot-dbx`
3. Open vs code and install extensions `Dev Containers` and `Remote - SSH`
4. In Remote Explorer pick `Remote` in drop down
5. Click on the `+` button for `New Remote` and enter above ssh command
6. Once complete add cloned github folder
7. In Remote Explorer pick `Dev Containers`
8. Click on the `+` button for `New Dev Container` and pick `Open Current Folder In Container`

***You will be prompted multiple times for your nwie credentials as it reconnects many times. This is a one time setup**

# Local environment setup (docker desktop required)

1. Install Dev Containers VS Code extension
2. Navigate to Remote Explorer section click on the + butten and select open current folder in container

# Running unit tests

- Make sure the python vs code extension is installed. Once installed navigate to the `Testing` icon (the beaker to the left) in the activty bar. From there you can run all the unit tests.

- To view the htmlcoverage run the view htmlcov task.

# Configure databricks token for running tasks on interactive cluster

1. User Settings > Developer > Access Tokens > Generate New Token (save the token)
2. Open terminal in vs code dbx project
3. Run in terminal `databricks configure --host <databricks_host> --token`


# Interactive execution on Databricks clusters

1. `dbx` expects that cluster for interactive execution supports `%pip` and `%conda` magic [commands](https://docs.databricks.com/libraries/notebooks-python-libraries.html).
2. Please configure your workflow (and tasks inside it) in `conf/deployment.yml` file.
3. To execute the code interactively, provide either `--cluster-id` or `--cluster-name`.
```bash
export ENV="dev"
export VERSION="RC"
dbx execute --jinja-variables-file=conf/vars.yml --deployment-file <deployment-file> <workflow-name> \
    --cluster-name="<some-cluster-name>"
```

***This has been configured as a run task. To run the task press `ctrl + shift + p` and search for `Task: Run Task` then pick `Run remote workflow task` and fill the prompts**


Multiple users also can use the same cluster for development. Libraries will be isolated per each user execution context.


# Testing and releasing via CI pipeline

- To trigger the CI pipeline, simply publish your code to a feature branch in the repository and submit a pull request.

# Running Interactive Shell in container

- To run the task press `ctrl + shift + p` and search for `Task: Run Task` then pick `pyspark shell`

# Spark-Submit Example container

- To run the task press `ctrl + shift + p` and search for `Task: Run Task` then pick `Run local workflow task` and fill the prompts