# poseidon-airflow

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for Docker, originally forked from [Puckel's docker-airflow](https://github.com/puckel/docker-airflow).

## Requirements


* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)

## Installation

Clone the [repository](https://github.com/DataSD/poseidon-airflow) with HTTPS:

    git clone https://github.com/DataSD/poseidon-airflow.git

or with SSH:

    git clone git@github.com:DataSD/poseidon-airflow.git

## Build

Navigate to the folder where the repository was cloned, and run the following commands:

    git pull

    
These will first ensure that your local repository is up to date and references the most recent [Docker image](https://hub.docker.com/orgs/cityofsandiego) (referenced by the docker-compose files).

Additionally, the following python packages must be installed: boto3, crypography, fire & envparse. The most common way to add these dependencies would be:

    python3 -m pip install boto3 crypogaphy fire envparse

Next, ensure that folders **/data/temp** & **/data/prod** exist within the directory.

For testing and development, run Airflow with **SequentialExecutor** :

    python3 commander.py up Sequential

This command will pull the referenced image from Dockerhub, and create a webserver container. 

## Usage

In order to view the container, run the following command:

    docker ps

If everything went as expected, you should see a running container. To view the webserver in browser, navigate to URL [localhost:1187](localhost:1187). 

To connect to the  container run:

    python3 commander.py connect_container poseidon-airflow_webserver_1

This will bring you to an airflow prompt within the container. To view dags:

    airflow list_dags

To test a dag:

    airflow test {DAG_ID} {TASK_ID} {EXECUTION_DATE}

## UI Links

- Airflow: [localhost:1187](http://localhost:1187/)
- Flower: [localhost:5555](http://localhost:5555/)


