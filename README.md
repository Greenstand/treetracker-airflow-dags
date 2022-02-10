# Treetracker Automation using Airflow

This repository contains automations for Treetracker processes using an open source job running called Airflow.  Airflow allows us to flexibly and rebustly create and schedule jobs to run on a schedule or be manually triggered.  Airflow uses the python language and requires python 3 to be installed to run.

# Set up your development environment

## Option 1: Develop without installing airflow

To run Python code without installing airflow:

1. Add Python code under `lib` folder.
2. Write unit test and use IDE to run it like this:
 ![image](https://user-images.githubusercontent.com/5744708/153327472-31f32345-ce36-4238-98cc-5a94024f3cd8.png)
3. When the test passed, add new dag file to invoke the script/function.

## Option 2: Install using pip
sudo pip3 install apache-airflow

This approach seems to work well on MacOS X, but on Windows requires many extra dependencies.
More information is available at https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#

## Option 3: Run using docker

You may run a ubuntu instace in docker, and install airflow there using pip3.  There may be a docker image available on dockerhub for this purpose as well, but we have not tested any publicly available images at this time.

If run airflow in docker, using ubuntu, all pre-requisites for airflow is just: python (2/3), pip and bind the 8080 port to allow locally visit to the admin dashboard of airflow, here is an example of docker command to run the container: docker run -it -d -v ~/temp/airflow/mydata:/mydata -p 8080:8080 --name myairflow ubuntu


# Author airflow jobs (DAGs)

Airflow DAGs are authored using any editor that you choose.  When you author DAGs in the configured airflow DAGs folder (defaults to /Users/{user}/airflow/dags on MacOS X), airflow detects the automatically and runs them.  You can view the outputs of each run in the airflow web panel.


1. Run airflow locally, execute ```airflow standalone``` on the command line, note that this command gives you an address for the airflow web panel
2. Open the airflow control panel using the provided web address
3. Enable any DAGs in the web panel that you want to develop
4. If you are creating a new DAG, copy an example DAG of interest to a new files in the DAGs folder and update the dag id in the file.  (see below to locate the dag id)
5. As you update your DAG, airflow will run it according to the schedule you specify.  Or you can manually trigger the DAG.

### Locating the DAG id
```
with DAG(
    'reporting-schema-copy',   <<<--- this is the DAG id, it cannot be duplicated
    default_args=default_args,
    description='Calculate earnings for FCC planters',
    schedule_interval= '* * * * *',
    #schedule_interval= '@hourly',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['earnings'],
)
```
