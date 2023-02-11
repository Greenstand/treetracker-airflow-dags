# Treetracker Automation using Airflow

This repository contains automations for Treetracker processes using an open source worflow orchestration tool called Airflow (https://airflow.apache.org/). Airflow allows us to flexibly and robustly create and schedule jobs to run on a schedule or be manually triggered.  Airflow uses the python language and requires python 3 to be installed to run.

# Set up your local development environment

## Option 1: Develop without installing airflow

To run Python code without installing airflow:

1. Add Python code under `lib` folder.
2. Write unit test and use IDE to run it like this:
 ![image](https://user-images.githubusercontent.com/5744708/153327472-31f32345-ce36-4238-98cc-5a94024f3cd8.png)
3. When the test passed, add new dag file to invoke the script/function.

## Option 2: Install using pip
- sudo pip3 install apache-airflow
- sudo pip3 install apache-airflow[postgres]
- sudo pip3 install apache-airflow[psycopg2]
- sudo pip3 install apache-airflow-providers-cncf-kubernetes

For this repo and clone it into your local directory:
- cd ~/Greenstand/git/
- git clone git@github.com:[your github profile name]/treetracker-airflow-dags.git

Add this to your ~/.bash_profile
- export AIRFLOW_HOME='~/airflow'
- export AIRFLOW_CONFIG=$AIRFLOW_HOME/airflow.cfg

then 
- source ~/.bash_profile

Modify your airflow.cfg:
- dags_folder = ~/Greenstand/git/treetracker-airflow-dags

This approach seems to work well on MacOS X, but on Windows requires many extra dependencies.
More information is available at https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#

## Option 3: Run using docker

You may run a ubuntu instace in docker, and install airflow there using pip3.  There may be a docker image available on dockerhub for this purpose as well, but we have not tested any publicly available images at this time.

If run airflow in docker, using ubuntu, all pre-requisites for airflow is just: python (2/3), pip and bind the 8080 port to allow locally visit to the admin dashboard of airflow, here is an example of docker command to run the container: docker run -it -d -v ~/temp/airflow/mydata:/mydata -p 8080:8080 --name myairflow ubuntu


# Author airflow jobs (DAGs)

Airflow DAGs are authored using any editor that you choose.  When you author DAGs in the configured airflow DAGs folder (defaults to /Users/{user}/airflow/dags on MacOS X), airflow detects the automatically and runs them.  You can view the outputs of each run in the airflow web panel.


1. Run airflow locally, execute ```airflow standalone``` on the command line, note that this command gives you an address for the airflow web panel as well as the password for the admin user
2. Open the airflow control panel using the provided web address and log in using the admin user and password
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

# Airflow Development Environment

Greenstand's Airflow development environment is set up like this:

- local Airflow -> dev Airflow -> prod Airflow

There is also a dev and prod / prod readonly PostgreSQL database that is used by Airflow. Some Airflow DAGs transform the data in the PostgreSQL databases.

- local Airflow and dev Airflow use the dev PostgreSQL database.
- prod Airflow uses the prod PostgreSQL database.

- dev Airflow is deployed on Greenstand's dev Kubernetes cluster on DigitalOcean.
- prod Airflow is deployed on Greenstand's prod Kubernetes cluster on DigitalOcean.

Please see: https://github.com/Greenstand/treetracker-infrastructure/tree/master/airflow for how Airflow is configured and installed on Kubernetes. Greenstand uses Ansible to deploy and configure an Airflow Helm Chart onto the Kubernetes cluster.

The local Airflow development environment is for Greenstand volunteers to develop Airflow DAGs locally on their machine. Once you have finished developing locally, you can create a Pull Request on Github and merge the changes back to the main branch of this repo. Changes to the main branch will automatically be deployed to the dev Airflow. Once the new DAG is working on dev Airflow, the production branch of this repo will be updated and the new changes will be deployed to the prod Airflow.

For access credentials to the dev Airflow and dev postgreSQL database: 
- please ask the Greenstand automations-working-group Slack channel.
- The notification-airflow Slack channel is also important.

# How to do a Github Pull Request

- Fork this repo.
- Clone the fork to your local system.
	- git clone git@github.com:[your github profile name]/treetracker-infrastructure.git
- Make a new branch.
- Make and test your changes on your local Airflow.
- Git add, commit, and push the changes back to your repo.
- Click the Compare & pull request button.
- Click Create pull request to open a new pull request.
