# Data Engineering Capstone Project

Automated Airflow data pipeline that loads data from AWS S3 to AWS Redshift, performs analytic queries, and creates accessible tables needed by the data team.

## Repo Directories and Files Dictionary

`airflow/dag/main_dag.py` : Main dag file that glues together the data pipeline

`airflow/plugins/operators/` : Custom built operators and operator queries used for ETL pipeline

`airflow/config/udacity_dwh.cfg` : Redshift cluster configuration file

`airflow/config/airflow.cfg` : Airflow configuration file

`airflow/scripts/connections.py`: Creates AWS redshift and s3 connections within airflow. Uses configuration values from udacity_dwh.cfg. 

`airflow/scripts/entrypoint.sh`: Initializes Airflow connections, webserver, and scheduler.

`airflow/docker-compose.yml`: Creates Docker container for Postgres and Airflow webserver

`airflow/Dockerfile`: Builds Docker image with needed python packages and configuration files

`iac_notebook.ipynb`: IaC (infrastructure as code) notebook for redshift cluster management

`data_generator.ipynb`: Functions used to create synthetic numerical data 


## Project Scope
Being enrolled in an online education program, inspired me to revolve this project around online learning user data.  I was hoping to access Udacity's user data for this project. Although after asking Udacity's support team, they politely declined my request. Given that this project is focused on the data pipeline process, data integrity isn't as important. As for plan b, I decided to create my own synthetic Udacity data and use publicly accesible text data (creating unique large scale Udacity text data would be a quite an ordeal).  

## Datasets

Student Userbase Count: 50,000

All data files are stored in a private AWS S3 bucket.

Data used for fact tables were partitioned by (in order) nanodegree name, year and month. 

Example S3 path:

s3://udacity-de-capstone/data_science/2019/1/project_feedback.csv

Data used for dimensional tables were stored in one directory. 

Example S3 path:

s3://udacity-de-capstone/dimensional_tables/users_dim.csv

### Data Dictionary

#### Fact Files

##### `project_feedback.csv`

    - user_id: Student's user id
    - project_id: Program's project id
    - submit_date: Date user submitted the feedback
    - text: Student's feedback on projet
    - rating: Student's section rating on a scale from one (bad) to three (good)

##### `section_feedback.csv` :

    - user_id: Student's user id
    - section_id: Program's project id
    - submit_date: Date user submitted the feedback
    - text: Student's feedback on section
    - rating: Student's section rating on a scale from one (bad) to three (good)

##### `mentor_activity.csv` : 
    - user_id: Student's user id
    - section_id: Program's section id 
    - project_id: Program's project id
    - post_date: Question's post date
    - prompt: Question's prompt/title
    - post_text: Question's explaination
    - post_score: Question's upvote count from other users
    - answer_date: Answer's date
    - answer_text: Answer
    - answer_score: Answer's upvote count from other users

##### `video_log.csv` : 
    - user_id: Student's user id
    - video_id: Program's video id
    - degree_id: Nanodegree id
    - first_view_date: Date user first viewed the video
    - last_view_date: Date user last viewed the video
    - views_per_user: Amount of time the user viewed the video


#### Dimension Files

`users_dim.csv` : 
    - user_id: Student's user id
    - name: Student's name
    - github_handle: Student's GitHub handle 
    - location: Student's given location
    - email: Student's email
    - company: Company student is currently working at


`projects_dim.csv`: 

    - project_id: Program's project id
    - section_id: Program's section id
    - project_name: Project name
    - section_name: Section name
    - degree_name: Nano degree name

`videos_dim.csv`:

    - video_id: Program's video id
    - video_name: Video name
    - section_name: Section name
    - degree_name: Nano degree name

## Data Model

![diagram](dwh_diagram.png)

## Airflow DAG Architecture 

### Staging Subdags
All S3 data was is processed either in the staging fact or staging dimension subdag. The fact subdag processes fact table related data while the dimensional subdag processes dimension table related data. The subdag uses AWS and Redshift connection hooks to execute three major tasks.

### Staging Subdag Tasks

- Create resdshift target table

- Load S3 data to target table

- Perform data quality check on target table

- Move rows that cause STL errors to error tables to prevent the staging task from breaking and eliminate the need to re-stage the table

### Dynamic Subdag Parameterization

Both the staging and fact subdags can easily scale up the amount of nanodegrees added to the data pipeline with minimal amount of extra code. Additional nanodegrees can be incorporated into the data pipeline by importing the necessary data to the s3 bucket and adding the nanodegree name to the degree_list within the main dag file. The degree list is looped through the subdag while the subdag formats the task and table names with the table type and degree name. Having dynamic subdags decreases the amount of hardcoded tasks and increases task parrelization. In addition, it allows efficient distributed bug fixes and clear high level visibility within the Airflow UI DAG graph view.


# Project Application
The data pipeline creates a large scale relational database that the data or product team can access by a shared Postrgres connection string.

## Data Science Use Cases
Build binary classification models to predict the student churn rate given the user activity in course, sentiment in feedback/mentor questions
A/B test email promotions/advertisiments with sample groups determined using the user activity data (e.g. advertise x degree based upon y user activity)

Create word counter visualization of the most common feedback themes
Report which videos have the most views per user and research why those videos are being replayed.

## Product Design Use Cases
Understanding what improvements made be needed given the section/feedback/mentor activity data.
Most common content questions that could be incoporated into video lessons

## Why technologies:

Airflow:
Airflow is has a simple UI and easy subdag and custom operator integration. Airflow's amazing graph and tree view UI gives a great bird eye view of the data pipeline design. Both visual views came in handy for debugging and optimizing task dependencies. The process of creating custom operators and subdags were both intuitive and easy to glue together in the main DAG. The only con would be the dreaded subdag deadlock error given it's foreign terms and it's inconsistency.

AWS S3:

S3 was used as the only data storage service given my previous experiences using it in this program and other projects. Since Airflow allows other cloud storage platforms, it would be seemless to use another service like Google Cloud. 

AWS Redshift:

Redshift was used to take advantage of Redshift's MPP (massive parrell processing), columnar storage and relational database storage system. Also the Redshift cluster pause ability was a huge time saver that allowed me to resume the cluster without having to configure it all over again.  

## Scenarios

Data Pipeline Scheduling:
The data pipeline was scheduled to run on a monthly basis. Although in a production environment it really all depend on the preference of the clients and application of the data pipeline. In my opinion, the data would probably be used for anayltics rather than day-to-day operational reports so a longer interval of bi-weekly or monthly seems appropriate. 

## Data Scalibility:

In a given situation where the data were increase by 100x, I would advise increasing the amount of DC2 compute nodes within the cluster to meet the demand. Although if the amount of DC2.xlarge nodes is greater than or equal to eight or any DS2.8xlarge nodes are used, it is recommended to switch over to RA3 according to [AWS.]('https://aws.amazon.com/redshift/pricing/')

## Data Accesibility:
If the data produced by the data pipeline was used on a daily operational level then the DAG scheduled interval parameter can be easily changed to execute a daily basis at an optimal time. In this case it may be helpful to define SLA.

In a scenario were the database was needed to be accessed by hundreds of people, each user will be given apporiate accesibility to the S3 bucket and Redshift cluster. This could be defined within the AWS IAM user page. User could be partiitoned into specified groups with different privileges. For example, different groups can have complete admin access, read and write access or only reading access.

## Query Questions:

Section/Project feedback:
- What are the most common negative feedback trends? (Tfdf word phrase analysis?)
- What percentage of feedback is negative? (Build a sentiment analysis model?)
- Are the students who provide negative feedback bouncing from program (not watching future videos) / not enrolling new programs?

Mentor activity:
- What are the most common technical/content changes requested in feedback/mentor pages?
- What are the most common question prompts?

Content Videos:
- What videos have the most views per user?
- What is the average views per user for each video?

# Run the Code

The sample data is within the data directory. Within the IaC_

Download Docker Desktop using this link: https://docs.docker.com/get-docker/

If homebrew and git is not installed (macOS/Linux), paste commands separately within your terminal:

`/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`

`brew install git`

Clone this repo: `git clone https://github.com/marshall7m/data_engineering_capstone.git`

To launch Airflow UI and run DAG:
    - Launch Docker
    - Change to airflow directory from within repo: `cd airflow`
    - Build Docker images: `docker build -t my-airflow .`
    - Compose Docker Container: `docker-compose up`
    - Go to http://localhost:8080/ on your web browser