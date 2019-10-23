# StackDriver Documentation and Automated Alerting System

## Goals:

* Documentations for SD policies, alerts and critical error and systems downtime messages. Create SD dashboard to closely monitor fivetran, and all other native ETL pipelines. This should complement the dbt tree (coming soon) in the process of emergency triage and diagnosis of our entire analytics platform.

* An automated alerting system that sends selected high priority error messages to slack channel (#analytics_errors), upon each log ingestion on SD. This should slowly roll out to all ETL pipelines.


## To do list:

* Deploy configurations for creating, maintaining and versioning alerting policies and user defined metrics on SD, by utilizing GCP's deployment manager. This also creates a version controlled documentation for a quick onboarding of new engineers.

* Filter top priority error messages from fivetran (`warehouse` project and `NStoSQL2` project), to be sent to slack channel (#analytics_errors). (This is done).

* Filter top priority error messages from airflow server to monitor native ELT pipelines. (This is done).

* Adopt SD best practices. Create analytics driven metrics by defining a set of SLI's (service level indicators) that best serve the analytics engineering warehouse.
