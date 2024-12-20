### Local Airflow configuration w/ MinIO and PostgreSQL

- Services:
    - Airflow
    - MinIO (file storage and remote logs)
    - Redis
    - PostgreSQL (file storage and Airflow's metadata db)

### Dependencies:

- Docker
- Docker Compose

### How to run locally:

- clone the repo and run `[sudo optional] docker compose up -d` to start the containers up
- after the containers are up, make sure to log on to MinIO Web Interface (http://localhost:9001) and create a MinIO Access Key ID and Secret Access Key
- update these (access key id and secret access key) in the .env file, if not working already
- rebuild and restart the containers with `docker compose down && docker compose up -d`

### Worth noting:

- the `dags/` and `data/` directories (as well as `config/`, `logs/` and `plugins/`) are mounted as a volume, so dags or data changes can be made without restarting the containers
- if configuration changes are made (e.g. MinIO secrets), the environment has to be restarted
- any configuration changes to airflow should be made via `config/airflow.cfg` file, please see configuration reference for help