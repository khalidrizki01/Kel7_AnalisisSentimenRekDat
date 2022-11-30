# Preparation

1. Clone this repository
2. Setup for user
   ```
   mkdir -p ./dags ./logs ./plugins
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```
3. Build image
   ```
   ./build.sh
   ```
4. Initiate database (once)
   ```
   docker-compose up airflow-init
   ```
5. Run airflow
   ```
   docker-compose up
   ```