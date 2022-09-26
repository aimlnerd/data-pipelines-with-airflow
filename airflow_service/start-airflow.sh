docker compose up -d #--build
#:: The last version of airflow doesn't create a default admin used to prevent the attacks tom the default installations, 
#:: So to create a user we should wait until the start of the airflow webserver is done
sleep 30s 
docker compose exec webserver airflow users create -r Admin -u admin -f Admin -l Admin -p admin -e admin@example.com
