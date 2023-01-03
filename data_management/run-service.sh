docker run -d --name data-manager-service --network dataLake --expose 5000 --env-file .env kingstonduo/data-management:latest flask run
