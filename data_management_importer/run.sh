docker run --log-driver=none -a stdin -a stdout -a stderr --network host --env-file .env kingstonduo/data-management:latest python ./main.py $1 $2 $3 $4
