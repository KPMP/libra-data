docker run --log-driver=none -a stdin -a stdout -a stderr --network dataLake --env-file .env kingstonduo/data-management:1.9 python ./main.py $1 $2 $3 $4
