docker run --log-driver=none -a stdin -a stdout -a stderr --network dataLake --env-file .env kingstonduo/data-management:KPMP-5472_remove-spectrack-connection python ./main.py $1 $2 $3 $4
