if [ "$#" -lt 1 ]; then
  echo "Error: Please provide a Slack passcode"
  exit 1
fi
python3.9 main.py -a update -d spectrack || curl -H "Content-type: application/json" \
--data '{"text":"WARNING: There was a problem during the Spectrack ingest."}' \
-X POST https://hooks.slack.com/services/$1
