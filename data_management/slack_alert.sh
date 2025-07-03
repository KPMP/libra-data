curl -H "Content-type: application/json" \
--data '{"text":"WARNING: There was a problem during the Spectrack ingest."}' \
-X POST https://hooks.slack.com/services/$1
