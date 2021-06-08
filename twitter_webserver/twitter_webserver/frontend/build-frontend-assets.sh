#!/bin/bash

#export RUN_ENV=local-dev
#BASE_DIR=/home/ross/code/twitter_webserver/twitter_webserver
BASE_DIR=/app/twitter_webserver

if [[ ! ($RUN_ENV == "local-dev" || $RUN_ENV == "dev" || $RUN_ENV == "prod") ]]; then
  echo "error: RUN_ENV must be one of: 'local-dev', 'dev', 'prod'"; exit 1
fi


if [[ $RUN_ENV == "local-dev" || $RUN_ENV == "dev" ]]; then
  NODE_ENV=development
  parcel build "$BASE_DIR/frontend/static-src/*" -d "$BASE_DIR/frontend/static-built-parcel" --public-url '.' --no-minify
else
  NODE_ENV=production
  parcel build "$BASE_DIR/frontend/static-src/*" -d "$BASE_DIR/frontend/static-built-parcel" --public-url '.'
fi

cd $BASE_DIR
python manage.py collectstatic --noinput --clear
