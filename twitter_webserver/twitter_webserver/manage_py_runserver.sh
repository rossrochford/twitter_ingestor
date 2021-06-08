#!/bin/bash

# rm -rf /home/ross/code/twitter_webserver/twitter_webserver/twitter/migrations/*

/app/twitter_webserver/frontend/build-frontend-assets.sh

# python manage.py makemigrations
python manage.py migrate

python manage.py shell << END
from django.contrib.auth.models import User
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@gmail.com', 'admin', id='123456')
END

rm -f /app/twitter_webserver/frontend/package.json
rm -f /app/twitter_webserver/frontend/package-lock.json
cp /tmp/package.json /app/twitter_webserver/frontend/package.json
cp /tmp/package-lock.json /app/twitter_webserver/frontend/package-lock.json


python manage.py create_quota_periods

python manage.py runserver 0.0.0.0:8000
