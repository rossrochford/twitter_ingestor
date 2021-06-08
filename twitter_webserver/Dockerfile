FROM python:3.9.5-buster

WORKDIR /tmp

COPY ./.build/*.tar.gz /tmp/.build/
COPY ./requirements.txt /tmp/requirements.txt
COPY ./.build/api-keys.json /app/api-keys.json

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# WORKDIR /app/twitter_webserver/frontend
RUN curl -sL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g parcel-bundler
RUN npm i --package-lock-only  # creates lock file that audit fix needs
RUN npm audit fix


#COPY twitter_webserver/ /app/twitter_webserver/
#RUN chmod -R 0777 /app/   # or do: 444 (read-only) for everything except frontend/static* (+w) and manage_by_runserver.sh (+x)


WORKDIR /app/twitter_webserver
ENV PYTHONPATH /app/twitter_webserver


CMD ["./manage_py_runserver.sh"]
# CMD ["python", "manage.py" , "runserver", "0.0.0.0:8000"]
