FROM python:3.9.5-buster

RUN apt-get update -y
RUN apt-get install -y vim

WORKDIR /tmp

COPY ./.build/*.tar.gz /tmp/.build/
COPY ./requirements.txt /tmp/requirements.txt
COPY ./.build/api-keys.json /app/api-keys.json

#  do this before copying code to avoid unnecessary rebuilds (due to docker layer change)
RUN pip install --no-cache-dir -r /tmp/requirements.txt


#COPY ./run.sh /app/run.sh
#COPY twitter_pqueue_scraper/ /app/twitter_pqueue_scraper/
#RUN chmod -R 0777 /app/     # todo: in prod use 444 (ro)
#RUN chmod +x /app/run.sh


WORKDIR /app/twitter_pqueue_scraper
ENV PYTHONPATH /app/twitter_pqueue_scraper

CMD ["python3", "twitter_pqueue_scraper/main.py"]
