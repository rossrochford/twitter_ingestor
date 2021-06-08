Twitter API Ingestor
=====================================

twitter_ingestor is a tool for ingesting data from Twitter's API at scale using a pool of API keys.

Each api-key/endpoint combination is fronted by a priority queue and managed by its own rate-limited worker. Data requests can be queued up, keeping the API keys maximally utilized while also responding quickly to any data requested in response to user behaviour.

Currently it scrapes the following endpoints:
* https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup
* https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/api-reference/get-statuses-user_timeline
* https://developer.twitter.com/en/docs/twitter-api/v1/tweets/post-and-engage/api-reference/get-favorites-list
* https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-friends-ids
* https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids


### getting started
```console

#  install invoke globally
$ pip3 install invoke       # or:  sudo apt install python3-invoke

#  clone both of these repos into the same parent directory
$ git clone https://github.com/rossrochford/twitter_ingestor.git
$ git clone https://github.com/rossrochford/util_repos.git

#  set BASE_DIR environment variable to the parent directory
$ export BASE_DIR=$(pwd)

#  create your Twitter API bearer token here: https://developer.twitter.com/en/portal/projects-and-apps
#  and create a new file: config/api-keys.json file with the token, you'll find an example in config/
$ cp config/api-keys-example.json config/api-keys.json

#  build the project and run the services:
$ cd twitter_ingestor/
$ invoke build
$ docker-compose up

#  Because the twitter-scraper service is complex and sometimes crashes,
#  I've left this as something you start manually within its container
$ docker exec -it twitter_ingestor_twitter-scraper_1 bash
$ python twitter_pqueue_scraper/main.py
```

Here are the webserver URLS:
* Django admin, login with username: admin, password: admin
    * http://localhost:8000/admin
* Import a spreadsheet file of profiles and tags (.xls or .xlsx) with lines in the form:  twitter-profile-url | tag1 | tag2 | tag3
    * http://localhost:8000/import-profiles
* Import a json file of tag-categories, this will help keep your tags organized
    * http://localhost:8000/import-tag-categories
* Select a collection of profiles by tag and submit them to be scraped
    * http://localhost:8000/select-tags
* Submit a single profile to be scraped using the screen_name or user_id
    * http://localhost:8000/send-one
* View a user's ingested tweets:
    * http://localhost:8000/feed/<screen_name_or_userid>
