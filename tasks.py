import json
import os
from os.path import exists as path_exists
from os.path import join as join_path
import time

from invoke import task
import requests


VERBOSE = False

BASE_DIR = os.environ.get('BASE_DIR',  '/home/ross/code')
assert os.path.exists(BASE_DIR)

PROJECT_DIR = os.getcwd()
CONFIG_DIR = f"{PROJECT_DIR}/config"

POSTGRES_IMAGE = "bitnami/postgresql:13.3.0-debian-10-r22"
POSTGRES_DATA_DIR = join_path(PROJECT_DIR, 'postgres/postgres-data')

PACKAGE_VERSIONS = '0.0.1'
PACKAGES = [
    join_path(BASE_DIR, 'util_repos/util_shared'),
    join_path(BASE_DIR, 'util_repos/trio_util'),
    join_path(BASE_DIR, 'util_repos/redis_util')
]

DESTINATIONS = [
    join_path(BASE_DIR, 'twitter_ingestor/twitter_pqueue_scraper/'),
    join_path(BASE_DIR, 'twitter_ingestor/twitter_webserver/'),
]


def _build_python_package(ctx, package_dir):
    egg_name = package_dir.rsplit('/', 1)[1].replace('_', '-')
    egg_dir = f'{egg_name}.egg-info/'
    tarball_path = f'.build/{egg_name}-0.0.1.tar.gz'

    ctx.run(f'rm -rf {egg_dir}')
    ctx.run('rm -rf ./build')
    hide_output = None if VERBOSE else 'both'
    ctx.run("python3 setup.py  --quiet sdist --dist-dir=.build/", hide=hide_output)
    for destination_dir in DESTINATIONS:
        if package_dir == destination_dir:
            continue
        destination_dir = join_path(destination_dir, '.build/')
        ctx.run(f'mkdir -p {destination_dir}')
        ctx.run(f'cp {tarball_path} {destination_dir}')


def _copy_api_keys_file(ctx):
    with ctx.cd(PROJECT_DIR):
        destination_dir = join_path(
            BASE_DIR, 'twitter_ingestor/twitter_pqueue_scraper/.build/'
        )
        ctx.run(f'mkdir -p {destination_dir}')
        ctx.run(f'cp {CONFIG_DIR}/api-keys.json {destination_dir}')


def _postgres_run_and_shutdown(ctx, wait_time):
    ctx.run('sudo docker rm -f postgres_temp', hide='both')
    ctx.run(
        f'sudo docker run \
        -e POSTGRESQL_USERNAME=my_user \
        -e POSTGRESQL_PASSWORD=my_password \
        -e POSTGRESQL_DATABASE=twitter_db \
        --name postgres_temp -d \
        -v "{POSTGRES_DATA_DIR}:/bitnami/postgresql" \
        {POSTGRES_IMAGE}'
    )
    time.sleep(wait_time)
    ctx.run('sudo docker rm -f postgres_temp', hide='both')


def _initialize_postgres_data_dir(ctx):

    if not path_exists(POSTGRES_DATA_DIR):
        ctx.run(f'mkdir -p {POSTGRES_DATA_DIR}')

    if len(os.listdir(POSTGRES_DATA_DIR)) > 0:
        print(f'postgres data_dir already initialized: {POSTGRES_DATA_DIR}')
        return

    print(f'initializing postgres data_dir: {POSTGRES_DATA_DIR}')
    ctx.run(f'sudo chmod -R 0777 {POSTGRES_DATA_DIR}')
    ctx.run(f'sudo chown -R 1001 {POSTGRES_DATA_DIR}')

    _postgres_run_and_shutdown(ctx, 9)

    ctx.run(f'sudo chmod -R 0777 {POSTGRES_DATA_DIR}')
    ctx.run(f'sudo chown -R 1001 {POSTGRES_DATA_DIR}')

    _postgres_run_and_shutdown(ctx, 9)


@task
def initialize_postgres(ctx):
    with ctx.cd(PROJECT_DIR):
        _initialize_postgres_data_dir(ctx)


@task
def build(ctx):
    if not path_exists(BASE_DIR):
        exit(f'error: BASE_DIR was not found: {BASE_DIR}')
    if not PROJECT_DIR.endswith('twitter_ingestor'):
        exit(f'current working directory must be the twitter_ingestor repo')
    if not path_exists(f"{CONFIG_DIR}/api-keys.json"):
        exit('error: no api-keys.json file found')

    with ctx.cd(PROJECT_DIR):
        _initialize_postgres_data_dir(ctx)

    for package_dir in PACKAGES:
        with ctx.cd(package_dir):
            _build_python_package(ctx, package_dir)

    _copy_api_keys_file(ctx)

    with ctx.cd(PROJECT_DIR):
        ctx.run('sudo docker-compose build')
