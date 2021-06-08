import trio

from twitter_pqueue_scraper.execution.main import main


if __name__ == '__main__':
    trio.run(main)
