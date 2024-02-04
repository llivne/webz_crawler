import configparser
import typer
import logging

from crawler import Crawler
from multiprocessing import Queue, Value, Lock
from login import login_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-10s | %(message)s')


def main(default_url: bool = True, pause: bool = True, login: bool = True):
    '''
    activate the crawler app
    :param default_url: if True run the program with the url from the config file. else input for url from user
    :param pause: if True pause for configured seconds between requests in order not to get blocked.
    :param login: if True login using cookies.json info
    '''
    logging.info("Crawler app is running...")

    # set workers shared resources
    urls_queue = Queue()
    counter = Value('i', 0)
    last_req_time = Value('d', 0)
    lock = Lock()

    config = configparser.ConfigParser()
    config.read("config.ini")

    session = login_session() if login else None

    crawler_instance = Crawler(
        pause_time=int(config['pause_time']['interval']) if pause else None,
        json_folder=config['json_folder']['name'],
        last_req_time=last_req_time,
        lock=lock,
        session=session,
        counter=counter
    )

    processes = crawler_instance.run_workers(int(config['workers']['num_of_processes']),
                                             int(config['workers']['number_of_threads']),
                                             urls_queue)

    if default_url:
        url = config['urls']['default_url']
        logging.info(f"Running default URL:{url}.")
        urls_queue.put(url)
    else:
        while True:
            input_val = input("Type URL to start the crawler work. [Q]uit to exit:\n")
            if input_val.startswith("http"):
                urls_queue.put(input_val)
                break
            elif input_val.upper() in ["Q", "QUIT"]:
                urls_queue.put(None) # signal the workers to stop
                break
            else:
                print("Input is not valid.")

    for p in processes:
        p.join()


if __name__ == '__main__':
    typer.run(main)
