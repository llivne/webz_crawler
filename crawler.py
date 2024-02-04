import json
import logging
import os
import requests
import time

from multiprocessing import Process
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-10s | %(message)s')

class Crawler:

    def __init__(self, pause_time, json_folder, last_req_time, lock, session, counter):
        '''

        :param pause_time: value, in seconds for pause between each request. None for no pausing
        :param json_folder: json saved results path
        :param last_req_time: last request time
        :param lock: Lock() object used for atomic operation during requests and pausing.
        :param session: Session object to save login info. None if running without login
        :param counter: int counting the number of posts
        '''
        self.pause_time = pause_time
        self.json_folder = json_folder
        self.last_req_time = last_req_time
        self.lock = lock
        self.session = session
        self.counter = counter

        self._setup()

    def _setup(self):
        '''
        perform required setup:
        - create json folder if not exists
        '''
        if not os.path.exists(self.json_folder):
            os.mkdir(self.json_folder)

    def _get_page_soup(self, page_url: str) -> BeautifulSoup:
        '''
        get soup for the given url
        :param page_url: the url for the page required
        :return: BeautifulSoup object
        '''
        # pause for configured seconds between requests in order not to get blocked.
        while self.pause_time:
            self.lock.acquire()
            req_delta = time.time() - self.last_req_time.value
            if req_delta < self.pause_time:
                self.lock.release()
                time.sleep(self.pause_time-req_delta)
            else:
                break

        if self.session:
            response = self.session.get(page_url, headers={"User-Agent": "XY"})
        else:
            response = requests.get(page_url, headers={"User-Agent": "XY"})

        if self.pause_time:
            self.last_req_time.value = time.time()
            self.lock.release()

        return BeautifulSoup(response.content, "html.parser")

    @staticmethod
    def _handle_next_url(soup, urls_queue):
        '''
        find the next url string and save it inside our urls_queue.
        if not found save None in order to signal  workers the work is done.
        :param soup: soup for the page we need to find the next elemnt within
        :param urls_queue: Queue() that holds all the urls of the posts pages
        '''
        next_url = soup.find("a", class_="wpf-next-button")
        if next_url:
            urls_queue.put(next_url["href"])
        else:
            # signal workers the work is done by putting None in the queue
            urls_queue.put(None)

    def _create_json_file_for_post(self, post_dict: dict, worker_id: str, thread_id: str):
        '''
        create single json file for each post, continig the post title, url, time, and comments/responses.
        :param post_dict: dict containg the post name (title), and url. The post Data info will be added to this dict
        :param worker_id: unique worker_id that will be used in the filename
        :param thread_id: unique thread_id that will be used in the filename
        '''
        soup = self._get_page_soup(post_dict["url"])

        published_times = soup.find_all("div", class_="cbleft wpfcl-0")
        posts_content = soup.find_all("div", class_="wpforo-post-content")

        if len(published_times) != len(posts_content):
            raise ValueError("Something went wrong in retrieving posts data...")
        else:
            for i in range(len(posts_content)):
                post_dict[published_times[i].text.strip().split("\t\t")[0]] = posts_content[i].text

            logging.info("Writing results to json file...")
            file_path = os.path.join(self.json_folder, f"{worker_id}_{thread_id}_{post_dict['url'].split('/')[-1]}.json")
            with open(file_path, "w") as jsonFile:
                json.dump(post_dict, jsonFile)

    def post_thread(self, post: BeautifulSoup, worker_id: str):
        '''
        thread worker that process each post
        :param post: post soup
        :param worker_id: unique worker_id that will be used in the filename
        '''
        try:
            post_dict = {}
            post_dict["name"] = post.find('a').text
            post_dict["url"] = post.find('a')['href']
            self._create_json_file_for_post(post_dict, worker_id, current_thread().name)
            self.counter.value += 1
            logging.info(f"Added post #{self.counter.value}")
        except Exception as e:
            logging.error(f"got the following error: {e}.\n probably not a valid post. skipping... ")

    def _handle_posts(self, soup: BeautifulSoup, worker_id: str, exe: ThreadPoolExecutor):
        '''
        helper function that trigger separate thread for each post in a given posts page (each page contains several posts)
        :param soup: soup contains information about several posts
        :param worker_id: unique worker_id that will be used in the filename
        :param exe: ThreadPoolExecutor object. a single thread from this threads pool will be call for each post
        '''
        posts_elements = soup.find_all("div", class_="topic-wrap")

        for post in posts_elements:
            exe.submit(self.post_thread, post, worker_id)

    def _crawler_worker(self, worker_id, urls_queue, number_of_threads):
        '''
        crawler worker handling pages that contains several posts, and spawn individual thread for each post.
        the worker gets url info from urls_queue and responsible on handling next_url in the obtained page, and also calling threads to handle the posts.
        :param worker_id: unique worker_id that will be used in the filename
        :param urls_queue: Queue() that holds all the urls of the posts pages
        :param number_of_threads: the number of threads from the config file
        '''
        logging.info(f"worker {worker_id} is up.")

        with ThreadPoolExecutor(max_workers=number_of_threads) as exe:
            while True:
                    url = urls_queue.get()
                    if url:
                        logging.info(f"worker [{worker_id}] handling the follwing url: {url}")
                        soup = self._get_page_soup(url)
                        self._handle_next_url(soup, urls_queue)
                        self._handle_posts(soup, worker_id, exe)
                    else:
                        # signal other workers the work is done by putting None in the queue
                        urls_queue.put(None)
                        break

    def run_workers(self, num_of_workers, number_of_threads, urls_queue) -> list:
        '''

        :param num_of_workers: the number of processes from the config file
        :param number_of_threads: the number of threads from the config file
        :param urls_queue: Queue() that holds all the urls of the posts pages
        :return: list of Process()
        '''
        processes = []
        for i in range(num_of_workers):
            worker_id = f"worker-{i+1}"
            p = Process(target=self._crawler_worker, args=(worker_id, urls_queue, number_of_threads))
            processes.append(p)
            p.start()

        return processes
