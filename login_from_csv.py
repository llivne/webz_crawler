import csv
import json
import logging
import requests
import os

from bs4 import BeautifulSoup
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-10s | %(message)s')

def login_session():
    '''
    In order to login Captcha Verification need to be performed. in order to bypass this I'm using a pre-configured cookies for the login.
    The cookie data is saved inside a json file.
    the logged session is return in the end.
    :return: session object
    '''

    cookies = {}

    if not os.path.isfile("cookies.json"):
        logging.info("generating json coolies files from csv")
        with open('cookies.csv', mode='r') as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                cookies[lines[0]] = lines[1]

        Path("cookies.json").write_text(json.dumps(cookies))  # save them to file as JSON

    # retrieve cookies:
    session = requests.session()
    cookies = json.loads(Path("cookies.json").read_text())
    cookies = requests.utils.cookiejar_from_dict(cookies)  # turn dict to cookiejar
    session.cookies.update(cookies)  # load cookiejar to current session

    response = session.get("https://foreternia.com", headers={"User-Agent": "XY"})
    soup = BeautifulSoup(response.content, "html.parser")
    test_res = soup.find("a", class_="btn-login")

    if test_res:
        raise ValueError("login failed")

    logging.info("Login was successful.")
    return session
