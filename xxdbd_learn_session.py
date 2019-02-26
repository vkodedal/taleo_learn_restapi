import requests
import xxdbd_learn_config as cfg
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def getSession():
    s = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])

    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s


def setToken():
    s = getSession()
    r = s.get(cfg.url, auth=(cfg.userName, cfg.password))
    cfg.headers['X-Learn-Access-Token'] = r.headers['X-Learn-Access-Token']