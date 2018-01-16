import requests
from pathlib import Path
import json
from string import ascii_lowercase
import urllib.request

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from utils.acurerate_utils import AcureRateUtils

from entities.acurerate_attributes import P


class F6SEngager(Engager):

    F6S_SEARCH_URL = r'https://www.f6s.com/system/search/search-dashboard-terms?q=%s&search_all_startups=true'

    ACURERATE_KEY = "no-key-is-needed-here :-)"
    THE_KEY = ACURERATE_KEY

    def __init__(self):
        super().__init__()

        self.proxy_url = 'http://lum-customer-acurerate-zone-residential:1b05274a7daf@zproxy.luminati.io:22225'
        user_agent1 = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36'
        # user_agents = [
        #     'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36',
        #     'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36',
        #     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
        #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
        # ]

        # opener = urllib.request.build_opener(
        #     urllib.request.ProxyHandler({'https': proxy_url})
        # )

        # opener.addheaders = [
        #     ('X-Csrf-Token', 'qHd20i05cSh_RVi6i_nqqHnG5fMTJPSrCeado5KWV8s'),
        #     ('X-Requested-With', 'XMLHttpRequest'),
        #     ('User-Agent', user_agent1)
        # ]

        self.headers = {
            # 'contentType': 'application/json; charset=utf-8',
            'User-Agent': user_agent1,
            'X-Csrf-Token': 'qHd20i05cSh_RVi6i_nqqHnG5fMTJPSrCeado5KWV8s',
            'X-Requested-With': 'XMLHttpRequest'
        }

        self.companies_file = None

        self.all_valid_chars = ' ._abcdefghijklmnopqrstuvwxyz'


    def __repr__(self):
        return "F6S Engager"

    def get_provider_name(self):
        return "F6S"

    def get_short_symbol(self):
        return "f6s"

    def get_api_key(self):
        return F6SEngager.THE_KEY

    def set_enrich_key(self):
        email = self.get_pivot_email()
        fname = self.enriched_entity.deduced.get(P.FIRST_NAME, None)
        lname = self.enriched_entity.deduced.get(P.LAST_NAME, None)
        if email and fname and lname:
            self.enrich_key = "%s %s %s" % (email, fname, lname)
        elif email:
            self.enrich_key = email
        elif fname and lname:
            self.enrich_key = "%s %s" % (fname, lname)
        else:
            raise EngagementException("F6s - cannot engage. Cannot create enrich key for %s", self.enriched_entity)

    def perform_request(self, url):
        try:
            res = requests.get(url, headers=self.headers)
            rc = res.status_code
            txt = res.content.decode("utf-8")
        except Exception as e:
            txt = '<%s>' % e
            rc = 901
        return rc, txt

    def search_ngram(self, ngram, index):

        print("%s: Searching for [%s]" % (AcureRateUtils.get_now_as_str(), ngram))

        search_url = F6SEngager.F6S_SEARCH_URL % ngram
        # rc, response, ip = SatoriMain.perform_request(search_url, opener, with_ip=False, should_delay=False)
        rc, response = self.perform_request(search_url)
        if rc != 200:
            print(">>> ERROR: %s: %s." % (rc, response))
            return

        # Check results
        results = json.loads(response)
        if results[0]['text'].find(' match') < 1:
            print("F6S Scraper: No mention of match(es) - %s." % results[0]['text'])
            return

        num_matches = int(results[0]['text'].split(' ')[0])
        if num_matches == 0:
            print("F6S Scraper: No hits returned when searching for %s." % ngram)
            return

        # Count how many of them are 'Startup'
        startups_only = [res for res in results[1:] if 'rightText' in res and res['rightText'] == 'Startup' and 'text' in res and res['text'] and res['text'].lower().find(ngram) == 0]
        if len(startups_only) == 0:
            return

        # Should we call recursively
        if len(startups_only) >= 20:
            for l in self.all_valid_chars:
                self.search_ngram(ngram + l, index+1)

        print("%s: Found %s results for [%s]. Writing:" % (AcureRateUtils.get_now_as_str(), len(startups_only), ngram))

        # Write to file
        self.extract_and_write(startups_only, ngram)

        pass


    def extract_and_write(self, startups, ngram):

        # Iterate over all startups
        for res in startups:
            text = res['text']
            the_type = res['type']
            value = res['value']
            if text.find(';') == 0:
                text = "'%s'" % text
            if text.lower().find(ngram) == 0:
                self.companies_file.write('%s; %s; %s\n' % (text, the_type, value))
                now_str = AcureRateUtils.get_now_as_str()
                print('%s: %s, %s, %s' % (now_str, text, the_type, value))

        self.companies_file.flush()
        pass


    def scrape_all_via_search(self):

        the_delay = 30  # 30 seconds
        accumulated_delay = the_delay
        output_file_name = r'C:\temp\f6s_entities.csv'

        # Get last line of file
        # my_file = Path(output_file_name)
        # if my_file.is_file():
        #     companies_file = open(output_file_name, 'r', encoding="utf-8")
        #     last_line = AcureRateUtils.tail(companies_file, 1, offset=None)
        #     start_ngram = last_line[0][0][0:3].lower()
        # else:
        #     start_ngram = 'aaa'

        start_ngram = 'grv'

        print('Starting from [%s]' % start_ngram)

        # Open file
        self.companies_file = open(output_file_name, 'a', encoding="utf-8")

        all_letters = ''.join(ascii_lowercase)
        for l1 in ascii_lowercase[all_letters.find(start_ngram[0]):]:
            for l2 in ascii_lowercase[all_letters.find(start_ngram[1]):]:
                for l3 in ascii_lowercase[all_letters.find(start_ngram[2]):]:
                    query_term = '%s%s%s' % (l1, l2, l3)
                    self.search_ngram(query_term, 0)
                start_ngram = start_ngram[0:2] + 'a'
            start_ngram = start_ngram[0] + 'a' + start_ngram[2]

        print('*** Done extracting all companies from F6S! ***')

        pass
