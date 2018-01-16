import requests
import re
#import google
from google import search

#from lxml import html
from bs4 import BeautifulSoup

from utils.acurerate_utils import AcureRateUtils

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C


class AngelListScraperEngager(Engager):

    THE_KEY = "<no-key>"

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "AngelList Scraper Engager"

    def get_provider_name(self):
        return "AngelListScraper"

    def get_short_symbol(self):
        return "als"

    def get_api_key(self):
        return AngelListScraperEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson' and P.FULL_NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[P.FULL_NAME]
        elif t == 'AcureRateCompany' and C.NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[C.NAME]
        else:
            raise EngagementException("AngelListScraper - cannot engage - cannot generate enrich key. Entity type: %s", t)
        self.enrich_key = name

    def enrich_person(self):
        try:
            if P.ANGELLIST_URL not in self.enriched_entity.deduced:
                # Search google for the person - the search string: 'site:bloomberg.com ploni almoni "executive profile"'
                url_prefix_1 = 'http://angel.co/'.lower()
                query = 'site:angel.co "%s"' % self.enrich_key
                res = search(query, tld='com', lang='en', num=3, start=0, stop=2, pause=2.0)
                matches = 0
                for url in res:
                    url_lower = url.lower().replace('https', 'http')
                    if url_lower.find(url_prefix_1) == 0:
                        matches += 1
                if matches == 0:
                    raise EngagementException('Unable to locate information in angel.co on %s' % self.enrich_key)
                elif matches > 1:
                    # TODO: we can improve search that will also consult working places and determine which person is the one we need... (try: Ariel Cohen)
                    raise EngagementException('Unable to locate information in angel.co - more than one match on %s' % self.enrich_key)

                # Grab person id from url
                p = url.rfind('/')
                person_id = url[:p-1]
                self.set_data(P.ANGELLIST_ID, person_id)
                # TODO: look into the full url google returns - what is capId?
                self.set_data(P.ANGELLIST_URL, url)
            else:
                url = self.enriched_entity.deduced[P.ANGELLIST_URL]

            # -----------------
            # CHECK: https://angel.co/alberto-roman
            # -----------------

            headers = requests.utils.default_headers()
            headers.update(
                {
                    'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36'
                }
            )

            # Get the person's page for parsing
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                s = 'Unable to load page in Angel.co on %s. Error: %s. (url=%s)' % (self.enrich_key, response.status_code, url)
                raise EngagementException(s)

            soup = BeautifulSoup(response.content, 'html.parser')

            # Get name
            try:
                elem = soup.find("h1", {"itemprop": "name"})
                if elem:
                    name = elem.text.strip()
                    self.set_data(P.FULL_NAME, name)
            except:
                self.logger.warning('Unable to locate name attribute for %s', self.enrich_key)

            # Get photo

            # Studied at...

            # Get socials
            try:
                elem = soup.find("a", {"data-field": "linkedin_url"})
                if elem:
                    linkedin_url = elem['href']
                    self.set_data(P.LINKEDIN_URL, linkedin_url)
            except:
                self.logger.warning('Unable to locate social attribute for %s', self.enrich_key)

            try:
                elem = soup.find("a", {"data-field": "twitter_url"})
                if elem:
                    twitter_url = elem['href']
                    self.set_data(P.TWITTER_URL, twitter_url)
            except:
                self.logger.warning('Unable to locate social attribute for %s', self.enrich_key)

            try:
                elem = soup.find("a", {"data-field": "facebook_url"})
                if elem:
                    facebook_url = elem['href']
                    self.set_data(P.FACEBOOK_URL, facebook_url)
            except:
                self.logger.warning('Unable to locate social attribute for %s', self.enrich_key)

            try:
                elem = soup.find("a", {"data-field": "blog_url"})
                if elem:
                    blog_url = elem['href']
                    self.set_data(P.BLOG_URL, blog_url)
            except:
                self.logger.warning('Unable to locate social attribute for %s', self.enrich_key)

            # Get experience
            try:
                experience_elem = soup.find("div", {"class": "experience_container"})
                startup_roles = experience_elem.findAll("div", {"class": "startup_roles"})
                for review in startup_roles:
                    current_job = {}

                    # Get logo of job
                    startup_photo_elem = review.find("div", {"class": "photo"})
                    startup_photo_url = startup_photo_elem.find("img")['src']

                    # Get details of job
                    startup_text_elem = review.find("div", {"class": "text"})
                    startup_elem = startup_text_elem.find("a", {"data-type": "Startup"})
                    current_job[P.JOB_NAME] = startup_elem.text.strip()

                    startup_angellist_url = startup_elem['href']

                    # Get other details
                    more_details_elems = startup_text_elem.findAll("span")
                    if len(more_details_elems) > 0:
                        current_job[P.JOB_TITLE] = more_details_elems[0].text.strip()
                    if len(more_details_elems) > 1:
                        role_years = more_details_elems[1].text.strip()
                        s, e, c = AcureRateUtils.parse_date_range(role_years)
                        if s:
                            current_job[P.JOB_STARTED] = s
                        if e:
                            current_job[P.JOB_ENDED] = e
                        if c:
                            current_job[P.JOB_CURRENT] = c
                        # TODO: parse start/end/current year from string line
                    if len(more_details_elems) > 2:
                        role_description = more_details_elems[2].text.strip()

                    self.add_data(P.JOBS, current_job)
            except:
                self.logger.warning('Unable to locate job title/name attribute for %s', self.enrich_key)

            # Get education records
            try:
                education_elem = soup.find("div", {"class": "education"})
                education_orgs = education_elem.findAll("div", {"class": "college-row-view"})
                for review in education_orgs:
                    school = review.find("div", {"class": "school"}).text.strip()
                    degree = review.find("div", {"class": "degree"}).text.strip()

            except:
                self.logger.warning('Unable to locate education attribute for %s', self.enrich_key)

            # Get investments
            try:
                investments_list_elem = soup.find("div", {"class": "investment_list"})
                investments = investments_list_elem.findAll("div", {"class": "investment"})
                for investment in investments:
                    company_name = investment.find("div", {"class": "company-link"}).text.strip()
                    self.add_data(P.INVESTMENTS, company_name)

            except:
                self.logger.warning('Unable to locate investments attribute for %s', self.enrich_key)

            # Get references/reviews
            try:
                reviews_section_elem = soup.find("div", {"class": "reviews"})
                reviews_elem = reviews_section_elem.findAll("li", {"class": "review"})
                for review in reviews_elem:
                    reference = {}
                    reference[P.REFERER_REVIEW] = review.find("div", {"class": "review-content"}).text.strip()
                    referencing_person_elem = review.find("div", {"class": "annotation"}).find("a", {"class": "profile-link"})
                    reference[P.REFERER_NAME] = referencing_person_elem.text.strip()
                    reference[P.REFERER_ANGELLIST_URL] = referencing_person_elem['href']
                    self.add_data(P.REFERENCES, reference)
            except:
                self.logger.warning('Unable to locate education attribute for %s', self.enrich_key)

            # Get business locations
            # TODO..

            # Get business markets
            # TODO..

        except Exception as e:
            self.logger.error('Unable to enrich person %s. %s', self.enriched_entity, e)
            raise e
        return [P.FULL_NAME]

