import requests
import re
#import google
from google import search

#from lxml import html
from bs4 import BeautifulSoup

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C


class BloombergScraperEngager(Engager):

    THE_KEY = "<no-key>"

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "Bloomberg Scraper Engager"

    def get_provider_name(self):
        return "BloombergScraper"

    def get_short_symbol(self):
        return "bl"

    def get_api_key(self):
        return BloombergScraperEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson' and P.FULL_NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[P.FULL_NAME]
        elif t == 'AcureRateCompany' and C.NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[C.NAME]
        else:
            raise EngagementException("BloombergScraper - cannot engage - cannot generate enrich key. Entity type: %s", t)
        self.enrich_key = name

    def enrich_person(self):
        try:
            if P.BLOOMBERG_URL not in self.enriched_entity.deduced:
                # Search google for the person - the search string: 'site:bloomberg.com ploni almoni "executive profile"'
                url_prefix_1 = 'http://www.bloomberg.com/research/stocks/private/person.asp?personId='.lower()
                url_prefix_2 = 'http://www.bloomberg.com/research/stocks/people/person.asp?personId='.lower()
                query = 'site:bloomberg.com "%s" "executive profile"' % self.enrich_key
                res = search(query, tld='com', lang='en', num=3, start=0, stop=2, pause=2.0)
                matches = 0
                for url in res:
                    url_lower = url.lower().replace('https', 'http')
                    if url_lower.find(url_prefix_1) == 0 or url_lower.find(url_prefix_2) == 0:
                        matches += 1
                if matches == 0:
                    raise EngagementException('Unable to locate information in Bloomberg.com on %s' % self.enrich_key)
                elif matches > 1:
                    # TODO: we can improve search that will also consult working places and determine which person is the one we need... (try: Ariel Cohen)
                    raise EngagementException('Unable to locate information in Bloomberg.com - more than one match on %s' % self.enrich_key)

                # Grab person id from url
                p = re.compile(r'asp\?personId=(\d+)&')
                person_id = p.search(url).group(1)
                self.set_data(P.BLOOMBERG_ID, person_id)
                # TODO: look into the full url google returns - what is capId?
                self.set_data(P.BLOOMBERG_URL, url)
            else:
                url = self.enriched_entity.deduced[P.BLOOMBERG_URL]

            # Get the person's page for parsing
            response = requests.get(url)
            if response.status_code != 200:
                s = 'Unable to load page in Bloomberg.com on %s. Error: %s. (url=%s)' % (self.enrich_key, response.status_code, url)
                raise EngagementException(s)

            soup = BeautifulSoup(response.content, 'html.parser')

            # Get age
            try:
                td_elem = soup.find("td", string='Age')
                tr_elem = td_elem.parent
                tr_elem2 = tr_elem.next_sibling
                td_elem2 = tr_elem2.find("td")
                age = td_elem2.text
                if age != "--":
                    self.set_data(P.DOB, "%s years old" % age)
            except:
                self.logger.warning('Unable to locate job title/name attribute for %s', self.enrich_key)

            # Get current job
            try:
                job = {}
                elem = soup.find("span", {"itemprop": "jobTitle"})
                if elem:
                    job_title = elem.text
                    if len(job_title.strip()) > 0:
                        job[P.JOB_TITLE] = job_title
                elem = soup.find("a", {"itemprop": "worksFor"})
                if elem:
                    job_name = elem.text
                    if len(job_name.strip()) > 0:
                        job[P.JOB_NAME] = job_name
                if len(job) > 0:
                    self.add_data(P.JOBS, job)
            except:
                self.logger.warning('Unable to locate job title/name attribute for %s', self.enrich_key)

            # Get person's description
            try:
                elem1 = soup.find("div", {"itemprop": "description"})
                elem2 = soup.find("p", {"itemprop": "description"})
                description = None
                if elem1:
                    description = elem1.text
                elif elem2:
                    description = elem2.text
                if description:
                    description = description.replace('\n', '').replace('Read Full Background', '').strip()
                    self.set_data(P.DESCRIPTION, description)
            except:
                self.logger.warning('Unable to locate description attribute for %s', self.enrich_key)

            # Get the board positions
            try:
                h2_elems = [soup.findAll('h2', text=re.compile('Corporate Headquarters'))]
                #h2_elems = [soup.find("h2", string='Corporate Headquarters')]
                #h2_elems += h2_elems[0].find_next_siblings("h2")
                for elem in h2_elems:
                    if elem.text.startswith('Board Members Memberships'):
                        for e in elem.next_siblings:
                            if 'no Board Members' in e:
                                break
                            if e.name == "h2":
                                break
                            if e.name == "div" and e.find("a") is not None:
                                company_name = e.find("a").text
                                # TODO: pick up the word 'Director' - or? what else is there to be...?
                                self.add_data(P.ADVISORY_JOBS, {P.JOB_NAME: company_name, P.JOB_TITLE: 'Director'})
            except:
                self.logger.warning('Unable to locate board positions information for %s', self.enrich_key)

            try:
                # Get the education organizations
                education_elems = soup.find_all("div", {"itemprop": "alumniOf"})
                institutes_names = []
                for e in education_elems:
                    # TODO: extract the Degree & Years, if available
                    institutes_names.append(e.text)
                    self.add_data(P.EDUCATIONS, {P.EDUCATION_INSTITUTE: e.text})
            except:
                self.logger.warning('Unable to locate other affiliations information for %s', self.enrich_key)

            try:
                # Get the other companies he worked for
                companies_elems = soup.find_all("a", {"itemprop": "affiliation"})
                for e in companies_elems:
                    if e.text not in institutes_names and len(e.text.strip()) > 0:
                        self.add_data(P.JOBS, {P.JOB_NAME: e.text})
            except:
                self.logger.warning('Unable to locate other affiliations information for %s', self.enrich_key)

        except Exception as e:
            self.logger.error('Unable to enrich person %s. %s', self.enriched_entity, e)
            raise e
        return [P.FULL_NAME]

    def enrich_company(self):
        try:
            if C.BLOOMBERG_URL not in self.enriched_entity.deduced:
                # Search google for the person - the search string: 'site:bloomberg.com ploni almoni "executive profile"'
                # url_prefix_1 = 'http://www.bloomberg.com/research/stocks/private/person.asp?personId='.lower()
                # url_prefix_2 = 'http://www.bloomberg.com/research/stocks/people/person.asp?personId='.lower()
                url_prefix_1 = 'http://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId='.lower()
                url_prefix_2 = 'http://something something'.lower()
                query = 'site:bloomberg.com snapshot "%s"' % self.enrich_key
                res = search(query, tld='com', lang='en', num=3, start=0, stop=2, pause=2.0)
                matches = 0
                for url in res:
                    url_lower = url.lower().replace('https', 'http')
                    if url_lower.find(url_prefix_1) == 0 or url_lower.find(url_prefix_2) == 0:
                        matches += 1
                if matches == 0:
                    raise EngagementException('Unable to locate information in Bloomberg.com on %s' % self.enrich_key)
                elif matches > 1:
                    # TODO: we can improve search that will also consult working places and determine which person is the one we need... (try: Ariel Cohen)
                    raise EngagementException('Unable to locate information in Bloomberg.com - more than one match on %s' % self.enrich_key)

                # Grab person id from url
                p = re.compile(r'asp\?personId=(\d+)&')
                person_id = p.search(url).group(1)
                self.set_data(P.BLOOMBERG_ID, person_id)
                # TODO: look into the full url google returns - what is capId?
                self.set_data(P.BLOOMBERG_URL, url)
            else:
                url = self.enriched_entity.deduced[P.BLOOMBERG_URL]

            # Get the person's page for parsing
            response = requests.get(url)
            if response.status_code != 200:
                s = 'Unable to load page in Bloomberg.com on %s. Error: %s. (url=%s)' % (self.enrich_key, response.status_code, url)
                raise EngagementException(s)

            soup = BeautifulSoup(response.content, 'html.parser')

            # Get company's overview
            try:
                elem1 = soup.find("div", {"itemprop": "description"})
                elem2 = soup.find("p", {"itemprop": "description"})
                description = None
                if elem1:
                    description = elem1.text
                elif elem2:
                    description = elem2.text
                if description:
                    description = description.replace('\n', '').replace('Read Full Background', '').strip()
                    self.set_data(C.DESCRIPTION, description)
            except:
                self.logger.warning('Unable to locate company overview attribute for %s', self.enrich_key)

            # Get key executives
            try:
                elems = soup.findAll("a", {"itemprop": "member"})
                for elem in elems:
                    name = elem.text.replace('Mr.', '').strip()
                    name_tokens = name.split(' ')
                    the_name = name
                    if len(name_tokens) == 3:
                        the_name = name_tokens[0] + ' ' + name_tokens[2]
                    elif len(name_tokens) != 2:
                        the_name = name
                        self.logger.warning('Not sure how many tokens are in this name - %s' % name)
                    self.add_data(C.TEAM, the_name)
            except Exception as e:
                self.logger.warning('Unable to locate company executives for %s (%s)' % (self.enrich_key, e) )

            # Get phones
            # TODO...

            # Get domain
            try:
                elem = soup.find("a", {"itemprop": "url"})
                domain = elem.text
                self.set_data(C.DOMAIN, domain)
            except:
                self.logger.warning('Unable to locate domain attribute for %s', self.enrich_key)

            # Get address
            # TODO...

            # Get founding year
            # try:
            #     elem = soup.find("div", {"itemprop": "address"})
            #     elem2 = elem.find("p")  # This is currently WRONG - need to find the next sibling of elem
            #     founding_year = elem2.text
            #     self.set_data(C.FOUNDING_YEAR, founding_year)
            # except:
            #     self.logger.warning('Unable to locate founding year attribute for %s', self.enrich_key)

        except Exception as e:
            self.logger.error('Unable to enrich company %s. %s', self.enriched_entity, e)
            raise e

        return [C.NAME]

