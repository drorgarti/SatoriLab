import requests
import requests_cache

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C
from utils.acurerate_utils import AcureRateUtils


class CrunchBaseScraperEngager(Engager):

    THE_KEY = "4568c46b5c97886c88b28f311616ed62"  # Correct Key
    APP_ID = "A0EF2HAQR0"

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "CrunchBase Scraper Engager"

    def get_provider_name(self):
        return "CrunchBaseScraper"

    def get_short_symbol(self):
        return "cbs"

    def get_api_key(self):
        return CrunchBaseScraperEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson' and P.FULL_NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[P.FULL_NAME]
        elif t == 'AcureRateCompany' and C.NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[C.NAME]
        else:
            raise EngagementException("CrunchBaseScraper - cannot engage - cannot generate enrich key. Entity type: %s", t)
        self.enrich_key = name

    # TODO: currently not used. Use...
    def _get_invetments(self):
        try:
            url = 'https://www.crunchbase.com/person/boaz-chalamish/experience'
            headers = {'Cookie': 'multivariate_bot=false; optimizelyEndUserId=oeu1468409577258r0.9582098215968213; optimizelySegments=%7B%225920640207%22%3A%22gc%22%2C%225928430071%22%3A%22referral%22%2C%225927500098%22%3A%22false%22%7D; optimizelyBuckets=%7B%7D; __qca=P0-487939305-1468586338553; D_SID=46.117.94.151:+w3ooexojQHEpIzZFcNc1aTaP970FIr2971eEtnvyZI; __uvt=; AMCV_6B25357E519160E40A490D44%40AdobeOrg=1256414278%7CMCMID%7C59538521024831969437726247797826206006%7CMCAAMLH-1472395068%7C6%7CMCAAMB-1472395068%7CNRX38WO0n5BH8Th-nqAG_A%7CMCAID%7CNONE; multivariate_bot=false; _gat=1; _gat_newTracker=1; _site_session=dC90Slk4bkR2YklCUFIwVlZOdEdLcnpuNHg0M2pWcmdtVGMvNHpYSXpROWt6MVlISks2ZFJvYU5CRlVIUUYxeWZyNURKb3BNRXZ0WXVKWndIOER3L2h4cFl3aVdSaUxSaXZCK1NwaWV4NmFyMGNIV2lKSjlJa0tlbGtyZ0UrWTRBWHVCbDRQYnNKYkRoVGRTSnN5bWcxdTJUVE4rdFJNcFcyQTF4WE1YRXpRTEM5dXlIYzF0VVdzcVJkWCtBMVJwLS1PdmRJUEs0TkJQZDRFOXp0aEd4N21nPT0%3D--63dcb713d6c1db2f03bcb3d335eb164564bd731f; s_sq=%5B%5BB%5D%5D; _hp2_props.973801186=%7B%22Logged%20In%22%3A%22false%22%7D; _ga=GA1.2.371488006.1468409576; _hp2_ses_props.973801186=%7B%22ts%22%3A1472043278904%2C%22d%22%3A%22www.crunchbase.com%22%2C%22h%22%3A%22%2Fperson%2Fyanki-margalit%22%7D; _hp2_id.973801186=1009782266958462.6901548183334881.2417570751435914; s_pers=%20s_getnr%3D1472044932998-Repeat%7C1535116932998%3B%20s_nrgvo%3DRepeat%7C1535116933001%3B; s_cc=true; D_PID=0D7DF155-DA02-37A3-B8D7-70CB495BD5F3; D_IID=5892DB12-C443-366D-A019-EA2697BD291B; D_UID=6802809C-0D33-3A53-A747-19A7602038C8; D_HID=WNOZYgu3llE2c5RMgzN7MJ4ljnWu5n+Qe/dZU47Cabk; D_ZID=A2C06678-7ECF-3993-AF2B-5B56979D42D6; D_ZUID=E96FFF3A-033D-318B-A657-D4BB32DE5CCE; uvts=4n84i08DtxiaI3E0; _px=eyJzIjp7ImEiOjAsImIiOjB9LCJ0IjoxNDcyMDQ1MjQxMzg0LCJoIjoiZWMxMzNhNzYxMWFkNGYxOTRiN2NkODgwZjZjN2VkZTYwYmM4NjI5ZjgwMTU3ZDU1OWM4ZDExODVkNTZlNWMxNiJ9',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                       'Accept-Encoding': 'gzip, deflate, sdch, br',
                        'Accept-Language': 'en-US,en;q=0.8',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive',
                        'Host': 'www.crunchbase.com',
                        'Pragma': 'no-cache',
                       'Upgrade-Insecure-Requests': '1',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            r = requests.get(url, headers=headers)
        except Exception as e:
            self.logger.warning('Unable to get investments')

    def enrich_person(self):
        try:
            # TODO: improve - run 3 searches - by full name, first name and last name. Check all results agains P.possible_names...
            url = 'https://a0ef2haqr0-3.algolia.io/1/indexes/main_production/query'
            query = 'query=%s&facetFilters=' % self.enrich_key
            payload = {"params": query, "apiKey": CrunchBaseScraperEngager.THE_KEY, "appID": CrunchBaseScraperEngager.APP_ID}
            headers = {'contentType': 'application/json; charset=utf-8',
                       'X-Algolia-API-Key': CrunchBaseScraperEngager.THE_KEY,
                       'X-Algolia-Application-Id': CrunchBaseScraperEngager.APP_ID}
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 429:
                raise EngagementException("%s. Exceeded requests quota. Error: %s." % (response.status_code, response.text), fatal=True)
            if response.status_code != 200:
                raise EngagementException("%s. %s." % (response.status_code, response.text), fatal=True)
            if response.json()['nbHits'] == 0:
                raise EngagementException("No hits returned when searching for %s." % self.enrich_key)

            # Check how many matches we have (if any)
            matches = []
            for person in response.json().get('hits', []):
                if person.get('type', '') == 'Person' and person.get('person', False) and person.get('name', '') == self.enrich_key:
                    matches.append(person)
            if len(matches) == 0:
                    raise EngagementException("None of the hits match the person name we're searching for (%s)." % self.enrich_key)
            if len(matches) > 1:
                raise EngagementException("Person name is ambiguous - got %d hits for %s. Not enriching." % (len(matches), self.enrich_key))

            # Iterate over matches (currently we get here only if there's one, but in future we may want to refine match)
            for person in matches:
                # Grab name
                f, m, l = AcureRateUtils.tokenize_full_name(person['name'])
                self.set_data(P.FIRST_NAME, f)
                self.set_data(P.LAST_NAME, l)
                if m:
                    self.set_data(P.MIDDLE_NAME, m)

                # Grab person photo
                if 'logo_url' in person:
                    logo_url = person['logo_url']
                    self.add_data(P.PHOTOS, {P.PHOTO_URL: logo_url, P.PHOTO_SOURCE: 'crunchbase'})

                # Grab location
                if 'location_name' in person:
                    self.add_data(P.LOCATIONS, person['location_name'])

                # Grab socials
                if 'permalink' in person:
                    self.set_data(P.CB_PERMALINK, person['permalink'])
                if 'url' in person:
                    self.set_data(P.CRUNCHBASE_URL, person['url'])
                if 'linkedin_url' in person:
                    self.set_data(P.LINKEDIN_URL, person['linkedin_url'])
                if 'twitter_url' in person:
                    self.set_data(P.TWITTER_URL, person['twitter_url'])

                # Grab current position
                title = None
                if 'title' in person:
                    title = person['title']

                company = None
                if 'organization_name' in person:
                    company = person['organization_name']
                if title and company:
                    current_job = {P.JOB_CURRENT: True, P.JOB_TITLE: title, P.JOB_NAME: company}
                    self.add_data(P.JOBS, current_job)
                    if AcureRateUtils.is_business(title):
                        self.logger.info('---->> %s - %s @ %s', person['name'], title, company)

                # Grab primary role
                if title is not None and company is not None:
                    role = '%s @ %s' % (title, company)
                    self.set_data(P.PRIMARY_ROLE, role)

                # Set as business as person was found in CB...
                self.set_data(P.BUSINESS, True)
                self.set_data(P.BUSINESS_REASON, 'appears in CB')

                # Investor?
                if 'n_investments' in person and person['n_investments'] > 0:
                    self.set_data(P.INVESTOR, True)
                    self.set_data(P.INVESTOR_REASON, '%s investments' % person['n_investments'])
                    self.logger.info('--==--==-->> Worth looking into %s', person['name'])
                # We found one person, we can break from loop
                # TODO: in the future, add the other persons we found to Queue for further enrichment
                break
            pass
        except Exception as e:
            self.logger.error('Failed to set some properties on person %s. Returning partial. (exception: %s)', self.enriched_entity, e)
        return [P.FULL_NAME]

    @staticmethod
    def company_exists(company_name):
        matches = CrunchBaseScraperEngager._company_exists(company_name)
        return matches[0]['permalink']

    @staticmethod
    def _company_exists(company_name, cb_url=None, permalink=None):
        # Issue a request to CB search server - if matches exist, compare using name or cb_url if provided.
        try:
            # Truncate possible parameters on url
            if cb_url and cb_url.find('?') > 0:
                cb_url = cb_url[:cb_url.index('?')]

            company_name_clean = AcureRateUtils.clean_company_name(company_name)

            url = 'https://a0ef2haqr0-3.algolia.io/1/indexes/main_production/query'
            query = 'query=%s&facetFilters=' % company_name_clean.replace('&', '%26')
            payload = {"params": query, "apiKey": CrunchBaseScraperEngager.THE_KEY, "appID": CrunchBaseScraperEngager.APP_ID}
            headers = {'contentType': 'application/json; charset=utf-8',
                       'X-Algolia-API-Key': CrunchBaseScraperEngager.THE_KEY,
                       'X-Algolia-Application-Id': CrunchBaseScraperEngager.APP_ID}
            with requests_cache.disabled():
                response = requests.post(url, json=payload, headers=headers)
            # @@@ fatal
            if response.status_code == 429:
                raise EngagementException("%s. Exceeded requests quota. Error: %s." % (response.status_code, response.text), fatal=True)
            if response.status_code != 200:
                raise EngagementException("%s. %s." % (response.status_code, response.text), fatal=True)
            if response.json()['nbHits'] == 0:
                raise EngagementException("CrunchBaseScraper: No hits returned when searching for %s (%s)." % (company_name_clean, company_name))

            # Check how many matches we have (if any)
            matches = []
            for company in response.json().get('hits', []):
                if company.get('type', '') == 'Organization' and company.get('organization', False) and 'name' in company:
                    if 'permalink' in company and permalink and company['permalink'].lower() == permalink:
                        matches.append(company)
                        break
                    # Compare URLs
                    if 'url' in company and cb_url and cb_url.endswith(company['url']):
                        matches.append(company)
                        break
                    # Check by name
                    result_company_name_clean = AcureRateUtils.clean_company_name(company.get('name'))
                    if result_company_name_clean.lower() == company_name_clean.lower():
                        matches.append(company)
            if len(matches) == 0:
                    raise EngagementException("CrunchBaseScraper: No match for %s (%s)" % (company_name_clean, company_name))
            if len(matches) > 1:
                raise EngagementException("CrunchBaseScraper: Ambiguous results - got %d hits for %s (%s)" % (len(matches), company_name_clean, company_name))
        except Exception as e:
            raise e
        return matches

    def enrich_company(self):
        try:
            company_name = self.enrich_key
            matches = CrunchBaseScraperEngager._company_exists(company_name, cb_url=self.enriched_entity.deduced.get(C.CRUNCHBASE_URL, None),
                                                               permalink=self.enriched_entity.deduced.get(C.CRUNCHBASE_PERMALINK, None))

            # Iterate over returned hits
            # TODO: there is currently only one results returned... so why loop over it? Any chance more than one match will return?
            for company in matches:
                # Grab name
                name = company['name']
                self.set_data(C.NAME, name)

                # Grab category names
                for c in company.get('category_names', []):
                    self.add_data(C.CATEGORIES, c)

                # Grab description
                if 'description' in company:
                    self.set_data(C.DESCRIPTION, company['description'])

                # Grab domain and homepage
                if 'domain' in company:
                    self.set_data(C.DOMAIN, company['domain'])
                if 'homepage' in company:
                    self.set_data(C.WEBSITE, company['homepage'])

                # Grab socials
                if 'facebook_url' in company:
                    self.set_data(C.FACEBOOK_URL, company['facebook_url'])
                if 'linkedin_url' in company:
                    self.set_data(C.LINKEDIN_URL, company['linkedin_url'])
                if 'twitter_url' in company:
                    self.set_data(C.TWITTER_URL, company['twitter_url'])
                if 'url' in company:
                    self.set_data(C.CRUNCHBASE_URL, company['url'])
                if 'permalink' in company:
                    self.set_data(C.CRUNCHBASE_PERMALINK, company['permalink'])

                # Grab logo
                if 'logo_url' in company:
                    self.add_data(C.LOGOS, {C.LOGO_URL: company['logo_url'], C.LOGO_SOURCE: 'crunchbase'})

                # Grab markets
                for m in company.get('markets', []):
                    self.add_data(C.MARKETS, m)

                # Grab location
                if 'region_name' in company:
                    self.set_data(C.HEADQUARTERS, company['region_name'])

                # Grab total funding
                if 'total_funding' in company:
                    self.set_data(C.TOTAL_FUNDING, company['total_funding'])  # Grab total funding

                # Grab primary role (e.g. company, school, investor, etc.)
                if 'primary_role' in company:
                    self.set_data(C.PRIMARY_ROLE, company['primary_role'])

                # TODO: can we differentiate between SCHOOL and UNIVERSITY...?
                if 'school' in company['primary_role'] or any(x.lower()=='school' for x in company.get('roles', [])):
                    self.set_data(C.ORGANIZATION_TYPE, C.ORGANIZATION_TYPE_SCHOOL)

                break
            pass
        except Exception as e:
            self.logger.error('Unable to enrich company %s. %s', self.enrich_key, e)
            raise e
        return [C.NAME]

