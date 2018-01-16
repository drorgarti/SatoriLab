import time
from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from fullcontact import FullContact
from entities.acurerate_attributes import P, C
from utils.acurerate_utils import AcureRateUtils


class FullContactEngager(Engager):

    #AYDRATE_KEY = "13edcff433f0c479"  # Aydrate
    #ACURATE_TRIAL_KEY = "2e7c73db16e677f8"  # AcureRate - Trial
    ACURATE_PRODUCTION_KEY = "401739667f580b02"  # AcureRate - Production

    THE_KEY = ACURATE_PRODUCTION_KEY  # Correct Key

    def __init__(self):
        super().__init__()
        self.fc = FullContact(self.THE_KEY)

    def __str__(self):
        return 'FullContact Engager'

    def __repr__(self):
        return 'FullContact Engager'

    def get_provider_name(self):
        return 'FullContact'

    def get_short_symbol(self):
        return 'fc'

    def get_api_key(self):
        return FullContactEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson':
            email = self.get_pivot_email()
            if email is None:
                raise EngagementException("FullContacts - cannot engage. No email available as enrich key")
            self.enrich_key = email
        elif t == 'AcureRateCompany':
            if C.DOMAIN not in self.enriched_entity.deduced:
                raise EngagementException("FullContacts - cannot engage - no domain property to use as key")
            self.enrich_key = self.enriched_entity.deduced.get(C.DOMAIN)
        else:
            raise EngagementException("FullContacts - cannot engage - cannot generate enrich key. Unknown entity type")

    def enrich_person(self):

        result_obj = self._get_person_info()

        self.set_data("score", result_obj['likelihood'])

        contact_info = result_obj.get('contactInfo', None)
        if contact_info:
            if 'givenName' in contact_info:
                self.set_data(P.FIRST_NAME, contact_info['givenName'])
            if 'familyName' in contact_info:
                self.set_data(P.LAST_NAME, contact_info['familyName'])

        demographics = result_obj.get('demographics', None)
        if demographics:
            gender = demographics.get('gender', None)
            if gender:
                self.add_data(P.GENDER, gender.lower())
            loc = demographics.get('locationGeneral', None)
            if loc:
                self.add_data(P.LOCATIONS, loc)

        photos = result_obj.get('photos', None)
        if photos:
            for photo in photos:
                new_photo = {}
                m = {"url": P.PHOTO_URL, "typeName": P.PHOTO_SOURCE}
                AcureRateUtils.dict2dict(photo, new_photo, m)
                self.add_data(P.PHOTOS, new_photo)

        organizations = result_obj.get('organizations', None)
        if organizations:
            for org in organizations:
                new_job = {}
                m = {"name": P.JOB_NAME, "title": P.JOB_TITLE, "current": P.JOB_CURRENT, "isPrimary": P.JOB_PRIMARY}
                AcureRateUtils.dict2dict(org, new_job, m)
                # If there are start/end dates, grab them (year only - drop the month)
                if 'startDate' in org:
                    new_job[P.JOB_STARTED] = org['startDate'][0:4]
                if 'endDate' in org:
                    new_job[P.JOB_ENDED] = org['endDate'][0:4]
                self.add_data(P.JOBS, new_job)

        social_profiles = result_obj.get('socialProfiles', None)
        if social_profiles:
            for social_profile in social_profiles:
                if social_profile.get('typeName', '') == 'Twitter':
                    self.set_data(P.TWITTER_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'LinkedIn':
                    self.set_data(P.LINKEDIN_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'GooglePlus':
                    self.set_data(P.GOOGLEPLUS_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'Facebook':
                    self.set_data(P.FACEBOOK_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'Gravatar':
                    self.set_data(P.GRAVATAR_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'Foursquare':
                    self.set_data(P.FOURSQUARE_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'Pinterest':
                    self.set_data(P.PINTEREST_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'Klout':
                    self.set_data(P.KLOUT_URL, social_profile['url'])
                elif social_profile.get('typeName', '') == 'AngelList':
                    self.set_data(P.ANGELLIST_URL, social_profile['url'])
                else:
                    print('Something else...')


        # TODO: add all other attributes received from FullContact

        return [P.JOBS]

    def enrich_company(self):

        domain = self.enriched_entity.deduced.get(C.DOMAIN, None)
        if domain is None:
            return []

        result_obj = self._get_company_info(domain)

        # Keep the logo url and website
        if 'logo' in result_obj:
            self.add_data(C.LOGOS, {C.LOGO_URL: result_obj['logo'], C.LOGO_SOURCE: 'fullcontact'})

        if 'website' in result_obj:
            self.set_data(C.WEBSITE, result_obj['website'])

        # Keep the founding year
        if 'founded' in result_obj['organization']:
            self.set_data(C.FOUNDING_YEAR, result_obj['organization']['founded'])

        # Approximate Employees
        if 'approxEmployees' in result_obj['organization']:
            self.set_data(C.EMPLOYEES_NUMBER, result_obj['organization']['approxEmployees'])

        # Keep keywords
        if 'keywords' in result_obj['organization']:
            self.set_data(C.KEYWORDS, result_obj['organization']['keywords'])

        # Keep name
        if 'name' in result_obj['organization']:
            self.set_data(C.NAME, result_obj['organization']['name'])

        # Keep social profiles URL
        # TODO: keep other social profiles...
        for profile in result_obj.get('socialProfiles', []):
            if profile['typeId'] == 'crunchbasecompany':
                self.set_data(C.CRUNCHBASE_URL, profile['url'])

        return [C.NAME]

    def _handle_fc_api_errors(self, response):
        if response.status_code == 200:  # All is ok.
            return
        # Handle different errors. Documentation - https://www.fullcontact.com/developer/docs/
        if response.status_code == 403:  # Quota exceeded - need special treatment
            raise EngagementException("403. Quota Exceeded.", True)
        elif response.status_code == 405 or response.status_code == 410 or response.status_code == 422:
            raise EngagementException("%s. Invalid request sent to FC %s" % (response.status_code, response.text), True)
        elif response.status_code == 404:
            raise EngagementException("404. Searched in the past 24 hours and nothing was found: %s" % response.text)
        elif response.status_code == 500 or response.status_code == 503:
            raise EngagementException("%s. Transient errors in FC server. Possible maintenance/downtime. %s" % (
            response.status_code, response.text), True)
        elif response.status_code == 202:  # being processed...
            raise EngagementException("202. Did not get info. Request is being processed. Return later.")
        else:
            raise EngagementException("%s. Unknown error: %s" % (response.status_code, response.text), True)

    def _get_person_info(self):

        try:
            response = self.fc.api_get('person', **{'email': self.enrich_key})
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
            self._handle_fc_api_errors(response)
            # TODO: check if we can inspect the header and see our limit remaining...
            #r.headers['x-rate-limit-remaining']
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        json = response.json()
        return json

    def _get_company_info(self, domain):
        try:
            response = self.fc.api_get('company', **{'domain': domain})
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
            self._handle_fc_api_errors(response)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        json = response.json()
        return json

