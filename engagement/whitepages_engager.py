import requests
import json

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C


class WhitePagesEngager(Engager):

    BASE_URL = 'https://proapi.whitepages.com/3.3'
    LEAD_VERIFY_SERVICE = 'lead_verify.json'
    FIND_PERSON_SERVICE = 'person'

    ACURATE__IDENTITY_CHECK__TRIAL_KEY = 'b77894765abb4497a14af0aaf53c5c4a'
    ACURATE__IDENTITY_CHECK__BUSINESS_KEY = '<TBD>'

    ACURATE__LEAD_VERIFY__TRIAL_KEY = '13dbdd1c68d6474f9ad4bf3da611bb2c'
    ACURATE__LEAD_VERIFY__BUSINESS_KEY = '<TBD>'

    ACURATE__FIND_PERSON__TRIAL_KEY = 'c14882a323a34b48b9193d5e840f2754'
    ACURATE__FIND_PERSON__BUSINESS_KEY = '<TBD>'

    THE_KEY = ACURATE__FIND_PERSON__TRIAL_KEY  # The key that will be used

    def __init__(self):
        super().__init__()

    def __str__(self):
        pass

    def __repr__(self):
        return "WhitePages Engager"

    def get_provider_name(self):
        return "WhitePages"

    def get_short_symbol(self):
        return "wp"

    def get_api_key(self):
        return WhitePagesEngager.THE_KEY

    def get_properties(self):
        my_props = super().get_properties()
        my_props['base_url'] = WhitePagesEngager.BASE_URL
        return my_props

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson':
            email = self.get_pivot_email()
            phone = None  # ToDo...
            fname = self.enriched_entity.deduced.get(P.FIRST_NAME, None)
            lname = self.enriched_entity.deduced.get(P.LAST_NAME, None)
            if email and fname and lname:
                self.enrich_key = "%s %s %s" % (email, fname, lname)
            elif email:
                self.enrich_key = email
            elif phone and fname and lname:
                self.enrich_key = "%s %s %s" % (phone, fname, lname)
            else:
                raise EngagementException("WhitePages - cannot engage. No properties avaialable to set enrich key")
        elif t == 'AcureRateCompany':
            if C.DOMAIN not in self.enriched_entity.deduced:
                raise EngagementException("WhitePages - cannot engage - no domain property available as enrich key")
            self.enrich_key = self.enriched_entity.deduced.get(C.DOMAIN)
        else:
            raise EngagementException("WhitePages - cannot engage - cannot generate enrich key. Unknown entity type")

    def enrich_person(self):

        result_obj = self._get_person_info()

        if len(result_obj['matches']) != 1:
            self.set_data(P.NUMBER_MATCHES, len(result_obj['matches']))
            return [P.NUMBER_MATCHES]

        contact_info = result_obj['matches'][0]

        # Get name related information
        if 'first_name' in contact_info:
            self.set_data(P.FIRST_NAME, contact_info['first_name'])
        if 'last_name' in contact_info:
            self.set_data(P.LAST_NAME, contact_info['last_name'])
        if 'display_name' in contact_info:
            self.set_data(P.FULL_NAME, contact_info['display_name'])

        # Get email
        if 'email' in contact_info:
            self.set_data(P.EMAIL, contact_info['email'])

        # Get phones
        for phone in contact_info.get('phone_numbers', []):
            # TODO: grab the type - mobile or landline, etc.
            self.add_data(P.PHONES, phone['value'])

        # Get job information
        new_job = {}
        if 'company' in contact_info:
            new_job[P.JOB_NAME] = contact_info['company']['name']
        if 'job_title' in contact_info:
            new_job[P.JOB_TITLE] = contact_info['job_title']
        if new_job != {}:
            self.add_data(P.JOBS, new_job)

        # Set location
        if 'addresses' in contact_info:
            for address in contact_info.get('addresses', []):
                self.add_data(P.LOCATIONS, '%s, %s' % (address.get('city', ''), address.get('country', '')))
        return [P.FULL_NAME]

    def _get_person_info(self):
        try:
            url = '%s/%s' % (WhitePagesEngager.BASE_URL, WhitePagesEngager.FIND_PERSON_SERVICE)
            email = self.get_pivot_email()
            phone = None  # TODO: implement
            fname = self.enriched_entity.deduced.get('first_name', None)
            lname = self.enriched_entity.deduced.get('last_name', None)
            req_id = self.enriched_entity.aid if hasattr(self.enriched_entity, 'aid') else 'no-attr'

            # build the payload for the request
            if fname and lname:
                parametrized_url = '%s?api_key=%s&name=%s%%20%s&address.city=Melville&address.state_code=NY&address.country_code=US' %\
                                   (url, WhitePagesEngager.THE_KEY, fname, lname)
            else:
                return None
            # if email and fname and lname:
            #     parametrized_url = '%s?api_key=%s&firstname=%s&lastname=%s&email_address=%s' % (url, WhitePagesEngager.THE_KEY, fname, lname, email)

            response = requests.get(parametrized_url)
            if response.status_code == 403:
                raise EngagementException("%s. Forbidden. Error: %s." % (response.status_code, response.text), fatal=True)
            if response.status_code == 429:
                raise EngagementException("%s. Exceeded requests quota. Error: %s." % (response.status_code, response.text), fatal=True)
            if response.status_code >= 500:
                raise EngagementException("Server Error (%d). Error: %s." % (response.status_code, response.reason), fatal=True)
            if response.status_code != 200:
                raise EngagementException("%s. %s." % (response.status_code, response.text), fatal=True)
            # if response.json()['nbHits'] == 0:
            #     raise EngagementException("No hits returned when searching for %s." % self.enrich_key)

            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        return response.json()

