import requests
import json

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C


class RocketReachEngager(Engager):

    BASE_URL = 'https://api.rocketreach.co/v1/api/lookupProfile'
    PEOPLE_SERVICE = 'service/people/match'
    COMPANY_SERVICE = 'service/company/match'

    ACURATE_KEY = "763cbkb12201d8f0788ce3c40101688ad9cd3d"

    THE_KEY = ACURATE_KEY  # Correct Key

    def __init__(self):
        super().__init__()

    def __str__(self):
        pass

    def __repr__(self):
        return "RocketReach Engager"

    def get_provider_name(self):
        return "RocketReach"

    def get_short_symbol(self):
        return "rr"

    def get_api_key(self):
        return CircleBackEngager.THE_KEY

    def get_properties(self):
        my_props = super().get_properties()
        my_props['base_url'] = CircleBackEngager.BASE_URL
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
                raise EngagementException("CircleBack - cannot engage. No properties avaialable to set enrich key")
        elif t == 'AcureRateCompany':
            if C.DOMAIN not in self.enriched_entity.deduced:
                raise EngagementException("CircleBack - cannot engage - no domain property available as enrich key")
            self.enrich_key = self.enriched_entity.deduced.get(C.DOMAIN)
        else:
            raise EngagementException("CircleBack - cannot engage - cannot generate enrich key. Unknown entity type")

    def enrich_person(self):

        result_obj = self._get_person_info()

        if len(result_obj['matches']) != 1:
            self.set_data("number_matches", len(result_obj['matches']))
            return []

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

    def enrich_company(self):

        domain = self.enriched_entity.deduced.get(C.DOMAIN, None)
        if domain is None:
            return []

        result_obj = self._get_company_info(domain)

        if len(result_obj['matched_domains']) != 1:
            return []

        if 'company' not in result_obj['matched_domains'][0]:
            return []

        company = result_obj['matched_domains'][0]['company']

        # Extract name
        if 'name' in company:
            self.set_data(C.NAME, company['name'])

        # Extract alternative names
        if 'alt_names' in company:
            for n in company['alt_names']:
                self.add_data(C.ALIASES, n)

        # Extract headquarters address
        if 'addresses' in company:
            for n in company['addresses']:
                address_str = '%s %s %s' % (n.get('city', ''), n.get('state', ''), n.get('country', ''))
                self.add_data(C.ADDRESSES, address_str)
                if n['is_hq']:
                    self.set_data(C.HEADQUARTERS, address_str)

        # Extract additional domains
        # TODO: do I want to handle multiple domains...? Now picking up the first one only.
        if 'domains' in company:
            self.set_data(C.DOMAIN, company['domains'][0])

        # Extract phone numbers (type/value)
        if 'phone_numbers' in company:
            for p in company['phone_numbers']:
                self.add_data(C.PHONES, p['value'])

        return [C.NAME]

    def _get_company_info(self, domain):

        try:
            url = '%s/%s' % (CircleBackEngager.BASE_URL, CircleBackEngager.COMPANY_SERVICE)
            company_name = self.enriched_entity.deduced.get('name', '<No-Company-Name>')
            domain = self.enriched_entity.deduced.get('domain', None)
            if domain is None:
                raise EngagementException("Domain property of company %s not found." % company_name, fatal=True)
            payload = {'domains': [domain]}
            headers = {'contentType': 'application/json; charset=utf-8', 'X-CB-ApiKey': CircleBackEngager.THE_KEY}
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 429:
                raise EngagementException("%s. Exceeded requests quota. Error: %s." % (response.status_code, response.text), fatal=True)
            if response.status_code >= 500:
                raise EngagementException("Server Error (%d). Error: %s." % (response.status_code, response.reason), fatal=True)
            if response.status_code != 200:
                raise EngagementException("%s. %s." % (response.status_code, response.text), fatal=True)
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        return response.json()

    def _get_person_info(self):
        try:
            url = '%s/%s' % (CircleBackEngager.BASE_URL, CircleBackEngager.PEOPLE_SERVICE)
            email = self.get_pivot_email()
            phone = None  # TODO: implement
            fname = self.enriched_entity.deduced.get('first_name', None)
            lname = self.enriched_entity.deduced.get('last_name', None)
            req_id = self.enriched_entity.aid if hasattr(self.enriched_entity, 'aid') else 'no-attr'

            # build the payload for the request
            match_request = {}
            if email and fname and lname:
                match_request = {'request_id': req_id, 'email': email, 'first_name': fname, 'last_name': lname}
            elif email:
                match_request = {'request_id': req_id, 'email': email}
            elif phone and fname and lname:
                match_request = {'request_id': req_id, 'phone_number': phone, 'first_name': fname, 'last_name': lname}
            else:
                return None

            # Build payload with the match requests. TODO: create more than one request (one with email, one with phone)
            payload = {'match_requests': [match_request]}
            headers = {'contentType': 'application/json; charset=utf-8',
                       'X-CB-ApiKey': CircleBackEngager.THE_KEY}
            response = requests.post(url, json=payload, headers=headers)
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

