from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C
from utils.acurerate_utils import AcureRateUtils
import clearbit


class ClearbitEngager(Engager):

    ACURATE_TRIAL_KEY = "sk_2a34f937f031587cb2bf4f6ee84a3c70"  # AcureRate - Trial
    #ACURATE_PRODUCTION_KEY = "no production key yet"  # AcureRate - Production

    THE_KEY = ACURATE_TRIAL_KEY

    def __init__(self):
        super().__init__()
        clearbit.key = ClearbitEngager.THE_KEY

    def __str__(self):
        return 'Clearbit Engager'

    def __repr__(self):
        return 'Clearbit Engager'

    def get_provider_name(self):
        return 'Clearbit'

    def get_short_symbol(self):
        return 'clb'

    def get_api_key(self):
        return ClearbitEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson':
            email = self.get_pivot_email()
            if email is None:
                raise EngagementException("Clearbit - cannot engage. No email available as enrich key")
            self.enrich_key = email
        elif t == 'AcureRateCompany':
            if C.DOMAIN not in self.enriched_entity.deduced:
                raise EngagementException("Clearbit - cannot engage - no domain property to use as key")
            self.enrich_key = self.enriched_entity.deduced.get(C.DOMAIN)
        else:
            raise EngagementException("Clearbit - cannot engage - cannot generate enrich key. Unknown entity type")

    def enrich_person(self):

        result_obj = self._get_person_info()

        if 'pending' in result_obj and result_obj['pending']:
            msg = 'Failed to get information on person %s. Pending (202)' % self.enrich_key
            raise EngagementException(msg)

        if 'person' not in result_obj or result_obj['person'] is None:
            msg = 'Failed to get information on person %s. Not Found (404)' % self.enrich_key
            raise EngagementException(msg)

        enriched = False

        person_data = result_obj['person']

        # Get the name properties
        if 'name' in person_data:
            self.set_data(P.FIRST_NAME, person_data['name']['givenName'])
            self.set_data(P.LAST_NAME, person_data['name']['familyName'])
            self.set_data(P.FULL_NAME, person_data['name']['fullName'])

        if 'email' in person_data and person_data['email'] != self.enrich_key:
            self.set_data(P.EMAIL, person_data['email'])
            self.add_data(P.EMAILS, person_data['email'])

        if 'gender' in person_data and person_data['gender']:
            #enriched = True
            self.add_data(P.GENDER, person_data['gender'])

        if 'bio' in person_data and person_data['bio']:
            enriched = True
            self.add_data(P.SHORT_DESCRIPTION, person_data['bio'])

        if 'location' in person_data and person_data['location']:
            enriched = True
            self.add_data(P.LOCATIONS, person_data['location'])

        if 'facebook' in person_data and person_data['facebook']['handle']:
            enriched = True
            self.add_data(P.FACEBOOK_URL, person_data['facebook']['handle'])

        if 'linkedin' in person_data and person_data['linkedin']['handle']:
            enriched = True
            self.add_data(P.LINKEDIN_URL, result_obj['person']['linkedin'])

        if 'twitter' in person_data and person_data['twitter']['handle']:
            enriched = True
            self.add_data(P.TWITTER_URL, result_obj['person']['twitter'])

        if 'googleplus' in person_data and person_data['googleplus']['handle']:
            enriched = True
            self.add_data(P.GOOGLEPLUS_URL, result_obj['person']['googleplus'])

        if 'employment' in person_data:
            job = {}
            if person_data['employment'].get('name', None) is not None:
                job[P.JOB_NAME] = person_data['employment'].get('name', [])
            if person_data['employment'].get('title', None) is not None:
                job[P.JOB_TITLE] = person_data['employment'].get('title', [])
            if person_data['employment'].get('role', None) is not None:
                job[P.JOB_ROLE] = person_data['employment'].get('role', [])
            if job != {}:
                enriched = True
                self.add_data(P.JOBS, job)

        # TODO: gravatar, aboutme, github

        if not enriched:
            msg = 'Failed: no information added to person %s' % self.enrich_key
            raise EngagementException(msg)

        return [P.JOBS]

    def enrich_company(self):

        result_obj = self._get_company_info()

        if 'pending' in result_obj and result_obj['pending']:
            msg = 'Failed to get information on person %s. Pending (202)' % self.enrich_key
            raise EngagementException(msg)

        if 'company' not in result_obj or result_obj['company'] is None:
            msg = 'Failed to get information on company %s. Not Found (404)' % self.enrich_key
            raise EngagementException(msg)

        enriched = False

        company_data = result_obj['company']

        return [C.NAME]

    def _get_person_info(self):

        try:
            response = clearbit.Enrichment.find(email=self.enrich_key)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        return response

    def _get_company_info(self):

        try:
            response = clearbit.Company.find(domain=self.enrich_key, stream=True)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        return response
