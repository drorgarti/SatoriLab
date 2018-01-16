import requests
import json

from piplapis.search import SearchAPIRequest
from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P


class PiplEngager(Engager):

    # Reference documentation of the Pipl API is here:
    # https://pipl.com/dev/reference/#overview7

    # --- Demo Keys ---
    ACURERATE_KEY__CONTACT_PREMIUM_DEMO = "CONTACT-PREMIUM-DEMO-1xf0ci7tf7tpvq2osnq9d40v"
    ACURERATE_KEY__SOCIAL_PREMIUM_DEMO = "SOCIAL-PREMIUM-DEMO-fdi1bub8q5zgyknt2o1x2e4v"
    ACURERATE_KEY__BUSINESS_PREMIUM_DEMO = "BUSINESS-PREMIUM-DEMO-1m1svfn5yo6d0dtn8vdyk5o4"

    AYDRATE_KEY__CONTACT_DEMO = "CONTACT-DEMO-s9428hx5zkviummqoqrp4j7r"
    AYDRATE_KEY__CONTACT_PREMIUM_DEMO = "CONTACT-PREMIUM-DEMO-f5b1q87tqz5l7cq5lnn37m44"
    AYDRATE_KEY__SOCIAL_DEMO = "SOCIAL-DEMO-2uzcpo1eoccnyw9pvzb398c5"
    AYDRATE_KEY__SOCIAL_PREMIUM_DEMO = "SOCIAL-PREMIUM-DEMO-upo8pb480i33wawb5blg04be"
    AYDRATE_KEY__BUSINESS_DEMO = "BUSINESS-DEMO-ygbkji2sqr9fr86gpxixxe7u"
    AYDRATE_KEY__BUSINESS_PREMIUM_DEMO = "BUSINESS-PREMIUM-DEMO-9ip8jqhsxjl6r1hw23o4xsyk"

    # --- Production Keys ---
    ACURERATE_KEY__BUSINESS = "BUSINESS-o8xcmsljt2wf3fphrb7k7vvh"
    ACURERATE_KEY__BUSINESS_PREMIUM = "BUSINESS-PREMIUM-7f566wthc1nt5440lqee6xi1"

    #THE_KEY = ACURERATE_KEY__BUSINESS_PREMIUM_DEMO
    #THE_KEY = AYDRATE_KEY__BUSINESS_PREMIUM_DEMO

    THE_KEY = ACURERATE_KEY__BUSINESS_PREMIUM

    def __init__(self):
        super().__init__()

        # Set configuration settings for the Pipl search queries
        # TODO: infer_persons - what's the default? Should we turn off?
        # TODO: match_requirements -- !
        SearchAPIRequest.set_default_settings(api_key=self.THE_KEY, minimum_probability=None,
                                              show_sources=None, minimum_match=None, hide_sponsored=None,
                                              live_feeds=None, use_https=False)
    def __repr__(self):
        return "Pipl Engager"

    def _handle_pipl_api_errors(self, response):
        if response.status_code == 200:  # All is ok.
            return
        # Handle different errors. Documentation - https://www.fullcontact.com/developer/docs/
        if response.status_code == 403:
            raise EngagementException("403. Quota Exceeded!", True)
        elif response.status_code == 400:
            raise EngagementException("400. Bad request", True)
        elif response.status_code == 500:
            raise EngagementException("500. Server Error", True)
        else:
            raise EngagementException("%s. Pipl engage error: %s" % (response.status_code, response.text))

    def get_provider_name(self):
        return "Pipl"

    def get_short_symbol(self):
        return "pp"

    def get_api_key(self):
        return PiplEngager.THE_KEY

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
            raise EngagementException("Pipl - cannot engage. Cannot create enrich key for %s", self.enriched_entity)

    def get_info(self):

        email = self.get_pivot_email()
        fname = self.enriched_entity.deduced.get('first_name', None)
        lname = self.enriched_entity.deduced.get('last_name', None)

        # build the Search request
        # TODO: need to pass in my request the matching criteria: "(email and name)" or "email", etc.
        if email and fname and lname:
            payload = {'key': PiplEngager.THE_KEY, 'email': email, 'first_name': fname, 'last_name': lname}
        elif email:
            payload = {'key': PiplEngager.THE_KEY, 'email': email}
        elif fname and lname:
            payload = {'key': PiplEngager.THE_KEY, 'first_name': fname, 'last_name': lname}
        else:
            return None

        # Set the match requirements
        payload['minimum_probability'] = 0.7
        payload['minimum_match'] = 1
        #payload['match_requirements'] = '(name and image)'

        try:
            # TODO: Look into header: {'X-APIKey-Quota-Current': '10'
            response = requests.get('https://api.pipl.com/search', params=payload)
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
            self._handle_pipl_api_errors(response)
            if hasattr(response, 'from_cache') and not response.from_cache:
                pass
            json_response = json.loads(response.text)
        except EngagementException as e:
            raise e
        except Exception as e:
            raise EngagementException(e, True)

        return json_response

    def enrich_person(self):

        # Go over response. If it contains possible persons, create a list of them, for future querying
        response = self.get_info()
        person = response.get('person', None)
        possible_persons = response.get('possible_persons', None)

        if False:  # TODO: Complete. This is for the purpose of 'hinting' via job name
            my_matches = []
            job_name = 'viber'
            for pp in response.get('possible_persons', []):
                for job in pp.get('jobs', []):
                    if 'display' in job and job_name in job['display'].lower():
                        my_matches.append(pp)

        if person:
            # Keep the score
            self.set_data(P.MATCH_SCORE, person['@match'])

            # Grab name
            if 'names' in person:
                if 'first' in person['names'][0]:
                    self.set_data(P.FIRST_NAME, person['names'][0]['first'])
                if 'last' in person['names'][0]:
                    self.set_data(P.LAST_NAME, person['names'][0]['last'])
                if 'middle' in person['names'][0]:
                    self.set_data(P.MIDDLE_NAME, person['names'][0]['middle'])

            # TODO: is it possible that person already contains the below info? Should we resolve conflicts?
            #       Otherwise, we're overriding it - in case of DOB and creating duplicates in Jobs
            for job in person.get('jobs', []):
                j = {}
                if 'organization' in job:
                    j[P.JOB_NAME] = job['organization']
                if 'title' in job:
                    j[P.JOB_TITLE] = job['title']
                if 'date_range' in job and 'start' in job['date_range']:
                    j[P.JOB_STARTED] = job['date_range']['start']
                if 'date_range' in job and 'end' in job['date_range']:
                    j[P.JOB_ENDED] = job['date_range']['end']
                if 'date_range' in job and 'start' in job['date_range'] and 'end' not in job['date_range']:
                    j[P.JOB_CURRENT] = True
                if len(j) > 0:  # Pipl sometimes return jobs with no organization or title, so we don't add empty
                    self.add_data(P.JOBS, j)

            # Grab photos
            for photo in person.get('images', []):
                self.add_data(P.PHOTOS, {P.PHOTO_URL: photo['url']})

            # Handle date of birth
            if 'dob' in person:
                self.add_data(P.DOB, person['dob'])

            # Collect emails
            for email in person.get('emails', []):
                self.add_data(P.EMAILS, email['address'])

            # Collect phones
            for phone in person.get('phones', []):
                self.add_data(P.PHONES, str(phone['number']))

            # Gender
            if 'gender' in person:
                self.add_data(P.GENDER, person['gender']['content'].lower())

            # Educations
            for education in person.get('educations', []):
                ed = {}
                if 'school' in education:
                    ed[P.EDUCATION_INSTITUTE] = education['school']
                if 'degree' in education:
                    ed[P.EDUCATION_DEGREE] = education['degree']
                if 'date_range' in education:
                    start_year_str = education['date_range']['start'][0:4] if 'start' in education['date_range'] else ''
                    end_year_str = education['date_range']['end'][0:4] if 'end' in education['date_range'] else ''
                    ed[P.EDUCATION_YEARS] = '%s-%s' % (start_year_str, end_year_str)
                if len(ed) > 1:
                    self.add_data(P.EDUCATIONS, ed)

            # Username
            for username in person.get('usernames', []):
                self.add_data(P.USERNAMES, {P.USERNAME_VALUE: username['content']})


            # TODO: can grab the following... - 'languages', 'urls' (of socials), 'user_ids', 'relationships'

        if possible_persons:
            self.set_data("possible_persons", len(possible_persons))

            if len(possible_persons) == 1:
                self.set_data("single_person_score", possible_persons[0].match)

            # TODO: implement handling of possible persons...
            #p = AcureRatePerson()
            #persons.append(p)

        else:
            self.set_data("possible_persons", 0)

        return [P.FULL_NAME]
