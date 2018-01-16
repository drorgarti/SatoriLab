import re
from urllib.parse import quote
import traceback
import datetime
from bson import json_util
import json
import copy

from entities.acurerate_attributes import P, C, R, T, G
from entities.acurerate_entity import AcureRateEntity
from entities.acurerate_job import AcureRateJob

from utils.email_util import EmailUtil
from utils.acurerate_utils import AcureRateUtils

from store.db_wrapper import DBWrapper


PERSON_MIN_AGE = 18
PERSON_MAX_AGE = 120


class AcureRatePerson(AcureRateEntity):

    def __init__(self, first_name=None, middle_name=None, last_name=None, prefix=None, suffix=None, age=None,
                 gender=None, socials=None, phones=None, email=None, emails=None, job_name=None, job_title=None):
        super().__init__()

        # TODO: Validate
        # 1) make sure there are no conflicts - email and emails...
        # 2) make sure that age is number, etc.

        if first_name and first_name.strip() != "":
            self.set_data('self', 'first_name', first_name)
        if middle_name and middle_name.strip() != "":
            self.set_data('self', 'middle_name', middle_name)
        if last_name and last_name.strip() != "":
            self.set_data('self', 'last_name', last_name)
        if prefix and prefix.strip() != "":
            self.set_data('self', 'prefix', prefix)
        if suffix and suffix.strip() != "":
            self.set_data('self', 'suffix', suffix)
        if age and PERSON_MIN_AGE < age < PERSON_MAX_AGE:
            self.set_data('self', 'age', age)
        if gender and gender.lower() in ["male", "female"]:
            self.set_data('self', 'gender', gender)
        if email:
            self.set_data('self', 'emails', [email])
        elif emails:
            self.set_data('self', 'emails', emails)
        if job_name or job_title:
            job = {"job_name": job_name, "job_title": job_title}
            self.set_data('self', 'jobs', [job])

    def __str__(self):
        full_name = self.deduced.get(P.FULL_NAME, "")
        if P.PREFIX in self.deduced:
            full_name = self.deduced[P.PREFIX] + " " + full_name
        if P.SUFFIX in self.deduced:
            full_name = full_name + " (" + self.deduced[P.SUFFIX] + ")"
        return full_name

    def __repr__(self):
        name_str = str(self)
        email = " (" + self.deduced[P.EMAIL] + ")" if P.EMAIL in self.deduced else ""
        return name_str + email

    @staticmethod
    def from_json_string(entity_json_string):
        data = json.loads(entity_json_string, object_hook=json_util.object_hook)
        p = AcureRatePerson()
        p.from_dictionary(data)
        return p

    @staticmethod
    # TODO: can this method be made generic and moved to base class (AcureRateEntity)
    def reconstruct(data):
        p = AcureRatePerson()
        p.from_dictionary(data)
        return p

    def contacts_referring_to_me(self):
        contacts = set()
        for d in self.sources(provider_filter='GoogleContacts'):
            if d['attribution_id'] != self.aid:
                contacts.add(d['attribution_id'])
        return list(contacts)

    def connected_via_contacts(self, person):
        for d in person.sources(provider_filter='GoogleContacts'):
            if d['attribution_id'] == self.aid:
                return True
        return False

    def connected_via_linkedin(self, person):
        for d in self.sources(provider_filter='LinkedInContacts'):
            if d['attribution_id'] == person.aid:
                return True
        return False

    def get_circles(self):
        """
        Returns all the circles the person belongs to.

        :return: list of circle names
        """
        circles = []
        # Check if founder, if 'yes' who are the other founders?

        # Check if in board, if 'yes' who are the other board members?

        # Check if person is in executive team of a company?

        # Is person part of an anonymous circle?

        return circles

    def get_labels(self):
        """
        Returns all the labels that characterize this person. The default is being a Person.
        It can be a User, Business, Founder, etc.

        :return: list of labels, capitalized
        """
        labeled = ['business', 'founder', 'investor', 'ceo']
        labels = [G.LABEL_PERSON]

        if hasattr(self, P.USER_ID):
            labels.append(G.LABEL_USER)

        # Iterate over all properties
        for k in self.deduced.keys():
            if k in labeled:
                labels.append(k.capitalize())
        return labels

    def get_properties(self):
        """
        Returns all the properties that can be externalized (not the temps, relations, labels, etc.)
        All those who should not be returned should be added to the exclude list and label list
        :return: Dict of keys, values
        """
        excluded = ['related_vcs', 'investments', 'advisory_jobs', 'educations', 'accredited_companies', 'accredited_jobs',
                    'jobs', 'accredited_jobs_2', 'related_investors', 'unrecognized_companies',
                    'twitter_friends', 'twitter_followers']
        labeled = ['business', 'founder', 'investor', 'ceo']
        properties = {}

        # Iterate over all properties
        for k, v in self.deduced.items():
            if k not in labeled and k not in excluded:
                properties[k] = v

        # Set summary of job areas, roles, seniorities:
        if P.ACCREDITED_JOBS_2 in self.deduced:
            jobs_names, jobs_areas, jobs_roles, jobs_seniorities = self.get_all_jobs_information()
            if jobs_names:
                properties['job_names'] = list(jobs_names)
            if jobs_areas:
                properties['job_areas'] = list(jobs_areas)
            if jobs_roles:
                properties['job_roles'] = list(jobs_roles)
            if jobs_seniorities:
                properties['jobs_seniorities'] = list(jobs_seniorities)

        return properties

    def get_relations(self, filter=None):
        """
        Looks at raw data of person entity and returns all relations.

        :return: List of tupples, each tupple: (target_aid, relationship type, relationship properties)
        """
        from store.store import Store

        relations = set()

        # Return a list of tuples (target_aid, relation_type, relation_properties)
        for d in self.sources(provider_filter='GoogleContacts'):
            if d[P.ATTRIBUTION_ID] == self.aid:  # Exclude myself. Don't want arrow point to me.
                continue
            person2 = Store.get_person_by_aid(d[P.ATTRIBUTION_ID])
            if person2:
                strength = self.contact_relation_strength_with_person(person2)
                relations.add((person2.aid, G.RELATION_LABEL_CONTACT, self.aid, 'strength: %s' % strength))

        for d in self.sources(provider_filter='LinkedInContacts'):
            if d[P.ATTRIBUTION_ID] == self.aid:  # Exclude myself. Don't want arrow point to me.
                continue
            person2 = Store.get_person_by_aid(d[P.ATTRIBUTION_ID])
            if person2:
                strength = 1
                if person2.connected_via_linkedin(self):
                    strength = 2
                relations.add((person2.aid, G.RELATION_LABEL_LINKEDIN_FRIEND, self.aid, 'strength: %s' % strength))

        # Go over all Twitter friends
        for twitter_friend_batch in self.deduced.get(P.TWITTER_FRIENDS, []):
            for screen_name in twitter_friend_batch:
                # Check if we have this screen_name in DB
                twitter_friend = Store.get_person({P.TWITTER_SCREEN_NAME: screen_name})
                if twitter_friend:
                    strength = 5
                    relations.add((self.aid, G.RELATION_LABEL_TWITTER_FRIEND, twitter_friend.aid, 'strength: %s' % strength))

        # Go over companies that the person is founder of
        for j in self.deduced.get(P.ACCREDITED_JOBS_2, []):
            company = Store.get_company({"deduced." + C.ALIASES: j['job_name'].lower()})
            if company:
                job = AcureRateJob.reconstruct(j)
                if job.is_founder():
                    strength = 10  # TODO: anything here? (maybe recency - if it was long time ago...)
                    relations.add((self.aid, G.RELATION_LABEL_FOUNDER_OF, company.aid, 'strength: %s' % strength))

        # Go over jobs and create EMPLOYEE_OF relations
        for j in self.deduced.get(P.ACCREDITED_JOBS_2, []):
            company = Store.get_company({"deduced." + C.ALIASES: j['job_name'].lower()})
            if company:
                job = AcureRateJob.reconstruct(j)
                jobs_areas, jobs_roles, jobs_seniorities = job.information()
                strength = self.working_relation_strength_with_company(job, company)
                relation_str = 'strength: %s, jobs_areas: %s, jobs_roles: %s, jobs_seniorities: %s' % \
                               (strength, list(jobs_areas), list(jobs_roles), list(jobs_seniorities))
                relations.add((self.aid, G.RELATION_LABEL_EMPLOYEE_OF, company.aid, relation_str))

        # Go over board positions and create BOARD_AT relations
        for advisory_job in self.deduced.get(P.ADVISORY_JOBS, []):
            company = Store.get_company({"deduced." + C.ALIASES: advisory_job['job_name'].lower()})
            if company:
                strength = self.board_relation_strength_with_company(advisory_job, company)
                relations.add((self.aid, G.RELATION_LABEL_ADVISOR_AT, company.aid, 'strength: %s' % strength))

        # Go over people who are references and provide recomendation
        for referer in self.deduced.get(P.REFERENCES, []):
            person = Store.get_person_by_aid(referer[P.REFERER_AID])
            if person:
                strength = 3  # TODO: Only God knows what should go here... :-)
                relations.add((person.aid, G.RELATION_LABEL_REFERENCED, self.aid, 'strength: %s' % strength))
            pass

        # Go over educations and create STUDIED_AT relations
        for education in self.deduced.get(P.EDUCATIONS, []):
            company = Store.get_company({"deduced." + C.ALIASES: education[P.EDUCATION_INSTITUTE].lower()})
            if company:
                strength = self.education_relation_strength_with_company(education, company)
                rels_str = ["strength: %s" % strength]
                if P.EDUCATION_DEGREE in education:
                    rels_str.append("degree: '%s'" % education[P.EDUCATION_DEGREE].replace("'", ""))
                if P.EDUCATION_YEARS in education:
                    rels_str.append("years: '%s'" % education[P.EDUCATION_YEARS].replace(",", ""))
                relation_str = ', '.join(rels_str)
                relations.add((self.aid, G.RELATION_LABEL_STUDIED_AT, company.aid, relation_str))

        # If filter provided, leave only relations that are relevant
        if filter:
            relations = [tup for tup in relations if tup[1].lower() == filter.lower()]

        return relations

    # Calculate the relation's strength of two persons
    def contact_relation_strength_with_person(self, person):
        # Persons: A, B
        # 1: A is connected to B in LinkedIn
        # 1: A is following B in Twitter
        # 1: A has invalid email of B (for every email)
        # 2: A has valid email of B (for every email)
        # 1: A has landline phone of B (for every email)
        # 3: A has mobile phone of B (for every email)
        # 3: A has DOB of B
        # 1: A has job/title of B
        # 3: From mobile phone: there's a photo/notes/...

        weight = 0
        # Check if self is connected to person via LinkedIn
        if self.connected_via_linkedin(person):
            weight += 1

        # Check if self is connected to person via Twitter (following/friend)
        # TODO: 1) Implement
        # TODO: 2) Factor it with the level of activity of the person in Twitter (account creation date, metrics)

        # Check if self is connected to person via Google Contacts
        for d in self.sources(provider_filter='GoogleContacts'):
            if d['attribution_id'] == person.aid:
                if 'emails' in d and d['emails']:
                    weight += len(d['emails']) * 2
                if 'phones' in d and d['phones']:
                    weight += len(d['phones']) * 2
                if 'phone' in d and d['phone']:  #  Backward compatability... (spelling mistake - not plural
                    weight += len(d['phone']) * 2
                if 'dob' in d and d['dob']:
                    weight += 3
                if 'jobs' in d and d['jobs']:
                    weight += 1

        # TODO: check valid/invalid emails...
        # TODO: check phones... landline or mobile...

        return weight if weight <= 10 else 10

    def education_relation_strength_with_company(self, education, organization):
        weight = 1
        current_year = datetime.datetime.now().year

        if P.EDUCATION_YEARS in education:
            if P.EDUCATION_YEARS in education:
                end_year = None
                pattern = re.sub('\d', 'y', education[P.EDUCATION_YEARS])
                if pattern == 'yyyy-yyyy':
                    start_year = int(education[P.EDUCATION_YEARS][0:4])
                    end_year = int(education[P.EDUCATION_YEARS][5:])
                elif pattern == 'yyyy':
                    end_year = int(education[P.EDUCATION_YEARS][0:4])
                if end_year:
                    recency = current_year - end_year
                    if recency < 2:
                        weight += 3
                    elif recency < 4:
                        weight += 2
                    elif weight < 8:
                        weight += 1

            # TODO: take into consideration the period of studies (3 years, 6 years, etc.)
            # TODO: take into consideration the organization.
        return weight

    # Calculate the relation strength between this person and a company
    # 1-3 points on veterance, 1-3 points on recency, 1-3 points on size of company
    def working_relation_strength_with_company(self, job, company):
        weight = 1
        current_year = datetime.datetime.now().year

        # Factor in how many years this person had worked in the company
        working_years = job.get_working_years()
        if working_years:
            if working_years <= 2:
                weight += 1
            elif working_years <= 5:
                weight += 2
            else:
                weight += 3
        else:
            weight += 1

        # Factor how long ago was it
        end_year = job.get_end_year()
        if end_year:
            recency = current_year - end_year
            if recency <= 2:  # 2 years ago
                weight += 3
            elif recency <= 5:
                weight += 2
            else:
                weight += 1
        else:
            weight += 1

        # Factor in the size of the company and the seniority of the person
        try:
            employee_range = company.deduced.get(C.EMPLOYEES_RANGE, C.RANGE_1_10)
            senior = job.is_senior(employee_range)
            if senior:
                weight += 3
            else:
                weight += 1
        except:
            weight += 1

        return weight

    def board_relation_strength_with_company(self, advisory_job, company):
        weight = 5

        # Check year (if exists)
        try:
            if P.JOB_STARTED in advisory_job:
                current_year = datetime.datetime.now().year
                years_passed = current_year - int(advisory_job[P.JOB_STARTED])
                if years_passed in [1, 2, 3]:
                    weight = 10
        except:
            weight = 3

        # TODO: Consider number of board members, company size, etc.

        return weight

    def merge_person(self, person):
        changed = False
        # Go over all the data sources of the new person and make sure we don't have this key already
        for ds in person.sources():
            if 'provider_name' not in ds or 'enrich_key' not in ds:
                raise Exception('Unable to merge person - missing provider_name or enrich_key attributes (person: %s)' % person)
            # Check to see if we have such provider/enich_key combination
            srcs = self.sources(provider_filter=ds['provider_name'], enrich_key_filter=ds['enrich_key'])
            if len(srcs) == 0:
                changed = True
                if ds['provider_name'] in self.data_sources:
                    self.data_sources[ds['provider_name']].append(ds)
                else:
                    self.data_sources[ds['provider_name']] = [ds]
            #else: TODO: we may want to compare dates and take the latter one that was resolved. They could be different!
        return changed

    def same_person(self, person):
        # TODO: need to refine this - email is not the only criteria for the person to be the same
        #       some people do not have emails - like new investors we put in the DB by name only
        if P.EMAIL not in self.deduced or P.EMAIL not in person.deduced:
            return False
        return self.deduced[P.EMAIL] == person.deduced[P.EMAIL]

    def studied_together(self, person, check_years=False):
        # TODO: implement...
        pass

    def worked_together(self, person, check_years=False):

        if 'accredited_companies' not in self.deduced or 'accredited_companies' not in person.deduced:
            return

        my_companies = set([c for c in self.deduced['accredited_companies']])
        person_companies = set([c for c in person.deduced['accredited_companies']])

        # TODO: implement checking by years

        # Cross both sets to find mutual values
        mutual_work_place = my_companies & person_companies
        return mutual_work_place if len(mutual_work_place) > 0 else None

    def business_related_contacts(self, high_profile=True):
        contacts = []
        my_name = self.deduced[P.FULL_NAME]

        # Get all of the people who are person's contacts:
        if high_profile:
            query = {"$and": [{"$or": [{"deduced.ceo": True},
                                       {"deduced.investor": True}]},
                              {"$or": [{"data_sources.GoogleContacts.attribution_id": self._aid},
                                       {"data_sources.LinkedInContacts.attribution_id": self._aid}]}]}
        else:
            query = {"$and": [{"deduced.business": True},
                              {"$or": [{"data_sources.GoogleContacts.attribution_id": self._aid},
                                       {"data_sources.LinkedInContacts.attribution_id": self._aid}]}]}
        cursor = DBWrapper.get_persons(query)
        contacts.extend([(r[P.DEDUCED][P.FULL_NAME], R.CONTACT_OF, my_name) for r in cursor if P.FULL_NAME in r[P.DEDUCED] and r[P.DEDUCED][P.FULL_NAME] != my_name])

        # Get all those investors he knows
        if P.RELATED_INVESTORS in self.deduced:
            contacts.extend([(n, r, c) for n, r, c in self.deduced[P.RELATED_INVESTORS]])

        try:
            # Go over all the companies the person sits in the board and get all those fellow board members, the founders and team
            # (even if they are not investors, thus may not appear in 'related_investors')
            for advisory_job in self.deduced.get(P.ADVISORY_JOBS, []):
                advisory_job_company_name = advisory_job[P.JOB_NAME]
                company = DBWrapper.get_companies({"deduced.aliases": advisory_job_company_name.lower()}, True)
                if company:
                    contacts.extend([(n, R.ADVISOR_AT, advisory_job_company_name) for n in company["deduced"].get(C.ADVISORS, []) if n != my_name])
                    contacts.extend([(n, R.FOUNDER_OF, advisory_job_company_name) for n in company["deduced"].get(C.FOUNDERS, []) if n != my_name])
                    contacts.extend([(n, R.WORKED_AT, advisory_job_company_name) for n in company["deduced"].get(C.TEAM, []) if n != my_name])
        except Exception as e:
            pass

        # TODO: get founders he worked with in past ventures. E.g. Eran Sher knows Alon Eizenman

        # TODO: it's possible we have the same person twice on the list: both with WORKED_AT relation and FOUNDER_AT, or BOARD_AT - reduce it?

        # Get all the people who worked with him in previous/current jobs
        contacts = list(set(contacts))
        return contacts

    def is_founder_at(self, company_name):
        t = self.title_at(company_name)
        return t is not None and AcureRateUtils.is_founder(t)

    def is_ceo_at(self, company_name):
        t = self.title_at(company_name)
        return t is not None and AcureRateUtils.is_ceo(t)

    def is_cfo_at(self, company_name):
        t = self.title_at(company_name)
        return t is not None and AcureRateUtils.is_cfo(t)

    def is_ceo(self):
        return P.CEO in self.deduced and self.deduced[P.CEO]

    def is_related_to_companies(self, companies):
        s1 = set(companies if companies is not None else [])
        s2 = set([c.lower() for c in self.deduced.get(P.ACCREDITED_COMPANIES, [])])
        mutual_work_place = s1 & s2
        return len(mutual_work_place) > 0

    def title_at(self, company_aliases):
        # Check if this company appears in the accredited companies:
        for aj in self.deduced.get(P.ACCREDITED_JOBS, []):
            job_name = aj[0]
            job_title = aj[1]
            if job_name.lower() in company_aliases:
                return job_title
        return None

    def board_at(self, company_name):
        if P.ADVISORY_JOBS not in self.deduced:
            return False
        return company_name in [job[P.JOB_NAME] for job in self.deduced[P.ADVISORY_JOBS]]

    def fuzzy_match_on_jobs(self, job):
        # Compare the job name and job roles against self job names and roles
        jobs_names, jobs_areas, jobs_roles, jobs_seniorities = self.get_all_jobs_information()
        if job.name() in jobs_names:
            return True
        roles_intersection = set(job.roles()).intersection(set(jobs_roles))
        areas_intersection = set(job.areas()).intersection(set(jobs_areas))
        if roles_intersection or areas_intersection:
            return True
        return False

    # Return 4 lists: jobs_names, jobs_seniority, jobs_areas, jobs_roles (unique values in each)
    def get_all_jobs_information(self):
        jobs_names = set()
        jobs_areas = set()
        jobs_roles = set()
        jobs_seniorities = set()
        for job in self.deduced.get(P.ACCREDITED_JOBS_2, []):
            jobs_names.add(job['job_name'])
            for job_role in job.get('job_roles', []):
                if 'job_area' in job_role and job_role['job_area'] and job_role['job_area'] != T.AREA_UNKNOWN:
                    jobs_areas.add(job_role['job_area'])
                if 'job_role' in job_role and job_role['job_role'] and job_role['job_role'] != T.ROLE_UNKNOWN:
                    jobs_roles.add(job_role['job_role'])
                if 'job_seniority' in job_role and job_role['job_seniority'] and job_role['job_seniority'] != T.SENIORITY_UNKNOWN:
                    jobs_seniorities.add(job_role['job_seniority'])
        return jobs_names, jobs_areas, jobs_roles, jobs_seniorities

    # Return 3 lists of a specific job: job_seniority, jobs_areas, jobs_roles (or None if job name not found)
    def get_job_information_by_job(self, job_name):
        jobs_areas = set()
        jobs_roles = set()
        jobs_seniorities = set()
        for job in self.deduced[P.ACCREDITED_JOBS_2]:
            if 'job_name' in job and job['job_name'] == job_name:
                for job_role in job.get('job_roles', []):
                    if 'job_area' in job_role and job_role['job_area'] and job_role['job_area'] != T.AREA_UNKNOWN:
                        jobs_areas.add(job_role['job_area'])
                    if 'job_role' in job_role and job_role['job_role'] and job_role['job_role'] != T.ROLE_UNKNOWN:
                        jobs_roles.add(job_role['job_role'])
                    if 'job_seniority' in job_role and job_role['job_seniority'] and job_role['job_seniority'] != T.SENIORITY_UNKNOWN:
                        jobs_seniorities.add(job_role['job_seniority'])
                return jobs_areas, jobs_roles, jobs_seniorities
        return None

    # The PIVOT is the new person who just joined the system and uploaded his contacts.
    # We want to check if this PIVOT person can join and cliques
    # @@@
    def can_join_cliques(self):
        # Run a query to get all those who point to pivot AND pivot points to them AND are part of Clique
        cliques_to_join = DBWrapper.get_potential_cliques(self.aid)

        # Connect self to these cliques
        # TODO: this is easy... implement when needed.

        pass

    def attr(self, attr_key):
        return self.deduced.get(attr_key, None)

    def _enqueue_for_enrichment(self, job_name):
        print("Should enqueue - %s", job_name)
        pass

    def _digest_is_founder(self):
        # Check all accredited companies' titles:
        if any(AcureRateUtils.is_founder(aj[1]) for aj in self.deduced.get(P.ACCREDITED_JOBS, [])):
            self.deduced[P.FOUNDER] = True
        pass

    def _digest_is_ceo(self):
        # Check all accredited companies' titles:
        if any(AcureRateUtils.is_ceo(aj[1]) for aj in self.deduced.get(P.ACCREDITED_JOBS, [])):
            self.deduced[P.CEO] = True

    def _digest_dob(self):
        me = self.deduced
        # TODO: improve this - we currently take only Pipl
        for ds in self.sources('Pipl'):
            if P.DOB in ds:
                me[P.DOB] = ds[P.DOB][0]['display']

    def _digest_location(self):
        # TODO: we currently select the first location we find... this could be done smarter.
        me = self.deduced
        for ds in self.sources():
            if P.LOCATIONS in ds and ds[P.LOCATIONS][0].strip() != '':
                me[P.LOCATIONS] = ds[P.LOCATIONS][0]
                break
        location = me.get(P.LOCATIONS, None)
        if location is not None:
            geo_tag = AcureRateUtils.geo_tagging_from_location_str(location)
            if geo_tag:
                me[P.GEO_TAG] = geo_tag
            else:
                print('Unable to resolve location: ', location)
        pass

    def _digest_name(self):
        me = self.deduced
        # Iterate over all names from different providers. Append name to list if it has at least first and last name
        all_names = []
        for ds in self.sources():
            if 'provider_name' not in ds:
                continue
            provider_key = ds['provider_name']
            f = ds.get(P.FIRST_NAME, None)
            f = AcureRateUtils.remove_parenthesized_content(f, True)
            m = ds.get(P.MIDDLE_NAME, None)
            m = AcureRateUtils.remove_parenthesized_content(m, True)
            l = ds.get(P.LAST_NAME, None)
            l = AcureRateUtils.remove_parenthesized_content(l, True)
            if f and l:
                all_names.append((provider_key.lower(), (f, m, l)))

        # Take the name which appears first in the list according to priority of providers
        if len(all_names) > 0:
            a = sorted(all_names, key=lambda v: {"system": 0, "linkedincontacts": 1, "fullcontact": 2, "crunchbasebot": 3, "twitter": 4}.get(v[0], 999))
        else:
            a = [('<unresolved>', None, '<unresolved>')]

        me[P.FIRST_NAME] = a[0][1][0]  # [0] -> 1st row, [1] -> 2nd tuple-value (name), [0] -> first Name
        me[P.LAST_NAME] = a[0][1][2]  # [0] -> 1st row, [1] -> 2nd tuple-value (name), [2] -> last Name
        if a[0][1][1]:
            me[P.MIDDLE_NAME] = a[0][1][1]  # [0] -> 1st row, [1] -> 2nd tuple-value (name), [1] -> middle Name
            me[P.FULL_NAME] = me[P.FIRST_NAME] + " " + me[P.MIDDLE_NAME] + " " + me[P.LAST_NAME]
        else:
            me[P.FULL_NAME] = me[P.FIRST_NAME] + " " + me[P.LAST_NAME]
        pass

    def _digest_phones(self):
        me = self.deduced
        for ds in self.sources():
            if P.PHONES in ds and ds[P.PHONES] is None:
                print('BAD phones entry in %s (ds=%s)', (me[P.FULL_NAME], ds))
                continue

            if type(ds.get(P.PHONES, [])) is list:
                phone_list = ds.get(P.PHONES, [])
            else:
                phone_list = [ds.get(P.PHONES, [])]
            for phone in phone_list:
                if True:  # PhoneUtil.is_valid(phone):
                    self._append_to_deduced(P.PHONES, str(phone))

            if type(ds.get(P.PHONE, [])) is list:
                phone_list = ds.get(P.PHONE, [])
            else:
                phone_list = [ds.get(P.PHONE, [])]
            for phone in phone_list:  # Backward compatability
                if True:  # PhoneUtil.is_valid(phone):
                    self._append_to_deduced(P.PHONES, str(phone))
        # Decide on the selected phone
        # TODO: decide on heuristics to grab the best phone number (landline vs. mobile, etc.)
        if P.PHONES in me and len(me[P.PHONES]) > 0:
            me[P.PHONE] = me[P.PHONES][0]


    def _digest_emails(self):
        me = self.deduced
        for ds in self.sources():
            for email in ds.get(P.EMAILS, []):
                if email != 'full.email.available@business.subscription' and EmailUtil.is_valid(email):
                    self._append_to_deduced(P.EMAILS, email.lower())
        # Decide on the selected email
        if P.EMAILS in me and len(me[P.EMAILS]) > 0:
            me[P.EMAIL] = EmailUtil.get_preferred_email_from_list(me[P.EMAILS]).lower()
            # Sort the emails
            me[P.EMAILS] = sorted(me[P.EMAILS], key=lambda v: v, reverse=False)
        pass


    def _digest_jobs_from_emails(self):
        me = self.deduced
        for email in me.get(P.EMAILS, []):
            email_domain = EmailUtil.get_email_domain_part(email)
            q = {'deduced.email_domains': email_domain}
            cursor = DBWrapper.get_companies(q, False)
            if cursor.count() == 1:
                self._append_to_deduced(P.JOBS, {P.JOB_NAME: cursor[0]['deduced'][C.NAME]})

    def _digest_photos(self):
        me = self.deduced
        # Iterate over all photos. If we have a linked-in one, grab it, otherwise, CB, otherwise, others...
        all_photos = []
        for ds in self.sources():
            for photo in ds.get(P.PHOTOS, []):
                # Analyze photo source
                photo_source = photo[P.PHOTO_SOURCE] if P.PHOTO_SOURCE in photo else 'unknown'
                if photo_source == 'unknown' and 'media.licdn' in photo[P.PHOTO_URL]:
                    photo_source = "linkedin"
                elif photo_source == 'unknown' and 'graph.facebook' in photo[P.PHOTO_URL]:
                    photo_source = "facebook"
                elif photo_source == 'unknown' and 'crunchbase' in photo[P.PHOTO_URL]:
                    photo_source = "crunchbase"
                all_photos.append((photo_source.lower(), photo[P.PHOTO_URL]))
        # take the first photo in the sorted array - it's URL only
        if len(all_photos) > 0:
            a = sorted(all_photos, key=lambda v: {"linkedin": 1, "crunchbase": 2, "twitter": 3, "angellist": 4}.get(v[0], 999))
            me[P.PHOTO_SELECTED] = a[0][1]
        pass

    def _digest_advisory_roles(self):
        me = self.deduced

        # Check data sources
        for ds in self.sources():
            for advisory_job in ds.get(P.ADVISORY_JOBS, []):
                self._append_to_deduced(P.ADVISORY_JOBS, advisory_job)

        # Go over companies that the person is founder of
        for j in self.deduced.get(P.ACCREDITED_JOBS_2, []):
            job = AcureRateJob.reconstruct(j)
            board_role = job.board_role()
            if board_role:
                role = board_role.get(P.JOB_ROLE, '')
                self._append_to_deduced(P.ADVISORY_JOBS, {P.JOB_NAME: job.job_name, P.JOB_TITLE: role})

        # Check the accredited jobs
        # for aj in me.get(P.ACCREDITED_JOBS, []):
        #     job_name = aj[0]
        #     job_title = aj[1]
        #     if AcureRateUtils.is_board_member(job_title):
        #         self._append_to_deduced(P.ADVISORY_JOBS, {P.JOB_NAME: job_name, P.JOB_TITLE: job_title})
        pass

    def _digest_jobs(self):
        me = self.deduced
        roles_by_company = {}
        companies_with_no_titles = set()
        for ds in self.sources():
            # Decide on jobs the provider has and add relations
            for job in ds.get(P.JOBS, []):
                # Add the 'raw' job as provider by engager
                self._append_to_deduced(P.JOBS, job)

                # If there's title, deduce if it's business
                if P.JOB_TITLE in job:
                    # if AcureRateUtils.is_business(job[P.JOB_TITLE]):
                    #     me[P.BUSINESS] = True
                    #     me[P.BUSINESS_REASON] = job[P.JOB_TITLE]
                    if AcureRateUtils.is_founder(job[P.JOB_TITLE]):
                        me[P.FOUNDER] = True

                if P.JOB_NAME not in job:
                    continue
                # Standartize the company name:
                company_alias = AcureRateUtils.aliasize(job[P.JOB_NAME])
                company = DBWrapper.get_companies({"deduced.aliases": company_alias}, True)
                if company is None:
                    continue
                company_name = company['deduced'][C.NAME]
                new_roles = []
                if P.JOB_TITLE in job:
                    ntitles = AcureRateUtils.normalized_titles(job[P.JOB_TITLE])
                    if len(ntitles) == 0:
                        ntitles.append((T.ROLE_UNKNOWN, T.SENIORITY_UNKNOWN, T.AREA_UNKNOWN))
                    # Iterate over all roles mentioned in the same title line (e.g. "Head of Product and R&D Manager")
                    for role, seniority, area in ntitles:
                        new_role = {P.JOB_TITLE: job[P.JOB_TITLE], P.JOB_ROLE: role, P.JOB_SENIORITY: seniority, P.JOB_AREA: area}
                        if P.JOB_STARTED in job:
                            year = AcureRateJob.get_year(job[P.JOB_STARTED])
                            if year:
                                new_role[P.JOB_STARTED] = year
                            else:
                                pass  # TODO: Remove. Just for the sake of trapping unhandled cases
                        if P.JOB_ENDED in job:
                            year = AcureRateJob.get_year(job[P.JOB_ENDED])
                            if year:
                                new_role[P.JOB_ENDED] = year
                            else:
                                pass
                        if P.JOB_CURRENT in job:
                            new_role[P.JOB_CURRENT] = datetime.datetime.now().year
                        # TODO: the below are due to backward compatability. Properties should be renamed in Mongo and then these can be removed...
                        if P.JOB_STARTED_BC in job:
                            year = AcureRateJob.get_year(job[P.JOB_STARTED_BC])
                            if year:
                                new_role[P.JOB_STARTED] = year
                            else:
                                pass
                        if P.JOB_ENDED_BC in job:
                            year = AcureRateJob.get_year(job[P.JOB_ENDED_BC])
                            if year:
                                new_role[P.JOB_ENDED] = year
                            else:
                                pass
                        new_roles.append(new_role)
                    companies_with_no_titles.discard(company_name)
                elif company_name not in roles_by_company:
                    companies_with_no_titles.add(company_name)
                for nr in new_roles:
                    if company_name in roles_by_company:
                        # TODO: could be improved - there are cases where a person has same role, but a new role with higher seniority
                        if P.JOB_ROLE in nr:
                            if all(r[P.JOB_ROLE] != nr[P.JOB_ROLE] for r in roles_by_company[company_name] if P.JOB_ROLE in r):
                                roles_by_company[company_name].append(nr)
                    else:
                        roles_by_company[company_name] = [nr]

        jobs_list = [{P.JOB_NAME: name, P.JOB_ROLES: roles} for name, roles in roles_by_company.items()]
        jobs_no_roles_list = [{P.JOB_NAME: name} for name in companies_with_no_titles]
        jobs_list.extend(jobs_no_roles_list)

        if jobs_list and len(jobs_list) > 0:
            me[P.ACCREDITED_JOBS_2] = sorted(jobs_list, key=lambda v: v[P.JOB_NAME], reverse=False)
            me[P.JOBS] = sorted(me[P.JOBS], key=lambda v: v.get(P.JOB_NAME, ''), reverse=False)
            #me[P.JOBS] = sorted(me[P.JOBS], key=lambda v: v[P.JOB_NAME], reverse=False)

            # Business Reason
            # TODO: needs re-vamp - maybe keep few titles as business reason
            for j in me[P.ACCREDITED_JOBS_2]:
                for jr in j.get(P.JOB_ROLES, []):
                    if P.JOB_TITLE in jr and AcureRateUtils.is_business(jr[P.JOB_TITLE]):
                        me[P.BUSINESS] = True
                        me[P.BUSINESS_REASON] = jr[P.JOB_TITLE]
                        break

        pass

    # Collect related investors from the following resources:
    # (1) Personal contacts
    # (2) Related Investors at companies the person worked at
    # (3) Related Investors at advisory board the person attended
    def _digest_related_investors(self):
        me = self.deduced

        # Get people who are: (a) in founder's contacts, (b) investors
        if hasattr(self, P.AID):
            query = {"$and": [{"deduced.investor": True},
                              {"$or": [{"data_sources.GoogleContacts.attribution_id": self._aid},
                                       {"data_sources.LinkedInContacts.attribution_id": self._aid}]}]}
            cursor = DBWrapper.get_persons(query)
            for r in cursor:
                investor = AcureRatePerson.reconstruct(r)
                #ri = (investor.deduced[P.FULL_NAME], R.CONTACT_OF, investor.deduced[P.INVESTOR_REASON])
                ri = (investor.deduced[P.FULL_NAME], R.CONTACT_OF, me[P.FULL_NAME])
                self._append_to_deduced(P.RELATED_INVESTORS, ri)

        # Go over all jobs and collect the related investors
        # Be noted that a person may have few job-entries which are different roles he had in the same company!
        aliases_checked = []
        unique_jobs = {}
        # Create list of unique jobs (original list may include few different names which are eventually same org)
        for job in me.get(P.JOBS, []):
            if P.JOB_NAME in job:
                unique_jobs[AcureRateUtils.aliasize(job[P.JOB_NAME])] = job
        # Go over the unique list and gather the related investors
        for alias, job in unique_jobs.items():
            if alias not in aliases_checked:
                query = {"deduced.aliases": alias}
                company = DBWrapper.get_companies(query, True)
                if company:
                    if C.ACQUIRED_BY in company[C.DEDUCED] and P.JOB_TITLE in job:
                        title = job[P.JOB_TITLE]
                        if AcureRateUtils.is_ceo(title) or AcureRateUtils.is_founder(title):
                            self._append_to_deduced(P.EXITS, company[C.DEDUCED][C.NAME])
                    if C.ALIASES in company[C.DEDUCED]:
                        aliases_checked += company[C.DEDUCED][C.ALIASES]
                        self._append_to_deduced(P.ACCREDITED_COMPANIES, company[C.DEDUCED][C.NAME])
                        self._append_to_deduced(P.ACCREDITED_JOBS, [company[C.DEDUCED][C.NAME], job.get(P.JOB_TITLE, None)])
                    if C.RELATED_VCS in company[C.DEDUCED]:
                        for ri in company[C.DEDUCED][C.RELATED_VCS]:
                            self._append_to_deduced(P.RELATED_VCS, ri)
                    if C.RELATED_INVESTORS in company[C.DEDUCED]:
                        # If there's no title, or the person is not senior, don't gather investors
                        if P.JOB_TITLE not in job or not AcureRateUtils.is_senior(company, job[P.JOB_TITLE]):
                            continue
                        for ri in company[C.DEDUCED][C.RELATED_INVESTORS]:
                            self._append_to_deduced(C.RELATED_INVESTORS, ri)
                else:
                    # Job name was unrecognized by the system - requires enrichment
                    self._append_to_deduced(P.UNRECOGNIZED_COMPANIES, job[P.JOB_NAME])

        # Go over all the companies that the person has an advisor role in:
        for ajob in me.get(P.ADVISORY_JOBS, []):
            query = {"deduced.aliases": ajob[P.JOB_NAME].lower()}
            company = DBWrapper.get_companies(query, True)
            if company and P.RELATED_INVESTORS in company['deduced']:
                for ri in company['deduced'][P.RELATED_INVESTORS]:
                    # Avoid adding the person itself in case he appears on the company's related investors
                    if ri[0] != me[P.FULL_NAME]:
                        self._append_to_deduced(P.RELATED_INVESTORS, ri)

        # TODO: is it possible that the person invests in a company, yet not mentioned to sit in its board?
        for investment_date, company_name, investment_details in me.get(P.INVESTMENTS, []):
            query = {"deduced.aliases": company_name.lower()}
            company = DBWrapper.get_companies(query, True)
            if company and P.RELATED_INVESTORS in company['deduced']:
                for ri in company['deduced'][P.RELATED_INVESTORS]:
                    # Avoid adding the person itself in case he appears on the company's related investors
                    if ri[0] != me[P.FULL_NAME]:
                        self._append_to_deduced(P.RELATED_INVESTORS, ri)

        # Sort the lists to sustain it between digests
        if P.ACCREDITED_JOBS in me:
            me[P.ACCREDITED_JOBS] = sorted(me[P.ACCREDITED_JOBS], key=lambda v: v[0], reverse=False)
        if P.ACCREDITED_COMPANIES in me:
            me[P.ACCREDITED_COMPANIES] = sorted(me[P.ACCREDITED_COMPANIES], key=lambda v: v, reverse=False)
        if P.UNRECOGNIZED_COMPANIES in me:
            me[P.UNRECOGNIZED_COMPANIES] = sorted(me[P.UNRECOGNIZED_COMPANIES], key=lambda v: v, reverse=False)
        pass

    def _digest_investments(self):
        me = self.deduced

        # Go over data of all providers
        for ds in self.sources():
            # TODO: need to make sure we're not adding the same investment from different sources. Currently, we have only CB*
            investments = ds.get(P.INVESTMENTS, None)
            if investments:
                for investment in investments:
                    self._append_to_deduced(P.INVESTMENTS, investment)
                    company = DBWrapper.get_companies({"deduced."+C.NAME: investment[1]}, True)
                    if company and C.CATEGORIES in company['deduced']:
                        for category in company['deduced'][C.CATEGORIES]:
                            self._append_to_deduced(P.INVESTMENT_CATEGORIES, category)
        pass

    def _digest_investor(self):
        me = self.deduced

        # We need to check (by this order) - a) person has investments? b) data_source declared him as investor c) his title suggests he's an investor
        investor_details = []
        for ds in self.sources():
            if 'provider_name' not in ds:
                continue
            provider_key = ds['provider_name']
            investments = ds.get(P.INVESTMENTS, None)
            if investments:
                reason = "%d known investments" % len(investments)
                investor_details.append((provider_key.lower(), reason))
            elif ds.get(P.INVESTOR):
                reason = ds.get(P.INVESTOR_REASON, ds['provider_name'])
                investor_details.append((provider_key.lower(), reason))
            else:
                for job in ds.get(P.JOBS, []):
                    if P.JOB_TITLE in job and AcureRateUtils.is_investor(job[P.JOB_TITLE]):
                        reason = job[P.JOB_TITLE]
                        investor_details.append((provider_key.lower(), reason))
                        break

        if len(investor_details) > 0:
            me[P.INVESTOR] = True
            a = sorted(investor_details, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "crunchbasescraper": 2, "linkedin": 3, "pipl": 4, "fullcontact": 5}.get(v[0], 999))
            me[P.INVESTOR_REASON] = a[0][1]


    def _digest_website(self):
        me = self.deduced

        # TODO: we may want to allow more than one website provided by providers and then choose the best/multiple
        for ds in self.sources():
            if P.WEBSITE in ds:
                me[P.WEBSITE] = ds[P.WEBSITE]
                return
        return

    def _digest_twitter_information(self):
        me = self.deduced

        for ds in self.sources('Twitter'):
            if P.TWITTER_SCREEN_NAME not in ds:
                continue
            me[P.TWITTER_SCREEN_NAME] = ds[P.TWITTER_SCREEN_NAME]
            if P.TWITTER_FOLLOWERS in ds:
                me[P.TWITTER_FOLLOWERS] = ds[P.TWITTER_FOLLOWERS]
            if P.TWITTER_FRIENDS in ds:
                me[P.TWITTER_FRIENDS] = ds[P.TWITTER_FRIENDS]
            # Create Twitter-Usage score
            twitter_activity = 'Low'
            if P.TWITTER_FOLLOWERS_COUNT in ds and P.TWITTER_FRIENDS_COUNT in ds:
                total = int(ds[P.TWITTER_FOLLOWERS_COUNT]) + int(ds[P.TWITTER_FRIENDS_COUNT])
                if total > 1000:
                    twitter_activity = 'Med'
            # TODO: weigh in other factors such as Tweats, likes, account creation date, etc.
            me[P.TWITTER_ACTIVITY] = twitter_activity

            # TODO: consider situations where a person may have multiple Twitter accounts and sources of info.
            break

    def _digest_socials(self):
        me = self.deduced

        # TODO: next time I visit this code to add another social, implement it smarter :-)
        linkedin_urls = []
        facebook_urls = []
        twitter_urls = []
        crunchbase_urls = []
        angellist_urls = []
        bloomberg_urls = []
        googleplus_urls = []
        for ds in self.sources():
            if 'provider_name' not in ds:
                continue
            provider_key = ds['provider_name']
            if P.LINKEDIN_URL in ds:
                linkedin_urls.append((provider_key.lower(), ds[P.LINKEDIN_URL]))
            if P.FACEBOOK_URL in ds:
                facebook_urls.append((provider_key.lower(), ds[P.FACEBOOK_URL]))
            if P.GOOGLEPLUS_URL in ds:
                googleplus_urls.append((provider_key.lower(), ds[P.GOOGLEPLUS_URL]))
            if P.TWITTER_URL in ds:
                twitter_urls.append((provider_key.lower(), ds[P.TWITTER_URL]))
            if P.CRUNCHBASE_URL in ds:
                crunchbase_urls.append((provider_key.lower(), ds[P.CRUNCHBASE_URL]))
            if P.ANGELLIST_URL in ds:
                angellist_urls.append((provider_key.lower(), ds[P.ANGELLIST_URL]))
            if P.BLOOMBERG_URL in ds:
                bloomberg_urls.append((provider_key.lower(), ds[P.BLOOMBERG_URL]))
                # TODO: make sure all URLs are quoted and safe
                # bloomberg_urls.append((provider_key.lower(), quote(ds[P.BLOOMBERG_URL], safe='')))

        # Take the name which appears first in the list according to priority of providers
        if len(linkedin_urls) > 0:
            a = sorted(linkedin_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.LINKEDIN_URL] = a[0][1]
        if len(facebook_urls) > 0:
            a = sorted(facebook_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.FACEBOOK_URL] = a[0][1]
        if len(googleplus_urls) > 0:
            a = sorted(googleplus_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.GOOGLEPLUS_URL] = a[0][1]
        if len(twitter_urls) > 0:
            a = sorted(twitter_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.TWITTER_URL] = a[0][1]
        if len(crunchbase_urls) > 0:
            a = sorted(crunchbase_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.CRUNCHBASE_URL] = a[0][1]
        if len(angellist_urls) > 0:
            a = sorted(angellist_urls, key=lambda v: {"crunchbase": 0, "crunchbasebot": 1, "fullcontact": 2, "linkedin": 3, "pipl": 4}.get(v[0], 999))
            me[P.ANGELLIST_URL] = a[0][1]
        if len(bloomberg_urls) > 0:
            a = sorted(bloomberg_urls, key=lambda v: {"bloombergscraper": 0, "crunchbase": 1, "crunchbasebot": 2, "fullcontact": 3, "linkedin": 4, "pipl": 5}.get(v[0], 999))
            me[P.BLOOMBERG_URL] = a[0][1]

        # TODO: implement CB url just like we do above for other socials...
        for ds in self.sources():
            if P.CB_PERMALINK in ds:
                me[P.CB_PERMALINK] = ds[P.CB_PERMALINK]

    def _digest_referers(self):
        me = self.deduced

        # Go over all data-sources and verify this institute is verified (in our DB)
        unknown_references = set()
        for ds in self.sources():
            for refererce in ds.get(P.REFERENCES, []):
                # Check if the person exists:
                r = DBWrapper.get_persons({P.DEDUCED+'.'+P.FULL_NAME: refererce[P.REFERER_NAME]}, True)
                if r and len(r) > 0:
                    o = {P.REFERER_AID: r['_aid'], P.REFERER_NAME: r['deduced'][P.FULL_NAME],
                         P.REFERER_REVIEW: refererce[P.REFERER_REVIEW], P.REFERER_SOURCE: ds['provider_name']}
                    self._append_to_deduced(P.REFERENCES, o)
                else:
                    unknown_references.add(refererce[P.REFERER_NAME])

            # TODO: do something with the unknown references...
            pass
        pass

    def _digest_education(self):
        me = self.deduced

        educations_map = {}
        institutes = set()
        unknown_institutes = set()
        # Go over all data-sources and verify this institute is verified (in our DB)
        for ds in self.sources():
            for education in ds.get(P.EDUCATIONS, []):
                if P.EDUCATION_INSTITUTE in education:
                    r = DBWrapper.get_companies({C.DEDUCED+"."+C.ALIASES: education[P.EDUCATION_INSTITUTE].lower()}, True)
                    if r:
                        the_name = r[C.DEDUCED][C.NAME]
                        institutes.add(the_name)
                        educations_map[the_name] = education
                    else:
                        unknown_institutes.add(education[P.EDUCATION_INSTITUTE])

        # Add all the unique/recognized educations we found
        for kn_ins in institutes:
            self._append_to_deduced(P.EDUCATIONS, educations_map[kn_ins])

        # Add all the unique/unrecognzed education institute names we DID NOT find
        for un_ins in unknown_institutes:
            self._append_to_deduced(P.UNRECOGNIZED_COMPANIES, un_ins)
        pass

    def _digest_sector(self):
        me = self.deduced

        academic_background = False

        # TODO: decide what to do with this....
        # Check if person is in Government, Academy, Business

        # Check title
        if P.PREFIX in me:
            academic_background |= AcureRateUtils.is_academic_prefix(me[P.PREFIX])

        # Check email
        if P.EMAIL in me:
            academic_background |= EmailUtil.is_academic(me[P.EMAIL])

        # TODO: Set education field...
        pass

    def _digest_related_urls(self):
        me = self.deduced

        # Iterate over all data-sources and get all related urls
        all_urls = set()
        for ds in self.sources():
            for url in ds.get(P.RELATED_URLS, []):
                all_urls.add(url[P.RELATED_URL_VALUE])

        # TODO: enhance this one... (need to also take the sources from the data_sources)
        if all_urls:
            me[P.RELATED_URLS] = list(all_urls)
        pass

    def _digest_gender(self):
        me = self.deduced

        # Go over data of all providers
        for ds in self.sources():
            # Decide on gender
            genders = ds.get(P.GENDER, None)
            if genders:
                if all(g.lower() == "male" for g in genders):
                    deduced_gender = "male"
                elif all(g.lower() == "female" for g in genders):
                    deduced_gender = "female"
                else:
                    deduced_gender = None
                # Determine gender only
                if deduced_gender and P.GENDER not in me:
                    me[P.GENDER] = deduced_gender
                # TODO: if gender is different than what we have from other providers, do we cancel it?

    # This method takes all information from providers - validates and merges it all together to "deduced"
    def digest(self):

        # Keep data before we reconstuct it - to check at the end if there were changes
        if self.deduced:
            before_reconstruct = copy.deepcopy(self.deduced)
        else:
            before_reconstruct = None

        me = self.deduced = {}
        try:

            # Digest the name from different providers
            self._digest_name()

            # Digest the phone numbers
            self._digest_phones()

            # Digest to locate best email address to work with
            self._digest_emails()

            self._digest_jobs_from_emails()

            self._digest_gender()

            self._digest_jobs()

            # Go over data of all providers
            for ds in self.sources():
                # Primary role
                if P.PRIMARY_ROLE in ds:
                    me[P.PRIMARY_ROLE] = ds[P.PRIMARY_ROLE]

            # TODO: implement _digest_founder() - replace the above code and also look into data_sources - like we do with digest_investor()
            # self._digest_founder()

            self._digest_investments()

            self._digest_investor()

            self._digest_advisory_roles()

            self._digest_related_investors()

            self._digest_photos()

            self._digest_dob()

            self._digest_location()

            self._digest_is_ceo()

            self._digest_is_founder()

            self._digest_website()

            self._digest_socials()

            self._digest_twitter_information()

            self._digest_education()

            self._digest_sector()

            self._digest_related_urls()

            self._digest_referers()

        except Exception as e:
            tb = traceback.format_exc()
            print(r'Exception occured during digest - %s\n%s' % (e, tb))

        # Decide on score (based on all the attributes we gathered)
        # TODO...

        # Check if anything changed during digest:
        if before_reconstruct is None:
            return True
        added, removed, modified, same = AcureRateUtils.dict_compare(self.deduced, before_reconstruct)
        if len(added) == 0 and len(removed) == 0 and len(modified) == 0:
            return False
        return True

