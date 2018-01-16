import requests_cache
from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C

from pycrunchbase import CrunchBase, Organization

import requests
import codecs
from pycrunchbase.resource import (
    Acquisition,
    FundingRound,
    Fund,
    IPO,
    Organization,
    Page,
    Person,
    Product,
)


class CrunchBaseEngager(Engager):

    #BASE_URL = 'https://api.crunchbase.com/v/3/'
    BASE_URL = 'https://api.crunchbase.com/v3.1/'
    PEOPLE_URL = BASE_URL + 'people'
    ORGANIZATIONS_URL = BASE_URL + 'organizations'
    LOCATIONS_URL = BASE_URL + 'locations'
    CATEGORIES_URL = BASE_URL + 'categories'
    PRODUCTS_URL = BASE_URL + 'products'

    #OLD_KEY = "04399888579f1e369106ba66d4b3b1c1"  # Past Key (where did I get it from?)

    THE_KEY = "19c22a44a27dbc6cafda3e8cc14f6618"  # Correct Key we got from CB after signing deal with them

    def __init__(self):
        super().__init__()
        self.cb = CrunchBase(CrunchBaseEngager.THE_KEY)

    def __str__(self):
        return 'CrunchBase Engager'

    def __repr__(self):
        return 'CrunchBase Engager'

    def get_provider_name(self):
        return 'CrunchBase'

    def get_short_symbol(self):
        return 'cb'

    def get_api_key(self):
        return CrunchBaseEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        self.enrich_key = None
        if t == 'AcureRatePerson':
            if P.CB_PERMALINK in self.enriched_entity.deduced:
                self.enrich_key = self.enriched_entity.deduced[P.CB_PERMALINK]
            # elif P.FULL_NAME in self.enriched_entity.deduced:
            #     name = self.enriched_entity.deduced[P.FULL_NAME]
            #     self.enrich_key = CrunchBaseEngager.formalize_permalink(name)
            # else:
            #     self.enrich_key = None
        elif t == 'AcureRateCompany':
            if C.CRUNCHBASE_PERMALINK in self.enriched_entity.deduced:
                self.enrich_key = self.enriched_entity.deduced[C.CRUNCHBASE_PERMALINK]
            # elif C.NAME in self.enriched_entity.deduced:
            #     name = self.enriched_entity.deduced[C.NAME]
            #     self.enrich_key = CrunchBaseEngager.formalize_permalink(name)
            # else:
            #     self.enrich_key = None
        else:
            self.enrich_key = None

    def engage_DEPRECATED(self, entity_type, person, force_refresh=False):
        if not super().engage(entity_type, person, force_refresh):
            return False  # no engagement occurred, thus return no change

        # Extract the information
        try:
            if entity_type == 'people':
                self.enrich_person()
            elif entity_type == 'company':
                self.enrich_company()
            success_status = True
            self.finalize(success_status)
            changed = self.is_change()
        except EngagementException as e:
            print("CrunchBase: unable to enrich person: %s" % e)
            success_status = False
            self.finalize(success_status, str(e))
            changed = True

        return changed

    def enrich_person(self):
        try:
            if not self.enrich_key:
                # Search for all the people with this name
                name = self.enriched_entity.deduced['first_name'] + " " + self.enriched_entity.deduced['last_name']
                response = self._make_request(CrunchBaseEngager.PEOPLE_URL, {'name': name})
                data = response.json().get('data')
                if not data or data.get('error'):
                    raise EngagementException("CrunchBaseEngager: error in retrieving person %s." % name)
                if len(data["items"]) > 1:
                    raise EngagementException("CrunchBaseEngager: person %s not ambiguous. Found %d people with this name." % (name, len(data["items"])))
                if len(data["items"]) == 0:
                    raise EngagementException("CrunchBaseEngager: person %s not found." % name)

                # TODO: Future: Iterate over the returned people and check if there's another matching attribute (like social url) we can use to choose the right person
                permalink = data['items'][0]['properties']['permalink']
            else:
                permalink = self.enrich_key

            # Get information on person via permalink
            response = self._make_request('https://api.crunchbase.com/v/3/people/'+permalink)
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
                if not response.from_cache:
                    pass  # code for debugging purposes
            data = response.json().get('data')
            people = Person(data)

            # TODO: deal with marking from cache... now I'm ignoring it

            # Keep the key email we used for the search
            #self.set_data("search_key", name)

            if people.data and 'relationships' in people.data and 'investments' in people.data['relationships']:
                for elem in people.data['relationships']['investments']['items']:
                    pass

            if people.data and 'relationships' in people.data and 'advisory_roles' in people.data['relationships']:
                for elem in people.data['relationships']['advisory_roles']['items']:
                    try:
                        job_title = elem["properties"]["title"]
                        company_name = elem["relationships"]["organization"]["properties"]["name"]
                        self.add_data(P.ADVISORY_JOBS, {P.JOB_TITLE: job_title, P.JOB_NAME: company_name})
                    except Exception as e:
                        print('Unable to get advisory roles for %s' % permalink)

            if people.data and 'properties' in people.data and 'gender' in people.data['properties']:
                self.set_data(P.GENDER, people.data['properties']['gender'])

            if people.data and 'properties' in people.data and 'bio' in people.data['properties']:
                self.set_data(P.SHORT_DESCRIPTION, people.data['properties']['bio'])

            if people.born_on:
                self.set_data(P.DOB, people.born_on)

            if people.degrees:
                for degree in people.degrees.items:
                    if degree.school and degree.school.name:
                        education = {}
                        education[P.EDUCATION_INSTITUTE] = degree.school.name
                        degree_years = None
                        if degree.started_on:
                            degree_years = '%s' % degree.started_on.year
                        if degree.started_on and degree.completed_on:
                            degree_years = '%s-%s' % (degree.started_on.year, degree.completed_on.year)
                        if degree_years:
                            education[P.EDUCATION_YEARS] = degree_years
                        if degree.degree_type_name:
                            education[P.EDUCATION_DEGREE] = degree.degree_type_name
                        if degree.degree_subject:
                            education[P.EDUCATION_SUBJECT] = degree.degree_subject
                        self.add_data(P.EDUCATIONS, education)

            if people.jobs:
                for job in people.jobs:
                    if job.data['type'] == 'Job':
                        j = {}
                        if job.title:
                            j["job_title"] = job.title
                        org_type = job.data['relationships']['organization']['type']
                        if org_type and org_type == 'Organization':
                            if job.data['relationships']['organization']['properties']['name']:
                                j["job_name"] = job.data['relationships']['organization']['properties']['name']
                        else:
                            pass
                        if job.data['properties']['started_on']:
                            j["started_on"] = job.data['properties']['started_on']
                        if job.data['properties']['ended_on']:
                            j["ended_on"] = job.data['properties']['ended_on']
                        if len(j) > 0:
                            self.add_data(P.JOBS, j)
                    else:
                        pass
            if len(people.founded_companies.items) > 0:
                for c in people.founded_companies.items:
                    self.add_data("founded_companies", c.name)

            pass
        except Exception as e:
            print("CrunchBaseEngager failed to enrich person (name: %s)" % name)
            if "quota" in str(e).lower():
                pass
            raise EngagementException(e)

        return [P.FULL_NAME]

    def enrich_company(self):

        try:
            name = self.enriched_entity.deduced['name']
            if not self.enrich_key:
                # Search for all the companies with this name
                response = self._make_request(CrunchBaseEngager.PEOPLE_URL, {'name': name})
                data = response.json().get('data')
                if not data or data.get('error'):
                    raise EngagementException("CrunchBaseEngager: error in retrieving company %s." % name)
                if len(data["items"]) > 1:
                    raise EngagementException(
                        "CrunchBaseEngager: company %s not ambiguous. Found %d people with this name." % (name, len(data["items"])))
                if len(data["items"]) == 0:
                    raise EngagementException("CrunchBaseEngager: company %s not found." % name)

                # TODO: Future: Iterate over the returned people and check if there's another matching attribute (like social url) we can use to choose the right person
                permalink = data['items'][0]['properties']['permalink']
            else:
                permalink = self.enrich_key

            response = self.get_node('organizations', permalink)
            if hasattr(response, 'from_cache'):
                self.set_data("from_cache", response.from_cache)
                if not response.from_cache:
                    pass  # code for debugging purposes

            data = response.json().get('data')
            org = Organization(data)

            # Name
            if org.name:
                self.set_data(C.NAME, org.name)

            # Get company logo
            if org.primary_image and len(org.primary_image) > 0:
                logo_url = org.primary_image[0].asset_path
                self.add_data(C.LOGOS, {C.LOGO_URL: logo_url, C.LOGO_SOURCE: 'crunchbase'})

            # Get overview stats (acquisitions, total funds, etc.)
            if org.acquired_by and hasattr(org.acquired_by, 'acquirer'):
                acquiring_company = org.acquired_by.acquirer.name
                self.set_data(C.ACQUIRED_BY, acquiring_company)

            # Get headquarters
            if org.headquarters and len(org.headquarters) > 0:
                headquarters = '%s, %s' % (org.headquarters[0].city, org.headquarters[0].country)
                self.set_data(C.HEADQUARTERS, headquarters)

            # Get description
            if org.short_description:
                description = org.short_description
                self.set_data(C.DESCRIPTION, description)

            # Get founders
            if org.founders and len(org.founders) > 0:
                founders = []
                for founder in org.founders:
                    full_name = '%s %s' % (founder.first_name, founder.last_name)
                    founders.append(full_name)
                self.set_data(C.FOUNDERS, founders)

            # Get categories
            if org.categories and len(org.categories) > 0:
                for category in org.categories:
                    self.add_data(C.CATEGORIES, category.name)

            # Grab aliases
            if org.also_known_as:
                self.set_data(C.ALIASES, org.also_known_as)

            # Grab websites --> homepage_url ?
            if org.homepage_url and len(org.homepage_url) > 0:
                self.set_data(C.WEBSITE, org.homepage_url)

            # Is it a VC company
            if org.role_investor:
                self.set_data(C.INVESTMENT_COMPANY_TYPE, C.ORGANIZATION_TYPE_VENTURE_CAPITAL)

            # Is it an educational organization
            if org.role_school:
                self.set_data(C.ORGANIZATION_TYPE, C.ORGANIZATION_TYPE_SCHOOL)

            # Get socials
            if org.websites and len(org.websites) > 0:
                for url in org.websites:
                    url_type = url.website_type.lower()
                    if url_type == 'twitter':
                        self.set_data(C.TWITTER_URL, url.url)
                    elif url_type == 'facebook':
                        self.set_data(C.FACEBOOK_URL, url.url)
                    elif url_type == 'linkedin':
                        self.set_data(C.LINKEDIN_URL, url.url)
                    elif url_type == 'angellist':
                        self.set_data(C.ANGELLIST_URL, url.url)
                    else:
                        pass

            # Get investments
            if org.investments and len(org.investments) > 0:
                all_investments = set()
                for investment in org.investments:
                    investment_name = investment.invested_in.name
                    all_investments.add(investment_name)
                self.set_data(C.PORTFOLIO_COMPANIES, list(all_investments))

            # Get founding year
            if org.founded_on:
                founding_year = org.founded_on.year
                self.set_data(C.FOUNDING_YEAR, founding_year)

            # Get contact email - for emails-domain info

            # Get number of employees
            if org.num_employees_min and org.num_employees_max:
                employees_range_str = '%s|%s' % (org.num_employees_min, org.num_employees_max)
                self.set_data(C.EMPLOYEES_RANGE, employees_range_str)

            # Go over all investors
            if org.investors and len(org.investors) > 0:
                investors = []
                for investor in org.investors:
                    investor_dict = investor.data
                    investor_type = investor_dict['type'].lower()
                    if investor_type == 'person':
                        investor_name = '%s %s' % (investor_dict['properties']['first_name'], investor_dict['properties']['last_name'])
                    elif investor_type == 'organization':
                        investor_name = investor_dict['properties']['name']
                    else:
                        pass
                    str = 'partner/round'
                    investors.append((investor_name, investor_type, str))
                self.set_data(C.INVESTORS, investors)

            # Go over all board members
            if org.board_members_and_advisors and len(org.board_members_and_advisors) > 0:
                board_members = []
                for board_member in org.board_members_and_advisors:
                    # Do we need this: board_member.person.role_investor:
                    full_name = '%s %s' % (board_member.person.first_name, board_member.person.last_name)
                    board_members.append(full_name)
                self.set_data(C.ADVISORS, board_members)

            if org.founders and len(org.founders) > 0:
                founders = []
                for founder in org.founders:
                    # Do we need thjs: founder.role_investor:
                    full_name = founder.first_name + " " + founder.last_name
                    founders.append(full_name)
                self.set_data(C.FOUNDERS, founders)

            team_members = []
            if org.past_team and len(org.past_team) > 0:
                for team_member in org.past_team:
                    full_name = team_member.person.first_name + " " + team_member.person.last_name
                    team_members.append(full_name)

            if org.current_team and len(org.current_team) > 0:
                for team_member in org.current_team:
                    full_name = team_member.person.first_name + " " + team_member.person.last_name
                    team_members.append(full_name)

            if len(team_members) > 0:
                self.set_data(C.TEAM, team_members)
            pass

            # Only if data was not found, get the companies by names
            # data = self.cb.organizations(company_name)
            # if 'items' in data:
            #     permalink = data.items[0].permalink
            #     self.set_data("permalink", permalink)
            #     response = self.get_node('organizations', permalink)
            #     node_data = response.json().get('data')
            #     # Add the company name and other information (and then I can go to sleep!
            #     pass

        except Exception as e:
            print("CrunchBasengager::enrich_company - failed to enrich company %s (%s)" % (name,e))
            raise EngagementException(e)

        return [C.NAME]

    # def company_exists(self, name):
    #     found = False
    #     try:
    #         # Search for all the companies with this name
    #         response = self._make_request(CrunchBaseEngager.ORGANIZATIONS_URL, {'name': name})
    #         data = response.json().get('data')
    #         found = True
    #     except Exception as e:
    #         print("CrunchBasengager::enrich_company - failed to enrich company (%s)" % name)
    #     return found

    def deduce_permalink(self):
        # Try from CB_URL:
        cb_url = self.enriched_entity.deduced.get('crunchbase_url', None)
        if cb_url:
            # TODO: write code better with regexp (and handle errors)
            i = cb_url.index('?')
            prefix = cb_url[:i]
            j = prefix.rindex('/')
            z = prefix[j+1:]
            return z

        company_name = self.enriched_entity.deduced.get('name', None)
        if company_name is None:
            return None
        return CrunchBaseEngager.formalize_permalink(company_name)

    @staticmethod
    def formalize_permalink(perma_str):
        # TODO: write this code better... one pass instead of many...
        perma_str = perma_str.replace("(", "")
        perma_str = perma_str.replace(")", "")
        perma_str = perma_str.replace(". ", "-")
        perma_str = perma_str.replace(".", "-")
        perma_str = perma_str.replace(" ", "-")
        return perma_str.lower()

    def get_node(self, node_type, uuid, params=None):

        node_url = self.BASE_URL + node_type + '/' + uuid
        return self._make_request(node_url, params=params)
        # data = self._make_request(node_url, params=params)
        # if not data or data.get('error'):
        #     return None
        # return data

    def _build_url(self, base_url, params=None):
        """Helper to build urls by appending all queries and the API key.
        The API key is always the last query parameter."""

        join_char = '&' if '?' in base_url else '?'

        if params is None:
            params_string = {}
        else:
            params_string = '&'.join('%s=%s' % (k, v) for k, v in params.items())
        #    '%s=%s' % (k, v) for k, v in six.iteritems(params or {}))

        if params_string:
            params_string += "&user_key=%s" % CrunchBaseEngager.THE_KEY
        else:
            params_string = "user_key=%s" % CrunchBaseEngager.THE_KEY

        return base_url + join_char + params_string

    def _make_request(self, url, params=None):
        """Makes the actual API call to CrunchBase"""
        final_url = self._build_url(url, params)
        response = requests.get(final_url)
        response.raise_for_status()
        #return response.json().get('data')
        return response

    def _write_response_to_file(self, permalink, response):
        path = "C:\\temp\\AcureRate\\crunchbase_cache\\"
        path = path + "CB_organizations___permalink=" + permalink
        f = codecs.open(path, 'w', 'utf-8')
        f.write(response.text)
        pass