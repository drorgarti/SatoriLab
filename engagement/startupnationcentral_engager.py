import requests
from bs4 import BeautifulSoup

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from utils.acurerate_utils import AcureRateUtils
from entities.acurerate_attributes import P, C


class StartupNationCentralEngager(Engager):

    THE_KEY = "<no-key>"
    BASE_URL = 'https://finder.startupnationcentral.org'

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "StartupNationCentral Engager"

    def get_provider_name(self):
        return "StartupNationCentral"

    def get_short_symbol(self):
        return "suc"

    def get_api_key(self):
        return StartupNationCentralEngager.THE_KEY

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson' and P.FULL_NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[P.FULL_NAME]
        elif t == 'AcureRateCompany' and C.NAME in self.enriched_entity.deduced:
            name = self.enriched_entity.deduced[C.NAME]
        else:
            raise EngagementException("StartupNationCentralEngager - cannot engage - cannot generate enrich key. Entity type: %s", t)
        self.enrich_key = name

    def enrich_company(self):
        try:
            # Construct URL to look for company
            company_name = self.enriched_entity.deduced[C.NAME]
            org_type = self.enriched_entity.deduced.get(C.ORGANIZATION_TYPE, None)
            end_point = 'i' if org_type == C.ORGANIZATION_TYPE_VENTURE_CAPITAL else 'c'
            url = '%s/%s' % (self.BASE_URL, end_point)

            # Search Google for the exact URL
            result_urls = AcureRateUtils.google_search(site=url, query='"%s"' % company_name)
            # TODO: it is possible that more than 1 result is returned, and the first is ok. Need to compare name.
            if len(result_urls) != 1:
                s = 'Unable to locate results page for company %s' % company_name
                raise EngagementException(s)

            # Get the company's page for parsing
            response = requests.get(result_urls[0])
            if response.status_code != 200:
                s = 'Unable to load page in StartupNationCentral.org on %s. Error: %s. (url=%s)' % (self.enrich_key, response.status_code, result_urls[0])
                raise EngagementException(s)

            self.set_data(C.STARTUPNATIONCENTRAL_URL, url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Get name
            try:
                name = soup.find("h1", {"class": "company__title"}).text
                self.set_data(C.NAME, name)
            except:
                self.logger.warning('Unable to locate name attribute for %s', self.enrich_key)

            # Get information if company was ACQUIRED or CLOSED
            # TODO...

            # Get short description
            try:
                short_description = soup.find("div", {"class": "company__short-description"}).text
                self.set_data(C.SHORT_DESCRIPTION, short_description.replace('\n', '').strip())
            except:
                self.logger.warning('Unable to locate short description attribute for %s', self.enrich_key)

            # Get description
            try:
                description = soup.find("div", {"class": "company__short-description"}).text
                self.set_data(C.DESCRIPTION, description.replace('\n', '').strip())
            except:
                self.logger.warning('Unable to locate description attribute for %s', self.enrich_key)

            # Get company logo
            try:
                logo_elem = soup.find("img", {"class": "company__logo"})
                logo_url = self.BASE_URL + logo_elem['src']
                self.set_data(C.LOGO_URL, logo_url)
            except:
                self.logger.warning('Unable to locate company logo attribute for %s', self.enrich_key)

            # Get homepage
            try:
                homepage = soup.find("strong", string='Homepage').parent.find('a').text
                self.set_data(C.DOMAIN, homepage)
            except:
                self.logger.warning('Unable to locate homepage attribute for %s', self.enrich_key)

            # Get Sector
            try:
                sector = soup.find("strong", string='Sector').parent.find('a').text
                self.set_data(C.SECTOR, sector)
            except:
                self.logger.warning('Unable to locate sector attribute for %s', self.enrich_key)

            # Get founding year
            try:
                founding_year = soup.find("strong", string='Founded').parent.find('div').text
                self.set_data(C.FOUNDING_YEAR, founding_year)
            except:
                self.logger.warning('Unable to locate founding year attribute for %s', self.enrich_key)

            # Get Business Model
            try:
                business_model = soup.find("strong", string='Business Model').parent.find('a').text
                self.set_data(C.BUSINESS_MODEL, business_model)
            except:
                self.logger.warning('Unable to locate business model attribute for %s', self.enrich_key)

            # Get Funding stage
            try:
                funding_stage = soup.find("strong", string='Funding Stage').parent.find('div').text
                self.set_data(C.FUNDING_STAGE, funding_stage)
            except:
                self.logger.warning('Unable to locate funding stage attribute for %s', self.enrich_key)

            # Get employees range
            try:
                employee_range = soup.find("strong", string='Employees').parent.find('div').text
                self.set_data(C.EMPLOYEES_RANGE, employee_range)
            except:
                self.logger.warning('Unable to locate employee range attribute for %s', self.enrich_key)

            # Get Product Stage
            try:
                product_stage = soup.find("strong", string='Product Stage').parent.find('div').text
                self.set_data(C.PRODUCT_STAGE, product_stage)
            except:
                self.logger.warning('Unable to locate product stage attribute for %s', self.enrich_key)

            # Get categories
            try:
                elems = soup.findAll("a", {"class": "tags__tag"})
                for elem in elems:
                    self.add_data(C.CATEGORIES, elem.text)
            except:
                self.logger.warning('Unable to locate categories attribute for %s', self.enrich_key)

            # Get Address
            try:
                pass
            except:
                self.logger.warning('Unable to locate address attribute for %s', self.enrich_key)

            # Get the team
            try:
                elems = soup.findAll("div", {"class": "company-team__info"})
                for elem in elems:
                    name_elem = elem.find("div", {"class": "company-team__name"})
                    self.add_data(C.TEAM, name_elem.text)

                    # TODO: enrich the person with this position
                    position_elem = elem.find("div", {"class": "company-team__position"})
                    the_position = position_elem.text.lower()
                    if any(x in the_position for x in ['cofounder', 'co-founder', 'co founder', 'founder', 'owner']):
                        self.add_data(C.FOUNDERS, name_elem.text)
            except:
                self.logger.warning('Unable to locate team members attribute for %s', self.enrich_key)

            # If this is an investment company, get their portfolio companies
            if org_type == C.ORGANIZATION_TYPE_VENTURE_CAPITAL:
                # TODO: Garb these fields:
                # TODO: 'In Israel Since', 'Investment Stages', 'Min Amount', 'Max Amount', 'Capital Managed', 'Industry Preferences'
                try:
                    portfolio_cards = soup.findAll("div", {"class": "investor-portfolio__company"})
                    for elem in portfolio_cards:
                        company_name_elem = elem.find("h2", {"class": "company-card__title"})
                        self.add_data(C.PORTFOLIO_COMPANIES, company_name_elem.text)
                        # TODO: grab more info from portfolio cards: logo, website url, short description - enrich the company data
                    pass
                except:
                    self.logger.warning('Unable to locate portfolio companies for %s', self.enrich_key)

            pass

        except Exception as e:
            self.logger.error('Unable to enrich person %s. %s', self.enriched_entity, e)
            raise e

        return []