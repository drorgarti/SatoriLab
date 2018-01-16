import tldextract
from bson import json_util
import json
import copy
import re

from entities.acurerate_entity import AcureRateEntity
from utils.acurerate_utils import AcureRateUtils
from entities.acurerate_attributes import P, C, R, G

from store.db_wrapper import DBWrapper


class AcureRateCompany(AcureRateEntity):

    def __init__(self,  company_name=None, company_name_aliases=None):
        super().__init__()

    def __str__(self):
        name = self.deduced.get(C.NAME, '<no name attr>')
        return name

    def __repr__(self):
        return "AcureRateCompany"

    @staticmethod
    def from_json_string(entity_json_string):
        data = json.loads(entity_json_string, object_hook=json_util.object_hook)
        c = AcureRateCompany()
        c.from_dictionary(data)
        return c

    @staticmethod
    def reconstruct(data):
        c = AcureRateCompany()
        c.from_dictionary(data)
        return c

    def _digest_logos(self):
        me = self.deduced

        # TODO: now we go simple (ignore logo_source), but we may want to prefer a specific logo source on another...
        for ds in self.sources():
            if C.LOGOS in ds:
                me[C.LOGO_SELECTED] = ds[C.LOGOS][0][C.LOGO_URL]
                return
            elif C.IMAGE_URL in ds:  # this is legacy key from CrunchBase2014 import
                me[C.LOGO_SELECTED] = ds[C.IMAGE_URL]
                return

    def _digest_investors(self):
        #me = self.deduced

        # Go over all sources
        for ds in self.sources():
            # Add the investors
            for i in ds.get(C.INVESTORS, []):
                self._append_to_deduced(C.INVESTORS, i)

            # if C.INVESTORS in ds:
            #     self._append_to_deduced(C.INVESTORS, ds[C.INVESTORS])
        pass

    def _digest_related_vcs(self):
        me = self.deduced

        if C.INVESTORS in me:
            for n, t, p in me[C.INVESTORS]:
                if t == 'organization':
                    if 'Seed' in p:
                        self._append_to_deduced(C.RELATED_VCS, (n, R.INVESTOR_AT, me[C.NAME]))
                    else:
                        # Get venture company, see if it invests in seed
                        query = {"deduced.name": n}
                        r = DBWrapper.get_companies(query, True)
                        if r:
                            if C.INVESTMENT_TYPES in r['deduced'] and 'seed' in r['deduced'][C.INVESTMENT_TYPES]:
                                self._append_to_deduced(C.RELATED_VCS, (n, R.INVESTOR_AT, me[C.NAME]))

    def _digest_portfolio_companies(self):

        # Go over all sources
        for ds in self.sources():
            # Add the portfolio companies
            for company_name in ds.get(C.PORTFOLIO_COMPANIES, []):
                #query = {"deduced.name": company_name}
                query = {"deduced.aliases": company_name.lower()}
                r = DBWrapper.get_companies(query, True)
                if r:
                    self._append_to_deduced(C.PORTFOLIO_COMPANIES, company_name)
                else:
                    self._append_to_deduced(C.UNRECOGNIZED_COMPANIES, company_name)

        pass

    def _digest_related_investors(self):
        me = self.deduced

        # Check if company has investors
        if C.INVESTORS in me:
            for n, t, p in me[C.INVESTORS]:
                if t == 'person':
                    self._append_to_deduced(C.RELATED_INVESTORS, (n, R.INVESTOR_AT, me[C.NAME]))

        # Check if company has advisors which are investors:
        if C.ADVISORS in me:
            for ri in me[C.ADVISORS]:
                query = {"deduced.full_name": ri}
                r = DBWrapper.get_persons(query, True)
                if r:
                    if P.INVESTOR in r['deduced']:
                        self._append_to_deduced(C.RELATED_INVESTORS, (ri, R.ADVISOR_AT, me[C.NAME]))
                else:
                    self._append_to_deduced(C.UNRECOGNIZED_PEOPLE, ri)

        # Check if the company founders are investors:
        if C.FOUNDERS in me:
            for ri in me[C.FOUNDERS]:
                query = {"deduced.full_name": ri}
                r = DBWrapper.get_persons(query, True)
                if r:
                    if P.INVESTOR in r['deduced']:
                        self._append_to_deduced(C.RELATED_INVESTORS, (ri, R.FOUNDER_OF, me[C.NAME]))
                else:
                    self._append_to_deduced(C.UNRECOGNIZED_PEOPLE, ri)

        # Check if one of the team members is an investor
        if C.TEAM in me:
            for ri in me[C.TEAM]:
                query = {"deduced.full_name": ri}
                r = DBWrapper.get_persons(query, True)
                if r:
                    if P.INVESTOR in r['deduced']:
                        self._append_to_deduced(C.RELATED_INVESTORS, (ri, R.WORKED_AT, me[C.NAME]))
                else:
                    self._append_to_deduced(C.UNRECOGNIZED_PEOPLE, ri)


    def _digest_name(self):
        me = self.deduced
        # Iterate over all name from providers. Append name to list if it has at least first and last name
        all_names = []
        for ds in self.sources():
            if 'provider_name' in ds and C.NAME in ds:
                all_names.append((ds['provider_name'].lower(), ds[C.NAME]))

        # Take the first name from list according to priority of providers
        if len(all_names) > 0:
            a = sorted(all_names, key=lambda v: {"crunchbase": 1, "crunchbasebot": 2, "fullcontact": 3}.get(v[0], 999))
            me[C.NAME] = a[0][1]
        else:
            me[C.NAME] = '<unresolved>'

    def _digest_aliases(self):
        me = self.deduced
        # Go over all sources
        for ds in self.sources():
            # Add name in source to aliases (regular and aliasized)
            if C.NAME in ds:
                self._append_to_deduced(C.ALIASES, AcureRateUtils.aliasize(ds[C.NAME]))
                self._append_to_deduced(C.ALIASES, ds[C.NAME].lower())

            # Add aliases which may have come from source
            for alias in ds.get(C.ALIASES, []):
                self._append_to_deduced(C.ALIASES, alias)

            # If permalink exists, add it to alias as well
            if P.CB_PERMALINK in ds:
                self._append_to_deduced(C.ALIASES, ds[P.CB_PERMALINK])
                self._append_to_deduced(C.ALIASES, ds[P.CB_PERMALINK].replace("-", " "))

        # Add suffix of linkedin/facebook/twitter/crunchbase urls
        # TODO: get also aliases from facebook and twitter and angellist...
        if C.LINKEDIN_URL in me:
            alias = AcureRateUtils.get_url_last_path_element(me[C.LINKEDIN_URL])
            if alias:
                self._append_to_deduced(C.ALIASES, alias)
        if C.CRUNCHBASE_URL in me:
            alias = AcureRateUtils.get_url_last_path_element(me[C.CRUNCHBASE_URL])
            if alias:
                self._append_to_deduced(C.ALIASES, alias)
        pass

    def _digest_domain(self):
        me = self.deduced

        # If domain was set or there's no website, return
        if C.DOMAIN in me or C.WEBSITE not in me:
            return

        # Deduce domain from website
        domain = AcureRateUtils.get_domain(me[C.WEBSITE])
        if domain:
            me[C.DOMAIN] = domain

    def _digest_email_domains(self):
        me = self.deduced

        email_domains = []
        # add what is usually the default email domain - the company domain:
        # TODO: Need to fix this!! Take the domain WITHOUT the "www."
        if C.DOMAIN in me:
            # @@@
            ext = tldextract.extract(me[C.DOMAIN])
            email_domains.append('%s.%s' % (ext.domain, ext.suffix))

        # Go over providers and get email domains
        for ds in self.sources():
            if C.EMAIL_DOMAINS in ds:
                if ds[C.EMAIL_DOMAINS]:
                    email_domains += ds[C.EMAIL_DOMAINS]
                else:
                    print("Company %s: Found null email_domain" % me[C.NAME])

        if len(email_domains) > 0:
            me[C.EMAIL_DOMAINS] = list(set(email_domains))

    def _digest_phones(self):
        me = self.deduced

        # Go over providers and get phones
        phones = []
        for ds in self.sources():
            if C.PHONES in ds and ds[C.PHONES]:
                phones.extend(ds[C.PHONES])

        if len(phones) > 0:
            me[C.PHONES] = list(set(phones))
        pass

    def _digest_employees_range(self):
        me = self.deduced

        # Go over providers and select the higher number
        for ds in self.sources():
            if C.EMPLOYEES_NUMBER in ds:
                if C.EMPLOYEES_NUMBER not in me:
                    me[C.EMPLOYEES_NUMBER] = ds[C.EMPLOYEES_NUMBER]
                elif ds[C.EMPLOYEES_NUMBER] > me[C.EMPLOYEES_NUMBER]:
                    me[C.EMPLOYEES_NUMBER] = ds[C.EMPLOYEES_NUMBER]
        if C.EMPLOYEES_NUMBER in me:
            me[C.EMPLOYEES_RANGE] = AcureRateUtils.get_employees_range(me[C.EMPLOYEES_NUMBER])
        pass

    def _digest_exits(self):
        me = self.deduced
        # TODO: improve this - we need to make sure all providers point at the same acquisition
        for ds in self.sources():
            if C.ACQUIRED_BY in ds:
                me[C.ACQUIRED_BY] = ds[C.ACQUIRED_BY]
                break
        pass

    def _digest_organization_type(self):
        me = self.deduced

        #  ACADEMY, GOVERNMENT, MILITARY, COMPANY, VENTURE-CAPITAL/INVESTOR

        # Check if something indicates an investment company
        for ds in self.sources():
            if C.ORGANIZATION_TYPE in ds and ds[C.ORGANIZATION_TYPE] == 'investor':  # CrunchBaseScraper
                me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_VENTURE_CAPITAL
                return
            if C.INVESTMENT_COMPANY_TYPE in ds:
                me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_VENTURE_CAPITAL
                return
            if C.PRIMARY_ROLE in ds and ds[C.PRIMARY_ROLE] == 'investor':  # CrunchBaseBot
                me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_VENTURE_CAPITAL
                return

        # TODO: improve this - we need to make sure all providers point at the same education - determine its Academy
        for ds in self.sources():
            if C.ORGANIZATION_TYPE in ds and ds[C.ORGANIZATION_TYPE] == 'school':
                me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_ACADEMY
                return
            elif C.DOMAIN in ds and AcureRateUtils.is_academic_domain(ds[C.DOMAIN]):
                me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_ACADEMY
                return

        # Default is company
        me[C.ORGANIZATION_TYPE] = C.ORGANIZATION_TYPE_COMPANY
        pass

    def _digest_email_convention(self):
        me = self.deduced

        email_domains = me.get(C.EMAIL_DOMAINS, None)
        if not email_domains:
            return
        # TODO: handle case where there are more than one domain
        email_domain = email_domains[0]
        email_formats = set()

        # Query DB to find people with emails that match the company domain
        from store.store import Store
        regx = re.compile(email_domain, re.IGNORECASE)
        persons = Store.get_persons({'deduced.'+P.EMAILS: regx}, single_result=False, mongo_query=True)
        for person in persons:
            # Check first name match
            fn = person.deduced.get(P.FIRST_NAME, '').lower()
            ln = person.deduced.get(P.LAST_NAME, '').lower()
            fni = fn[0]
            lni = ln[0]
            for email in person.deduced.get(P.EMAILS, []):
                if email.endswith(email_domain):
                    break
            else:
                continue  # Strange..., no email found with the relevant domain

            email = email.lower()
            email_delimiters = ['', '_', '.', '-']
            email_format = None

            # Go over all delimiters and combinations and try replacing
            for delimiter in email_delimiters:
                if email.startswith(fn + delimiter + ln):
                    email_format = '<fn>' + delimiter + '<ln>'  # jane_doe@...
                elif email.startswith(ln + delimiter + fn):
                    email_format = '<ln>' + delimiter + '<fn>'  # doe_jane@...
                elif email.startswith(fni + delimiter + ln):
                    email_format = '<fni>' + delimiter + '<ln>'   # j_dow@...
                elif email.startswith(lni + delimiter + fn):
                    email_format = '<lni>' + delimiter + '<fn>'  # d_jane@...
                elif email.startswith(fn + delimiter + lni):
                    email_format = '<fn>' + delimiter + '<lni>'  # jane_d@...
                elif email.startswith(ln + delimiter + fni):
                    email_format = '<ln>' + delimiter + '<fni>'  # dow_j@...
                if email_format:
                    break

            if not email_format:
                if email.startswith(fn):
                    email_format = '<fn>'  # jane@...
                elif email.startswith(ln):
                    email_format = '<ln>'  # dow@...

            if email_format:
                email_formats.add(email_format)

        me[C.EMAIL_FORMATS] = list(email_formats)

        return email_formats

    def attr(self, attr_key):
        return self.deduced.get(attr_key, None)

    def get_labels(self):
        """
        Returns all the labels that characterize this company. The default is being a Company.
        It can be a Academy, Investment Company, etc.

        :return: list of labels, capitalized
        """
        labeled = [C.ACQUIRED]
        labels = [G.LABEL_COMPANY]

        # Iterate over all properties
        for k in self.deduced.keys():
            if k in labeled:
                labels.append(k.capitalize())

        if C.ORGANIZATION_TYPE in self.deduced and self.deduced[C.ORGANIZATION_TYPE] == C.ORGANIZATION_TYPE_ACADEMY:
            labels.append(G.LABEL_EDUCATION)

        return labels

    def get_properties(self):
        """
        Returns all the properties that can be externalized (not the temps, relations, labels, etc.)
        All those who should not be returned should be added to the exclude list and label list
        :return: Dict of keys, values
        """
        excluded = ['related_investors', 'related_vcs', 'unrecognized_people', 'investors']
        labeled = [C.ACQUIRED]

        # Iterate over all properties
        properties = {}
        for k, v in self.deduced.items():
            if k not in labeled and k not in excluded:
                properties[k] = v

        return properties

    def get_relations(self, filter=None):
        """
        Looks at raw data of person entity and returns all relations.

        :return: List of tupples, each tupple: (target_aid, relationship type, relationship properties)
        """
        from store.store import Store

        relations = set()

        # C:C - Create ACQUIRED_BY relation
        if C.ACQUIRED_BY in self.deduced:
            #acquiring_company = Store.get_company({C.NAME: self.deduced[C.ACQUIRED_BY]})
            acquiring_company = Store.get_company({C.ALIASES: self.deduced[C.ACQUIRED_BY].lower()})
            if acquiring_company:
                relations.add((self.aid, G.RELATION_LABEL_ACQUIRED_BY, acquiring_company.aid, ''))

        # C:C - Create the INVESTED_IN relation
        if C.ORGANIZATION_TYPE in self.deduced and self.deduced[C.ORGANIZATION_TYPE] == C.ORGANIZATION_TYPE_VENTURE_CAPITAL:
            for portfolio_company in self.deduced.get(C.PORTFOLIO_COMPANIES, []):
                ccc_company = Store.get_company({C.ALIASES: portfolio_company.lower()})
                if ccc_company:
                    relations.add((self.aid, G.RELATION_LABEL_INVESTS_IN, ccc_company.aid, ''))

        # P:C - Create EMPLOYEE_OF relation (Team. past_team)
        for team_mate in self.deduced.get(C.TEAM, []):
            person = Store.get_person({P.FULL_NAME: team_mate})
            if person:
                relations.add((person.aid, G.RELATION_LABEL_EMPLOYEE_OF, self.aid, ''))

        # P:C - Create BOARD_AT relation (Advisors)
        for advisor in self.deduced.get(C.ADVISORS, []):
            person = Store.get_person({P.FULL_NAME: advisor})
            if person:
                relations.add((person.aid, G.RELATION_LABEL_ADVISOR_AT, self.aid, ''))

        # P:C - Create FOUNDER_OF relation (Company)
        for founder in self.deduced.get(C.FOUNDERS, []):
            person = Store.get_person({P.FULL_NAME: founder})
            if person:
                relations.add((person.aid, G.RELATION_LABEL_FOUNDER_OF, self.aid, ''))

        # P:C - Create INVESTS_AT relation (Investors)
        for investor_name, investor_type, investment_info in self.deduced.get(C.INVESTORS, []):

            # Find info on investment type -> relation_properties
            relation_properties = []
            investment_round = AcureRateUtils.get_investment_round(investment_info)
            if investment_round:
                relation_properties.append("investment_type: '%s'" % investment_round)
            investment_lead = AcureRateUtils.is_investment_lead(investment_info)
            if investment_lead:  # TODO: should be label and not property
                relation_properties.append("investment_lead: True")

            if investor_type == 'person':
                person = Store.get_person({'deduced.' + P.FULL_NAME: investor_name})
                if person:
                    relations.add((person.aid, G.RELATION_LABEL_INVESTS_IN, self.aid, ', '.join(relation_properties)))
            elif investor_type == 'organization':
                investing_company = Store.get_company({C.NAME: investor_name})
                if investing_company:
                    relations.add((investing_company.aid, G.RELATION_LABEL_INVESTS_IN, self.aid, ', '.join(relation_properties)))

        # If filter provided, leave only relations that are relevant
        if filter:
            relations = [tup for tup in relations if tup[1].lower() == filter.lower()]

        return relations

    def digest(self):

        # Keep data before we reconstuct it - to check at the end if there were changes
        if self.deduced:
            before_reconstruct = copy.deepcopy(self.deduced)
        else:
            before_reconstruct = None

        # Reset 'deduced' - we're starting *clean* when digesting
        me = self.deduced = {}

        self._digest_name()

        # Go over data of all providers
        for ds in self.sources():

            # Collect related investors from providers
            if C.RELATED_INVESTORS in ds:
                for investor in ds[C.RELATED_INVESTORS]:
                    self._append_to_deduced(C.RELATED_INVESTORS, investor)

            # if 'name' not in me and 'name' in provider:
            #     me['name'] = provider['name']

            # TODO: revisit this code. We currently have only one provider, so we just copy the attributes values
            attrs = [
                "company_type",
                "crunchbase_url",
                "domain",
                "homepage_url",
                "stock_symbol",
                "short_description",
                "image_url",
                "facebook_url",
                "twitter_url",
                "linkedin_url",
                C.ADVISORS,
                C.FOUNDERS,
                C.CATEGORIES,
                C.TEAM,
                C.FOUNDING_YEAR,
                C.WEBSITE,
                C.CRUNCHBASE_PERMALINK,
                C.BLOOMBERG_URL
            ]
            for a in attrs:
                if a in ds:
                    me[a] = ds[a]

        # Select the company logo
        self._digest_logos()

        self._digest_domain()

        self._digest_email_domains()

        self._digest_phones()

        self._digest_investors()

        # Go over related people - check if they are investors:
        self._digest_related_investors()

        self._digest_portfolio_companies()

        self._digest_related_vcs()

        self._digest_aliases()

        self._digest_employees_range()

        self._digest_exits()

        self._digest_organization_type()

        self._digest_email_convention()

        # Check if anything changed during digest:
        if before_reconstruct is None:
            return True
        added, removed, modified, same = AcureRateUtils.dict_compare(self.deduced, before_reconstruct)
        if len(added) == 0 and len(removed) == 0 and len(modified) == 0:
            return False
        return True
