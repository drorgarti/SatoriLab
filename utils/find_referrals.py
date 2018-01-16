import json
import re
from time import gmtime, strftime

from store.db_wrapper import DBWrapper

from entities.acurerate_attributes import P, C, R, T
from entities.acurerate_person import AcureRatePerson
from entities.acurerate_company import AcureRateCompany

from enrichment.enrichment_service import EnrichmentData, EnrichmentService
from utils.acurerate_utils import AcureRateUtils


class FindReferrals:

    DEFAULT_PHOTO_URL2 = "https://dl.dropboxusercontent.com/u/19954023/marvel_force_chart_img/top_thor.png"
    DEFAULT_PHOTO_URL3 = "http://www.iconsdb.com/icons/preview/caribbean-blue/contacts-xxl.png"
    DEFAULT_PHOTO_URL = "http://lh4.googleusercontent.com/-Y6g6TY2MFXU/AAAAAAAAAAI/AAAAAAAAAAA/aeONCB2XiEM/photo.jpg"

    GROUP_FOUNDER = "founder"
    GROUP_REFERRALS = "referral"
    GROUP_CONTACT_INVESTORS = "contact_investor"
    GROUP_INVESTORS = "investor"
    GROUP_COMPANY = "company"

    LINK_STRENGTH_WEAK = 2
    LINK_STRENGTH_MEDIUM = 3
    LINK_STRENGTH_STRONG = 4

    LINK_TYPE_MOVER_AND_SHAKER = 2
    LINK_TYPE_TECHNICAL = 3
    LINK_TYPE_DEFAULT = 1

    def __init__(self, founder_email):
        DBWrapper.connect()
        self.es = EnrichmentService.singleton()
        self.graph = {"nodes": [], "links": []}
        self.unique_id = 0
        self.names = {}
        self.stats = {"num_referrals": 0,
                      "num_unique_investors": 0,
                      "num_investors": 0,
                      "num_ceos": 0,
                      "num_founders": 0,
                      "num_contacts_investors": 0}

        q = {"deduced.email": founder_email}
        data = DBWrapper.get_persons(q, True)
        if data is None:
            raise Exception("Unable to locate founder (%s)" % founder_email)
        self.founder = AcureRatePerson.reconstruct(data)
        self.founder_aid = self.founder._aid
        self.founder_name = self.founder.deduced[P.FULL_NAME]
        pass

    def _clean_graph(self):
        str = strftime("%d-%m-%Y %H:%M:%S")
        self.graph = {"timestamp": str, "nodes": [], "links": []}

    @staticmethod
    def get_person_from_db(query):
        r = DBWrapper.get_persons(query, True)
        if r is None:
            return None
        return AcureRatePerson().reconstruct(r)

    def _add_entity_to_d3_json_graph(self, name, entity, group):
        node_id = self._unique_node_name2(name)
        if entity:
            node = {"id": node_id, "data": entity.deduced, "group": group}
        else:  # TODO: this is temp code - remove eventually. Entity should not be None - it's because we cuurently don't have investors in our DB
            node = {"id": node_id, "data": {"full_name": name}, "group": group}
        # Ensure uniqueness of nodes
        if node not in self.graph['nodes']:
            self.graph['nodes'].append(node)
        return node_id

    def _add_person_to_d3_json_graph(self, person, group, alt_name=None):
        if person:
            node_id = self._unique_node_name2(person.deduced[P.FULL_NAME])
            node = {"id": node_id, "data": person.deduced, "group": group}
        else:
            node_id = self._unique_node_name2(alt_name)
            node = {"id": node_id, "data": {"full_name": alt_name}, "group": group}
        # Ensure uniqueness of nodes
        if node not in self.graph['nodes']:
            self.graph['nodes'].append(node)
        return node_id

    def _add_node_to_d3_json_graph(self, id, name, img, bio, group):
        node = {"id": id, "group": group, "name": name, "img": img, "bio": bio}
        # Ensure uniqueness of nodes
        if node not in self.graph['nodes']:
            self.graph['nodes'].append(node)

    def _add_link_to_d3_json_graph(self, source, target, value, relation=None, link_type=None):
        link = {"source": source, "target": target, "value": value}
        if relation:
            link['relation'] = relation
        if link_type:
            link['link_type'] = link_type
        # Ensure uniqueness of links
        if link not in self.graph['links']:
            self.graph['links'].append(link)

    def _add_catgories_to_d3_json_graph(self, categories):
        self.graph['categories'] = categories

    def _unique_node_name2(self, name):
        name = name.replace(" ", "_").lower()
        name = name.replace(",", "_").lower()
        name = name.replace(".", "_").lower()
        name = name.replace("'", "_").lower()
        n = self.names.get(name, None)
        if n:
            formatted = "%s_%d" % (name, n)
            self.names[name] += 1
        else:
            formatted = "%s_%d" % (name, 1)
            self.names[name] = 2
        return formatted

    def _unique_node_name(self, name):
        self.unique_id += 1
        formatted = "%s_%d" % (name.replace(" ", "_"), self.unique_id)
        return formatted.lower()

    def _write_d3_json_to_file(self, file_name):
        output_file_path = r'C:\temp\AcureRate\UI\%s_%s' % (self.founder_name.lower().replace(' ', '_'), file_name)
        with open(output_file_path, 'w') as outfile:
            json.dump(self.graph, outfile)
        print("Done exporting graph to file")

    def _calculate_link_strength(self, referral_person, investor_person, investor_relation, investor_company_name):

        # Relation can consist of these:
        # R.WORKED_AT, R.ADVISOR_AT, R.CONTACT_OF, R.INVESTOR_AT, R.FOUNDER_OF

        # (a) If INV is in the board, check if REF is also in board or Founder/CEO
        if investor_relation == R.ADVISOR_AT:
            if referral_person.board_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_founder_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_ceo_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_cfo_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_MEDIUM
            else:
                return FindReferrals.LINK_STRENGTH_WEAK

        # (b) If REF is CEO/CFO/Founder at the company where INV invested
        if investor_relation == R.INVESTOR_AT:
            if referral_person.board_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_founder_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_ceo_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_cfo_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_MEDIUM
            else:
                return FindReferrals.LINK_STRENGTH_WEAK

        # (c) If INV is in REF contacts
        if investor_relation == R.CONTACT_OF:
            return FindReferrals.LINK_STRENGTH_MEDIUM

        if investor_relation == R.FOUNDER_OF:
            if referral_person.board_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_founder_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            elif referral_person.is_ceo_at(investor_company_name):
                return FindReferrals.LINK_STRENGTH_STRONG
            else:
                return FindReferrals.LINK_STRENGTH_MEDIUM

        if investor_relation == R.WORKED_AT:
            return FindReferrals.LINK_STRENGTH_STRONG

        # Find the title of the referral when he worked at the company the investor is related to
        investor_related_company = DBWrapper.get_companies({C.NAME: investor_company_name}, True)
        if investor_related_company is None:
            return FindReferrals.LINK_STRENGTH_WEAK
        job_title = referral_person.title_at(investor_company_name)
        # TODO: continue implementation - is_senior - according to company size.
        #if job_title is not None and investor_related_company.is_familiar_with_investor(job_title):
        #    return FindReferrals.LINK_STRONG

        return FindReferrals.LINK_STRENGTH_WEAK

    def generate_vcs_map(self):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_entity_to_d3_json_graph(self.founder.deduced[P.FULL_NAME], self.founder,
                                                            FindReferrals.GROUP_FOUNDER)

        targetted_vcs = ["Carmel Ventures", "Intel Capital", "Evergreen Venture Partners", "Gemini Israel Ventures",
                         "Pitango Venture Capital", "Apax Partners", "Qumra Capital", "JVP", "Silver Lake Partners",
                         "Scopus Ventures", "Janvest", "Greenfield Cities Holdings", "GFC"]

        targetted_companies_map = {}
        for vc_name in targetted_vcs:
            # Get company details from db:
            #company_r = DBWrapper.get_companies({"deduced.name": vc_name.lower()}, True)
            company_r = DBWrapper.get_companies({"deduced.aliases": vc_name.lower()}, True)
            if company_r is None:
                continue
            company = AcureRateCompany.reconstruct(company_r)
            targetted_companies_map[vc_name] = company

            # Get all people who are (a) in founder's contacts; (b) worked in this venture
            regx = re.compile(vc_name, re.IGNORECASE)
            query = {"$and": [{"$or": [{"deduced.jobs.job_name": regx},
                                       {"deduced.advisory_jobs.job_name": regx}]},
                              {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                       {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
            cursor = DBWrapper.get_persons(query)
            for r in cursor:
                person = AcureRatePerson.reconstruct(r)
                person.deduced['company_referred'] = company.deduced
                deduced_link_type = FindReferrals.LINK_TYPE_DEFAULT

                person.deduced['title_at_company_referred'] = "@ " + vc_name

                # Create in graph the referral node and link to it
                person_node_id = self._add_entity_to_d3_json_graph(person.deduced[P.FULL_NAME], person,
                                                                   FindReferrals.GROUP_REFERRALS)
                self._add_link_to_d3_json_graph(founder_node_id, person_node_id,
                                                value=FindReferrals.LINK_STRENGTH_MEDIUM, link_type=deduced_link_type,
                                                relation="@ %s" % vc_name)
        # Find CEOs
        query = {"$and": [{"deduced.ceo": True},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
        cursor = DBWrapper.get_persons(query)

        # For each CEO, check if his company has a VC investor
        print("--> Number of CEOs found related to founder: %d" % cursor.count())
        for r in cursor:
            ceo_person = AcureRatePerson.reconstruct(r)
            # Add link from FOUNDER to CONTACT-CEO
            mutual_work = self.founder.worked_together(ceo_person)
            if mutual_work:
                relation_phrase = FindReferrals._generate_founder_2_referral_phrase(self.founder, ceo_person, mutual_work)
            else:
                relation_phrase = "Appears in %s's contacts" % self.founder.deduced[P.FIRST_NAME]
            ceo_person.deduced['relation_phrase'] = relation_phrase

            ceo_node_id = self._add_person_to_d3_json_graph(ceo_person, FindReferrals.GROUP_CONTACT_INVESTORS)
            # self._add_link_to_d3_json_graph(founder_node_id, ceo_node_id, 4, relation_phrase)

            # Go over the list of accredited jobs and collect those who the person is CEO of...
            ll = []
            for job in ceo_person.deduced.get(P.ACCREDITED_JOBS_2, []):
                for job_role in job.get(P.JOB_ROLES, []):
                    if job_role[P.JOB_ROLE] == T.ROLE_CEO or job_role[P.JOB_ROLE] == T.ROLE_BOARD_MEMBER:
                        job_name = job[P.JOB_NAME]
                        ll.append(job_name)

            # Go over the companies which the person CEO-ed.. & check if the company has VC investors
            should_add_ceo_to_map = False
            for company_name in ll:
                company_r = DBWrapper.get_companies({"deduced.aliases": company_name.lower()}, True)
                if company_r:
                    company = AcureRateCompany.reconstruct(company_r)
                    # Go over investors - find the organizations
                    processed_investors = []
                    for investor_name, investor_type, investor_round in company.deduced.get(C.INVESTORS, []):
                        if investor_name in processed_investors:
                            continue
                        if 'organization' in investor_type:
                            processed_investors.append(investor_name)
                            i = investor_round.find('/')
                            if i > 0:
                                lead_name = investor_round[0:i-1].strip()
                                lead_person = self._get_person(lead_name)
                            if not lead_person:
                                lead_person = AcureRatePerson()
                                lead_person.deduced = {P.FULL_NAME: investor_name}
                                link_description = "%s funds %s" % (investor_name, company_name)
                            else:
                                link_description = "%s (%s) funds %s" % (investor_name, lead_name, company_name)

                            # Add node to graph
                            vc_node_id = self._add_person_to_d3_json_graph(lead_person, FindReferrals.GROUP_INVESTORS, alt_name=investor_name)
                            # Add link from CEO-REFERRAL to VC
                            self._add_link_to_d3_json_graph(ceo_node_id, vc_node_id, FindReferrals.LINK_STRENGTH_STRONG, link_description)
                            pass
                    if len(processed_investors) > 0:
                        should_add_ceo_to_map = True

            # Add CEO to graph
            if should_add_ceo_to_map:
                # ceo_node_id = self._add_person_to_d3_json_graph(ceo_person, FindReferrals.GROUP_CONTACT_INVESTORS)
                self._add_link_to_d3_json_graph(founder_node_id, ceo_node_id, 4, relation_phrase)
            else:
                str = "Not adding %s !" % ceo_person.deduced[P.FULL_NAME]
                pass

        self._write_d3_json_to_file('vcs_map.json')

    def generate_companies_map(self):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_entity_to_d3_json_graph(self.founder.deduced[P.FULL_NAME], self.founder, FindReferrals.GROUP_FOUNDER)

        # Get all the contacts that have these places of work in their jobs
        targetted_companies_1 = ["SAP", "VMware", "Hewlett-Packard", "Facebook", "Google", "NICE Systems", "LinkedIn",
                               "Microsoft", "Waze", "Salesforce", "Kenshoo", "Cisco", "EMC-ZZZ", "Intel", "Twitter", "Apple",
                               "NASA", "General Electric", "United Nations"]
        targetted_companies_2 = ["SAP", "Facebook", "Google", "NICE Systems", "LinkedIn", "Microsoft", "Salesforce",
                                 "Twitter", "Apple", "NASA", "General Electric", "United Nations"]
        targetted_companies_3 = ["Carmel Ventures", "Intel Capital", "Evergreen Venture Partners", "Gemini Israel Ventures",
                                 "Pitango Venture Capital", "Apax Partners", "Qumra Capital", "JVP"]

        targetted_companies = targetted_companies_3
        #targetted_companies = ["Google"]
        targetted_companies_map = {}
        for company_name in targetted_companies:
            # Get company details from db:
            # r = DBWrapper.get_companies({"deduced.name": company_name}, True)
            company_r = DBWrapper.get_companies({"deduced.aliases": company_name.lower()}, True)
            if company_r is None:
                continue
            company = AcureRateCompany.reconstruct(company_r)
            targetted_companies_map[company_name] = company
            # if company_name == "Microsoft" or \
            #                 company_name == "Twitter" or \
            #                 company_name == "LinkedIn" or \
            #                 company_name == "Google" or \
            #                 company_name == "SAP" or \
            #                 company_name == "Apple" or \
            #                 company_name == "Salesforce" or \
            #                 company_name == "NASA" or \
            #                 company_name == "General Electric" or \
            #                 company_name == "United Nations" or \
            #                 company_name == "Facebook":
            #     targetted_companies_map[company_name] = company
            # else:
            #     pass

            # Get all people who are (a) in founder's contacts; (b) worked in this company
            regx = re.compile(company_name, re.IGNORECASE)
            query = {"$and": [{"deduced.jobs.job_name": regx},
                              {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                       {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
            cursor = DBWrapper.get_persons(query)
            for r in cursor:
                person = AcureRatePerson.reconstruct(r)
                person.deduced['company_referred'] = company.deduced
                deduced_link_type = FindReferrals.LINK_TYPE_DEFAULT
                title = person.title_at(company.deduced[C.NAME])
                if title:
                    # TODO: temp code:
                    if not AcureRateUtils.is_senior(company_r, title) and 'Director' not in title:
                        continue
                    person.deduced['title_at_company_referred'] = title + " @ " + company_name
                    # TODO: complete this...
                    if 'president' in title.lower():  # TODO: remove... done to catch Miki Migdal... need to use isSenior
                        deduced_link_type = FindReferrals.LINK_TYPE_MOVER_AND_SHAKER
                    # Create in graph the referral node and link to it
                    person_node_id = self._add_entity_to_d3_json_graph(person.deduced[P.FULL_NAME], person, FindReferrals.GROUP_REFERRALS)
                    self._add_link_to_d3_json_graph(founder_node_id, person_node_id, value=FindReferrals.LINK_STRENGTH_MEDIUM, link_type=deduced_link_type)

        # Get all people who are (a) in founder's contacts; (b) have related investors
        query = {"$and": [{"$or": [{"deduced.investor": {"$exists": True}},
                                   {"deduced.business": {"$exists": True}}]},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
        cursor = DBWrapper.get_persons(query)
        contacts = [AcureRatePerson.reconstruct(r) for r in cursor]
        for contact in contacts:
            contact_contacts = contact.business_related_contacts(high_profile=True)
            for contact_contact_name, contact_contact_relation, contact_contact_company in contact_contacts:
                r = DBWrapper.get_persons({"deduced.full_name": contact_contact_name}, True)
                if r:
                    contact_contact = AcureRatePerson.reconstruct(r)
                    for company_name, company in targetted_companies_map.items():
                        if contact_contact.is_related_to_companies(company.deduced[C.ALIASES]):
                            # Create in graph the referral node and link to it

                            contact_node_id = self._add_entity_to_d3_json_graph(contact.deduced[P.FULL_NAME], contact, FindReferrals.GROUP_REFERRALS)
                            self._add_link_to_d3_json_graph(founder_node_id, contact_node_id, value=FindReferrals.LINK_STRENGTH_MEDIUM, link_type=FindReferrals.LINK_TYPE_MOVER_AND_SHAKER)

                            # Create the contact's contact that will lead to the company
                            contact_contact.deduced['company_referred'] = company.deduced
                            title = contact_contact.title_at(company.deduced[C.ALIASES])
                            if title:
                                contact_contact.deduced['title_at_company_referred'] = title + " @ " + company_name
                            else:
                                # no title, we can't know if it's a "serious" connection
                                continue
                                # contact_contact.deduced['title_at_company_referred'] = "Related to " + company_name

                            relation_phrase = FindReferrals._generate_referral_2_investor_phrase(contact, contact_contact_name, contact_contact_relation, contact_contact_company)
                            contact_contact.deduced['referral'] = contact.deduced[P.FULL_NAME]
                            contact_contact.deduced['relation_phrase'] = relation_phrase

                            link_strength = self._calculate_link_strength(contact, contact_contact_name, contact_contact_relation, contact_contact_company)

                            contact_contact_node_id = self._add_entity_to_d3_json_graph(contact_contact.deduced[P.FULL_NAME], contact_contact, FindReferrals.GROUP_REFERRALS)
                            #self._add_link_to_d3_json_graph(contact_node_id, contact_contact_node_id, relation=relation_phrase, value=FindReferrals.LINK_STRENGTH_MEDIUM, link_type=FindReferrals.LINK_TYPE_MOVER_AND_SHAKER)
                            self._add_link_to_d3_json_graph(contact_node_id, contact_contact_node_id, relation=relation_phrase, value=link_strength, link_type=FindReferrals.LINK_TYPE_MOVER_AND_SHAKER)


        self._write_d3_json_to_file('companies_map.json')

    def generate_founders_map(self):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_person_to_d3_json_graph(self.founder, FindReferrals.GROUP_FOUNDER)

        # Get all the people who are: (a) in my contacts; and (b) Founders in their title
        query = {"$and": [{"deduced.founder": True},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
        cursor = DBWrapper.get_persons(query)
        if cursor.count() == 0:
            return
        print("--> Number of Founders found related to founder: %d" % cursor.count())
        for r in cursor:
            person = AcureRatePerson.reconstruct(r)
            # Add link from FOUNDER to CONTACT-INVESTOR
            ceo_id = self._add_person_to_d3_json_graph(person, FindReferrals.GROUP_CONTACT_INVESTORS)
            self._add_link_to_d3_json_graph(founder_node_id, ceo_id, 4)

        self._write_d3_json_to_file('founders_map.json')

    def generate_ceos_map(self):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_person_to_d3_json_graph(self.founder, FindReferrals.GROUP_FOUNDER)

        # Get all the CEOs the contact knows
        query = {"$and": [{"deduced.ceo": True},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
        cursor = DBWrapper.get_persons(query)
        if cursor.count() == 0:
            return

        # Mainly for debug
        # ceos = []
        # for r in cursor:
        #     ceo_person = AcureRatePerson.reconstruct(r)
        #     ceos.append(ceo_person.deduced[P.FULL_NAME])

        print("--> Number of CEOs found related to founder: %d" % cursor.count())
        for r in cursor:
            ceo_person = AcureRatePerson.reconstruct(r)
            # Add link from FOUNDER to CONTACT-INVESTOR
            mutual_work = self.founder.worked_together(ceo_person)
            if mutual_work:
                relation_phrase = FindReferrals._generate_founder_2_referral_phrase(self.founder, ceo_person, mutual_work)
            else:
                relation_phrase = "Appears in %s's contacts" % self.founder.deduced[P.FIRST_NAME]
            ceo_person.deduced['relation_phrase'] = relation_phrase

            ceo_node_id = self._add_person_to_d3_json_graph(ceo_person, FindReferrals.GROUP_CONTACT_INVESTORS)
            self._add_link_to_d3_json_graph(founder_node_id, ceo_node_id, 4, relation_phrase)

            # Add link from REFERRAL to INVESTOR (email serves as id)
            for investor_name, investor_relation, investor_company in ceo_person.deduced.get(P.RELATED_INVESTORS, []):
                relation_phrase = FindReferrals._generate_referral_2_investor_phrase(ceo_person, investor_name, investor_relation, investor_company)
                investor_person = self._get_person(investor_name)
                if investor_person is None:
                    investor_person = AcureRatePerson()
                    investor_person.deduced = {P.FULL_NAME: investor_name}
                investor_person.deduced['referral'] = ceo_person.deduced[P.FULL_NAME]
                investor_person.deduced['relation_phrase'] = relation_phrase
                investor_node_id = self._add_person_to_d3_json_graph(investor_person, FindReferrals.GROUP_INVESTORS, alt_name=investor_name)
                link_strength = self._calculate_link_strength(ceo_person, investor_person, investor_relation, investor_company)
                self._add_link_to_d3_json_graph(ceo_node_id, investor_node_id, link_strength, relation_phrase)

        self._write_d3_json_to_file('ceos_map.json')

    def generate_micro_vcs_map(self):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_person_to_d3_json_graph(self.founder, FindReferrals.GROUP_FOUNDER)

        query = {"$and": [
                          {"deduced.related_vcs": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}
        ]}
        cursor = DBWrapper.get_persons(query)
        print("--> Number of contacts of founder with micro-VCs: %d" % cursor.count())
        #names2_str = ", ".join([c['deduced']['full_name'] for c in cursor])
        if cursor.count() == 0:
            return
        # Iterate over results and check who worked with founder
        for r in cursor:
            referral_person = AcureRatePerson.reconstruct(r)
            if referral_person.same_person(self.founder):
                continue
            mutual_work = ('kawa', 'banga!')
            relation_phrase = FindReferrals._generate_founder_2_referral_phrase(self.founder, referral_person, mutual_work)
            referral_person.deduced['referral'] = self.founder.deduced[P.FULL_NAME]
            referral_person.deduced['relation_phrase'] = relation_phrase
            referral_node_id = self._add_person_to_d3_json_graph(referral_person, FindReferrals.GROUP_REFERRALS)
            self._add_link_to_d3_json_graph(founder_node_id, referral_node_id, FindReferrals.LINK_STRENGTH_STRONG, relation_phrase)

            # Add link from REFERRAL to INVESTOR (email serves as id)
            for investor_name, investor_relation, investor_company in referral_person.deduced[P.RELATED_VCS]:
                relation_phrase = FindReferrals._generate_referral_2_investor_phrase(referral_person, investor_name, investor_relation, investor_company)
                investor_person = self._get_person(investor_name)
                if investor_person is None:
                    investor_person = AcureRatePerson()
                    investor_person.deduced = {P.FULL_NAME: investor_name}
                investor_person.deduced['referral'] = referral_person.deduced[P.FULL_NAME]
                investor_person.deduced['relation_phrase'] = relation_phrase
                investor_node_id = self._add_person_to_d3_json_graph(investor_person, FindReferrals.GROUP_INVESTORS, alt_name=investor_name)
                link_strength = self._calculate_link_strength(referral_person, investor_person, investor_relation, investor_company)
                self._add_link_to_d3_json_graph(referral_node_id, investor_node_id, link_strength, relation_phrase)

        self._write_d3_json_to_file('micro_vcs_map.json')
        pass

    def generate_referrals_map(self, conservative=False):
        self._clean_graph()

        # Add founder to graph
        founder_node_id = self._add_person_to_d3_json_graph(self.founder, FindReferrals.GROUP_FOUNDER)

        # Get people who are: (a) in founder's contacts, (b) business contacts, (c) have related investors
        # query = {"$and": [{"deduced.business": True},
        #                   {"deduced.related_investors": {"$exists": True}},
        #                   {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
        #                            {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}]}
        query = {"$and": [
                          {"deduced.related_investors": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.founder_aid}]}
        ]}
        cursor = DBWrapper.get_persons(query)
        print("--> Number of business contacts of founder: %d" % cursor.count())
        #names2_str = ", ".join([c['deduced']['full_name'] for c in cursor])

        if cursor.count() == 0:
            return

        investment_categories = {}

        # Iterate over results and check who worked with founder
        for r in cursor:
            referral_person = AcureRatePerson.reconstruct(r)
            if referral_person.same_person(self.founder):
                continue
            # TODO: remove - TEMP code!
            if "David Oren" in self.founder_name and "Dror Garti" in referral_person.deduced[P.FULL_NAME]:
                continue
            if conservative:
                mutual_work = self.founder.worked_together(referral_person)
            else:
                mutual_work = ('kawa', 'banga!')
            if mutual_work:
                relation_phrase = FindReferrals._generate_founder_2_referral_phrase(self.founder, referral_person, mutual_work)
                referral_person.deduced['referral'] = self.founder.deduced[P.FULL_NAME]
                referral_person.deduced['relation_phrase'] = relation_phrase
                referral_node_id = self._add_person_to_d3_json_graph(referral_person, FindReferrals.GROUP_REFERRALS)
                #self._add_link_to_d3_json_graph(founder_node_id, referral_node_id, FindReferrals.LINK_STRENGTH_WEAK, relation_phrase)
                self._add_link_to_d3_json_graph(founder_node_id, referral_node_id, FindReferrals.LINK_STRENGTH_STRONG, relation_phrase)

                # Number of referrals is the number of nodes we pushed in so far (excluding the founder)
                self.stats["num_referrals"] += 1

                # Add link from REFERRAL to INVESTOR (email serves as id)
                for investor_name, investor_relation, investor_company in referral_person.deduced[P.RELATED_INVESTORS]:
                    relation_phrase = FindReferrals._generate_referral_2_investor_phrase(referral_person, investor_name, investor_relation, investor_company)
                    investor_person = self._get_person(investor_name)
                    if investor_person is None:
                        investor_person = AcureRatePerson()
                        investor_person.deduced = {P.FULL_NAME: investor_name}
                    investor_person.deduced['referral'] = referral_person.deduced[P.FULL_NAME]
                    investor_person.deduced['relation_phrase'] = relation_phrase
                    investor_node_id = self._add_person_to_d3_json_graph(investor_person, FindReferrals.GROUP_INVESTORS, alt_name=investor_name)
                    link_strength = self._calculate_link_strength(referral_person, investor_person, investor_relation, investor_company)
                    self._add_link_to_d3_json_graph(referral_node_id, investor_node_id, link_strength, relation_phrase)
                    # Handle investments categories
                    for ic in investor_person.deduced.get(P.INVESTMENT_CATEGORIES, []):
                        ics = ic.strip()
                        if ics not in investment_categories:
                            investment_categories[ics] = 1
                        else:
                            investment_categories[ics] += 1

        # Number of unique investors is number of unique names we have excluding the referrals and the founder
        self.stats["num_unique_investors"] = len(self.names) - self.stats["num_referrals"] - 1

        # Number of all investors' paths is number of all nodes, excluding the referrals and the founder
        self.stats["num_investors"] = len(self.graph['nodes']) - self.stats["num_referrals"] - 1


        # Show all the direct links between founder and his related investors
        for tupple in self.founder.deduced.get(P.RELATED_INVESTORS, []):
            # TODO: we can open it up to those investors the founder knows - not only via his contacts!
            if True:  # tupple[1] == R.CONTACT_OF:
                person = FindReferrals.get_person_from_db({P.DEDUCED+"."+P.FULL_NAME: tupple[0]})
                if person:
                    # Add link from FOUNDER to CONTACT-INVESTOR
                    #rel_str = FindReferrals._generate_founder_2_investor_phrase(self.founder, person)
                    rel_str = FindReferrals._generate_referral_2_investor_phrase(self.founder, tupple[0], tupple[1], tupple[2],)
                    contact_investor_id = self._add_person_to_d3_json_graph(person, FindReferrals.GROUP_CONTACT_INVESTORS)
                self._add_link_to_d3_json_graph(founder_node_id, contact_investor_id, FindReferrals.LINK_STRENGTH_WEAK, rel_str)

        popular_catgories = [k for k, v in investment_categories.items() if v > 2]
        self._add_catgories_to_d3_json_graph(popular_catgories)

        self._write_d3_json_to_file('referrals_map.json')
        pass

    @staticmethod
    def _generate_founder_2_investor_phrase(founder, investor):
        rel_str = "%s is an investor who knows %s" % (investor.deduced[P.FIRST_NAME], founder.deduced[P.FIRST_NAME])
        return rel_str

    @staticmethod
    def _generate_referral_2_investor_phrase(referral, investor_name, investor_relation, investor_company):
        investor_first_name = investor_name.split(" ")[0]
        if investor_relation == R.CONTACT_OF:
            return "%s appears in %s's contacts" % (investor_first_name, referral.deduced[P.FIRST_NAME])

        if referral.board_at(investor_company):
            role_str = "is board member"
        elif referral.title_at(investor_company) is not None:
            role_str = "is " + referral.title_at(investor_company)
        else:
            role_str = "works"
        map = {
            R.WORKED_AT: "colleague executive at",
            R.ADVISOR_AT: "board member at",
            R.CONTACT_OF: "contact of",
            R.INVESTOR_AT: "investor at",
            R.FOUNDER_OF: "founder of",

            R.INVESTOR_BC: "investor at",
            R.ADVISOR_AT_BC: "board member at",
            R.FOUNDER_OF_BC: "founder of",
            R.PAST_TEAM_BC: "past team at"
        }
        rel_str = "%s is %s %s where %s %s" % (investor_first_name,
                                               map[investor_relation],
                                               investor_company,
                                               referral.deduced[P.FIRST_NAME],
                                               role_str)
        return rel_str

    @staticmethod
    def _generate_founder_2_referral_phrase(founder, referral, mutual_work):
        # TOO: Remove, this is currently TEMP code:
        if list(mutual_work)[0] == 'kawa':
            return "%s appears in %s contacts" % (referral.deduced[P.FIRST_NAME], founder.deduced[P.FIRST_NAME])

        return "%s worked with %s at %s" % (referral.deduced[P.FIRST_NAME],
                                            founder.deduced[P.FIRST_NAME],
                                            ', '.join(list(mutual_work)))

    def _get_person(self, full_name):
        f, m, l = AcureRateUtils.tokenize_full_name(full_name)
        q = {"deduced.first_name": f, "deduced.last_name": l}
        r = DBWrapper.get_persons(q, True)
        return AcureRatePerson().reconstruct(r) if r else None

    def _get_image(self, person):
        img_url = person.deduced[P.PHOTO_SELECTED] if person and P.PHOTO_SELECTED in person.deduced else FindReferrals.DEFAULT_PHOTO_URL
        return img_url


