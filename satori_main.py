from datetime import datetime
import collections
import logging
from collections import Counter
import datetime
import re
import time
import requests
import urllib
import http
import random
from SatoriConfig import GeneralConfig

from utils.acurerate_utils import AcureRateUtils

from importer.csv_contacts_importer import CSVContactsImporter
from importer.csv_companies_importer import CSVCompaniesImporter

from store.db_wrapper import DBWrapper
from store.neo_wrapper import NeoWrapper

from enrichment.enrichment_service import EnrichmentSource, EnrichmentData, EnrichmentBehavior
from enrichment.enrichment_service import EnrichmentService

from utils.find_referrals import FindReferrals

from entities.acurerate_attributes import P, C, G
from entities.acurerate_job import AcureRateJob
from entities.acurerate_person import AcureRatePerson
from entities.acurerate_company import AcureRateCompany

from neo4j.v1 import GraphDatabase, basic_auth, CypherError

from engagement.f6s_engager import F6SEngager


class SatoriMain(object):

    def __init__(self):
        # Setup logger
        self._setup_logger(None)

        # Initialize Enrichment Service
        self.es = EnrichmentService.singleton()
        self.eps = self.es.get_providers()
        self.all_providers = self.es.get_providers()

        # Initialize Referrals engine
        self.fr = FindReferrals("drorgarti@gmail.com")
        #self.fr = FindReferrals("doron.herzlich@gmail.com")
        #self.fr = FindReferrals("david@pirveliventures.com")
        #self.fr = FindReferrals("omrik@yahoo.com")

        # Initialize others...
        self.should_exit = False

        pass

    def _setup_logger(self, logger):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a file handler
        # TODO: handle cases where we don't want to override
        file_path = '%s\%s' % (GeneralConfig.LOGS_FOLDER, 'main.log')
        if GeneralConfig.LOGS_APPEND:
            file_handler = logging.FileHandler(file_path, mode='a', encoding='utf-8')  # append
        else:
            file_handler = logging.FileHandler(file_path, mode='w', encoding='utf-8')  # override
        file_handler.setLevel(logging.INFO)

        # Create a logging format
        format = "%(asctime)s | %(filename)s:%(lineno)s | %(name)s::%(funcName)s() | %(levelname)s | %(message)s"
        format2 = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format)
        file_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.info('-=' * 50)
        self.logger.info('Main started')
        self.logger.info('-=' * 50)

    def _menu_and_dispatch(self, menu):
        cont = False
        while not cont:
            for d, f in menu.values():
                print(d)
            input_var = input(">>>")
            disp, func = menu.get(input_var, (None,None))
            if func:
                func()
                cont = True
            else:
                print("Invalid command " + input_var)
        return input_var

    def start(self):

        # if sys.stdout.encoding != 'utf-8':  # cp850
        #     sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        # if sys.stderr.encoding != 'utf-8':
        #     sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

        # Test Log for Unicode characters
        # heb_string = 'התוכנית החלה'
        # heb_string_u = u'התוכנית החלה'
        # self.logger.info(heb_string)
        # self.logger.info(heb_string_u)

        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Data Import", self.import_data)
        main_menu["2"] = ("2. Data Export", self.export_data)
        main_menu["3"] = ("3. Enrichment", self.enrichment)
        main_menu["4"] = ("4. Redigest", self.redigest)
        main_menu["5"] = ("5. Switch Founder", self.switch_founder)
        main_menu["6"] = ("6. Create new entity", self.create)
        main_menu["7"] = ("7. Generate Maps", self.generate_maps)
        main_menu["8"] = ("8. Dedup", self.dedup)
        #main_menu["8"] = ("8. Maintain Mongo", self.maintain_mongo)
        main_menu["9"] = ("9. BlackOps Enrichment", self.blackops_enrichment)
        main_menu["b"] = ("b. Remove Provider", self.remove_provider)
        main_menu["c"] = ("c. Display Stats", self.show_stats)
        main_menu["n"] = ("n. Neo4J", self.neo4j)

        while not self.should_exit:
            self._menu_and_dispatch(main_menu)

    def exit_program(self):
        self.should_exit = True

    def maintain_mongo(self):
        input_var = input("Are you sure? (answer 'yes') >>>")
        if input_var.lower() != "yes":
            return

        DBWrapper.maintain_mongo()
        self.logger.info('Done maintenance!')

    def import_data(self):
        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Import Contacts", self.import_contacts)
        main_menu["2"] = ("2. Import Companies", self.import_companies)
        main_menu["3"] = ("3. Check for cliques", self.check_for_cliques)

        print("Data Import Menu:")
        print("-----------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def check_for_cliques(self):
        full_name = input('Enter person full name >')
        r = DBWrapper.get_persons({'deduced.'+P.FULL_NAME: full_name}, True)
        if not r:
            self.logger.error('Unable to locate person/user %s in MongoDB', full_name)
            return
        person = AcureRatePerson.reconstruct(r)
        person.can_join_cliques()

        self.logger.info('Starting cliques check for %s', full_name)

    def import_contacts(self):
        from store.store import Store

        providers, full_name = self._input_prompt('Enter person full name ([providers:]>')

        if full_name == 'ns':
            full_name = 'Noemi Schmayer'

        file_prefix = full_name.replace(' ', '-').lower()

        self.logger.info('Starting contacts import for %s', full_name)

        # Get person from DB:
        persons = Store.get_persons({P.FULL_NAME: full_name}, single_result=True)
        if len(persons) == 0:
            self.logger.error('Unable to locate person/user %s in MongoDB', full_name)
            return
        elif len(persons) > 1:
            self.logger.error('More than one match to person/user %s in MongoDB', full_name)
            return
        for person_user in persons:
            contacts_file_name = r"C:\temp\AcureRate\Contact Files\%s-google_contacts_export_utf8.csv" % file_prefix
            ci = CSVContactsImporter(path=contacts_file_name,
                                     encoding=None,  #encoding="utf-8",
                                     source="GoogleContacts",
                                     attribution_id=person_user.aid,
                                     attribution_name=full_name,
                                     providers=providers,
                                     mapping=CSVContactsImporter.google_mapping2,
                                     test_import=False)
            ci.import_now()

        self.logger.info('Done importing Google contacts of %s', full_name)

        # Mark person in MongoDB as User
        # TODO: ...

        return

        # dror_aid = "578f49f348d72719070ea206"
        # doron_aid = "578b58cb48d72719070d6528"
        # david_oren_aid = "57bc901ce61f1193742e8756"
        #omri_kedem_aid = "578b58e648d72719070d67c3"

        # omri_kedem_google_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\omri-kedem-google_contacts_export_utf8.csv"
        # ci = CSVContactsImporter(path=omri_kedem_google_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="GoogleContacts",
        #                          attribution_id=omri_kedem_aid,
        #                          attribution_name="Omri Kedem",
        #                          mapping=CSVContactsImporter.google_mapping2,
        #                          test_import=True)
        # ci.import_now()

        # omri_kedem_linkedin_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\omri-kedem-linkedin_connections_export_microsoft_outlook_utf8.csv"
        # ci = CSVContactsImporter(path=omri_kedem_linkedin_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="LinkedInContacts",
        #                          attribution_id=omri_kedem_aid,
        #                          attribution_name="Omri Kedem",
        #                          mapping=CSVContactsImporter.linkedin_outlook_mapping,
        #                          test_import=True)
        # ci.import_now()

        # david_oren_google_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\david-oren-google_contacts_1_export_utf8.csv"
        # ci = CSVContactsImporter(path=david_oren_google_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="GoogleContacts",
        #                          attribution_id=david_oren_aid,
        #                          attribution_name="David Oren",
        #                          mapping=CSVContactsImporter.google_mapping2,
        #                          test_import=False)
        # ci.import_now()

        # david_oren_linkedin_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\david-oren-linkedin_connections_export_microsoft_outlook_utf8.csv"
        # ci = CSVContactsImporter(path=david_oren_linkedin_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="LinkedInContacts",
        #                          attribution_id=david_oren_aid,
        #                          attribution_name="David Oren",
        #                          mapping=CSVContactsImporter.linkedin_outlook_mapping,
        #                          test_import=False)
        # ci.import_now()

        # doron_google_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\doron-google_contacts_export_utf8.csv"
        # ci = CSVContactsImporter(path=doron_google_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="GoogleContacts",
        #                          attribution_id=doron_aid,
        #                          attribution_name="Doron Herzlich",
        #                          mapping=CSVContactsImporter.google_mapping2,
        #                          test_import=True)
        # ci.import_now()

        # doron_linkedin_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\doron-linkedin_connections_export_microsoft_outlook_utf8.csv"
        # ci = CSVContactsImporter(path=doron_linkedin_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="LinkedInContacts",
        #                          attribution_id=doron_aid,
        #                          attribution_name="Doron Herzlich",
        #                          mapping=CSVContactsImporter.linkedin_outlook_mapping,
        #                          test_import=False)
        # ci.import_now()

        # dror_google_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\dror-google_contacts_export_utf8.csv"
        # ci = CSVContactsImporter(path=dror_google_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="GoogleContacts",
        #                          attribution_id=dror_aid,
        #                          attribution_name="Dror Garti",
        #                          mapping=CSVContactsImporter.google_mapping,
        #                          test_import=True)
        # ci.import_now()
        #
        # dror_linkedin_contacts_utf8 = r"C:\temp\AcureRate\Contact Files\dror-linkedin_connections_export_microsoft_outlook_utf8.csv"
        # ci = CSVContactsImporter(path=dror_linkedin_contacts_utf8,
        #                          encoding="utf-8",
        #                          source="LinkedInContacts",
        #                          attribution_id=dror_aid,
        #                          attribution_name="Dror Garti",
        #                          mapping=CSVContactsImporter.linkedin_outlook_mapping,
        #                          test_import=True)
        # ci.import_now()

        pass

    def import_companies(self):

        # cb_companies_file = r"C:\temp\AcureRate\Company Files\CrunchBase\CrunchBase_2014.csv"
        al_companies_file = r"C:\temp\AcureRate\Company Files\Angellist\angellist_companies--incubator--MERGE.csv"

        ci = CSVCompaniesImporter(path=al_companies_file,
                                  encoding="utf-8",
                                  source="AngelList",
                                  mapping=CSVCompaniesImporter.angellist_mapping,
                                  test_import=True)
        ci.import_now()

    def enrichment(self):

        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Only Business Contacts", None)
        main_menu["2"] = ("2. Working Colleagues", None)
        main_menu["3"] = ("3. Person by Email", self.enrich_person_by_email)
        main_menu["4"] = ("4. Person by Name", self.enrich_person_by_name)
        main_menu["5"] = ("5. Remove from Excluded Providers", self.enrichment_exclude_provider)
        main_menu["6"] = ("6. Add to Excluded Providers", self.enrichment_include_provider)
        main_menu["8"] = ("8. Company(ies) by Name", self.enrich_companies_by_name)
        main_menu["m"] = ("m. Enrich Multiple (predefined query)", self.enrich_multiple)
        main_menu["t"] = ("t. Test Enrichment Module", self.test_test)

        print("Enrichment (excluded providers: %s):" % self.eps)
        print("-------------------------------------------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def enrichment_exclude_provider(self):
        m = {"bl": "BloombergScraper", "cb": "CrunchBase", "cbb": "CrunchBaseBot", "cbs": "CrunchBaseScraper", "fc": "FullContact", "pp": "Pipl", "d": "Dummy", "li": "LinkedIn"}
        print("Current Excluded providers: %s:" % self.eps)
        input_var = input("Enter Provider to exclude (or few providers, comma separated) >>>")
        prs = [m[x.strip()] for x in input_var.split(",") if m.get(x.strip(), None) is not None]
        self.eps = list(set(self.eps) - set(prs))

    def enrichment_include_provider(self):
        m = {"bl": "BloombergScraper", "cb": "CrunchBase", "cbb": "CrunchBaseBot", "cbs": "CrunchBaseScraper", "fc": "FullContact", "pp": "Pipl", "d": "Dummy", "li": "LinkedIn"}
        print("Current Excluded providers: %s:" % self.eps)
        input_var = input("Enter Provider to include >>>")
        prs = [m[x.strip()] for x in input_var.split(",") if m.get(x.strip(), None) is not None]
        self.eps = list(set(self.eps).union(set(prs)))
        pass

    def enrich_person_by_email(self):
        providers, email = self._input_prompt('Enter email')
        if email is None or providers is None:
            print('No providers selected. No enrichment done.')
            return
        key = {P.EMAIL: email}
        behavior = EnrichmentBehavior(providers=providers, force=True, enrich_multiple=False)
        enriched_persons = self.es.enrich_person(enrichment_key=key, enrichment_behavior=behavior)
        self.logger.info('Done enriching person by email - %s (enriched: %s)', email, enriched_persons)

    def enrich_person_by_name(self):
        providers, full_name = self._input_prompt('Enter FULL Name')
        if full_name is None or providers is None:
            print('No providers selected. No enrichment done.')
            return
        f, m, l = AcureRateUtils.tokenize_full_name(full_name)
        key = {P.FIRST_NAME: f, P.LAST_NAME: l}
        if m:
            key[P.MIDDLE_NAME] = m
        behavior = EnrichmentBehavior(providers=providers, force=True, enrich_multiple=False)
        enriched_persons = self.es.enrich_person(enrichment_key=key, enrichment_behavior=behavior)
        self.logger.info('Done enriching person by name %s. Enriched: %s', full_name, enriched_persons)


    def _input_prompt(self, msg):
        input_str = input("%s >>> " % msg)
        if input_str.strip() == '*' or len(input_str.strip()) == 0:
            print("Action Aborted.")
            return None
        tokens = input_str.split(':')
        if len(tokens) == 1:
            return None, tokens[0]
        elif len(tokens) == 2:
            if tokens[0] == 'all':
                return self.es.get_providers(), tokens[1]
            else:
                m = self.es.providers_symbols_map()
                prs = [m[x.strip()] for x in tokens[0].split(",") if m.get(x.strip(), None) is not None]
                return prs, tokens[1]
        else:
            return None, None

    def enrich_companies_by_name(self):
        providers, company_names = self._input_prompt('Enter company name (or few comma-separated)')
        if company_names is None or providers is None:
            print('No providers selected or no company names. No enrichment done.')
            return
        companies = company_names.replace(", ", ",").split(",")
        for company_name in companies:
            my_key = {C.ALIASES: company_name.lower()}
            my_behavior = EnrichmentBehavior(providers=providers, force=True, auto_dedup=False)
            enriched = self.es.enrich_company(enrichment_key=my_key, enrichment_behavior=my_behavior)

            self.logger.info('Done enriching company %s. Enriched: %s', company_name, enriched)
        pass

    def enrich_multiple(self):

        providers, approval = self._input_prompt("Enrich multiple entities? [providers:yes]")
        if approval != 'yes' and approval != 'y':
            return

        # m = self.es.providers_symbols_map()
        # providers = [m[x.strip()] for x in input_var.split(",") if m.get(x.strip(), None) is not None]

        # All of Founder's contacts who do not have FullContact enrichment
        founder_name = 'Omri Kedem'
        query1 = {"$and": [{"data_sources.FullContact": {"$exists": False}},
                           {"data_sources.GoogleContacts.attribution_name": founder_name}]}

        # All of Founder's contacts who do not have Pipl enrichment
        query3 = {"$and": [{"data_sources.Pipl": {"$exists": False}},
                           {"data_sources.GoogleContacts.attribution_name": founder_name}]}

        # All of Founder's contacts who have FullContact data source, but it's NOT success
        query2 = {"$and": [{"data_sources.FullContact": {"$exists": True}},
                           {"data_sources.FullContact.last_run_status": {"$ne": "success"}},
                           {"data_sources.GoogleContacts.attribution_name": founder_name}]}

        query4 = {"$and": [{"data_sources.CrunchBaseBot": {"$exists": False}},
                           {"deduced.business": True},
                           {"deduced.ceo": True},
                           {"data_sources.GoogleContacts.attribution_name": founder_name}]}

        # All of the GO & LI contacts which the founder attributed & don't have yet FullContact enrichment
        query5 = {"$and": [
            {"data_sources.FullContact": {"$exists": False}},
            {"data_sources.CrunchBaseBot": {"$exists": False}},
            {"data_sources.CrunchBaseScraper": {"$exists": False}},
            {"data_sources.GoogleContacts.attribution_name": founder_name}
        ]}

        # regx = re.compile("@hotmail", re.IGNORECASE)
        # the_query = {"$and": [
        #     {"data_sources.FullContact": {"$exists": False}},
        #     {"deduced.email": regx},
        #     {"data_sources.GoogleContacts.attribution_name": founder_name}
        # ]}

        # Rerun all those pending on FC - status 202
        # regx = re.compile("202", re.IGNORECASE)
        # the_query = {"$and": [
        #     {"data_sources.FullContact": {"$exists": True}},
        #     {"data_sources.FullContact.last_run_status": regx},
        #     {"data_sources.GoogleContacts.attribution_name": founder_name}
        # ]}

        # the_query = {"$and": [
        #     {"data_sources.Pipl": {"$exists": False}},
        #     {"deduced.business": True},
        #     #{"data_sources.GoogleContacts.attribution_name": founder_name}
        #     {"data_sources.GoogleContacts.attribution_name": founder_name}
        # ]}

        # my_key = {"$and": [
        #     {"data_sources.CrunchBaseBot": {"$exists": False}},
        #     {"data_sources.CrunchBaseScraper": {"$exists": False}},
        #     {"deduced.business": {"$exists": False} },
        #     {"data_sources.GoogleContacts.attribution_name": founder_name}
        # ]}

        # my_key = {"$and": [
        #     {"data_sources.Twitter": {"$exists": False}},
        #     {"deduced.twitter_url": {"$exists": True}}
        # ]}

        # For CBB enrichment
        my_key = {"$or": [
            {"deduced.full_name": "Yanki Margalit"},
            {"deduced.full_name": "Yuval Baharav"}
        ]}
        # For Twitter enrichment
        my_key = {"$or": [
            #{"deduced.full_name": "Guy Nirpaz"},
            #{"deduced.full_name": "Manoj Ramnani"},
            #{"deduced.full_name": "Andy Cohn"}
            # {"deduced.full_name": "Gil Dibner"},
            # {"deduced.full_name": "Hanan Lavy"},
            {"deduced.full_name": "Sasha Gilenson"}
            # {"deduced.full_name": "Shay Segev"},
            # {"deduced.full_name": "Christopher Morace"}
        ]}

        my_key = {"$or": [
            {"deduced.full_name": "Semion Rotshtein"},
            {"deduced.full_name": "Moti Gust"},
            {"deduced.full_name": "Irit Barzily"}
        ]}

        my_key = {"$and":[
            {"data_sources.GoogleContacts.attribution_name": "Noemi Schmayer"},
            {"data_sources.FullContact": {"$exists": True}},
            {"data_sources.FullContact.last_run_status": {"$ne": "success"}}
        ]}

        regx = re.compile("quota exceeded", re.IGNORECASE)
        my_key = {"$and":[
            {"data_sources.GoogleContacts.attribution_name": "Noemi Schmayer"},
            {"data_sources.FullContact": {"$exists": True}},
            {"data_sources.FullContact.last_run_status": regx}
        ]}

        my_behavior = EnrichmentBehavior(providers=providers, force=True, enrich_multiple=True, auto_dedup=False, mongo_query=True)
        #ed = []
        #ed.append(EnrichmentData('shoe_size', 44, 'override'))
        my_src = EnrichmentSource('AcureRateWeb', 'console')

        enriched = self.es.enrich_person(enrichment_key=my_key, enrichment_source=my_src, enrichment_behavior=my_behavior)

        self.logger.info('Done enriching multiple!')

    def test_test(self):
        # Create new entities
        ed = []
        ed.append(EnrichmentData(P.FIRST_NAME, 'Talmon', 'override'))
        ed.append(EnrichmentData(P.LAST_NAME, 'Marco', 'override'))
        ed.append(EnrichmentData(P.FULL_NAME, 'Talmon Marco', 'override'))
        ed.append(EnrichmentData(P.EMAIL, 'talmon@viber.com', 'override'))
        ed.append(EnrichmentData(P.EMAILS, 'talmon@viber.com', 'add'))
        ed.append(EnrichmentData(P.CB_PERMALINK, 'talmon-marco', 'override'))
        my_behavior = EnrichmentBehavior(providers='FullContact', force=True, create_new=True)
        my_src = EnrichmentSource('AcureRateWeb', 'console')
        my_key1 = {P.FIRST_NAME: 'Talmon', P.LAST_NAME: 'Marco'}
        self.es.enrich_person(enrichment_key=my_key1, enrichment_data=ed, enrichment_source=my_src,
                              enrichment_behavior=EnrichmentBehavior(providers='CrunchBaseScraper', force=True, create_new=True))

        ed = []
        ed.append(EnrichmentData(P.FIRST_NAME, 'Djamel', 'override'))
        ed.append(EnrichmentData(P.LAST_NAME, 'Agaoua', 'override'))
        ed.append(EnrichmentData(P.FULL_NAME, 'Djamel Agaoua', 'override'))
        ed.append(EnrichmentData(P.EMAIL, 'djamel@viber.com', 'override'))
        ed.append(EnrichmentData(P.EMAILS, 'djamel@viber.com', 'add'))
        ed.append(EnrichmentData(P.CB_PERMALINK, 'djamel-agaoua', 'override'))
        my_behavior = EnrichmentBehavior(providers='FullContact', force=True, create_new=True)
        my_src = EnrichmentSource('AcureRateWeb', 'console')
        my_key2 = {P.FIRST_NAME: 'Djamel', P.LAST_NAME: 'Agaoua'}
        self.es.enrich_person(enrichment_key=my_key2, enrichment_data=ed, enrichment_source=my_src,
                              enrichment_behavior=EnrichmentBehavior(providers='CrunchBaseScraper', force=True, create_new=True))

        # Enrich them
        if False:
            self.es.enrich_person(enrichment_key=my_key1,
                                  enrichment_behavior=EnrichmentBehavior(providers='CircleBack, Pipl, FullContact, Twitter', force=True, create_new=False))
            self.es.enrich_person(enrichment_key=my_key2,
                                  enrichment_behavior=EnrichmentBehavior(providers='CircleBack, Pipl, FullContact, Twitter', force=True, create_new=False))

        # Delete them
        pass

    def redigest(self):

        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Redigest ALL Contacts", self.redigest_all_contacts)
        main_menu["2"] = ("2. Redigest Business Contacts", self.redigest_business_contacts)
        main_menu["3"] = ("3. Redigest Specific Contact by email", self.redigest_contact_by_email)
        main_menu["4"] = ("4. Redigest Specific Contact by name", self.redigest_contact_by_name)
        main_menu["5"] = ("5. Redigest ALL Companies", self.redigest_all_companies)
        main_menu["6"] = ("6. Redigest Companies by name", self.redigest_companies)
        main_menu["7"] = ("7. Redigest Contacts by query", self.redigest_contacts_by_query)
        main_menu["8"] = ("8. Redigest Companies by query", self.redigest_companies_by_query)

        print("Redigest Menu:")
        print("--------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def redigest_all_contacts(self):
        self.es.redigest_contacts({})
        self.logger.info('Done redigesting all contacts!')

    def redigest_all_companies(self):
        self.es.redigest_companies({})
        self.logger.info('Done redigesting all companies!')

    def redigest_companies(self):
        input_var = input("Enter name of company(s) to re-digest >>> ")
        companies = input_var.replace(", ", ",").split(",")
        behavior = EnrichmentBehavior(auto_dedup=False, digest=True, force_save=True)
        for company_name in companies:
            key = {C.ALIASES: company_name.lower()}
            self.es.enrich_company(enrichment_key=key, enrichment_behavior=behavior)
            self.logger.info('Done re-digesting company %s!', company_name)
        pass

    def redigest_business_contacts(self):
        # TODO: do this with email, more comfortable than aid...
        # input_var = input("Enter aid of person to re-digest his business contacts >>> ")
        # aid = input_var
        # q = {"$and": [{"deduced.exits": {"$exists": True} },
        #               {"$or": [{"data_sources.GoogleContacts.attribution_id": aid},
        #                        {"data_sources.LinkedInContacts.attribution_id": aid}]}]}

        input_var = input("Are you sure you want to redigest all business contacts? >>> ")
        q = {"deduced.business": True}
        self.es.redigest_contacts(q)
        self.logger.info('Done re-digesting %s!\n', input_var)

    def redigest_contacts_by_query(self):
        input_var = input("About to redigest contacts by query. Are you sure? (yes) >>> ")
        if input_var != 'yes':
            return
        q = {"deduced.investor": True}

        # q = {"$and": [{"deduced.investor": True},
        #               {"$or": [{"data_sources.GoogleContacts.attribution_id": aid},
        #                        {"data_sources.LinkedInContacts.attribution_id": aid}]}]}
        self.es.redigest_contacts(q)
        self.logger.info('Done re-digesting contacts by query!')

    def redigest_companies_by_query(self):
        input_var = input("About to redigest companies by query. Are you sure? (yes) >>> ")
        if input_var != 'yes':
            return
        q = {"data_sources.CrunchBaseScraper.primary_role": "investor"}
        #q = {"data_sources.CrunchBaseBot.investment_company_type": {"$exists": True}}
        self.es.redigest_companies(q)
        self.logger.info('Done re-digesting companies by query')

    def redigest_contact_by_email(self):
        input_var = input("Enter EMAIL of person to re-digest >>> ")
        key = {P.EMAIL: input_var}
        behavior = EnrichmentBehavior(auto_dedup=False, digest=True)
        self.es.enrich_person(enrichment_key=key, enrichment_behavior=behavior)
        self.logger.info('Done re-digesting by email %s!', input_var)

    def redigest_contact_by_name(self):
        full_name = input("Enter FULL name >>> ")
        f, m, l = AcureRateUtils.tokenize_full_name(full_name)
        key = {P.FIRST_NAME: f, P.LAST_NAME: l}
        if m:
            key[P.MIDDLE_NAME] = m
        behavior = EnrichmentBehavior(auto_dedup=False, digest=True, force_save=True)
        self.es.enrich_person(enrichment_key=key, enrichment_behavior=behavior)
        self.logger.info('Done re-digesting person %s!', full_name)

    def create(self):

        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Create new person", self.create_new_person)
        main_menu["2"] = ("2. Create new company", self.create_new_company)
        main_menu["3"] = ("3. Create multiple new companies", self.create_new_multiple_companies)

        print("Enrichment (excluded providers: %s):" % self.eps)
        print("-------------------------------------------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def create_new_person(self):
        from store.store import Store
        ed = []
        fullname = input("Person FULL Name? >>>").strip()
        if fullname == "*":
            return
        if fullname != "":
            first_name = fullname.split(" ")[0]
            last_name = fullname.split(" ")[1]
            ed.append(EnrichmentData(P.FIRST_NAME, first_name, 'override'))
            ed.append(EnrichmentData(P.LAST_NAME, last_name, 'override'))
        email = input("Person Email? >>>").strip()
        if email == "*":
            return
        if email != "":
            ed.append(EnrichmentData(P.EMAIL, email, 'override'))
            ed.append(EnrichmentData(P.EMAILS, email, 'add'))

        # Run a safety check to make sure no such person exists (check name and email)
        if Store.person_exists_regex(fullname):
            self.logger.info('Person with this NAME %s already exists. Aborting.', fullname)
            return
        q = {"deduced.email": email}
        cursor = DBWrapper.get_persons(q)
        if cursor.count() > 0:
            self.logger.info('Person with this EMAIL already exists. Aborting.')
            return

        permalink = input("Person Permalink? >>>").strip()
        if permalink == "*":
            return
        if permalink != "":
            ed.append(EnrichmentData(P.CB_PERMALINK, permalink.lower(), 'override'))

        phone = input("Person Phone Number? >>>").strip()
        if phone == "*":
            return
        if phone != "":
            ed.append(EnrichmentData(P.PHONES, phone, 'add'))

        # Enrich
        providers = input("Enrich via which providers (ENTER to skip): >>>").strip()
        if len(providers) > 0:
            m = self.es.providers_symbols_map()
            prs = [m[x.strip()] for x in providers.split(",") if m.get(x.strip(), None) is not None]
            my_behavior = EnrichmentBehavior(providers=prs, force=True, create_new=True)
        else:
            my_behavior = EnrichmentBehavior(force=False, create_new=True)

        #my_src = EnrichmentSource('AcureRateWeb', 'console')
        my_src = EnrichmentSource('System', 'nokey')
        my_key = {P.FIRST_NAME: first_name, P.LAST_NAME: last_name}
        self.es.enrich_person(enrichment_key=my_key, enrichment_data=ed, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Redigest all those companies who have this person in their 'unrecognized'
        q = {"deduced.unrecognized_people": fullname}
        self.es.redigest_companies(q)

        self.logger.info('Done inserting new person successfully!')

    def create_new_company(self):
        from store.store import Store
        ed = []
        name = input("Company Name? >>>").strip()
        if name == "*":
            return
        if name != "":
            ed.append(EnrichmentData(C.NAME, name, 'override'))

        # Run a safety check to make sure no such company exists (check name)
        if Store.company_exists(name):
            self.logger.info('Company with the NAME %s already exists. Aborting.', name)
            return

        domain = input("Company Domain? >>>").strip()
        if domain == "*":
            return
        if domain != "":
            ed.append(EnrichmentData(C.DOMAIN, domain, 'override'))
        permalink = input("Company Permalink? >>>").strip()
        if permalink == "*":
            return
        if permalink != "":
            ed.append(EnrichmentData(C.CRUNCHBASE_PERMALINK, permalink, 'override'))
        aliases = input("Company Aliases? (comma separated) >>>").strip()
        if aliases == "*":
            return
        if aliases != "":
            for a in aliases.replace(", ", ",").split(","):
                ed.append(EnrichmentData(C.ALIASES, a, 'add'))
        # Enrich
        providers = input("Enrich via providers (ENTER to skip): >>>").strip()
        if len(providers) > 0:
            m = self.es.providers_symbols_map()
            prs = [m[x.strip()] for x in providers.split(",") if m.get(x.strip(), None) is not None]
            my_behavior = EnrichmentBehavior(providers=prs, force=True, create_new=True)
        else:
            my_behavior = EnrichmentBehavior(force=False, create_new=True)

        my_src = EnrichmentSource('AcureRateWeb', 'console')
        my_key = {C.NAME: name}
        self.es.enrich_company(enrichment_key=my_key, enrichment_behavior=my_behavior, enrichment_data=ed,
                               enrichment_source=my_src)

        # Redigest all those who have this company in their 'unrecognized'
        q = {"deduced.unrecognized_companies": name}
        self.es.redigest_contacts(q)

        print("Done inserting new company successfully and redigesting all related contacts!")

    def create_new_multiple_companies(self):
        providers = input("Enrich by which PROVIDERS ? (ENTER to skip): >>>").strip()
        if len(providers) > 0:
            m = self.es.providers_symbols_map()
            prs = [m[x.strip()] for x in providers.split(",") if m.get(x.strip(), None) is not None]
        else:
            print('No providers specified. Aborting.')
            return

        my_behavior = EnrichmentBehavior(providers=prs, force=True)
        my_src = EnrichmentSource('AcureRateWeb', 'console')

        companies_names = input("Enter MULTIPLE Company Names? (comma separated) >>>").strip()
        if companies_names == "*" or companies_names == "":
            return
        # Iterate over all companies and enrich them one by one
        for company_name in companies_names.replace(", ", ",").split(","):
            ed = []
            ed.append(EnrichmentData(C.NAME, company_name, 'override'))
            my_key = {C.ALIASES: company_name.lower()}
            self.es.enrich_company(enrichment_key=my_key, enrichment_behavior=my_behavior, enrichment_data=ed,
                                   enrichment_source=my_src)

            # Redigest all those who have this company in their 'unrecognized'
            q = {"deduced.unrecognized_companies": company_name}
            self.es.redigest_contacts(q)

        pass

    def switch_founder(self):
        input_var = input("Enter name of founder? >>>")
        if input_var.strip() == '*':
            return
        self.logger.info('Done switching founder!')
        # TODO: implement switching...
        pass

    def generate_maps(self):
        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Generate Referrals Map", self.generate_referrals_map)
        main_menu["2"] = ("2. Generate Conservative Referrals Map", self.generate_conservative_referrals_map)
        main_menu["3"] = ("3. Generate CEOs Map", self.generate_ceos_map)
        main_menu["4"] = ("4. Generate Founders Map", self.generate_founders_map)
        main_menu["5"] = ("5. Generate Companies Map", self.generate_companies_map)
        main_menu["6"] = ("6. Generate Micro-VCs Map", self.generate_microvcs_map)
        main_menu["7"] = ("7. Generate VCs Map", self.generate_vcs_map)

        print("Generate Maps Menu:")
        print("-------------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def generate_referrals_map(self):
        self.fr.generate_referrals_map()

    def generate_conservative_referrals_map(self):
        self.fr.generate_referrals_map(True)

    def generate_microvcs_map(self):
        self.fr.generate_micro_vcs_map()

    def generate_vcs_map(self):
        self.fr.generate_vcs_map()

    def generate_ceos_map(self):
        self.fr.generate_ceos_map()

    def generate_companies_map(self):
        self.fr.generate_companies_map()

    def generate_founders_map(self):
        self.fr.generate_founders_map()

    def dedup(self):

        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Dedup ALL Contacts", self.dedup_all_contacts)
        main_menu["2"] = ("2. Dedup Business Contacts", self.dedup_business_contacts)
        main_menu["3"] = ("3. Dedup Specific Contact by email", self.dedup_contact_by_email)
        main_menu["4"] = ("4. Dedup Specific Contact by name", self.dedup_contact_by_name)
        main_menu["5"] = ("5. Dedup via Mongo Aggregation", self.dedup_via_mongo_aggregation)

        print("Dedup Menu:")
        print("--------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def dedup_all_contacts(self):
        pass

    def dedup_business_contacts(self):
        pass

    def dedup_contact_by_email(self):
        pass

    def dedup_contact_by_name(self):
        full_name = input("Enter FULL name >>> ")
        full_name = full_name.strip()
        if len(full_name) == 0 or full_name.strip() == '':
            return
        #query = {"deduced.full_name": full_name}
        #self._dedup_manually(query, P.FULL_NAME)

        query = {"deduced.full_name": re.compile(full_name, re.IGNORECASE)}
        self._dedup_manually_2(query)
        pass

    def dedup_via_mongo_aggregation(self):

        name_query = {'first_name': "$deduced.first_name", 'last_name': "$deduced.last_name"}
        email_query = {'emails': "$deduced.emails"}
        DBWrapper.get_aggregation(email_query)
        pass

    def show_stats(self):
        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. General Stats", self.show_stats_general)
        main_menu["2"] = ("2. Mutual Investors", self.show_stats_mutual_investors)
        main_menu["3"] = ("3. Levenshtein", self.levenshtein)
        main_menu["4"] = ("4. Find family clique", self.find_common_substrings_in_person_contacts)

        print("Show Stats Menu:")
        print("----------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above

    def show_stats_general(self):
        people = {}
        query1 = {"$and": [{"deduced.business": True},
                           {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                    {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        query2 = {"$and": [{"deduced": {"$exists": True}},
                           {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                    {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        cursor = DBWrapper.get_persons(query2)
        self.logger.info('Number of records to check - %d', cursor.count())
        for r in cursor:
            full_name = r['deduced'][P.FULL_NAME]
            people[full_name] = people.get(full_name, 0) + 1
        # Sort by number of appearances:
        a = sorted(people.items(), key=lambda v: v[1], reverse=True)

        # How many contacts in db of founder?
        query = {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                         {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}
        res = DBWrapper.get_persons(query)
        general_total_contacts = res.count()

        # How many contacts enriched with "something"?

        # How many contacts were not enriched at all?
        query = {"$and": [{"data_sources.FullContact": {"$exists": False}},
                          {"data_sources.Pipl": {"$exists": False}},
                          {"data_sources.CrunchBaseBot": {"$exists": False}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        names_list = [p[P.DEDUCED][P.FULL_NAME] for p in res]
        names_list_str = ",".join(names_list)
        self.logger.info('Contacts not enriched at all: %s', names_list_str)
        total_not_enriched = len(names_list)

        # How many contacts were enriched with something...?
        query = {"$and": [{"$or": [{"data_sources.FullContact": {"$exists": True}},
                                   {"data_sources.Pipl": {"$exists": True}},
                                   {"data_sources.CrunchBaseBot": {"$exists": True}}]},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}
                         ]}
        res = DBWrapper.get_persons(query)
        names_list = [p[P.DEDUCED][P.FULL_NAME] for p in res]
        names_list_str = ",".join(names_list)
        self.logger.info('Contacts enriched with something: %s', names_list_str)
        total_enriched = len(names_list)


        # How many contacts are marked business?
        query = {"$and": [{"deduced.business": True},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        general_business_contacts = res.count()

        # Get stats on different providers.
        # TODO: automate this as we may add more providers to the game...

        # How many contacts were enriched by FullContact?
        query = {"$and": [{"data_sources.FullContact": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        fc_total_enriched = res.count()

        # How many contacts did I fail to enrich with FullContact?
        query = {"$and": [{"data_sources.FullContact": {"$exists": True}},
                          {"data_sources.FullContact.last_run_status": {"$ne": "success"}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        names_list = [p[P.DEDUCED][P.FULL_NAME] for p in res]
        names_list_str = ",".join(names_list)
        print("FC failed to enrich:\n" + names_list_str)
        fc_total_failed = len(names_list)

        # How many contacts were enriched with CrunchBaseBot?
        query = {"$and": [{"data_sources.CrunchBaseBot": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        cbb_total_enriched = res.count()

        # How many contacts did I enrich via Pipl?
        query = {"$and": [{"data_sources.Pipl": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        pipl_total_enriched = res.count()

        # How many contacts did PIPL fail on quota problem?
        query = {"$and": [{"data_sources.Pipl": {"$exists": True}},
                          {"data_sources.Pipl.last_run_status": "EngagementException('Quota Exceeded!',)"},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        names_list = [p[P.DEDUCED][P.FULL_NAME] for p in res]
        names_list_str = ",".join(names_list)
        print("PP failed due to quota:\n" + names_list_str)
        pipl_quota_failed = len(names_list_str)

        # How many contacts did PIPL completely fail to enrich?
        query = {"$and": [{"data_sources.Pipl": {"$exists": True}},
                          {"data_sources.Pipl.last_run_status": {"$ne": "success"}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        pipl_total_failed = res.count()

        # How many contacts did I enrich which returned with possible persons?
        query = {"$and": [{"data_sources.Pipl.possible_persons": {"$exists": True}},
                          {"$or": [{"data_sources.GoogleContacts.attribution_id": self.fr.founder_aid},
                                   {"data_sources.LinkedInContacts.attribution_id": self.fr.founder_aid}]}]}
        res = DBWrapper.get_persons(query)
        pipl_possible_person_result = res.count()

        print("Total - contacts: %d, enriched: %d, business: %d, fc_enriched: %d, pp_enriched: %d, cb_enriched: %d" % (general_total_contacts, total_enriched, general_business_contacts, fc_total_enriched, pipl_total_enriched, cbb_total_enriched))
        #print("Pipl - Enriched: %d, Failed: %d, Partial: %d" % (pipl_total_enriched, pipl_total_failed, pipl_possible_person_result))
        pass

    def show_stats_mutual_investors(self):
        investors = {}
        q = {"deduced.investors": {"$exists": True}}
        cursor = DBWrapper.get_companies(q)
        for r in cursor:
            company_name = r['deduced'][C.NAME]
            # Iterate over investors and keep
            for investor in r['deduced'].get(C.INVESTORS, []):
                if investor[1] == 'person':
                    investor_name = investor[0]
                    if investor_name in investors:
                        investors[investor_name].append(company_name)
                    else:
                        investors[investor_name] = [company_name]
        pass

    def neo4j(self):
        main_menu = collections.OrderedDict()
        main_menu["0"] = ("0. Exit", self.exit_program)
        main_menu["1"] = ("1. Export Users to Neo4J", self.export_users_to_neo4j)
        main_menu["2"] = ("2. Export Business Contacts to Neo4J", self.export_contacts_to_neo4j)
        main_menu["3"] = ("3. Export Business Companies to Neo4J", self.export_companies_to_neo4j)
        main_menu["4"] = ("4. Export Specific Contacts to Neo4J", self.export_specific_people_to_neo4j)
        main_menu["5"] = ("5. Export Specific Companies to Neo4J", self.export_specific_companies_to_neo4j)
        main_menu["6"] = ("6. Drop nodes/relations of person", self.drop_person_nodes_and_relations_in_neo4j)
        main_menu["7"] = ("7. EXPERIMENTAL", self.experimental)
        main_menu["8"] = ("8. Migrate ALL !", self.mongo2neo4j_migration)
        main_menu["9"] = ("9. Drop all nodes & relations", self.drop_all_nodes_and_relations_in_neo4j)

        print("---------------------")
        print("Neo4J Migration Menu:")
        print("---------------------")
        while not self.should_exit:
            self._menu_and_dispatch(main_menu)
        self.should_exit = False  # Reset if for the menu above


    def export_specific_people_to_neo4j(self):

        input_var = input("Enter people to migrate (separated by commas) >>> ")
        people = input_var.replace(", ", ",").split(",")
        for p in people:
            q = {'deduced.full_name': '%s' % p}
            self._people_to_neo4j_nodes(q)
        for p in people:
            q = {'deduced.full_name': '%s' % p}
            self._people_to_neo4j_relations(q)

        self.logger.info('Done exporting %s people (nodes/relations) to Neo4J !' % len(people))
        pass

    def export_specific_companies_to_neo4j(self):

        input_var = input("Enter companies to migrate (separated by commas) >>> ")
        companies = input_var.replace(", ", ",").split(",")
        for c in companies:
            q = {'deduced.name': '%s' % c}
            self._companies_to_neo4j_nodes(q)
        for c in companies:
            q = {'deduced.name': '%s' % c}
            self._companies_to_neo4j_relations(q)

        self.logger.info('Done exporting %s companies (nodes/relations) to Neo4J !' % len(companies))
        pass

    # Check_duplicate_first_name_in_data_sources
    def levenshtein(self):

        q = {"deduced.business": True}
        cursor = DBWrapper.get_persons(q)
        for r in cursor:
            person = AcureRatePerson.reconstruct(r)
            # Iterate over all name from providers. Append name to list if it has at least first and last name
            all_names = []
            for ds in person.sources():
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
                    #all_names.append((provider_key.lower(), (f, m, l)))
                    all_names.append((f, m, l))
            # Check the names gathered
            if len(all_names) > 1:
                first_name_the_same = all(f == all_names[0][0] for f, m, l in all_names[1:])
                if first_name_the_same:
                    print('All first names are the same')
                else:
                    print('Not all first names are the same...')
        pass

    def find_common_substrings_in_person_contacts(self):

        # @@@
        dror_aid = '578f49f348d72719070ea206'
        omri_aid = '578b58e648d72719070d67c3'
        doron_aid = '578b58cb48d72719070d6528'

        pivot_aid = omri_aid
        string_list = []
        emails_list = []

        # Get all the people connected to this person
        query = {"data_sources.GoogleContacts.attribution_id": pivot_aid}
        cursor = DBWrapper.get_persons(query)
        for r in cursor:
            person = AcureRatePerson.reconstruct(r)
            string_list.append(person.deduced[P.LAST_NAME])
            emails_list.extend(person.deduced.get(P.EMAILS, []))

        cohen_counter = 0
        garti_counter = 0
        herzlich_counter = 0
        for e in emails_list:
            if 'garti' in e:
                garti_counter += 1
            elif 'cohen' in e:
                cohen_counter += 1
            elif 'herzlich' in e:
                herzlich_counter += 1

        # @@@
        #p0 = self._long_substr(string_list)

        c1 = Counter(string_list)
        a3 = c1.most_common(10)

        names_in_emails = []
        for last_name, last_name_count in a3:
            for email in emails_list:
                if last_name.lower() in email.lower():
                    names_in_emails.append(last_name)

        c2 = Counter(names_in_emails)
        b5 = c2.most_common(10)

        pass

    def mongo2neo4j_migration(self):

        input_var = input("Are you sure? (answer 'fresh' or 'all') >>>")
        if input_var.lower() not in ['fresh', 'all']:
            print('Wrong answer... expecting FRESH or ALL. Aborting.')
            return

        AcureRateUtils.boxit(self.logger.info, 'Migrating MongoDB to Neo4J...')

        # @@@ FRESH OR NOT...

        # TODO: add timers around things so I know how much time it takes...

        # Create constraints
        self.logger.info('Creating constraints on database...')
        self.create_constraints()

        # Migrate all business contacts
        self.logger.info('Starting migration of all business contacts...')
        #q = {"deduced.business": True}
        q = {"deduced.jobs": {"$exists": True}}  # Get all those enriched
        self._people_to_neo4j_nodes(q)

        # Migrate all companies
        self.logger.info('Starting migration of all companies...')
        q = {}
        self._companies_to_neo4j_nodes(q)

        # Migrate all people relations
        self.logger.info('Starting migration of all people relations...')
        #q = {"deduced.business": True}
        q = {"deduced.jobs": {"$exists": True}}  # Get all those enriched
        self._people_to_neo4j_relations(q)

        # Migrate all companies relations
        self.logger.info('Starting migration of all company relations...')
        # TODO: find how I can ignore all the CB2014 only... maybe they dont have aid?
        # q = { NEED TO FIND THE QUERY }
        q = {"$or": [
                {"data_sources.AcureRateSpider": {"$exists": True}},
                {"data_sources.AcureRateWeb": {"$exists": True}},
                {"data_sources.FullContact": {"$exists": True}},
                {"data_sources.CrunchBaseBot": {"$exists": True}},
                {"data_sources.CrunchBaseScraper": {"$exists": True}},
                {"data_sources.StartupNationCentral": {"$exists": True}}
        ]}
        self._companies_to_neo4j_relations(q)

        pass

    def export_users_to_neo4j(self):

        # q = {"user_id": {"$exists": True}}
        q = {"deduced.full_name": {'$in': ['Ran Oz', 'Avi Nir', 'Dror Garti', 'Adi Garti', 'Sharon Garti', 'Omri Kedem', 'Doron Herzlich']}}
        self._people_to_neo4j_nodes(q)
        self._people_to_neo4j_relations(q)

        self.logger.info('Done exporting users to Neo4J !')
        pass

    def export_contacts_to_neo4j(self):

        AcureRateUtils.boxit(self.logger.info, 'Exporting contacts to Neo4J...')

        #q = {"deduced.investor": True}
        #q = {"deduced.business": True}

        start = datetime.datetime(2017, 2, 5, 13, 00, 00)  # Year, Month, Day, Hour, Min, Sec
        end = datetime.datetime(2018, 12, 31, 23, 59, 59)
        q = {"last_update": {'$gte': start, '$lt': end}}
        self._people_to_neo4j_nodes(q)
        self._people_to_neo4j_relations(q)
        pass

    def export_companies_to_neo4j(self):

        AcureRateUtils.boxit(self.logger.info, 'Exporting companies to Neo4J...')

        #q = {"data_sources.FullContact": {"$exists": True} }  #  156 companies
        #q = {}

        start = datetime.datetime(2017, 2, 5, 13, 00, 00)  # Year, Month, Day, Hour, Min, Sec
        end = datetime.datetime(2018, 12, 31, 23, 59, 59)
        q = {"last_update": {'$gte': start, '$lt': end}}
        self._companies_to_neo4j_nodes(q)
        self._companies_to_neo4j_relations(q)

        pass

    def create_constraints(self):

        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()

        # Create constraints:
        session.run("CREATE CONSTRAINT ON (p:Person) ASSERT p.aid IS UNIQUE")
        # session.run("CREATE CONSTRAINT ON (p:Person) ASSERT exists(p.aid)")
        # session.run("CREATE CONSTRAINT ON (p:Person) ASSERT exists(p.email)")
        # session.run("CREATE CONSTRAINT ON (p:Person) ASSERT exists(p.name)")

        session.run("CREATE CONSTRAINT ON (c:Company) ASSERT c.aid IS UNIQUE")
        # session.run("CREATE CONSTRAINT ON (c:Company) ASSERT exists(p.aid)")
        # session.run("CREATE CONSTRAINT ON (c:Company) ASSERT exists(p.email)")
        # session.run("CREATE CONSTRAINT ON (c:Company) ASSERT exists(p.name)")

        session.close()

    def _companies_to_neo4j_nodes(self, query):
        excluded = ['related_investors', 'related_vcs', 'unrecognized_people', 'investors']
        labeled = [C.ACQUIRED]
        session = None

        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()

        cursor = DBWrapper.get_companies(query)
        num_companies = 0
        for r in cursor:

            try:
                # session = driver.session()
                company = AcureRateCompany.reconstruct(r)

                # TODO: TEMP TEMP!!  We ignore all those who were initially inserted only by CB Excel
                if len(company.data_sources) == 1:
                    continue

                num_companies += 1
                properties_list = []
                label_list = [G.LABEL_COMPANY]

                # TODO: do I need to create aid for all companies in mongo? (or can I use the _id as reference)
                if not company._aid:
                    self.logger.warning('No aid for company %s. Not migrated.' % company.deduced[P.NAME])
                    continue

                # Iterate over all properties
                for p in company.deduced.keys():
                    if p in labeled:
                        label_list.append(p.capitalize())
                        continue
                    if p in excluded:
                        continue
                    key_str = NeoWrapper.get_property_by_type(p, company.deduced[p], '=')
                    if key_str:
                        properties_list.append(key_str)

                # Set labels and properties for the Cypher query
                lbl_str = ":".join(label_list)
                properties_list_with_prefix = ['a.' + i for i in properties_list]
                all_properties_comma_delimited = ", ".join(properties_list_with_prefix)
                cql_str = 'MERGE (a:%s {aid: "%s"})' % (lbl_str, company._aid) + \
                          'SET %s' % all_properties_comma_delimited
                statement_res = session.run(cql_str)

                self.logger.info('Migrated company %s successfully! (so far, migrated %d nodes)' %
                                 (company.deduced[C.NAME], num_companies))
            except CypherError as ce:
                print("CypherError raised: %s" % ce)
            except Exception as e:
                print("Exception raised: %s" % e)

        # Close the session after all is done
        session.close()

        pass

    def _companies_to_neo4j_relations(self, query):
        session = None
        try:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
            session = driver.session()

            cursor = DBWrapper.get_companies(query)
            num_companies = 0
            for r in cursor:
                #session = driver.session()
                company = AcureRateCompany.reconstruct(r)

                # TODO: TEMP TEMP!!  We ignore all those who were initially inserted only by CB Excel
                if len(company.data_sources) == 1:
                    continue

                num_companies += 1
                self.logger.info('Attempting to create relations for company %s (%d)' % (company.deduced[C.NAME], num_companies))

                company_to_company_relations = []
                person_to_company_relations = []

                # C:C - Create ACQUIRED_BY relation
                if C.ACQUIRED_BY in company.deduced:
                    ac = DBWrapper.get_companies({'deduced.'+C.NAME: company.deduced[C.ACQUIRED_BY]}, single_result=True)
                    if ac:
                        acquiring_company = AcureRateCompany.reconstruct(ac)
                        company_to_company_relations.append((company._aid, G.RELATION_LABEL_ACQUIRED_BY, acquiring_company._aid, []))
                    else:
                        self.logger.warning('Unable to located acquiring company %s. Relation not created.' % company.deduced[C.ACQUIRED_BY])

                # C:C - Create the INVESTED_IN relation
                if C.ORGANIZATION_TYPE in company.deduced and company.deduced[C.ORGANIZATION_TYPE] == C.ORGANIZATION_TYPE_VENTURE_CAPITAL:
                    for portfolio_company in company.deduced.get(C.PORTFOLIO_COMPANIES, []):
                        ccc = DBWrapper.get_companies({'deduced.'+C.ALIASES: portfolio_company.lower()}, True)
                        if ccc:
                            ccc_company = AcureRateCompany.reconstruct(ccc)
                            company_to_company_relations.append(
                                (company._aid, G.RELATION_LABEL_INVESTS_IN, ccc_company._aid, []))
                        else:
                            self.logger.warning(
                                'Unable to locate in db the portfolio company %s that got investment from %s. Relation not created.' %
                                (portfolio_company, company.deduced[C.NAME]))

                # P:C - Create EMPLOYEE_OF relation (Team. past_team)
                for team_mate in company.deduced.get(C.TEAM, []):
                    p = DBWrapper.get_persons({'deduced.' + P.FULL_NAME: team_mate}, single_result=True)
                    if p:
                        person = AcureRatePerson.reconstruct(p)
                        person_to_company_relations.append(
                            (person.aid, G.RELATION_LABEL_EMPLOYEE_OF, company._aid, []))
                    else:
                        self.logger.warning('Unable to create EMPLOYEE_OF relation to person %s (not found in Mongo)', team_mate)

                # P:C - Create BOARD_AT relation (Advisors)
                for advisor in company.deduced.get(C.ADVISORS, []):
                    p = DBWrapper.get_persons({'deduced.' + P.FULL_NAME: advisor}, single_result=True)
                    if p:
                        person = AcureRatePerson.reconstruct(p)
                        person_to_company_relations.append(
                            (person.aid, G.RELATION_LABEL_ADVISOR_AT, company._aid, []))
                    else:
                        self.logger.warning('Unable to create ADVISOR_AT relation to person %s (not found in Mongo)', advisor)

                # P:C - Create FOUNDER_OF relation (Company)
                for founder in company.deduced.get(C.FOUNDERS, []):
                    p = DBWrapper.get_persons({'deduced.' + P.FULL_NAME: founder}, single_result=True)
                    if p:
                        person = AcureRatePerson.reconstruct(p)
                        person_to_company_relations.append(
                            (person.aid, G.RELATION_LABEL_FOUNDER_OF, company._aid, []))
                    else:
                        self.logger.warning('Unable to create FOUNDER_AT relation to person %s (not found in Mongo)', founder)

                # P:C - Create INVESTS_AT relation (Investors)
                for investor_name, investor_type, investment_info in company.deduced.get(C.INVESTORS, []):

                    # Find info on investment type -> relation_properties
                    relation_properties = []
                    investment_round = AcureRateUtils.get_investment_round(investment_info)
                    if investment_round:
                        relation_properties.append("investment_type: '%s'" % investment_round)
                    investment_lead = AcureRateUtils.is_investment_lead(investment_info)
                    if investment_lead:  # TODO: should be label and not property
                        relation_properties.append("investment_lead: True")

                    if investor_type == 'person':
                        p = DBWrapper.get_persons({'deduced.'+P.FULL_NAME: investor_name}, single_result=True)
                        if p:
                            person = AcureRatePerson.reconstruct(p)
                            person_to_company_relations.append((person.aid, G.RELATION_LABEL_INVESTS_IN, company._aid, relation_properties))
                        else:
                            self.logger.warning('Unable to create INVESTS_IN relation to person %s (not found in Mongo)', investor_name)
                    elif investor_type == 'organization':
                        c = DBWrapper.get_companies({'deduced.'+C.NAME: investor_name}, single_result=True)
                        if c:
                            investing_company = AcureRateCompany.reconstruct(c)
                            company_to_company_relations.append((investing_company._aid, G.RELATION_LABEL_INVESTS_IN, company._aid, relation_properties))
                        else:
                            self.logger.warning('Unable to create INVESTS_IN relation to company %s (not found in Mongo)', investor_name)

                # TODO: I can combine the 2 loops below... (only difference is the LABEL of the object)

                # Go over C2C relations and create them in Neo4J
                for source_aid, relation_label, target_id, relation_properties in company_to_company_relations:
                    relations_str = ','.join(relation_properties)
                    cql_r_str = "MATCH (c1:Company),(c2:Company) " + \
                                "WHERE c1.aid = '%s' AND c2.aid = '%s' " % (source_aid, target_id) + \
                                "MERGE (c1)-[r:%s{%s}]->(c2)" % (relation_label, relations_str)
                    statement_res = session.run(cql_r_str)

                # Go over P2C relations and create them in Neo4J
                for source_aid, relation_label, target_id, relation_properties in person_to_company_relations:
                    relations_str = ','.join(relation_properties)
                    cql_r_str = "MATCH (p:Person),(c:Company) " + \
                                "WHERE p.aid = '%s' AND c.aid = '%s' " % (source_aid, target_id) + \
                                "MERGE (p)-[r:%s{%s}]->(c)" % (relation_label, relations_str)
                    statement_res = session.run(cql_r_str)

                pass

                #session.close()
        except Exception as e:
            print("Exception raised: %s" % e)
        finally:
            session.close()

        pass

    def _people_to_neo4j_nodes(self, query):

        excluded = ['related_vcs', 'investments', 'advisory_jobs', 'educations', 'accredited_companies', 'accredited_jobs', 'jobs', 'accredited_jobs_2', 'related_investors', 'unrecognized_companies']
        labeled = ['business', 'founder', 'investor', 'ceo']
        person = None

        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()

        try:
            cursor = DBWrapper.get_persons(query)
            num_people = 0
            for r in cursor:
                person = AcureRatePerson.reconstruct(r)

                num_people += 1

                properties_list = []
                label_list = [G.LABEL_PERSON]

                if P.FULL_NAME in person.deduced:
                    key_str = NeoWrapper.get_property_by_type('name', person.deduced[P.FULL_NAME].replace("'", ""), '=')
                    properties_list.append(key_str)

                if not person.aid:
                    self.logger.warning('No aid for person %s. Not migrated.' % person.deduced[P.FULL_NAME])
                    continue

                if 'user_id' in r:
                    label_list.append('User')

                if P.ACCREDITED_JOBS_2 in person.deduced:
                    jobs_names, jobs_areas, jobs_roles, jobs_seniorities = person.get_all_jobs_information()
                    key_str = NeoWrapper.get_property_by_type('job_names', jobs_names, '=')
                    properties_list.append(key_str)
                    key_str = NeoWrapper.get_property_by_type('job_areas', jobs_areas, '=')
                    properties_list.append(key_str)
                    key_str = NeoWrapper.get_property_by_type('job_roles', jobs_roles, '=')
                    properties_list.append(key_str)
                    key_str = NeoWrapper.get_property_by_type('jobs_seniorities', jobs_seniorities, '=')
                    properties_list.append(key_str)

                # Iterate over all properties
                for p in person.deduced.keys():
                    if p in labeled:
                        label_list.append(p.capitalize())
                        continue
                    if p in excluded:
                        continue
                    key_str = NeoWrapper.get_property_by_type(p, person.deduced[p], '=')
                    if key_str:
                        properties_list.append(key_str)

                # Set labels and properties for the Cypher query
                lbl_str = ":".join(label_list)
                properties_list_with_prefix = ['a.'+i for i in properties_list]
                all_properties_comma_delimited = ", ".join(properties_list_with_prefix)
                cql_str = 'MERGE (a:%s {aid: "%s"}) ' % (lbl_str, person.aid) + \
                          'SET %s' % all_properties_comma_delimited
                statement_res = session.run(cql_str)
                self.logger.info('Migrated person %s successfully! (so far, migrated %d nodes). Details: %s' %
                                 (person.deduced[P.FULL_NAME], num_people,
                                  AcureRateUtils.obj2string(statement_res.consume().counters)))

        except Exception as e:
            self.logger.error('Migration of person node %s failed. Exception raised: %s' %
                              (person.deduced[P.FULL_NAME] if person else 'none', e))
        finally:
            session.close()
        pass

    def _people_to_neo4j_relations(self, query):
        session = None
        pivot_person = None
        try:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
            session = driver.session()

            cursor = DBWrapper.get_persons(query)
            num_people = 0
            for r in cursor:
                pivot_person = AcureRatePerson.reconstruct(r)

                num_people += 1
                unique_relation_aids = set()
                if 'GoogleContacts' in pivot_person.data_sources:
                    for d in pivot_person.sources(provider_filter='GoogleContacts'):
                        if d[P.ATTRIBUTION_ID] != pivot_person.aid:  # Exclude myself. Don't want arrow point to me.
                            unique_relation_aids.add(d[P.ATTRIBUTION_ID])

                # if 'LinkedInContacts' in person.data_sources:
                #     relations.append('LinkedIn_Contact')

                # Iterate over all those contacts this person points to
                # p2p_relations = set()
                # q = {}
                # cursor = DBWrapper.get_persons(q)
                # for p in cursor:
                #     p2p_relations.add((p[P.AID], pivot_person.aid))

                # Go over aids and create a relation between two people
                for aid in unique_relation_aids:
                    r2 = DBWrapper.get_persons({'_aid': aid}, True)
                    if r2:
                        person2 = AcureRatePerson.reconstruct(r2)
                        strength = pivot_person.contact_relation_strength_with_person(person2)
                        relation_str = 'strength: %s' % strength
                        cql_r_str = "MATCH (p1:Person),(p2:Person) " + \
                                    "WHERE p1.aid = '%s' AND p2.aid = '%s' " % (person2.aid, pivot_person.aid) + \
                                    "MERGE (p1)-[r:%s{%s}]->(p2)" % (G.RELATION_LABEL_CONTACT, relation_str)
                        statement_res = session.run(cql_r_str)

                # Go over companies that the person is founder of
                for j in pivot_person.deduced.get(P.ACCREDITED_JOBS_2, []):
                    c = DBWrapper.get_companies({"deduced."+C.ALIASES: j['job_name'].lower()}, single_result=True)
                    if c:
                        company = AcureRateCompany.reconstruct(c)
                        job = AcureRateJob.reconstruct(j)
                        if job.is_founder():
                            strength = 10  # TODO: anything here?
                            relation_str = 'strength: %s' % strength
                            cql_r_str = "MATCH (p:Person),(c:Company) " + \
                                        "WHERE p.aid = '%s' AND c.aid = '%s' " % (pivot_person.aid, company._aid) + \
                                        "MERGE (p)-[r:%s{%s}]->(c)" % (G.RELATION_LABEL_FOUNDER_OF, relation_str)
                            statement_res = session.run(cql_r_str)
                            self.logger.info('Inserted %s FOUNDER_OF %s relation successfully!. Details: %s' %
                                             (pivot_person.deduced[P.FULL_NAME], company.deduced[C.NAME],
                                              AcureRateUtils.obj2string(statement_res.consume().counters)))
                    else:
                        self.logger.info('Unable to create EMPLOYEE_OF relation for person %s (company %s not found).' %
                                         (pivot_person.deduced[P.FULL_NAME], j['job_name']))

                # Go over jobs and create EMPLOYEE_OF relations
                for j in pivot_person.deduced.get(P.ACCREDITED_JOBS_2, []):
                    c = DBWrapper.get_companies({"deduced."+C.ALIASES: j['job_name'].lower()}, single_result=True)
                    if c:
                        company = AcureRateCompany.reconstruct(c)
                        job = AcureRateJob.reconstruct(j)
                        jobs_areas, jobs_roles, jobs_seniorities = job.information()
                        strength = pivot_person.working_relation_strength_with_company(job, company)
                        relation_str = 'strength: %s, jobs_areas: %s, jobs_roles: %s, jobs_seniorities: %s' %\
                                       (strength, list(jobs_areas), list(jobs_roles), list(jobs_seniorities))
                        cql_r_str = "MATCH (p:Person),(c:Company) " + \
                                    "WHERE p.aid = '%s' AND c.aid = '%s' " % (pivot_person.aid, company._aid) + \
                                    "MERGE (p)-[r:%s{%s}]->(c)" % (G.RELATION_LABEL_EMPLOYEE_OF, relation_str)
                        statement_res = session.run(cql_r_str)
                        self.logger.info('Inserted %s EMPLOYEE_OF %s relation successfully!. Details: %s' %
                                         (pivot_person.deduced[P.FULL_NAME], company.deduced[C.NAME],
                                          AcureRateUtils.obj2string(statement_res.consume().counters)))
                    else:
                        self.logger.info('Unable to create EMPLOYEE_OF relation for person %s (company %s not found).' %
                                         (pivot_person.deduced[P.FULL_NAME], j['job_name']))

                # Go over board positions and create BOARD_AT relations
                for advisory_job in pivot_person.deduced.get(P.ADVISORY_JOBS, []):
                    c = DBWrapper.get_companies({"deduced."+C.ALIASES: advisory_job['job_name'].lower()}, single_result=True)
                    if c:
                        company = AcureRateCompany.reconstruct(c)
                        strength = pivot_person.board_relation_strength_with_company(advisory_job, company)
                        relation_str = 'strength: %s' % strength
                        cql_r_str = "MATCH (p:Person),(c:Company) " + \
                                    "WHERE p.aid = '%s' AND c.aid = '%s' " % (pivot_person.aid, company._aid) + \
                                    "MERGE (p)-[r:%s{%s}]->(c)" % (G.RELATION_LABEL_ADVISOR_AT, relation_str)
                        statement_res = session.run(cql_r_str)
                    else:
                        self.logger.info('Unable to create ADVISOR_AT relation for person %s (company %s not found).' %
                                         (pivot_person.deduced[P.FULL_NAME], advisory_job['job_name']))

                # Go over educations and create STUDIED_AT relations
                for education in pivot_person.deduced.get(P.EDUCATIONS, []):
                    c = DBWrapper.get_companies({"deduced."+C.ALIASES: education[P.EDUCATION_INSTITUTE].lower()}, single_result=True)
                    if c:
                        company = AcureRateCompany.reconstruct(c)
                        rels_str = []
                        if P.EDUCATION_DEGREE in education:
                            rels_str.append("degree: '%s'" % education[P.EDUCATION_DEGREE].replace("'", ""))
                        if P.EDUCATION_YEARS in education:
                            rels_str.append("years: '%s'" % education[P.EDUCATION_YEARS].replace(",", ""))
                        relation_str = ', '.join(rels_str)
                        cql_r_str = "MATCH (p:Person),(c:Company) " + \
                                    "WHERE p.aid = '%s' AND c.aid = '%s' " % (pivot_person.aid, company._aid) + \
                                    "MERGE (p)-[r:%s{%s}]->(c)" % (G.RELATION_LABEL_STUDIED_AT, relation_str)
                        statement_res = session.run(cql_r_str)
                    else:
                        self.logger.info('Unable to create STUDIES_AT relation for person %s (company %s not found).' %
                                         (pivot_person.deduced[P.FULL_NAME], education[P.EDUCATION_INSTITUTE]))

                self.logger.info('Migrated relations of person %s successfully (done so far - %d)' % (pivot_person.deduced[P.FULL_NAME], num_people))
        except Exception as e:
            self.logger.error('Migrated relations of person %s failed. Exception raised: %s' %
                              (pivot_person.deduced[P.FULL_NAME] if pivot_person else 'none', e))
        finally:
            session.close()

        pass

    def drop_all_nodes_and_relations_in_neo4j(self):
        session = None
        try:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
            session = driver.session()

            # Query to delete all nodes and relations in database
            cql_r_str = "MATCH(n) " + \
                        "OPTIONAL MATCH (n)-[r]-() " + \
                        "DELETE n, r"
            statement_res = session.run(cql_r_str)
        except Exception as e:
            print("Exception raised: %s" % e)
        finally:
            session.close()
        pass

    def drop_person_nodes_and_relations_in_neo4j(self):
        session = None

        person_name = input('Enter person name to remove (enter to abort) >')
        if person_name == '':
            return

        try:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
            session = driver.session()

            # Create constraints:
            cql_r_str = "MATCH(n) " + \
                        "WHERE n.full_name = '%s' " % person_name + \
                        "DETACH DELETE n"
            statement_res = session.run(cql_r_str)
        except Exception as e:
            print("Exception raised: %s" % e)
        finally:
            session.close()

        self.logger.info('Removed node and related relations of %s successfully!' % person_name)
        pass

    def export_data(self):

        self.logger.info('-----------------------')
        self.logger.info('Exporting all investors')
        self.logger.info('-----------------------')

        # Get all the investors: @@@
        q = {"deduced.investor": True}
        cursor = DBWrapper.get_persons(q)
        self.logger.info('Exporting %d contacts', cursor.count())
        for r in cursor:
            person = AcureRatePerson.reconstruct(r)
            li_url = person.deduced[P.LINKEDIN_URL] if P.LINKEDIN_URL in person.deduced else "n/a"
            self.logger.info(", %s, %s", person.deduced[P.FULL_NAME], li_url)

        pass

    def enter_founder_profile(self):
        my_src = EnrichmentSource('AcureRateSpider', 'blackops')
        my_behavior = EnrichmentBehavior(providers='Dummy')

        # David Oren's profile
        my_key = {P.FIRST_NAME: 'David', P.LAST_NAME: 'Oren'}
        ed = [
            EnrichmentData('first_name', 'David', 'override'),
            EnrichmentData('last_name', 'Oren', 'override'),
            EnrichmentData('email', 'david@pirveliventures.com', 'key'),
            EnrichmentData('jobs', {'job_name': 'Vipeg LTD.', 'job_title': 'Team Lead'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'IDF', 'job_title': 'Team Lead'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'NDS', 'job_title': 'Developer'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Taldor', 'job_title': 'Consultant'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Arakamba', 'job_title': 'Developer'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'CastUP', 'job_title': 'Developer'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'HP', 'job_title': 'Developer'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'FloraFotonica', 'job_title': 'Board Member'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Gordian Surgical Ltd.', 'job_title': 'Board Member'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'SoftWheel', 'job_title': 'Board Member'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Eco Wave Power Ltd.', 'job_title': 'Director'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Pirveli Ventures', 'job_title': 'Managing Director / Partner'}, 'add')
        ]
        self.es.enrich_person(enrichment_key=my_key, enrichment_data=ed,
                              enrichment_source=my_src, enrichment_behavior=my_behavior)

        if True:
            return

        my_key = {P.FIRST_NAME: 'Dror', P.LAST_NAME: 'Garti'}
        ed = [
            EnrichmentData('first_name', 'Dror', 'override'),
            EnrichmentData('last_name', 'Garti', 'override'),
            EnrichmentData('emails', 'drorgarti@gmail.com', 'add'),
            #EnrichmentData('emails', 'dror_g@optimove.com', 'add'),
            EnrichmentData('jobs', {'job_name': 'IDF', 'job_title': 'Officer, IAF, Team Lead'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'VP R&D'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'HP', 'job_title': 'VP R&D'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'ComVaya', 'job_title': 'CoFounder, VP R&D'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Telmap', 'job_title': 'VP R&D'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Optimove', 'job_title': 'CTO'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'AcureRate', 'job_title': 'CTO & CoFounder'}, 'add')
        ]
        self.es.enrich_person(enrichment_key=my_key, enrichment_data=ed,
                              enrichment_source=my_src, enrichment_behavior=my_behavior)

    def blackops_enrichment(self):

        my_src = EnrichmentSource('AcureRateSpider', 'blackops')
        my_behavior = EnrichmentBehavior(providers='Dummy')

        ed1 = EnrichmentData(P.EMAIL, 'another.email@somemail.com', 'override')
        ed2 = EnrichmentData(P.LAST_NAME, 'Dudai-Plus', 'override')
        ed3 = EnrichmentData(P.JOBS, {'job_name': 'Mercury', 'job_title': 'R&D Group Manager'}, 'add')
        my_key = {P.FIRST_NAME: 'Sagi', P.LAST_NAME: 'Dudai'}
        my_data = [ed1, ed2, ed3]
        self.es.enrich_person(enrichment_key=my_key, enrichment_data=my_data,
                              enrichment_source=my_src, enrichment_behavior=my_behavior)

        if True:
            return

        # Insert Sagi Dudai working in Mercury (as if we found it in YaTeDo):
        # (taken from Bloomberg directory)
        my_data = [EnrichmentData('email', 'sagi.dudai@gmail.com', 'override'),
                   EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'R&D Group Manager'}, 'add')]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'sagi.dudai@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Add the fact Benny Ifhar is an investor (should be taken from AngelList)
        my_data = [
            EnrichmentData(P.EMAIL, 'bennyifhar@gmail.com', 'override'),
            EnrichmentData(P.INVESTOR, True, 'override'),
            EnrichmentData(P.INVESTOR_REASON, 'Angellist', 'override')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'bennyifhar@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Add the fact Asaf Barkan worked at HP (this can be deduced from the "personal details" text in CB
        my_data = [
            EnrichmentData(P.EMAIL, 'asaf@skyformation.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'HP', P.JOB_TITLE: 'CTO, Head of SDLC'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'asaf@skyformation.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Add the fact Ariel Cohen worked at Mercury
        my_data = [
            EnrichmentData(P.EMAIL, 'arielco00@gmail.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Mercury'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'arielco00@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Lior Prosor's email and website - lior@elevatorfund.com,  http://www.elevatorfund.com/
        my_data = [
            EnrichmentData(P.FIRST_NAME, 'Lior', 'override'),
            EnrichmentData(P.LAST_NAME, 'Prosor', 'override'),
            EnrichmentData(P.EMAILS, 'lior@elevatorfund.com', 'add'),
            EnrichmentData(P.WEBSITE, 'http://www.elevatorfund.com/', 'override'),
            EnrichmentData(P.FACEBOOK_URL, 'https://www.facebook.com/lior.prosor/', 'override'),
            EnrichmentData(P.ANGELLIST_URL, 'https://angel.co/lior-prosor', 'override')
        ]
        self.es.enrich_person(enrichment_key={P.FIRST_NAME: 'Lior', P.LAST_NAME: 'Prosor'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Vitek Tracz as investor at Telmap (via Bloomberg) and fake the needed ALIASES
        # TODO: the aliases logic should be implemented when a new company is inserted
        my_data = [
            EnrichmentData(C.NAME, 'Telmap', 'override'),
            EnrichmentData(C.INVESTORS, ('Vitek Tracz', 'person', ''), 'add'),
            EnrichmentData(C.TEAM, 'Oren Nissim', 'add'),
            EnrichmentData(C.CATEGORIES, 'Navigation', 'add'),
            EnrichmentData(C.ALIASES, 'telmap inc', 'add'),
            EnrichmentData(C.ALIASES, 'telmap inc.', 'add'),
            EnrichmentData(C.ALIASES, 'telmap ltd', 'add'),
            EnrichmentData(C.ALIASES, 'telmap ltd.', 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'Telmap'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Vitek Tracz relation to Telmap (something we can get from Bloomberg directory)
        my_data = [
            EnrichmentData(P.EMAIL, 'vitek@sciencenow.com', 'override'),
            EnrichmentData(P.FIRST_NAME, 'Vitek', 'override'),
            EnrichmentData(P.LAST_NAME, 'Tracz', 'override'),
            EnrichmentData(P.INVESTMENTS, ("Jan, 2002", "Telmap", "$80M/Seed"), 'add'),
            EnrichmentData(P.ADVISORY_JOBS, {P.JOB_TITLE: "Chairman of the Board", P.JOB_NAME: "Telmap"}, 'add'),
            EnrichmentData(P.ADVISORY_JOBS, {P.JOB_TITLE: "Group Chairman", P.JOB_NAME: "Science Navigation Group"}, 'add'),
            EnrichmentData(P.ADVISORY_JOBS, {P.JOB_TITLE: "Chairman of the Board", P.JOB_NAME: "MAPA - Mapping and Publishing Ltd."}, 'add'),
            EnrichmentData('photos', {'source': 'wikipedia', 'url': 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/33/Vitektracz.large.jpg/220px-Vitektracz.large.jpg'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'vitek@sciencenow.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Gigi Weiss's email (as will be available via Doron's contacts)
        my_data = [
            EnrichmentData(P.FIRST_NAME, 'Gigi', 'override'),
            EnrichmentData(P.LAST_NAME, 'Weiss', 'override'),
            EnrichmentData(P.EMAILS, 'gigi@iapple.com', 'add')
        ]
        self.es.enrich_person(enrichment_key={P.FIRST_NAME: 'Gigi', P.LAST_NAME: 'Weiss'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Haniel Ilouz working in Mercury (can be found in Bloomberg):
        my_data = [
            EnrichmentData(P.EMAIL, 'ilouz.haniel@gmail.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Hewlett-Packard', 'job_title': 'R&D Director, BSM Essentials'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'SiteScope R&D Manager'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'ilouz.haniel@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Boaz Chalamish working as CEO at Clarizen (as will be produced by dedup + email domain inspection):
        my_data = [
            EnrichmentData(P.EMAIL, 'boaz.cha@gmail.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Clarizen', 'job_title': 'CEO'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'boaz.cha@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Gigi Levy as investor at iMingle - a company Pini Mogilvesky founded
        # (taken from http://finder.startupnationcentral.org/c/imingle)
        my_data = [
            EnrichmentData(C.NAME, 'iMingle', 'override'),
            EnrichmentData(C.INVESTORS, ('Gigi Levy', 'person', ''), 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'iMingle'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Optimove investors:
        # (taken from Joey Low web-site)
        my_data = [
            EnrichmentData(C.NAME, 'Optimove', 'override'),
            EnrichmentData(C.INVESTORS, ('Joey Low', 'person', ''), 'add'),
            EnrichmentData(C.INVESTORS, ('Gigi Levy', 'person', ''), 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'Optimove'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert AcureRate as it's not mentioned anywhere yet...
        my_data = [
            EnrichmentData(C.NAME, 'AcureRate', 'override'),
            EnrichmentData(C.DESCRIPTION, 'AcureRate is currently in stealth mode.', 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'AcureRate'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Yaniv Bronstein working in Mercury (as if we found it in YaTeDo):
        my_data = [
            EnrichmentData(P.EMAIL, 'bronstein.yaniv@gmail.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'System Architect'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Shunra Software', 'job_title': 'VP R&D and Site Manager'}, 'add'),
            EnrichmentData('jobs', {'job_name': 'Robinhood', 'job_title': 'Founder, CEO'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'bronstein.yaniv@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Pini Mogilevsky working in Mercury (as if we found it in ...):
        my_data = [
            EnrichmentData(P.EMAIL, 'pinimog@gmail.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'Functional Architect'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'pinimog@gmail.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Sasha Gelinson working in Mercury (as if we found it in Bloomberg):
        my_data = [
            EnrichmentData(P.EMAIL, 'sasha@evolven.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Mercury', 'job_title': 'VP'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'sasha@evolven.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        #--------------------------------------------------------------------
        # Push aliases
        #--------------------------------------------------------------------
        my_data = [
            EnrichmentData(C.NAME, 'Hewlett-Packard', 'override'),
            EnrichmentData(C.ALIASES, 'hewlett-packard', 'add'),
            EnrichmentData(C.ALIASES, 'mercury / hp software', 'add'),
            EnrichmentData(C.ALIASES, 'hp software', 'add'),
            EnrichmentData(C.ALIASES, 'hp', 'add'),
            EnrichmentData(C.ALIASES, 'hp software (formerly mercury)', 'add'),
            EnrichmentData(C.ALIASES, 'hewlett-packard laboratories', 'add'),
            EnrichmentData(C.ALIASES, 'hp laboratories', 'add'),
            EnrichmentData(C.ALIASES, 'hp (software-as-a-service)', 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'Hewlett-Packard'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        my_data = [
            EnrichmentData(C.NAME, 'Mercury', 'override'),
            EnrichmentData(C.ALIASES, 'mercury', 'add'),
            EnrichmentData(C.ALIASES, 'mercury interactive', 'add'),
            EnrichmentData(C.ALIASES, 'mercury-interactive', 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'Mercury'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Push missing images
        images_info = [
            ("email", "shanizamir84@gmail.com", "acurerateweb", "http://www.optimove.com/wp-content/uploads/2015/09/TP_Shani_Z.jpg"),
            ("full_name", "Eran Sher", "acurerateweb", "https://www.idevnews.com/mailers/130523_CA/sher.png"),
            ("full_name", "Roman R", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/p/4/000/1b1/247/1dfde38.jpg"),
            ("email", "edo.zurel@hp.com", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/p/8/000/218/397/23a8404.jpg"),
            ("full_name", "Dana Messina", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/p/6/005/047/04e/199b9e1.jpg"),
#            ("full_name", "Giora Yaron", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/p/6/005/0b6/35f/0a24423.jpg"),
            ("email", "gmalka@hp.com", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/p/2/000/193/3ba/3fb76f1.jpg"),
            ("full_name", "Brad Lovering", "linkedin", "http://zdnet1.cbsistatic.com/hub/i/2014/10/04/0e175d06-4b8f-11e4-b6a0-d4ae52e95e57/37f7fca781bf4452ba75126b4ba0d2e6/lovering.jpg"),
            ("email", "cohen.gilad@gmail.com", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_200_200/p/4/000/15a/0e1/2aed6f8.jpg"),
            ("email", "vitek@sciencenow.com", "wikipedia", "https://upload.wikimedia.org/wikipedia/commons/thumb/3/33/Vitektracz.large.jpg/220px-Vitektracz.large.jpg"),
            ("email", "yaki.avimor@gmail.com", "crunchbase", "https://crunchbase-production-res.cloudinary.com/image/upload/c_pad,h_140,w_140/v1404625766/vkbaj515hvrtjps3aoa5.jpg"),
            ("email", "sh@shaysapir.com", "linkedin", "https://media.licdn.com/media/p/2/005/057/23a/319567d.jpg"),
            ("email", "ben@sheats.net", "linkedin", "https://media.licdn.com/mpr/mpr/shrinknp_200_200/AAEAAQAAAAAAAALHAAAAJGNhMWFmNzBlLTcyYTctNGM4Zi04OWY5LWJhMzQ0ZmQxYzkzYw.jpg")
        ]
        for k, v, s, u in images_info:
            my_data = [
                EnrichmentData(k, v, 'override'),
                EnrichmentData('photos', {'source': s, 'url': u}, 'add')
            ]
            self.es.enrich_person(enrichment_key={k: v},
                                  enrichment_data=my_data, enrichment_source=my_src,
                                  enrichment_behavior=my_behavior)

        #--------------------------------------------------------------------
        # These are all inserts we are doing AS IF our spider could find it
        #--------------------------------------------------------------------

        my_data = [
            EnrichmentData(P.FULL_NAME, 'Ran Oz', 'override'),
            EnrichmentData('emails', "ran.oz@wochit.com", 'add')
        ]
        self.es.enrich_person(enrichment_key={P.FULL_NAME: 'Ran Oz'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        my_data = [
            EnrichmentData(C.NAME, 'NICE Systems', 'override'),
            EnrichmentData(C.LOGOS, {"source": "crunchbase", "url": "https://crunchbase-production-res.cloudinary.com/image/upload/c_pad,h_140,w_140/v1397182816/060043031439e6695476599908f326d1.gif"}, 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'NICE Systems'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        my_data = [
            EnrichmentData(C.NAME, 'VMware', 'override'),
            EnrichmentData(C.LOGOS, {"source": "crunchbase", "url": "https://crunchbase-production-res.cloudinary.com/image/upload/c_pad,h_140,w_140/v1446263489/nueqdgo0lfv2arzze9qe.png"}, 'add')
        ]
        self.es.enrich_company(enrichment_key={C.NAME: 'VMware'},
                               enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        # Insert Ori Soen working in Mercury (as if we found it in Bloomberg):
        # (taken from Bloomberg directory)
        my_data = [
            EnrichmentData(P.EMAIL, 'oris@musestorm.com', 'override'),
            EnrichmentData('jobs', {'job_name': 'Mercury'}, 'add')
        ]
        self.es.enrich_person(enrichment_key={P.EMAIL: 'oris@musestorm.com'},
                              enrichment_data=my_data, enrichment_source=my_src, enrichment_behavior=my_behavior)

        print("Done inserting spider information!\n")

        pass

    def remove_provider(self):
        input_var = input("Enter name of provider to remove from all records >>>")
        if input_var.strip() == "":
            return

        query = {}  #  {"deduced.email": "drorgarti@gmail.com"}
        field_name = "data_sources." + input_var
        DBWrapper.remove_fields(query, field_name)
        pass

    def _dedup_manually_2(self, query):
        cursor = DBWrapper.get_persons(query)
        if cursor.count() < 2:
            print("No duplicates found (%d). No de-duplications to do" % cursor.count())
            return
        else:
            print("Number of records to look for duplications - %d" % cursor.count())

        persons = []
        for r in cursor:
            p = AcureRatePerson().reconstruct(r)
            persons.append(p)
            emails_str = ', '.join(p.deduced.get('emails', []))
            jobs_str = ', '.join(p.get_all_jobs_information()[0])
            print('-- %d) EMAILS: %s:  JOBS: %s' % (len(persons), emails_str, jobs_str))

        input_var = input('Dedup? (e.g. 1,3,4) >>>')
        if input_var.count(',') > 0:
            try:
                # dedup_indices = input_var.split(",")
                dedup_indices = [int(x) - 1 for x in input_var.split(",")]
                # Merge all duplicates into a the first person
                changed = False
                deduped_person = persons[dedup_indices[0]]
                for i in dedup_indices[1:]:
                    changed |= deduped_person.merge_person(persons[i])
                # Redigest - to have the latest as deduced from all sources
                if changed:
                    deduped_person.digest()
                    deduped_person.dedup_update = datetime.datetime.now()
                    # Replace in DB
                    DBWrapper.replace_person(deduped_person)
                    # Delete the deduped instances
                    for i in dedup_indices[1:]:
                        DBWrapper.delete_person(persons[i])
                    print('Succesfully deduped %d items into a single one.' % len(dedup_indices))
            except Exception as e:
                print('Failed to dedup. Error: ', e)
        else:
            print('No valid entries selected or not more than one item selected. No dedup action taken.')

        pass

    def _dedup_manually(self, query, key_property):
        people_dict = {}
        cursor = DBWrapper.get_persons(query)
        print("Number of records to look for duplications - %d" % cursor.count())
        # Group all contacts by the key requested
        for r in cursor:
            p = AcureRatePerson().reconstruct(r)
            key_str = p.deduced[key_property]
            if key_str in people_dict:
                people_dict[key_str].append(p)
            else:
                people_dict[key_str] = [p]
        num_duplicates = 0
        for name, persons in people_dict.items():
            if len(persons) > 1:
                num_duplicates += 1
        print("Number of different duplicate entries found - %d" % num_duplicates)

        # Go over groups, treat only those who have more than one contact
        for key, persons in people_dict.items():
            if len(persons) > 1:
                print('Key: ', key)
                c = 0
                for p in persons:
                    c += 1
                    emails_str = ', '.join(p.deduced.get('emails', []))
                    jobs = p.deduced.get('jobs', None)
                    if jobs is not None:
                        jobs_str = ', '.join([j['job_name'] for j in jobs if 'job_name' in j])
                    else:
                        jobs_str = '<none>'
                    print('-- %d) EMAILS: %s:  JOBS: %s' % (c, emails_str, jobs_str))
                input_var = input('Dedup? (e.g. 1,3,4) >>>')
                if input_var.count(',') > 0:
                    try:
                        #dedup_indices = input_var.split(",")
                        dedup_indices = [int(x)-1 for x in input_var.split(",")]
                        # Merge all duplicates into a the first person
                        changed = False
                        deduped_person = persons[dedup_indices[0]]
                        for i in dedup_indices[1:]:
                            changed |= deduped_person.merge_person(persons[i])
                        # Redigest - to have the latest as deduced from all sources
                        if changed:
                            deduped_person.digest()
                            deduped_person.dedup_update = datetime.datetime.now()
                            # Replace in DB
                            DBWrapper.replace_person(deduped_person)
                            # Delete the deduped instances
                            for i in dedup_indices[1:]:
                                DBWrapper.delete_person(persons[i])
                            print('Succesfully deduped %d items into a single one.' % len(dedup_indices))
                    except Exception as e:
                        print('Failed to dedup. Error: ', e)
                else:
                    print('No valid entries selected or not more than one item selected. No dedup action taken.')
        pass

    def experimental(self):

        # Try translating something
        AcureRateUtils.translate_name('Doron Herzlich')

        from store.store import Store

        source = '578f49f348d72719070ea206'  # Dror
        #target = '5880ffe0bc65103e98014d26'  # CircleBack
        target2 = '58810096bc65103e98014d27'  # Manoj Ramnani
        target3 = '58810174bc65103e98014d28'  # Alan Masarek
        target4 = '57bc9028e61f1193742e882f'  # Moty Rokach
        target5 = '578b58e648d72719070d67c3'  # Omri Kedem
        target6 = '578b58e848d72719070d68c6'  # Irit Barzily
        target7 = '578d200548d72719070e163a'  # Viber
        #paths = Store.get_paths_to_company(source, target)
        #paths = Store.get_paths_to_person(source, target6)

        company = Store.get_company_by_aid(target7)
        relations = company.get_relations(filter='EMPLOYEE_OF')
        return

        # @@@
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()

        # Create connection between Sagi and myself
        if True:
            from_str = 'Dror Garti'
            to_str = 'Sagi Dudai'
            relations_str = "{jobs: 'Mercury, Telmap', years: '2005, 2010'}"
            cql_str = "MATCH (p1:Person {name:'%s'}), (p2:Person {name:'%s'}) " % (from_str, to_str) + \
                      "MERGE (p1)-[r:%s%s]->(p2) " % (G.RELATION_LABEL_WORKED_WITH, relations_str) + \
                      "RETURN r"
            statement_res = session.run(cql_str)

        # Create a clique node and get it's ID
        cql_str = "CREATE (c:Clique) " +\
                  "RETURN c"
        statement_res = session.run(cql_str)
        for clique_node in statement_res:
            the_id = clique_node['c'].id

        # Connect a list of people to the new node
        for node_name in ['Dror Garti', 'Adi Garti', 'Sharon Garti']:
            params = {"name_param": node_name}
            relations_str = ''  # TODO: anything here? Maybe the family probability ?
            cql_str = "MATCH (p:Person), (c:Clique) " +\
                      "WHERE p.name = {name_param} AND ID(c) = %s " % the_id + \
                      "MERGE (p)-[r:%s]->(c)" % G.RELATION_LABEL_IN_CLIQUE + \
                      "RETURN r"
            statement_res = session.run(cql_str, params)
            for new_relationships in statement_res:
                pass

        session.close()
        pass

    @staticmethod
    def results_to_string_list(res):
        results = []
        for r in res['hits']['hits']:
            # res['hits']['hits'][0]['_source']['hair_color']
            name = r['_source'].get('name', 'N/A')
            hair_color = r['_source'].get('hair_color', 'N/A')
            eye_color = r['_source'].get('eye_color  ', 'N/A')
            height = r['_source'].get('height', 'N/A')
            gender = r['_source'].get('gender', 'N/A')
            str = '%s (%s, height: %s, eyes: %s hair: %s)' % (name, gender, height, eye_color, hair_color)
            results.append(str)
        return results


    @staticmethod
    def perform_request_old(url, headers):
        try:
            res = requests.get(url, headers=headers)
            rc = res.status_code
            txt = res.content.decode("utf-8")
        except Exception as e:
            txt = '<%s>' % e
            rc = 901
        return rc, txt

    @staticmethod
    def perform_request(url, opener, data=None, with_ip=True, should_delay=True):

        if should_delay:
            delay = random.uniform(0.05, 0.15)
            print('Going to sleep: %s secs' % delay)
            time.sleep(delay)

        ip = None

        try:
            if data:
                response = opener.open(url, data)
            else:
                response = opener.open(url)
            content = response.read()
            txt = content.decode("utf-8")
            rc = response.status
            if with_ip:
                for (k, v) in response.headers._headers:
                    if k == 'X-Process':
                        ip = v
                        break
        except http.client.IncompleteRead as e:
            txt = '<%s>' % e
            rc = 902
        except urllib.error.HTTPError as e:
            txt = '<%s>' % e
            rc = e.code
        except urllib.error.URLError as e:
            txt = '<%s>' % e
            rc = 901
        except OSError as e:
            txt = '<%s>' % e
            rc = 900
        except Exception as e:
            txt = '<%s>' % e
            rc = 901

        return rc, txt, ip

    @staticmethod
    def comapare_al_with_cb():
        al_companies_file_path = r'C:\temp\angellist_companies.csv'
        number_companies_found_in_cb = 0
        number_companies_checked = 0
        start_after = 1070
        delay_between_calls = 0.2  # 2.0

        import csv
        with open(al_companies_file_path, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';', quotechar="'")
            for row in spamreader:
                if 'Unable to read url' in row[0]:
                    continue
                # Remove whitespace, dbl-spaces, newlines
                company_name = row[1].strip()
                company_name = company_name.replace('  ', ' ')
                company_name = company_name.replace('\n', ' ')

                #print('Searching for %s... (%s)' % (company_name, number_companies_checked))

                if company_name.count(' ') > 5:
                    continue

                number_companies_checked += 1
                if number_companies_checked < start_after:
                    continue

                time.sleep(delay_between_calls)

                print('Searching for %s...' % company_name)

                url = 'https://api.crunchbase.com/v3.1/organizations/%s?user_key=19c22a44a27dbc6cafda3e8cc14f6618' % company_name.lower()
                url = url.replace(" ", "%20")

                # Time it
                start = time.time()
                res = requests.get(url)
                end = time.time()
                delta = end - start
                print('[req time: %s]' % delta)

                if res.status_code == 404:
                    continue
                if res.status_code != 200:
                    print('ERROR: %s: %s' % (company_name, res.status_code))
                    continue

                result_json = res.json()
                if 'data' in result_json and 'properties' in result_json['data'] and 'name' in result_json['data']['properties']:
                    res_name = result_json['data']['properties']['name']
                    if res_name.lower() != company_name.lower():
                        continue

                    number_companies_found_in_cb += 1
                    print('---> FOUND! <--- (so far, found %s)' % number_companies_found_in_cb)

        print('Finished. Found %s matches.' % number_companies_checked)
        pass

    @staticmethod
    def get_people_from_cb_es():

        from engagement.crunchbasescraper_engager import CrunchBaseScraperEngager
        from string import ascii_lowercase
        import urllib.request
        import json

        # from algoliasearch import algoliasearch
        # client = algoliasearch.Client(CrunchBaseScraperEngager.APP_ID, CrunchBaseScraperEngager.THE_KEY)
        # index = client.init_index('main_production')
        # query = {
        #     "prefix": {
        #         "name": "bca"
        #     }
        # }
        #res = index.search(query, {"attributesToRetrieve": "name, organization_name", "hitsPerPage": 20})
        #res = index.search("bbm", {"attributesToRetrieve": "name, organization_name", "hitsPerPage": 20})

        headers = {'contentType': 'application/json; charset=utf-8',
                   'X-Algolia-API-Key': CrunchBaseScraperEngager.THE_KEY,
                   'X-Algolia-Application-Id': CrunchBaseScraperEngager.APP_ID}

        proxy_url = 'http://lum-customer-acurerate-zone-residential:1b05274a7daf@zproxy.luminati.io:22225'
        user_agent1 = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36'
        user_agents = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
        ]
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({'https': proxy_url})
        )
        opener.addheaders = [('User-Agent', user_agent1),
                             ('contentType', 'application/json; charset=utf-8'),
                             ('X-Algolia-API-Key', CrunchBaseScraperEngager.THE_KEY),
                             ('X-Algolia-Application-Id', CrunchBaseScraperEngager.APP_ID)]

        the_delay = 30  # 30 seconds
        accumulated_delay = the_delay
        algolia_search_url = 'https://a0ef2haqr0-3.algolia.io/1/indexes/main_production/query'
        output_file_name = r'C:\temp\crunchbase_entities_2.csv'

        # Get last line of file
        companies_file = open(output_file_name, 'r', encoding="utf-8")
        last_line = AcureRateUtils.tail(companies_file, 1, offset=None)
        start_ngram = last_line[0][0][0:3].lower()

        print('Starting from [%s]' % start_ngram)

        # start_ngram = 'fuu'
        special_test = None

        # restrictSearchableAttributes = organization name
        # attributesToRetrieve=name
        # attributesToHighlight
        # collection_ids=organization.companies,people
        query_params = [
            'query=%s',
            #'page=%s',
            #'exactOnSingleWordQuery=attribute',
            'getRankingInfo=true',
            'attributesToRetrieve=name,organization_name,type,permalink,organization_permalink',
            #'facetFilters=organization.companies'
            'facetFilters='
        ]
        query_url = '&'.join(query_params)

        # Open file
        #companies_file = open(r'C:\temp\crunchbase_entities.csv', 'a', encoding="utf-8")
        companies_file = open(output_file_name, 'a', encoding="utf-8")
        all_letters = ''.join(ascii_lowercase)
        for l1 in ascii_lowercase[all_letters.find(start_ngram[0]):]:
            for l2 in ascii_lowercase[all_letters.find(start_ngram[1]):]:
                for l3 in ascii_lowercase[all_letters.find(start_ngram[2]):]:
                    page_number = 1
                    if special_test:
                        query_term = special_test
                    else:
                        query_term = '%s%s%s' % (l1, l2, l3)
                    query_term = query_term.replace('&', '%26')
                    query = query_url % query_term
                    while True:  # Iterate over pages of same n-gram

                        # Prepare query and payload for request
                        if page_number > 1:
                            query += '&page=%s' % page_number
                        payload = {"params": query,
                                   "apiKey": CrunchBaseScraperEngager.THE_KEY,
                                   "appID": CrunchBaseScraperEngager.APP_ID}
                        json_payload = json.dumps(payload).encode('utf8')

                        # Issure the request
                        rc, response, ip = SatoriMain.perform_request(algolia_search_url, opener, json_payload, with_ip=False, should_delay=False)

                        # Handle bad status codes:
                        if rc == 429:
                            print("RC: %s. Exceeded requests quota. Error: %s." % (rc, response))
                            time.sleep(accumulated_delay)
                            accumulated_delay = accumulated_delay * 2
                            break
                        if rc != 200:
                            print("RC: %s. Error: %s." % (rc, response))
                            continue

                        if accumulated_delay != the_delay:
                            print("*** Before accumulated delay is reset, its value was: %s" % accumulated_delay)
                            accumulated_delay = the_delay

                        # Convert json to dict object
                        results = json.loads(response)

                        if results['nbHits'] == 0:
                            print("CrunchBaseScraper: No hits returned when searching for %s." % query_term)
                            break

                        if page_number == 1:
                            print("-- Num results for search %s is %s" % (query_term, results['nbHits']))

                        page_number += 1

                        # Did we exhaust the number of pages?
                        if page_number < results['nbPages']:
                            break_page = False
                            written_to_file = set()
                            # Iterate over results
                            for i in results['hits']:
                                if '_rankingInfo' in i and i['_rankingInfo']['nbTypos'] > 0:
                                    break_page = True
                                    break
                                if 'name' not in i or 'permalink' not in i or 'type' not in i:  # or 'organization_name' not in i:
                                    continue
                                hresult = i['_highlightResult']
                                if 'name' in hresult and hresult['name']['value'].find('<em>') > 0:
                                    break_page = True
                                    break
                                try:
                                    if 'type' in i and i['type'].lower() in ['person', 'organization']:
                                        name = i['name'].lower()
                                        if name.find(query_term) == 0 and name not in written_to_file:
                                            companies_file.write('%s, %s, %s\n' % (i['name'], i['type'], i['permalink']))
                                            now_str = AcureRateUtils.get_now_as_str()
                                            print('%s: Page %s: %s, %s, %s' % (now_str, page_number, i['name'], i['type'], i['permalink']))
                                            written_to_file.add(name)
                                except Exception as e:
                                    print('  >>> Got exception %s on %s' % (e, i['name']))
                            if break_page:
                                break
                        else:
                            print('--------- Completed page %s ----------' % page_number)
                            companies_file.write('>>> Over 50 pages for query term: [%s] <<<\n' % query_term)
                            break
                    if special_test:
                        print('Should stop')
                        pass  # Place to break before flushing results to file
                    companies_file.flush()
                start_ngram = start_ngram[0:2] + 'a'
            start_ngram = start_ngram[0] + 'a' + start_ngram[2]
        pass


    @staticmethod
    def get_info_from_al():

        from bs4 import BeautifulSoup
        from string import ascii_lowercase

        import urllib.request
        import sys

        #cj = CookieJar()
        #opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        #proxy_url = 'http://127.0.0.1:24001'
        #proxy_url = 'http://127.0.0.1:22225'
        #proxy_url = 'http://lum-customer-hl_7303b046-zone-static:difwir8myhu1@zproxy.luminati.io:22225'
        #proxy_url = 'http://lum-customer-acurerate-zone-static:difwir8myhu1@zproxy.luminati.io:22225'
        proxy_url = 'http://lum-customer-acurerate-zone-residential:1b05274a7daf@zproxy.luminati.io:22225'

        # See list of agenst here:
        # --> http://www.useragentstring.com/pages/useragentstring.php
        user_agent1 = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36'
        user_agents = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
        ]

        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({'https': proxy_url})
        )
        opener.addheaders = [('User-Agent', user_agent1)]

        # Read the company names:
        # Example: https://angel.co/directory/companies/f-51'
        base_url = r'https://angel.co/directory/companies/%s-%s-%s'
        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36'})

        output_file_name = ''
        go_go_go = True

        problem = 0
        user_agent_index = 0
        rotate_user_agent_count = 0
        requests_count = 0

        captch_bypass_attempts = 3
        delay_after_captch = 600  # 10 minutes
        main_page_range = range(1, 125)
        sub_page_range = range(1, 125)
        custom_main_page_start = 1
        custom_sub_page_start = 1
        #letters_range = ['p', 'f', 'z', 'b', 'c', 'a', 'd']
        letters_range = ['n', 'z']
        #letters_range = ascii_lowercase

        # Read last line of file
        # Todo...
        # companies_file = open(output_file_name, 'r', encoding="utf-8")
        # last_line = AcureRateUtils.tail(companies_file, 1, offset=None)

        rc = 0
        for c in letters_range:
            # Open file
            companies_file = open(r'C:\temp\%s_angellist_companies.csv' % c.upper(), 'a', encoding="utf-8")
            for i in main_page_range:
                if custom_main_page_start and i < custom_main_page_start:
                    continue
                custom_main_page_start = None
                if rc == 404:
                    break
                for j in sub_page_range:
                    if custom_sub_page_start and j < custom_sub_page_start:
                        continue
                    custom_sub_page_start = None
                    if rc == 404:
                        if j > 1:
                            rc = 0
                        else:
                            pass
                        break
                    url = base_url % (c, i, j)
                    # Issue out the request until we succeed or give-up:
                    local_delay = delay_after_captch
                    for attempt in range(1, captch_bypass_attempts):
                        ip = 'unknown'
                        if False:
                            rc, txt = SatoriMain.perform_request_old(url, headers)
                        else:
                            start = time.time()
                            rc, txt, ip = SatoriMain.perform_request(url, opener, with_ip=True, should_delay=False)
                            end = time.time()
                            delta = end-start
                            print('[req time: %s]' % delta)

                        if rc >= 900:
                            print('Something really fishy is happening... Retrying.')
                            continue

                        requests_count += 1

                        if rc == 302 or txt.find('some unusual activity') != -1:
                            random_session_id = int(random.uniform(60000, 99999))
                            proxy_url = 'http://lum-customer-acurerate-zone-residential-session-rand%s:1b05274a7daf@zproxy.luminati.io:22225' % random_session_id
                            opener = urllib.request.build_opener(
                                urllib.request.ProxyHandler({'https': proxy_url})
                            )
                            opener.addheaders = [('User-Agent', user_agent1)]
                            requests_count = 0
                            now_str = AcureRateUtils.get_now_as_str()
                            print('%s: Got CAPTCHA! Ran %s requests. Reseting the session :)' % (now_str, requests_count))
                            continue

                        # Check if we hit a CAPTCHA
                        if rc == 302 or txt.find('some unusual activity') != -1:
                            now_str = AcureRateUtils.get_now_as_str()
                            print('%s: Got CAPTCHA! Ran %s requests. Going to sleep for %s seconds... Attempt #%s' % (now_str, requests_count, local_delay, attempt))
                            companies_file.write('Unable to read url %s (got CAPTCHA). Attempt #%s\n' % (url, attempt))
                            requests_count = 0
                            time.sleep(local_delay)
                            local_delay = local_delay * 2
                            continue
                        else:
                            break

                    # Check if we hit a problem:
                    if rc != 200:
                        print("Error %s: %s" % (rc, txt))
                        continue

                    # Rotate the user-agent
                    rotate_user_agent_count = (rotate_user_agent_count + 1) % 3
                    if rotate_user_agent_count == 0:
                        user_agent_index = (user_agent_index + 1) % len(user_agents)
                        # Set the new user-agent
                        opener.addheaders = [('User-Agent', user_agents[user_agent_index])]

                    # Parse page and write names
                    soup = BeautifulSoup(txt, 'html.parser')

                    now_str = AcureRateUtils.get_now_as_str()
                    print('%s: Processing now (req %s): %s-%s-%s' % (now_str, requests_count, c, i, j))
                    # Get company names
                    try:
                        elems = soup.findAll("div", {"class": "s-grid-colSm12"})
                        if elems:
                            for elem in elems:
                                name = elem.text.strip()
                                link = elem.find("a").get('href', None)
                                if ';' in name:
                                    name = "'" + name + "'"
                                # Write to file:
                                line = '%s;%s;%s;%s\n' % (ip, name, link, url)
                                line_fixed = ''.join([i if ord(i) < 128 else ' ' for i in line])
                                companies_file.write(line_fixed)
                    except Exception as e:
                        print('Exception raised while parsing elements in page: %s' % e)
                        companies_file.write('Exception raised while parsing elements in page: %s\n' % url)

                    # Flush all data collected so far
                    companies_file.flush()
            companies_file.close()

        if rc != 200:
            print('=====\nBROKE at %s (rc=%s)\n=====\n' % (url, rc))

        pass


    @staticmethod
    def get_people_from_f6s():
        f6s_engager = F6SEngager()
        f6s_engager.scrape_all_via_search()
        pass



if __name__ == '__main__':

    # date_formats = ['1997-2010', '2010 - 1990', '2002 -Present', 'Present', '2010']
    # for dr in date_formats:
    #     s, e, c = AcureRateUtils.parse_date_range(dr)

    # Elastic Search Tests...
    #SatoriMain.autocomplete()
    #SatoriMain.test_elastic_search()

    # AngelList Scraping...
    #SatoriMain.get_info_from_al()

    # F6s Scraping...
    #SatoriMain.get_people_from_f6s()

    # Read companies from file and compare them with CrunchBase
    #SatoriMain.comapare_al_with_cb()

    # Extract people from CB
    #SatoriMain.get_people_from_cb_es()

    # Satori Main
    main = SatoriMain()
    main.start()

