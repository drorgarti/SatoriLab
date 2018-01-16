import codecs
import sys
import traceback
import datetime
import logging
import requests_cache
import time
import json
import random
from SatoriConfig import GeneralConfig

from random import randint
from time import sleep

#from kombu.serialization import register
from utils.myjson import my_dumps, my_loads

#from nameko.standalone.rpc import ClusterRpcProxy
#from nameko.rpc import rpc, RpcProxy

from enrichment.enrichment_service_config import EnrichmentServiceConfig

from engagement.engagement_exception import EngagementException

from engagement.test_engager import TestEngager
from engagement.system_engager import SystemEngager
from engagement.fullcontact_engager import FullContactEngager
from engagement.clearbit_engager import ClearbitEngager
from engagement.pipl_engager import PiplEngager
from engagement.crunchbase_engager import CrunchBaseEngager
from engagement.crunchbasebot_engager import CrunchBaseBotEngager
from engagement.crunchbasescraper_engager import CrunchBaseScraperEngager
from engagement.bloombergscraper_engager import BloombergScraperEngager
from engagement.angellistscraper_engager import AngelListScraperEngager
from engagement.opencnam_engager import OpenCnamEngager
from engagement.startupnationcentral_engager import StartupNationCentralEngager
from engagement.circleback_engager import CircleBackEngager
from engagement.twitter_engager import TwitterEngager
from engagement.whitepages_engager import WhitePagesEngager

from engagement.engagement_result import EngagementResult
from engagement.engager_launcher import EngagerLauncher
from engagement.engager_launcher import EngagementManager

from utils.acurerate_utils import AcureRateUtils
from entities.acurerate_entity import AcureRateEntity
from entities.acurerate_person import AcureRatePerson
from entities.acurerate_company import AcureRateCompany
from entities.acurerate_attributes import P, C

from store.db_wrapper import DBWrapper
from store.store import Store

from utils.queue_wrapper import QueueWrapper


class EnrichmentException(Exception):

    ALL_OK = 200
    BAD_REQUEST = 400
    CONTACT_NOT_FOUND = 404
    MULTIPLE_CONTACTS = 404
    FATAL_ERROR = 500

    def __init__(self, message, code=None, fatal=False):
        self.message = message
        self.code = code if code else EnrichmentException.ALL_OK
        self.fatal = fatal

    def __str__(self):
        return repr('%s (%s). (fatal=%s)' % (self.message, self.code, self.fatal))

    def is_fatal(self):
        return self.fatal


class EnrichmentSource:

    def __init__(self, source_type, source_key):
        self._source_type = source_type
        self._source_key = source_key

    def __str__(self):
        the_str = 'enrichment source: %s::%s' % (self._source_type, self._source_key)
        return the_str

    def __repr__(self):
        the_str = 'enrichment source: %s::%s' % (self._source_type, self._source_key)
        return the_str

    @property
    def source_type(self):
        return self._source_type

    @property
    def source_key(self):
        return self._source_key


class EnrichmentData:

    def __init__(self, attr, data, policy='add'):
        self.attr = attr
        self.data = data
        self.policy = policy

    def __repr__(self):
        msg = AcureRateUtils.obj2string(self)
        return msg

    def __str__(self):
        msg = AcureRateUtils.obj2string(self)
        return msg


class EnrichmentBehavior:
    """ Determines the enrichment behavior

    - providers - single or list of providers to use in enrichment. If 'None', no providers will be used
    - all_providers - if True, all should be used. Cannot be used if providers list was specified
    - force - tells providers to override, even if already enriched by it
    - last_update_time - if date is > last_update_time, enrichment will happen
    - enrich_multiple - if True, allows to enrich multiple objects that fit key
    - digest - determines if enrichment will run digest after enrichment
    - auto-dedup - should the system attempt dedup after enrichment
    - mongo_query - if enrichment requires complex $and/$or/$regex - tell system not to attempt fixing query
    - create_new - if query does not return any object, create a new one, ernrich and save.
    - force_save - even if no changes were detected after enrichment/digest, a save to Store will be done (mainly for debug/testing purposes)
    - webhook - a url for announcing when enrichment is completed. A post request will be performed with timestamp and id
    """

    def __init__(self, providers=None, all_providers=False, force=False, last_update_time=None, enrich_multiple=False,
                 digest=True, auto_dedup=True, mongo_query=False, create_new=False, force_save=False, webhook=None):
        if providers and len(providers) > 0 and all_providers:
            raise EnrichmentException("Conflicting 'providers' and 'all_providers' policy.", EnrichmentException.BAD_REQUEST)
        if providers and isinstance(providers, str):
            providers = [providers]
        elif providers and not isinstance(providers, list):
            raise EnrichmentException("'providers' must be a string or list of strings.", EnrichmentException.BAD_REQUEST)
        # TODO: check that list includes only relevant providers...
        self._providers = providers
        self._all_providers = all_providers
        self._force = force
        self._last_update_time = last_update_time
        self._enrich_multiple = enrich_multiple
        self._digest = digest
        self._auto_dedup = auto_dedup
        self._mongo_query = mongo_query
        self._create_new = create_new
        self._force_save = force_save
        self._webhook = webhook

    def __repr__(self):
        msg = AcureRateUtils.obj2string(self)
        return msg

    def __str__(self):
        msg = AcureRateUtils.obj2string(self)
        return msg

    def from_dictionary(self, attrs):
        if 'providers' in attrs:
            self._providers = attrs['providers']
        if 'all_providers' in attrs:
            self._all_providers = attrs['all_providers']
        if 'force' in attrs:
            self._force = attrs['force']
        if 'last_update_time' in attrs:
            self._last_update_time = attrs['last_update_time']
        if 'enrich_multiple' in attrs:
            self._enrich_multiple = attrs['enrich_multiple']
        if 'digest' in attrs:
            self._digest = attrs['digest']
        if 'auto_dedup' in attrs:
            self._auto_dedup = attrs['auto_dedup']
        if 'mongo_query' in attrs:
            self._mongo_query = attrs['mongo_query']
        if 'create_new' in attrs:
            self._create_new = attrs['create_new']
        if 'force_save' in attrs:
            self._force_save = attrs['force_save']
        if 'webhook' in attrs:
            self._webhook = attrs['webhook']
        return self

    @property
    def providers(self):
        return self._providers

    @property
    def all_providers(self):
        return self._all_providers

    @property
    def force(self):
        return self._force

    @property
    def last_update_time(self):
        return self._last_update_time

    @property
    def enrich_multiple(self):
        return self._enrich_multiple

    @property
    def digest(self):
        return self._digest

    @property
    def auto_dedup(self):
        return self._auto_dedup

    @property
    def mongo_query(self):
        return self._mongo_query

    @property
    def create_new(self):
        return self._create_new

    @property
    def force_save(self):
        return self._force_save

    @property
    def webhook(self):
        return self._webhook


class EnrichmentService:

    myself = None
    found = 0

    @staticmethod
    def singleton():
        if EnrichmentService.myself is None:
            cache_file = '%s\%s' % (GeneralConfig.CACHE_FOLDER, GeneralConfig.REQUESTS_CACHE_DB)
            EnrichmentService.myself = EnrichmentService(cache_db=cache_file)
            #EnrichmentService.myself = EnrichmentService()
        return EnrichmentService.myself

    def __init__(self, cache_db=None, logger=None):

        if cache_db:
            # TODO: use ignored_parameters="key"
            # TODO: read from providers their key name and add dynamically - don't miss a key...
            requests_cache.install_cache(cache_db,
                                         ignored_parameters=['key', 'apiKey', 'user_key'],
                                         allowable_methods=('GET', 'POST'))

        # Set up a new logger or append to existing one
        self._setup_logger(logger)

        # Log the python interpreter version
        self.logger.info('Initializing EnrichmentService (python version: %s.%s.%s, cache db: %s)',
                         sys.version_info[0], sys.version_info[1], sys.version_info[2], str(cache_db))

        # Set up the store and connect
        Store.connect()

        # Set up providers
        self.registered_providers = {}
        self.registered_providers["FullContact"] = FullContactEngager()
        self.registered_providers["Clearbit"] = ClearbitEngager()
        self.registered_providers["Pipl"] = PiplEngager()
        self.registered_providers["System"] = SystemEngager()
        self.registered_providers["Test"] = TestEngager()
        self.registered_providers["CrunchBase"] = CrunchBaseEngager()
        self.registered_providers["CrunchBaseBot"] = CrunchBaseBotEngager()
        self.registered_providers["CrunchBaseScraper"] = CrunchBaseScraperEngager()
        self.registered_providers["BloombergScraper"] = BloombergScraperEngager()
        self.registered_providers["AngelListScraper"] = AngelListScraperEngager()
        self.registered_providers["OpenCnam"] = OpenCnamEngager()
        # self.registered_providers["ZoomInfo"] = ZoomInfoEngager()
        self.registered_providers["StartupNationCentral"] = StartupNationCentralEngager()
        self.registered_providers["CircleBack"] = CircleBackEngager()
        self.registered_providers["Twitter"] = TwitterEngager()
        self.registered_providers["WhitePages"] = WhitePagesEngager()

        # Create providers map
        self._providers_symbols_map = {}
        for provider_name, provider in self.registered_providers.items():
            self._providers_symbols_map[provider.get_short_symbol()] = provider_name

        # Setup queues
        self.fc_companies_queue = QueueWrapper(DBWrapper.db(), "fc_companies", consumer_id="consumer-1", timeout=300, max_attempts=3)
        self.fc_people_queue = QueueWrapper(DBWrapper.db(), "fc_people", consumer_id="consumer-1", timeout=300, max_attempts=3)
        #queue.put({"name": "appilog", "requestor": "Israel David"})
        #queue.put({"name": "VolcanicData", "requestor": "Uzy Hadad"})
        #d1 = queue.next()
        #d2 = queue.next()

        pass

    def _setup_logger(self, logger):
        #self.logger = logger or logging.getLogger(__name__)
        self.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        self.logger.setLevel(logging.DEBUG)

        # Create console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a file handler
        file_path = '%s\%s' % (GeneralConfig.LOGS_FOLDER, EnrichmentServiceConfig.LOGFILE_NAME)
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
        self.logger.info('Enrichment Service started')
        self.logger.info('-=' * 50)

    def providers_symbols_map(self):
        return self._providers_symbols_map

    def register_engager(self, engager):
        # TODO: implement dynamic engager registration
        pass

    def get_providers(self):
        return list(self.registered_providers.keys())

    def get_provider_info(self, provider_name):
        if provider_name not in self.registered_providers:
            return None
        provider = self.registered_providers[provider_name]
        return provider.get_properties()

    def enqueue_people(self, people_list):
        self.fc_people_queue.put_multiple(people_list)

    def enqueue_companies(self, companies_list):
        if companies_list is None or len(companies_list) == 0:
            return
        self.fc_companies_queue.put_multiple(companies_list)

    def redigest_contacts(self, query={}):
        self.logger.info('Redigesting contacts. Query: %s', query)
        try:
            cursor = DBWrapper.get_persons(query)
            self.logger.info('Redigesting %d contacts', cursor.count())
            t = time.time()
            for r in cursor:
                person = AcureRatePerson.reconstruct(r)
                person.digest()
                r["deduced"] = person.deduced
                # TODO: isn't this redundant? Why do I need to store the data_sources? They don't change on digest.
                r["data_sources"] = person.data_sources
                r["_aid"] = person.aid
                r["last_update"] = datetime.datetime.now()
                DBWrapper.people_collection.save(r)
                self.logger.info('Done redigesting %s (%s) succesfully.', person.deduced[P.FULL_NAME], person.deduced.get(P.EMAIL, 'no email'))
                # Enqueue all the companies which were not accredited (also add to payload the person who needs to be notified upon resolving):
                list = [(c, person.aid) for c in person.deduced.get(P.UNRECOGNIZED_COMPANIES, [])]
                self.enqueue_companies(list)
            end_t = time.time() - t
            self.logger.info("Done redigesting. Took %s secs.", end_t)
        except Exception as e:
            self.logger.error('Failed during contacts redigest.', exc_info=True)
        pass

    def redigest_companies(self, query={}):
        self.logger.info('Redigesting companies. Query: %s', query)
        try:
            cursor = DBWrapper.get_companies(query)
            self.logger.info('Redigesting %d companies', cursor.count())
            for r in cursor:
                company = AcureRateCompany.reconstruct(r)
                company.digest()
                r["deduced"] = company.deduced
                r["data_sources"] = company.data_sources
                r["_aid"] = company._aid
                r["last_update"] = datetime.datetime.now()
                DBWrapper.company_collection.save(r)
                # Enqueue all the people that were not accredited:
                list = [(p, company._aid) for p in company.deduced.get(C.UNRECOGNIZED_PEOPLE, [])]
                self.enqueue_people(list)
        except Exception as e:
            self.logger.error('Failed during companies redigest.', exc_info=True)

    def _decide_providers(self, enrichment_behavior):
        if enrichment_behavior.all_providers:
            providers = self.registered_providers.keys()
        elif enrichment_behavior.providers:
            providers = list(set(enrichment_behavior.providers))
        else:
            providers = []
        return providers

    def _enrich_entity(self, entity_type, enrichment_key, enrichment_behavior, enrichment_data=None, enrichment_source=None):
        """
        Enrich a person - either with provided data or external enrichment (or both)

        :param enrichment_key: the search key to be used to retrieve the object
        :param enrichment_behavior: object determining external enrichment, dates, force, new, etc.
        ;param enrichment_data: an EnrichmentData object or Array of objects including data rows to add
        ;param enrichment_source: an EnrichmentSource object specifying the source of the added data
        :return: the person entity after the enrichment process
        """

        status_code = EnrichmentException.ALL_OK
        status_message = "Enrichment completed succesfully (behavior: %s)" % str(enrichment_behavior)

        # Validate parameters
        if enrichment_data and not enrichment_source:
            raise EnrichmentException("Cannot enrich with additional data without enrichment source.", EnrichmentException.BAD_REQUEST)

        # Decide which external providers are to be used (all, selective list or empty list)
        providers = self._decide_providers(enrichment_behavior)

        try:
            updated_entities = []
            changed = False
            # Get person from the Store
            # TODO: in case too many results are returned - they are in-memory - need to limit
            entities = Store.get_entities(entity_type, enrichment_key,
                                          single_result=False, mongo_query=enrichment_behavior.mongo_query)
            if len(entities) == 0:
                if enrichment_behavior.create_new:
                    self.logger.info('Enriching on %s. Could not locate entities in %s collection, creating a new entity.', enrichment_key, entity_type)
                    if entity_type == 'people':
                        entities = [AcureRatePerson()]
                    elif entity_type == 'company':
                        entities = [AcureRateCompany()]
                    # If no provider, add a Dummy engager, so the system digests and stores the data
                    if not providers:
                        providers = ['System']
                    elif 'System' not in providers:
                        providers.insert(0, 'System')
                else:
                    msg = 'Attempting enrichment on key %s. Could not locate entities matching key (Behavior::create_new = False)' % enrichment_key
                    raise EnrichmentException(msg, EnrichmentException.CONTACT_NOT_FOUND)
            elif len(entities) > 1 and not enrichment_behavior.enrich_multiple:
                msg = 'Enrichment data %s returns %d entities but enrich_multiple=False. Not enriching' % (enrichment_key, len(entities))
                raise EnrichmentException(msg, EnrichmentException.MULTIPLE_CONTACTS)

            # Go over all entities retrieved from store (per given key)
            #with ClusterRpcProxy(EnrichmentServiceConfig.AMQP_CONFIG, timeout=None) as rpc:
            rpc = None
            if True:
                for entity in entities:
                    # If new enriched data provided, merge it into received entity
                    if enrichment_data and len(enrichment_data) > 0:
                        enrichment_data.append(EnrichmentData('last_run_time', datetime.datetime.now(), 'override-no-change'))
                        # enrichment_data.append(EnrichmentData('data_source', enrichment_source.source_type, 'override'))
                        # enrichment_data.append(EnrichmentData('enrich_key', enrichment_source.source_key, 'override'))
                        changed |= entity.merge_data(enrichment_source.source_type, enrichment_source.source_key, enrichment_data)
                        #changed |= entity.merge_data('System', 'nokey', enrichment_data)
                    if changed or enrichment_behavior.digest:
                        changed = entity.digest()

                    # Initiate engagement manager to enrich via providers
                    if True:
                        EngagementManager().spawn_engagers_sequentially(providers, entity_type, entity, enrichment_behavior, changed)
                    else:
                        rpc.engagement_manager.spawn_engagers.call_async(providers, entity_type, entity.to_json_string(), enrichment_behavior.force, enrichment_behavior.force_save)
        except EnrichmentException as e:
            self.logger.warning(e)
            if enrichment_behavior.webhook:
                r = AcureRateUtils.announce(enrichment_behavior.webhook, {'status_message': e.message, 'status_code': e.code, 'ts': time.time()})
                if r:
                    self.logger.info('Sent post request to webhook at %s. Content: %s. Code: %s',
                                     enrichment_behavior.webhook, r.content, r.status_code)
        except Exception as e:
            msg = 'Failed to enrich %s entity. Key: %s. Reason: %s' % (entity_type, enrichment_key, e)
            self.logger.error(msg, exc_info=True)
            if enrichment_behavior.webhook:
                r = AcureRateUtils.announce(enrichment_behavior.webhook, {'status_message': msg, 'status_code': EnrichmentException.FATAL_ERROR, 'ts': time.time()})
                if r:
                    self.logger.info('Sent post request to webhook at %s. Content: %s. Code: %s',
                                     enrichment_behavior.webhook, r.content, r.status_code)

        return updated_entities

    def enrich_person(self, enrichment_key, enrichment_behavior, enrichment_data=None, enrichment_source=None):
        obj = self._enrich_entity('people', enrichment_key, enrichment_behavior, enrichment_data, enrichment_source)
        return obj

    def enrich_company(self, enrichment_key, enrichment_behavior, enrichment_data=None, enrichment_source=None):
        obj = self._enrich_entity('company', enrichment_key, enrichment_behavior, enrichment_data, enrichment_source)
        return obj

    def check_company(self, name, domain):
        if domain == 'angel.co' or domain == 'itunes.apple.com':
            return

        # Check if appears in database
        #q = {'deduced.aliases': name}
        q = {'deduced.domain': domain}
        cursor = Store.get_entities('company', q, mongo_query=True)
        for entity in cursor:
            self.found += 1
            self.logger.info('Found %s in our DB (so far found %d)', name, self.found)
        pass

    def dedup(self, person):

        # db.getCollection('people').aggregate([
        #     { $group: {
        #       _id: {firstField: "$deduced.first_name", secondField: "$deduced.last_name"},
        #       uniqueIds: { $addToSet: "$_id"},
        #       count: { $sum: 1}
        #     }},
        #     { $match: {
        #       count: { $gt: 3}
        #     }}
        # ])

        # Run a query to retrieve all people with the same email
        match_field = {"_aid": person.aid}
        unwind_field = "$deduced.emails"
        aggregate_field = {'emailField': "$deduced.emails"}
        persons = DBWrapper.get_aggregation_2(match=match_field, fields=aggregate_field, unwind=unwind_field)
        # Dedup them from DB...

        # Run a query to retrieve all people with the same first_normalized_name and last_normalized_name
        match_field = {"_aid": person.aid}
        aggregate_field = {'firstField': "$deduced.first_name", 'secondField': "$deduced.last_name"}
        persons = DBWrapper.get_aggregation_2(match=match_field, fields=aggregate_field)
        # Compare with jobs
        # Dedup them from DB...

        pass