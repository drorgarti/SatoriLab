import re
import logging

from store.db_wrapper import DBWrapper
from store.neo_wrapper import NeoWrapper

from entities.acurerate_person import AcureRatePerson
from entities.acurerate_company import AcureRateCompany

from enrichment.enrichment_service_config import EnrichmentServiceConfig


class Store(object):

    ready = False
    logger = None

    def __init__(self):
        pass

    @staticmethod
    def connect():
        if Store.ready:
            return
        # Set the logger
        logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        # DBWrapper.set_logger(logger)
        # NeoWrapper.set_logger(logger)

        # Connect the stores
        DBWrapper.connect()
        NeoWrapper.connect()

        Store.ready = True

    @staticmethod
    def get_entities(entity_type, key, single_result=False, mongo_query=False):
        Store.connect()
        entities = []
        if not mongo_query:
            key = Store._normalize_key(key)
        cursor = DBWrapper.get_entities(entity_type, key, single_result)
        if cursor:
            for data in cursor:
                if entity_type == DBWrapper.PEOPLE_COLLECTION_NAME:
                    entities.append(AcureRatePerson.reconstruct(data))
                elif entity_type == DBWrapper.COMPANY_COLLECTION_NAME:
                    entities.append(AcureRateCompany.reconstruct(data))
        return entities

    @staticmethod
    def set_entity(entity_type, entity):
        Store.connect()
        DBWrapper.set_entity(entity_type, entity)

        if entity_type == DBWrapper.PEOPLE_COLLECTION_NAME:
            NeoWrapper.set_person(entity)
        elif entity_type == DBWrapper.COMPANY_COLLECTION_NAME:
            NeoWrapper.set_company(entity)

        pass

    @staticmethod
    def get_person_by_aid(aid):
        Store.connect()
        p = DBWrapper.get_persons({'_aid': aid}, True)
        return AcureRatePerson.reconstruct(p) if p else None

    @staticmethod
    def get_person(key, mongo_query=False):
        Store.connect()
        if not mongo_query:
            key = Store._normalize_key(key)
        p = DBWrapper.get_persons(key, True)
        return AcureRatePerson.reconstruct(p) if p else None

    @staticmethod
    def get_persons(key, single_result=False, mongo_query=False):
        Store.connect()
        persons = []
        # Get person from MongoDB
        if not mongo_query:
            key = Store._normalize_key(key)
        res = DBWrapper.get_persons(key, single_result)
        if res:
            if single_result:
                return [AcureRatePerson.reconstruct(res)]
            else:
                for p in res:
                    persons.append(AcureRatePerson.reconstruct(p))
        return persons

    @staticmethod
    def get_company_by_aid(aid):
        Store.connect()
        c = DBWrapper.get_companies({'_aid': aid}, True)
        return AcureRateCompany.reconstruct(c) if c else None

    @staticmethod
    def get_company(key, mongo_query=False):
        Store.connect()
        if not mongo_query:
            key = Store._normalize_key(key)
        c = DBWrapper.get_companies(key, True)
        return AcureRateCompany.reconstruct(c) if c else None

    @staticmethod
    def get_companies(key, single_result=False, mongo_query=False):
        Store.connect()
        # Get company from MongoDB
        companies = []
        if not mongo_query:
            key = Store._normalize_key(key)
        cursor = DBWrapper.get_companies(key, single_result)
        if cursor:
            for c in cursor:
                companies.append(AcureRatePerson.reconstruct(c))
        return companies

    @staticmethod
    def get_paths_to_person(source_id, target_id):
        Store.connect()
        return NeoWrapper.get_paths_to_person(source_id, target_id)

    @staticmethod
    def get_paths_to_company(source_id, target_id, seniority=None, area=None):
        Store.connect()
        return NeoWrapper.get_paths_to_company(source_id, target_id, seniority, area)

    @staticmethod
    def person_exists_regex(name):
        Store.connect()
        regx = re.compile(name, re.IGNORECASE)
        q = {"deduced.full_name": regx}
        cursor = DBWrapper.get_persons(q)
        return True if cursor.count() > 0 else False

    @staticmethod
    def company_exists(name):
        Store.connect()
        q = {"deduced.aliases": name}
        cursor = DBWrapper.get_companies(q)
        return True if cursor.count() > 0 else False

    @staticmethod
    def company_exists_regex(name):
        Store.connect()
        regx = re.compile(name, re.IGNORECASE)
        q = {"deduced.aliases": regx}
        cursor = DBWrapper.get_companies(q)
        return True if cursor.count() > 0 else False

    @staticmethod
    def _normalize_key(key):
        normalized_key = {}
        for k in key.keys():
            if k.startswith('deduced.'):
                normalized_key[k] = key[k]
            else:
                normalized_key['deduced.%s' % k] = key[k]
        return normalized_key
