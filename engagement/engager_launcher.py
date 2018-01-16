import importlib
import traceback
import logging
import datetime
import json
import copy
import time

from bson import json_util

from nameko.rpc import rpc, RpcProxy
from enrichment.enrichment_service_config import EnrichmentServiceConfig
from engagement.engagement_exception import EngagementException
from engagement.engagement_result import EngagementResult

from engagement.system_engager import SystemEngager
from engagement.twitter_engager import TwitterEngager
from engagement.fullcontact_engager import FullContactEngager
from engagement.pipl_engager import PiplEngager
from engagement.startupnationcentral_engager import StartupNationCentralEngager
from engagement.crunchbasescraper_engager import CrunchBaseScraperEngager
from engagement.crunchbasebot_engager import CrunchBaseBotEngager
from engagement.bloombergscraper_engager import BloombergScraperEngager
from engagement.circleback_engager import CircleBackEngager
from engagement.opencnam_engager import OpenCnamEngager

from entities.acurerate_attributes import P, C, LISTS
from entities.acurerate_person import AcureRatePerson
from entities.acurerate_company import AcureRateCompany

from utils.acurerate_utils import AcureRateUtils

from store.db_wrapper import DBWrapper
from store.store import Store


class EngagementManager(object):
    name = "engagement_manager"
    launcher = RpcProxy('engager_launcher')

    def __init__(self):
        self.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        pass

    def _get_entity_by_type(self, entity_type):
        if entity_type == 'people':
            entity = AcureRatePerson()
        elif entity_type == 'company':
            entity = AcureRateCompany()
        else:
            raise EngagementException('Unknown entity type - %s', entity_type)
        return entity

    def spawn_engagers_sequentially(self, providers, entity_type, entity, enrichment_behavior, enriched=False):

        org_entity = copy.deepcopy(entity)

        # Iterate over all required providers and run enrichment
        engagement_results = {}
        el = EngagerLauncher()
        for provider_name in providers:
            try:
                res = el.launch(provider_name, entity_type, entity, enrichment_behavior.force)
                engagement_results[provider_name] = EngagementResult.from_json_string(res)
            except EngagementException as ee:
                self.logger.error('Failed to engage via %s on entity %s (exception: %s)', provider_name, entity.aid, ee)

        # Recreate entity
        new_entity = org_entity

        # Merge all results into entity
        #changed = False
        changed = enriched
        redigest_properties = {}
        for provider_name, engagement_result in engagement_results.items():
            if engagement_result.status != EngagementResult.SKIPPED and engagement_result.status != EngagementResult.NOCHANGE:
                enrich_key = engagement_result.properties_changed['enrich_key']
                for k, v in engagement_result.properties_changed.items():
                    property_changed = new_entity.set_data(provider_name, enrich_key, k, v)
                    if property_changed and k in LISTS.TRIGGERING_PROPERTIES:
                        redigest_properties[k] = v
                    changed |= property_changed
            self.logger.info('Done merging properties of %s. Changed = %s', provider_name, changed)
            pass

        if changed or enrichment_behavior.force_save:
            new_entity.last_update = datetime.datetime.now()
            new_entity.digest()
            Store.set_entity(entity_type, new_entity)
            msg = 'Stored in Store! (changed=%s, force_save=%s)' % (changed, enrichment_behavior.force_save)

            # Redigest other entities
            self.redigest(redigest_properties)
        else:
            msg = 'Not stored. No change detected'

        self.logger.info(msg)

        # Prepare information to send to webhook
        if enrichment_behavior.webhook:
            payload = {'status_message': msg, 'status_code': 200, 'ts': time.time(), 'aid': new_entity.aid}
            r = AcureRateUtils.announce(enrichment_behavior.webhook, payload)

        self.logger.info('Done merging enrichment result into entity. Changed = %s', changed)

    @rpc
    def spawn_engagers(self, providers, entity_type, entity_json_string, force, force_save):

        self.logger.info('Started spawning engagers flows.')

        # Iterate over all required providers and launch enrichment
        result_futures = {}
        for provider_name in providers:
            try:
                result_futures[provider_name] = self.launcher.launch.call_async(provider_name, entity_type, entity_json_string, force)
                self.logger.info('Launched (async) engagement with %s', provider_name)
            except EngagementException as ee:
                self.logger.error('Failed to engage via %s on entity <to specify...> (exception: %s)', provider_name, ee)

        self.logger.info('Completed spawning all engagers.')

        engagement_results = {}
        for provider_name in result_futures.keys():
            try:
                res = result_futures[provider_name].result()
                engagement_results[provider_name] = EngagementResult.from_json_string(res)
                self.logger.info('Done collecting results of provider %s', provider_name)
            except Exception as ee:
                self.logger.info('Exception raised getting future result of %s: %s. Ignoring this result.', provider_name, ee)

        self.logger.info('Completed collecting results from all engagers')

        # Recreate entity
        new_entity = self._get_entity_by_type(entity_type)
        entity = new_entity.from_json_string(entity_json_string)

        self.logger.info('Done recreating entity from json - %s', entity)

        # Merge all results into entity
        changed = False
        redigest_properties = {}
        for provider_name, engagement_result in engagement_results.items():
            if engagement_result.status != EngagementResult.SKIPPED and engagement_result.status != EngagementResult.NOCHANGE:
                enrich_key = engagement_result.properties_changed['enrich_key']
                for k, v in engagement_result.properties_changed.items():
                    property_changed = entity.set_data(provider_name, enrich_key, k, v)
                    if property_changed and k in LISTS.TRIGGERING_PROPERTIES:
                        redigest_properties[k] = v
                    changed |= property_changed
            self.logger.info('Done merging properties of %s. Changed = %s', provider_name, changed)
            pass

        if changed or force_save:
            entity.last_update = datetime.datetime.now()

            # Digest and store
            entity.digest()
            Store.set_entity(entity_type, entity)
            self.logger.info('Stored in Store! (changed=%s, force_save=%s)', changed, force_save)

            # Redigest other entities
            self.redigest(redigest_properties)
        else:
            self.logger.info('Not stored. No change detected')

        pass


    def redigest(self, redigest_properties):

        if True:
            return

        self.logger.info('Started redigest according to these properties: %s', redigest_properties)

        # TODO: fix the below to work also with companies - be agnostic to the entity type!

        # Run a query to get all those entities who have the value in the field "unrecognized_<prop>":
        for k, v in redigest_properties.items():
            if k == 'name':  # TODO: temp code. Need to align Mongo properties and all is well...
                k = 'companies'
            q = {'deduced.unrecognized_%s' % k: v}
            cursor = Store.get_entities('people', q, mongo_query=True)
            for entity in cursor:
                entity.digest()
                Store.set_entity('people', entity)
            self.logger.info('Done redigesting entities of property %s', k)

        self.logger.info('Done redigesting all entities.')
        pass

class EngagerLauncher(object):
    name = "engager_launcher"

    def __init__(self):
        self.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        # Make sure store is connected
        from store.store import Store
        Store.connect()
        pass

    def _instantiate_provider(self, provider):
        instance = None
        try:
            module_name = 'engagement'
            class_name = '%s_engager' % provider.lower()
            module = importlib.import_module(module_name)  # module = __import__(module_name)
            classes_ = getattr(module, class_name)
            engager_class_ = getattr(classes_, '%sEngager' % provider)
            instance = engager_class_()
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error('Failed to instantiate provider class for provider %s (%s\n%s)', provider, e, tb)
        return instance

    def _get_entity_by_type(self, entity_type):
        if entity_type == 'people':
            entity = AcureRatePerson()
        elif entity_type == 'company':
            entity = AcureRateCompany()
        else:
            raise EngagementException('Unknown entity type - %s', entity_type)
        return entity

    def _deserialize(self, entity_type, _entity):
        if type(_entity) is str:
            entity_json = json.loads(_entity, object_hook=json_util.object_hook)
            if entity_type == 'people':
                entity = AcureRatePerson.reconstruct(entity_json)
            elif entity_type == 'company':
                entity = AcureRateCompany.reconstruct(entity_json)
            else:
                raise EngagementException('Unknown entity type - %s', entity_type)
        else:
            entity = _entity
        return entity

    @rpc
    def launch(self, provider, entity_type, entity_string, force):
        # Instantiate the provider
        instance = self._instantiate_provider(provider)
        if instance is None:
            raise EngagementException('Aborting launch. Failed to instantiate provider %s' % provider)

        self.logger.info('Provider %s instantiated and ready.', provider)

        try:
            entity = self._deserialize(entity_type, entity_string)
            self.logger.info('About to launch an engagement via %s on %s', provider, entity)
            engagement_result = instance.engage(entity_type, entity, force)
        except EngagementException as e:
            self.logger.error('Exception raised: %s', e)
            engagement_result = None

        return engagement_result.to_json_string()
