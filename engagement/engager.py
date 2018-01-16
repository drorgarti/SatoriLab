import logging
import datetime
import time
from enrichment.enrichment_service_config import EnrichmentServiceConfig
from engagement.engagement_result import EngagementResult
from engagement.engagement_exception import EngagementException


class Engager:

    # TODO: Class should provide services to inheriting classes:
    # 1. Throttling

    # Class variables to be shared by all engagers
    throttling_interval = 1

    def __init__(self):
        self.enriched_entity = None
        self.hash = None
        self.engagement_start_time = None
        self.enrich_key = None
        self.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)

    def __repr__(self):
        if self.enriched_entity is not None:
            return "Engager Base Class (enriched entity %r)" % self.enriched_entity
        else:
            return "Engager Base Class"

    def _get_enrich_function(self, entity):
        t = entity.__class__.__name__
        if t == 'AcureRatePerson':
            method_name_suffix = 'person'
        elif t == 'AcureRateCompany':
            method_name_suffix = 'company'
        else:
            method_name_suffix = 'unknown'
        func = getattr(self, 'enrich_' + method_name_suffix, 'enrich_unknown')
        return func

    def is_run_succesful(self, msg):
        return True

    def should_skip_engagement(self):
        # By default, we check if the provider data is already there and whether it's the same search key. If so, no need to engage
        sources = self.enriched_entity.sources(self.get_provider_name())
        for s in sources:
            if s['enrich_key'] == self.enrich_key and self.is_run_succesful(s['last_run_status']):
                return True

        #if self.get_provider_name() in self.enriched_entity.data_sources:
        #    return True

        # TODO: implement: Check dates, provider filter, different failure statuses (some will rerun/ others not) etc.
        return False

    def is_change(self):
        # TODO: should check error - if exists, it's also a change!

        # TODO: need to ignore the update_time -> it will always cause it to be changed
        changed = self.hash != self.hash_after_change
        return changed

    def get_api_key(self):
        return None

    def get_quota(self):
        return None

    def get_short_symbol(self):
        return None

    def get_properties(self):
        my_props = {}
        my_props['key'] = self.get_api_key() if self.get_api_key() else 'N/A'
        my_props['short_symbol'] = self.get_short_symbol() if self.get_short_symbol() else 'N/A'
        my_props['quota'] = self.get_quota() if self.get_quota() else 'N/A'
        return my_props

    def engage_multi(self, entities):
        if entities is None:
            return

        # This is a default behavior. Engagers can implement bulk contacts enrichment through API
        new_entities = []
        for pp in entities:
            np = self.engage(pp)
            new_entities += np
            #new_entities += self.engage(pp)
        return new_entities

    def get_enrich_key(self):
        return self.enrich_key

    def set_enrich_key(self):
        self.logger.error('Engager %s did not implement set_enrich_key method. Ignoring engage request.', self.get_provider_name())

    def debug_print(self, message):
        print('%s: %s' % (self.get_provider_name(), message))

    def engage(self, entity_type, entity, force_refresh=False):

        self.enriched_entity = entity
        self.hash = entity.get_hash()

        self.set_enrich_key()
        should_skip = self.should_skip_engagement()

        if not force_refresh and should_skip:
            self.logger.info('Skipping enrichment of %s (%s/%s exists)', self.enriched_entity, self.get_provider_name(), self.enrich_key)
            return EngagementResult('skipped', None, None, None)

        self.engagement_start_time = time.time()

        enrich_func = self._get_enrich_function(entity)
        try:
            properties_changed = enrich_func()
            if properties_changed:  # Only if enrichment took place (maybe provider cannot enrich this type)
                #self.enriched_entity.digest()
                self.logger.info('Done digesting entity after engagement via %s', self.get_provider_name())
                self.finalize(True)
                res = self.enriched_entity.sources(self.get_provider_name(), self.enrich_key)[0]
                er = EngagementResult(EngagementResult.SUCCESS, self.enriched_entity.__dict__, res, None)
            else:
                er = EngagementResult(EngagementResult.NOCHANGE, self.enriched_entity.__dict__, None, None)
        except EngagementException as e:
            self.finalize(False, str(e))
            self.logger.info('Engagement exception raised during engagement via %s', self.get_provider_name())
            res = self.enriched_entity.sources(self.get_provider_name(), self.enrich_key)[0]
            er = EngagementResult('failed', self.enriched_entity.__dict__, res, str(e))

        return er

    def enrich_person(self):
        return None

    def enrich_company(self):
        return None

    def enrich_unknown(self):
        return None

    def finalize(self, success_status, the_error=None):

        res_str = 'success' if success_status else the_error
        self.set_data('last_run_status', res_str)

        # Keep the key used
        if self.get_api_key():
            self.set_data('last_run_api_key', self.get_api_key())

        self.hash_after_change = self.enriched_entity.get_hash()

        # Mark the engagement time
        #self.set_data('updated', datetime.date.today())
        self.set_data('last_run_time', datetime.datetime.now())

        if self.engagement_start_time:
            end_time = time.time() - self.engagement_start_time
            self.set_data('last_run_engagement_time', "%s" % end_time)
            self.engagement_start_time = None

        self.logger.info('Done enriching %s via %s: status: %s', self.enrich_key, self.get_provider_name(), res_str)

    def delete_data(self, attr):
        if not self.get_provider_name():
            self.logger.error("Provider did not implement get_provider_name(). Cannot add attribute")
            return
        if self.enriched_entity:
            self.enriched_entity.delete_data(self.get_provider_name(), self.enrich_key, attr)

    def set_data(self, attr, data):
        self.add_data(attr, data, False)

    def add_data(self, attr, data, aggregate=True):
        if not self.get_provider_name():
            self.logger.error("Provider did not implement get_provider_name(). Cannot add attribute")
            return
        if self.enriched_entity:
            self.enriched_entity.add_data(self.get_provider_name(), self.enrich_key, attr, data, aggregate)

    def get_provider_name(self):
        return None

    def get_pivot_email(self):
        return self.enriched_entity.deduced.get('email', None)

    def get_pivot_phone(self):
        return self.enriched_entity.deduced.get('phone', None)

    def get_from_cache(self, key):
        pass

    def update_cache(self, key, value):
        pass
