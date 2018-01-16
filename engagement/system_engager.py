from engagement.engager import Engager
from engagement.engagement_result import EngagementResult

from entities.acurerate_attributes import P, C


class SystemEngager(Engager):

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "System Engager"

    def should_skip_engagement(self):
        return False

    def get_provider_name(self):
        return "System"

    def get_short_symbol(self):
        return "dd"

    def set_enrich_key(self):
        self.enrich_key = 'nokey'

    # This method overrides the regular Engager method as no need to do various things done in the base class
    # def engage(self, entity_type, entity, force_refresh=False):
    #     self.enriched_entity = entity
    #     self.set_enrich_key()
    #
    #     enrich_func = self._get_enrich_function(entity)
    #     properties_changed = enrich_func()
    #     return EngagementResult('success', entity.__dict__, properties_changed, None)

    def enrich_person(self):
        # Iterate over all entities - these were all added by user, return them all
        for k, v in self.enriched_entity.deduced.items():
            self.set_data(k, v)
        return [k for k in self.enriched_entity.deduced.keys()]

    def enrich_company(self):
        # Iterate over all entities - these were all added by user, return them all
        for k, v in self.enriched_entity.deduced.items():
            self.set_data(k, v)
        return [k for k in self.enriched_entity.deduced.keys()]


