from engagement.engager import Engager
from engagement.engagement_exception import EngagementException


class TestEngager(Engager):

    def __init__(self):
        super().__init__()
        self.what_to_do = 'change'  # could also be 'error'
        pass

    def __repr__(self):
        return "Test Engager"

    def should_skip_engagement(self):
        return False

    def get_provider_name(self):
        return "Test"

    def get_short_symbol(self):
        return "tt"

    def enrich_person(self):

        if self.what_to_do == 'error':
            raise EngagementException('Test Engager throwing test exception.')
        elif self.what_to_do == 'change':
            self.set_data('shoe_size', 44)
            self.set_data('eyes_color', 'green')

            name_dbl = self.enriched_entity.deduced.get(P.FULL_NAME, '<Noname>') * 2
            self.add_data('emails', 'name_dbl%s' % '@nowhere.com')

        return ['show_size', 'eyes_color']
