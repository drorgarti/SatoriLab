import hashlib
import logging
from enrichment.enrichment_service_config import EnrichmentServiceConfig
from bson import json_util
import json


class AcureRateEntity(object):

    def __init__(self):
        self.data_sources = {}
        self.deduced = {}
        self.last_update = None
        pass

    def __str__(self):
        return "Entity"

    def __repr__(self):
        return "AcureRateEntity"

    @property
    def aid(self):
        return self._aid

    @aid.setter
    def aid(self, value):
        self._aid = value

    def sources(self, provider_filter=None, enrich_key_filter=None):
        res = []
        providers = [provider_filter] if provider_filter else list(self.data_sources)
        for provider in providers:
            # Iterate over the the specific provider entries
            for ds in self.data_sources.get(provider, []):
                # If required, check against key filter
                if enrich_key_filter is not None:
                    if 'enrich_key' in ds:
                        if ds['enrich_key'] != enrich_key_filter:
                            continue
                    else:
                        raise Exception('Unable to locate source by key (person: %s, key: %s' % (self, enrich_key_filter) )
                res.append(ds)
        return res

    def to_json_string(self):
        json_string = json.dumps(self.__dict__, default=json_util.default)
        return json_string

    def from_dictionary(self, attrs_dictionary):
        for k, v in attrs_dictionary.items():
            if k == '_id':
                setattr(self, '_aid', str(v))
            else:
                setattr(self, k, v)
        pass

    def get_hash(self):
        key = hashlib.sha256()
        entity_str = str(self.__dict__)
        key.update(entity_str.encode('utf-8'))
        return key.hexdigest()

    # Merge logic: if key/data exists, add it, otherwise, create new
    def merge_data(self, data_source_name, data_source_key, enrichment_data):
        changed = False
        for ed in enrichment_data:
            if ed.policy == 'add':
                changed |= self.add_data(data_source_name, data_source_key, ed.attr, ed.data)
            elif ed.policy == 'override' or ed.policy == 'key':
                changed |= self.set_data(data_source_name, data_source_key, ed.attr, ed.data)
            elif ed.policy == 'override-no-change':
                self.set_data(data_source_name, data_source_key, ed.attr, ed.data)
        return changed

    def delete_data(self, data_source_name, data_source_key, attr):
        if data_source_name not in self.data_sources.keys():
            return
        for ds in self.data_sources[data_source_name]:
            if attr in ds:
                del ds[attr]

    def set_data(self, data_source_name, data_source_key, attr, data):
        return self.add_data(data_source_name, data_source_key, attr, data, False)

    def add_data(self, data_source_name, data_source_key,  attr, data, aggregate=True):
        changed = False
        data_source = None
        new_data_source = False

        # Check if we already added information from this provider, if not, open one and mark the name of provider
        for ds in self.data_sources.get(data_source_name, []):
            if 'enrich_key' not in ds:
                self.logger.warning('Found data_source with no enrich_key. (%s, %s)', data_source_name, data_source_key)
            if 'enrich_key' in ds and ds['enrich_key'] == data_source_key:
                data_source = ds
                break
        if data_source is None:
            new_data_source = True
            data_source = {"provider_name": data_source_name,
                           "enrich_key": data_source_key}

        # Check if its the first time we're adding this attribute data
        if attr not in data_source:
            changed = True
            if aggregate:  # set it as an array
                data_source[attr] = [data]
            else:
                data_source[attr] = data
        else:
            if aggregate and data not in data_source[attr]:
                data_source[attr].append(data)
                changed = True
            elif not aggregate and data_source[attr] != data:
                data_source[attr] = data
                changed = True

        if changed and new_data_source:
            if data_source_name in self.data_sources:
                self.data_sources[data_source_name].append(data_source)
            else:
                self.data_sources[data_source_name] = [data_source]

        return changed

    def _append_to_deduced(self, attr, data):
        if attr not in self.deduced:
            self.deduced[attr] = [data]
        elif data not in self.deduced[attr]:
            self.deduced[attr].append(data)
        else:
            pass
