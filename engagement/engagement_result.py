from bson import json_util
import json
from utils.acurerate_utils import AcureRateUtils


class EngagementResult(object):

    SUCCESS = 'success'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    NOCHANGE = 'nochange'

    def __init__(self, status, result, properties_changed, error):
        self._status = status
        self._result = result
        self._properties_changed = properties_changed
        self._error = error
        pass

    def __str__(self):
        if self._properties_changed:
            prop_str = ', '.join(['%s:%s' % (k, v) for k, v in self._properties_changed.items()])
        else:
            prop_str = 'None'
        err_str = ' (error: %s)' % self._error if self._error else ''
        the_str = 'status: %s%s, properties: %s' % (self._status, err_str, prop_str)
        return the_str

    def __repr__(self):
        if self._properties_changed:
            prop_str = ', '.join(['%s:%s' % (k, v) for k, v in self._properties_changed.items()])
        else:
            prop_str = 'None'
        err_str = ' (error: %s)' % self._error if self._error else ''
        the_str = 'status: %s%s, properties: %s' % (self._status, err_str, prop_str)
        return the_str

    @staticmethod
    def from_json_string(engagement_result_json_string):
        obj = json.loads(engagement_result_json_string, object_hook=json_util.object_hook)
        result = json.loads(obj['_result'], object_hook=json_util.object_hook)
        properties_changed = json.loads(obj['_properties_changed'], object_hook=json_util.object_hook)
        er = EngagementResult(obj['_status'], result, properties_changed, obj['_error'])
        return er

    def to_json_string(self):
        result_json_str = json.dumps(self._result, default=json_util.default)
        properties_json_str = json.dumps(self._properties_changed, default=json_util.default)
        new_er = EngagementResult(self._status, result_json_str, properties_json_str, self._error)
        er_json_string = json.dumps(new_er.__dict__, default=json_util.default)
        return er_json_string

    @property
    def status(self):
        return self._status

    @property
    def result(self):
        return self._result

    @property
    def properties_changed(self):
        return self._properties_changed

    @property
    def error(self):
        return self._error
