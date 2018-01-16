import json
import requests
import requests_cache

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_person import P
from opencnam import Phone
from utils.acurerate_utils import AcureRateUtils


class OpenCnamEngager(Engager):

    ACURERATE_SID = "AC6063ab209012419ab601efbb7b132eb3"
    ACURERATE_AUTH_TOKEN = "AU60c97948e8254cedb8f7ec2153898df4"

    def __init__(self):
        super().__init__()

        # Set configuration settings for the OpenCNAM library
        pass

    def __repr__(self):
        return "OpenCnam Engager"

    def get_provider_name(self):
        return "OpenCNAM"

    def get_short_symbol(self):
        return "oc"

    def get_api_key(self):
        return OpenCnamEngager.ACURERATE_AUTH_TOKEN

    def set_enrich_key(self):
        phone = self.get_pivot_phone()
        if phone:
            self.enrich_key = "%s" % phone
        else:
            raise EngagementException("OpenCnam Engager - cannot engage. Cannot create enrich key for %s", self.enriched_entity)

    def enrich_person(self):
        #phone = Phone('+16313673937')  # +15169831142
        #phone = Phone('+16313673937', account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)
        #phone = Phone('+972528113499', account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)
        #phone = Phone('+97289714324', account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)
        #phone = Phone('+972779707328', account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)
        #phone = Phone('+16313673937', account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)

        cnam_list = []
        for phone in self.enriched_entity.deduced.get(P.PHONES, []):
            phone_res = Phone(phone, account_sid=OpenCnamEngager.ACURERATE_SID, auth_token=OpenCnamEngager.ACURERATE_AUTH_TOKEN)
            if phone_res.cnam:
                cnam_list.append(phone_res.cnam)

            # if len(phone_res.cnam) > 5:
            #     json_obj = json.loads(phone_res.cnam)
            #     full_name = json_obj.get('name', [])
            #     # l, m, f = AcureRateUtils.tokenize_full_name(full_name)
            #     if phone_res.cnam and len(full_name) > 0:
            #         cnam_list.append(full_name)

        for cnam in cnam_list:
            self.add_data(P.CNAM, cnam)
        return [P.CNAM]

    def enrich_person__truecaller(self):

        cookie_str = '__cfduid=d2554926f77b4da885e17e2e453d5e4c71481629449; tcToken=eyJpdiI6ImdLOFwvcFUxWWc0NnZHVnA4WkNRYVZLXC9PNjRMUGtmeXUzTldMSzZNZ1RNYz0iLCJ2YWx1ZSI6Ilpvb21DWVVFN0ptdUNKTFl4QnhiRjdKMVB0WTFrTHliRk13dFwvZHFzTklrMXczdnZPeEgzU2RXdnliNVg0bGxISG1IZU15dlRmaDNcL2xIclJoSCtwZUE9PSIsIm1hYyI6IjljOGJmMWQ4NzJmMDIzMDNjMjEwM2U2MzUwY2IxZDcyZWJlZjJhNWUzNGM3NmNhNDEwYzc3MThkMGM5Nzc3NGIifQ%3D%3D; _gat=1; _ga=GA1.2.979500326.1481629451; tcSession=eyJpdiI6IlZ6U2hsaGd1VDRPRHF1V1dwNDFHNnpVbkN6OEpFMk5LeElleWJrdDZuSms9IiwidmFsdWUiOiJzXC9TWldPYXV5WXBlWTk5dEdEZmxlQWwwR29OUThhc2k0eFBqMmF0VFJjVTVtNVhKYmZLR3hHOFdjMFJzYVVuOVlBSERpbVNnK0RNcEk4ME9IV1dnVnc9PSIsIm1hYyI6IjE1OWIzODM0MTFkOWZmOWRlNWVkNDQ1ZjhjYWIwOTEwYmQwNzc2MDIyZmQwMTY0NjM1MTdhNjlhMzgxNjdhNWEifQ%3D%3D; XLBS3=XLBS1|WE/k1|WE/fD'
        #cookie_str ='__cfduid=d2554926f77b4da885e17e2e453d5e4c71481629449; tcToken=eyJpdiI6ImdLOFwvcFUxWWc0NnZHVnA4WkNRYVZLXC9PNjRMUGtmeXUzTldMSzZNZ1RNYz0iLCJ2YWx1ZSI6Ilpvb21DWVVFN0ptdUNKTFl4QnhiRjdKMVB0WTFrTHliRk13dFwvZHFzTklrMXczdnZPeEgzU2RXdnliNVg0bGxISG1IZU15dlRmaDNcL2xIclJoSCtwZUE9PSIsIm1hYyI6IjljOGJmMWQ4NzJmMDIzMDNjMjEwM2U2MzUwY2IxZDcyZWJlZjJhNWUzNGM3NmNhNDEwYzc3MThkMGM5Nzc3NGIifQ%3D%3D; __gads=ID=69a98525476c8ae1:T=1481638323:S=ALNI_MYwuM-deZPXbwoejhOrxXl3ejZJyw; XLBS3=XLBS3|WFAMh|WFABt; tcSession=eyJpdiI6IjBuRFdMb1lmejZhSUFzVWI2cnVCRWdSaHE0QzFjQVNrajZ3RDVidjRTYms9IiwidmFsdWUiOiJMTThFZmtTTkhjeVZSV1FtTWx5MG9xaW55SFp4ZzEyUWY5SFhjMDZZNkpORDllVVp5Y3RScUlpdFU2OGxnbzdEOHJHQzBqaUFtZVZLSk5icGRKc1wvT0E9PSIsIm1hYyI6IjYxOTczZjMyMTQ1NGU2MGM3NjMyOGE0YWYyMTE4Zjg5OWEzOTE5MDMwODQzYjNjOWUyOTYwMWU0MTU2MDZiYWIifQ%3D%3D; _ga=GA1.2.979500326.1481629451'

        headers = {'contentType': 'application/json; charset=utf-8',
                   'Cookie': cookie_str}

        # Create request
        url = 'https://www.truecaller.com/throttle/reset/throttleSearch'
        with requests_cache.disabled():
            response = requests.post(url, headers=headers)
        if response.status_code != 200:
            raise EngagementException("%s. %s." % (response.status_code, response.text))


        url = 'https://www.truecaller.com/il/0504333102'
        with requests_cache.disabled():
            response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise EngagementException("%s. %s." % (response.status_code, response.text))

        pass