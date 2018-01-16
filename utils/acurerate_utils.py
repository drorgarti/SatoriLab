import re
import os
import math
import google
import requests
import pickle
from hashlib import sha512

from dateutil.parser import parse

from entities.acurerate_attributes import P, C, T
from urllib.parse import urlparse
from string import ascii_letters

# Go to Settings - get package google.cloud.translate, google.cloud.core
#from google.cloud import translate


class AcureRateUtils(object):

    def __init__(self):
        pass

    @staticmethod
    # def import_non_local(name, custom_name=None):
    #     import importlib.util, sys
    #
    #     #spec = importlib.util.spec_from_file_location("module.name", "/path/to/file.py")
    #     spec = importlib.util.spec_from_file_location("google", "C:\Python353\Lib\site-packages\google-1.9.3.dist-info\file.py")
    #     foo = importlib.util.module_from_spec(spec)
    #     spec.loader.exec_module(foo)
    #     #foo.MyClass()
    #
    #     return foo

    @staticmethod
    def translate(str, source_language, target_language):

        # -----------------------------------------------------
        # Language codes can be found here: https://cloud.google.com/translate/docs/languages
        # -----------------------------------------------------

        # TODO: we shouldn't be doing this here (on every call..!)  Move somewhere else... :-)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './Satori-5834c119ed73.json'

        # Instantiates a client
        translate_client = translate.Client()

        # The text to translate
        text = u'Hello, world!'
        # The target language
        target = 'ru'

        # Translates some text into Russian
        translation = translate_client.translate(text, target_language=target)
        translation = translate_client.translate(u'דורון הרצליך', target_language='en')

        print(u'Text: {}'.format(text))
        print(u'Translation: {}'.format(translation['translatedText']))

        translated_name = name
        return translated_name

    # [END translate_quickstart]

    @staticmethod
    def object_hash(_obj):
        h = pickle.dumps(_obj)
        s = hash(h)
        #s = sha512(h).hexdigest()
        return s

    @staticmethod
    def dict_compare(d1, d2):
        d1_keys = set(d1.keys())
        d2_keys = set(d2.keys())
        intersect_keys = d1_keys.intersection(d2_keys)
        added = d1_keys - d2_keys
        removed = d2_keys - d1_keys
        modified = {o: (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
        same = set(o for o in intersect_keys if d1[o] == d2[o])
        return added, removed, modified, same

    @staticmethod
    def is_alphanumeric(str):
        valid = re.match('^[\w-]+$', str) is not None
        return valid

    @staticmethod
    def valid_name(str):
        return all(c in ascii_letters + '-. ' for c in str)

    @staticmethod
    def aliasize(the_string):
        if the_string is None:
            return None
        alias = the_string.lower()

        # Remove incorporation suffixes ("Ltd", "Inc", etc.)
        alias = AcureRateUtils.remove_incorporation_suffix(alias)

        # Check if we have "strA/strB" or "strA,strB" or "strA(...)"
        alias = AcureRateUtils.get_until_char(alias, '/')
        alias = AcureRateUtils.get_until_char(alias, ',')
        alias = AcureRateUtils.get_until_char(alias, '(')
        # Remove dots, hyphens, dbl-spaces, etc.
        alias = alias.replace("  ", " ")
        alias = alias.replace(".", " ")
        alias = alias.replace("-", " ")
        # Strip leading/trailing spaces
        return alias.strip()

    @staticmethod
    def tokenize_full_name(full_name):
        tokens = full_name.split(" ")
        if len(tokens) == 1:
            return (tokens[0], None, None)
        elif len(tokens) == 2:
            return (tokens[0], None, tokens[1])
        else:
            return (tokens[0], tokens[1], tokens[2])

    @staticmethod
    def get_until_char(the_string, char):
        try:
            s = the_string.index(char)
            return the_string[:s]
        except Exception as e:
            return the_string

    @staticmethod
    def remove_parenthesized_content(the_string, to_the_right=False):
        # to_the_right=False:  "Ploni (mr) Almoni" --> "Ploni Almoni" (removed dbl-space in the middle!)
        # to_the_right=False:  "Ploni (mr)" --> "Ploni" (removed space at the end!)
        # to_the_right=True:   "Ploni (mr) Almoni" --> "Ploni" (removed also the space at the end!)
        # if no closing parenthesis, original string is returned
        # if multiple parenthesis, only one is removed...  "ploni (a) bb (b) almoni" --> "ploni bb (b) almoni"
        if the_string is None:
            return None
        try:
            s = the_string.index("(")
            if to_the_right:
                cleaned_str = the_string[:s]
            else:
                e = the_string.index(")")
                cleaned_str = the_string[:s] + the_string[e+1:]
        except Exception as e:
            cleaned_str = the_string
        return cleaned_str.replace("  ", " ").strip()

    @staticmethod
    # x = {"age": 2, "color": "red"}
    # y = {"color": "yellow", "shoe size": 42}
    # z = {"age": 2, "color": "yellow", "shoe size": 42}
    def merge_two_dicts(x, y):
        '''Given two dicts, merge them into a new dict as a shallow copy.'''
        z = x.copy()
        z.update(y)
        return z

    @staticmethod
    # Given a map: { "attr-name-in-source": "attr-name-in-target" }
    # Copy all fields from source_dict[attr-name-in-source] to target_dict[attr-name-in-target]
    def dict2dict(source_dict, target_dict, themap):
        try:
            for k, v in themap.items():
                if k in source_dict:
                    target_dict[v] = source_dict[k]
        except Exception as e:
            print(e)
            raise e
        return target_dict

    @staticmethod
    def contains_words(string, key_words):
        words = set(re.findall(r'\w+', string))
        key_words_set = set(key_words)
        u = words & key_words_set
        if len(u) > 0:
            return True
        return False

    @staticmethod
    def obj2string(obj, delimiter=', '):
        if obj is None:
            return ''
        formatted = delimiter.join(['%s: %s' % (key, value) for (key, value) in obj.__dict__.items()])
        return formatted

    academic_prefixes = ['prof', 'prof.', 'professor', 'dr', 'dr.', 'doctor']

    @staticmethod
    def announce(webhook, payload):
        if webhook is None or payload is None:
            return None
        r = requests.post(webhook, payload)
        return r

    @staticmethod
    def _longest_common_substring(strings_list):
        substring = ''
        if len(strings_list) > 1 and len(strings_list[0]) > 0:
            for i in range(len(strings_list[0])):
                for j in range(len(strings_list[0]) - i + 1):
                    if j > len(substring) and all(strings_list[0][i:i + j] in x for x in strings_list):
                        substring = strings_list[0][i:i + j]
        return substring

    @staticmethod
    def google_search(site, query):
        full_query_line = 'site:%s %s' % (site, query)
        res = google.search(full_query_line, tld='com', lang='en', num=3, start=0, stop=2, pause=2.0)
        matches = []
        for url in res:
            if url.lower().find(site) == 0:
                matches.append(url)
        return matches

    @staticmethod
    def boxit(logger_func, msg):
        l = len(msg)
        logger_func('-' * (l + 2))
        logger_func(' %s ' % msg)
        logger_func('-' * (l + 2))

    @staticmethod
    def is_academic_domain(domain):
        if not domain:
            return False

        # TODO: should be replaced by predefined list in DB, return also the country, etc.
        if domain.endswith('ac.il') or domain.endswith('ac.uk') or domain.endswith('edu'):
            return True
        return False

    @staticmethod
    def is_academic_prefix(prefix):
        if prefix in AcureRateUtils.academic_prefixes:
            return True
        return False

    # See more info here: https://en.wikipedia.org/wiki/Types_of_business_entity
    # TODO: auto-generate those - make sure we also have the ',' or ', ' in the beginning, like in 'ltd' below.
    company_incoporation_symbols = ['ltd', 'ltd.', 'limited',
                                    'inc', 'inc.', 'incorporated', 'incorporation',
                                    'co', 'corp', 'corp.', 'corporated', 'corporation',
                                    'holdings',
                                    'plc', 'plc.', 'p.l.c', 'p.l.c.',
                                    'pllc', 'pllc.', 'p.l.l.c', 'p.l.l.c.'  # US: Professional Limited Liability Company
                                    'llc', 'llc.', 'l.l.c', 'l.l.c.'  # US: Limited Liability Company
                                    'lp', 'lp.', 'l.p', 'l.p.' 'limited partnership',  # Limited Partnership
                                    'llp', 'llp.', 'l.l.p', 'l.l.p.' 'limited liability partnership',
                                    'lllp', 'lllp.', 'l.l.l.p', 'l.l.l.p.' 'limited liability limited partnership',
                                    'gmbh', 'gmbh.', 'gesellschaft mit beschränkter haftung',
                                    'gesmbh', 'gesmbh.', 'ges.m.b.H']

    @staticmethod
    def remove_incorporation_suffix2(company_name):
        tokens = re.split(';|,| ', company_name)
        num_tokens = len(tokens)
        for t in tokens:
            if t in AcureRateUtils.company_incoporation_symbols:
                t = ''
                break
        return 'something....'

    @staticmethod
    def remove_incorporation_suffix(company_name):
        for symbol in AcureRateUtils.company_incoporation_symbols:
            s = ' %s' % symbol
            if company_name.lower().endswith(s):
                i = company_name.lower().find(s)
                return company_name[:i]
            s = ',%s' % symbol
            if company_name.lower().endswith(s):
                i = company_name.lower().find(s)
                return company_name[:i]
            s = ', %s' % symbol
            if company_name.lower().endswith(s):
                i = company_name.lower().find(s)
                return company_name[:i]
        return company_name

    @staticmethod
    def clean_company_name(company_name):
        company_name_clean = AcureRateUtils.remove_parenthesized_content(company_name)
        company_name_clean = AcureRateUtils.remove_incorporation_suffix(company_name_clean)
        return company_name_clean

    @staticmethod
    def is_same_company__ignore_incoporation(company_str1, company_str2):
        if company_str1 is None or company_str2 is None:
            return False
        c1 = company_str1.lower().strip()
        c2 = company_str2.lower().strip()
        for symbol in AcureRateUtils.company_incoporation_symbols:
            s = ' %s' % symbol
            if c1.endswith(s):
                c1 = c1.replace(s, '')
            if c2.endswith(s):
                c2 = c2.replace(s, '')
        return c1 == c2

    @staticmethod
    def is_company_public(company):
        if company is None:
            return False
        return C.STOCK_SYMBOL in company['deduced'] and company['deduced'][C.STOCK_SYMBOL].strip() != ""

    @staticmethod
    def get_employees_range(num_employees):
        b = math.floor(math.log10(num_employees))
        lower = math.pow(10, b)
        higher = math.pow(10, b+1)
        range = "%d-%d" % (lower, higher)
        return range

    seniority_table = {
        C.RANGE_1_10: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL, T.SENIORITY_SVP, T.SENIORITY_VP, T.SENIORITY_DIRECTOR, T.ROLE_SITE_MANAGER],
        C.RANGE_10_100: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL, T.SENIORITY_SVP, T.SENIORITY_VP],
        C.RANGE_100_1000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL, T.SENIORITY_SVP, T.SENIORITY_VP],
        C.RANGE_1000_10000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL, T.SENIORITY_VP, T.SENIORITY_SVP, T.SENIORITY_EVP],
        C.RANGE_10000_100000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL, T.SENIORITY_VP, T.SENIORITY_SVP, T.SENIORITY_EVP]}

    @staticmethod
    def is_senior(company, title_line):
        # Need to determine if person is senior, dependant on company Size, IPO, Years, etc.
        employees_range = company[C.DEDUCED].get(C.EMPLOYEES_RANGE, C.RANGE_1_10)

        titles = AcureRateUtils.normalized_titles(title_line)
        for title, seniority, area in titles:
            if title in AcureRateUtils.seniority_table[employees_range] or seniority in AcureRateUtils.seniority_table[employees_range]:
                return True
        return False

    @staticmethod
    def is_founder(title):
        if title is None:
            return False
        return AcureRateUtils.examine_string(
            string=title,
            tokens_case_insensitive=['Co Founder', 'Co-Founder' 'Founding Member'],
            words_case_insensitive=['Founder', 'CoFounder'])

    @staticmethod
    def is_ceo(title):
        if title is None:
            return False
        return AcureRateUtils.examine_string(
            string=title,
            tokens_case_sensitive=['CEO'],
            tokens_case_insensitive=['c.e.o', 'chief executive officer', 'chief execution officer'],
            words_case_insensitive=['ceo'])

    @staticmethod
    def is_cfo(title):
        if title is None:
            return False
        return AcureRateUtils.examine_string(
            string=title,
            tokens_case_sensitive=['CFO'],
            tokens_case_insensitive=['c.f.o', 'chief finance officer', 'chief financial officer', 'chief financials officer',
                                     'senior financial analyst', 'senior financials analyst'],
            words_case_insensitive=['cfo'])

    @staticmethod
    def is_board_member(title):
        if title is None:
            return False

        return AcureRateUtils.examine_string(
            string=title,
            tokens_case_sensitive=[],
            tokens_case_insensitive=['chairman', 'chairman of the board', 'advisory board', 'senior advisor', 'board advisor', 'board member'],
            words_case_insensitive=[])

    @staticmethod
    def is_investor(title):
        if title is None:
            return False
        return AcureRateUtils.examine_string(
            string=title,
            #words_case_insensitive=['investor', 'angel', 'investment'])
            words_case_insensitive=['investor', 'angel'])

    @staticmethod
    def is_investment_lead(investment_info):
        if investment_info and 'lead' in investment_info.lower():
            return True
        return False

    @staticmethod
    def get_investment_round(investment_info):
        if investment_info is None:
            return None
        info = investment_info.lower()
        if 'pre-seed' in info or 'preseed' in info:
            return 'Pre-Seed'
        elif 'seed' in info:
            return 'Seed'
        elif 'series a' in info:
            return 'Series A'
        elif 'series b' in info:
            return 'Series B'
        elif 'series c' in info:
            return 'Series C'
        elif 'series d' in info:
            return 'Series D'
        elif 'series e' in info:
            return 'Series E'
        return None

    @staticmethod
    def parse_date_range(date_range):
        # Handle cases as the following:
        #   1992-Jul 2000
        #   January 2001 - 2010 (9 years)
        #   2007 (everything in parenthesis goes away....)
        #   1/6/2008 -Present
        #   Present

        start = None
        end = None
        current = None

        # Trim and Remove parenthesis
        date_range = AcureRateUtils.remove_parenthesized_content(date_range, to_the_right=True)
        date_range = date_range.strip().lower()

        # Detect delimiter (' - ' or '-')
        date_range = date_range.replace(' - ', '-')
        date_range = date_range.replace(' -', '-')
        date_range = date_range.replace('- ', '-')
        date_range = date_range.replace(' : ', '-')
        date_range = date_range.replace(':', '-')
        date_elements = date_range.split("-")

        # Check format of elements
        if len(date_elements) == 1:
            if date_elements[0] == 'present':
                current = True
            else:
                start = date_elements[0]
        elif len(date_elements) == 2:
            start = date_elements[0]
            if date_elements[1] == 'present':
                current = True
            else:
                end = date_elements[1]

        # Validate format
        try:
            if start:
                parsed = parse(start)
        except ValueError:
            start = None

        try:
            if end:
                parsed = parse(end)
        except ValueError:
            end = None

        return (start, end, current)

    @staticmethod
    def normalized_dob(dob_str):
        # Attempt to parse date: @@@
        # TODO: handle case where was unable to parse the date
        try:
            parsed_date = parse(dob_str)
        except ValueError as e:
            err = "Unable to parse dob str (%s)" % dob_str
            return err

        # Return as timestamp
        return parsed_date

        # Calculate how many years is this person living
        # what_year_is_it_now = 2016
        # person_was_born_at = parsed_date.year
        # years_on_the_planet = what_year_is_it_now - person_was_born_at
        # dob_str = "%s years old" % years_on_the_planet
        #
        # return dob_str

    @staticmethod
    def normalized_titles(title):
        # return a tuple: (normalized_title, seniority, area)
        if title is None or len(title.strip()) == 0:
            return []

        area = T.AREA_UNKNOWN
        role_tupples = []  # Holds tupples: (role, seniority, area)

        # Check areas/departments
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['R&D', 'RnD'],
                                         tokens_case_insensitive=['r&d', 'engineering', 'research & development',
                                                                  'research and development', 'research/development']):
            area = T.AREA_ENGINEERING
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['product marketing', 'product/marketing', 'product-marketing',
                                                                  'product management', 'consumer product', 'consumer products'],
                                         words_case_insensitive=['product', 'products']):
            area = T.AREA_PRODUCT
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['marketing', 'online marketing'],
                                         words_case_insensitive=['marketing']):
            area = T.AREA_MARKETING
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['sales', 'sales operation', 'sales operations', 'sales department'],
                                         words_case_insensitive=['sales']):
            area = T.AREA_SALES
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['biz dev', 'business development'],
                                         words_case_insensitive=['bizdev']):
            area = T.AREA_BUSINESS_DEVELOPMENT
        # TODO: add lots of areas....

        # Check C-Level positions
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CEO'],
                                         tokens_case_insensitive=['c.e.o', 'chief executive officer', 'chief execution officer'],
                                         words_case_insensitive=['ceo']):
            role_tupples.append((T.ROLE_CEO, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CFO'],
                                         tokens_case_insensitive=['c.f.o', 'chief finance officer',
                                                                  'chief financial officer', 'chief financials officer',
                                                                  'corporate finance', 'senior financial analyst'],
                                         words_case_insensitive=['cfo']):
            role_tupples.append((T.ROLE_CFO, T.SENIORITY_CLEVEL, T.AREA_FINANCE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['COO'],
                                         tokens_case_insensitive=['c.o.o', 'chief operation officer',
                                                                  'chief operations officer', 'chief operating officer'],
                                         words_case_insensitive=['coo']):
            role_tupples.append((T.ROLE_COO, T.SENIORITY_CLEVEL, T.AREA_OPERATIONS))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CRO'],
                                         tokens_case_insensitive=['c.r.o', 'chief revenue officer', 'chief revenues officer'],
                                         words_case_insensitive=['cro']):
            role_tupples.append((T.ROLE_CRO, T.SENIORITY_CLEVEL, T.AREA_FINANCE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CTO'],
                                         tokens_case_insensitive=['c.t.o', 'chief technology officer', 'chief technologies officer'],
                                         words_case_insensitive=['cto']):
            role_tupples.append((T.ROLE_CTO, T.SENIORITY_CLEVEL, T.AREA_ENGINEERING))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CMO'],
                                         tokens_case_insensitive=['c.m.o', 'chief marketing officer'],
                                         words_case_insensitive=['cmo']):
            role_tupples.append((T.ROLE_CMO, T.SENIORITY_CLEVEL, T.AREA_MARKETING))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CIO'],
                                         tokens_case_insensitive=['c.i.o', 'chief information officer', 'chief it officer'],
                                         words_case_insensitive=['cio']):
            role_tupples.append((T.ROLE_CIO, T.SENIORITY_CLEVEL, T.AREA_INFORMATION_TECHNOLOGY))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CISO'],
                                         tokens_case_insensitive=['c.i.s.o', 'chief information security officer', 'chief security officer'],
                                         words_case_insensitive=['ciso']):
            role_tupples.append((T.ROLE_CISO, T.SENIORITY_CLEVEL, T.AREA_INFORMATION_TECHNOLOGY))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CCO'],
                                         tokens_case_insensitive=['c.c.o', 'chief customer officer', 'chief customers officer',
                                                                  'chief commercial officer', 'chief commercials officer', 'chief comercial officer'],
                                         words_case_insensitive=['cco']):
            role_tupples.append((T.ROLE_CCO, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CSO'],
                                         tokens_case_insensitive=['c.s.o', 'chief strategy officer', 'chief strategies officer'],
                                         words_case_insensitive=['cso']):
            role_tupples.append((T.ROLE_CSO, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['CBO'],
                                         tokens_case_insensitive=['c.b.o', 'chief business officer'],
                                         words_case_insensitive=['cbo']):
            role_tupples.append((T.ROLE_CBO, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))

        # Check for architect roles:
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['chief architect'],
                                         words_case_insensitive=[]):
            role_tupples.append((T.ROLE_CHIEF_ARCHITECT, T.SENIORITY_SENIOR, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['senior architect'],
                                         words_case_insensitive=[]):
            role_tupples.append((T.ROLE_SENIOR_ARCHITECT, T.SENIORITY_SENIOR, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=[],
                                         tokens_case_insensitive=['architect', 'software architect', 'software systems architect', 'software system architect'],
                                         words_case_insensitive=[]):
            role_tupples.append((T.ROLE_ARCHITECT, T.SENIORITY_NONE, T.AREA_ENGINEERING))

        # Check General-Manager/Site-Manager
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['GM'],
                                         tokens_case_insensitive=['general manager']):
            role_tupples.append((T.ROLE_GENERAL_MANAGER, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['site manager']):
            role_tupples.append((T.ROLE_SITE_MANAGER, T.SENIORITY_CLEVEL, T.AREA_GENERAL_AND_ADMINISTRATIVE))

        # Check R&D Managerial positions
        if AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['SVP R&D', 'SVP RND'],
                                         tokens_case_insensitive=['svp r&d', 'svp rnd', 'svp of r&d', 'svp of rnd', 'svp engineering', 'svp of engineering']):
            role_tupples.append((T.ROLE_SVP_RND, T.SENIORITY_SVP, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['EVP R&D', 'EVP RND'],
                                         tokens_case_insensitive=['evp r&d', 'evp rnd', 'evp of r&d', 'evp of rnd', 'evp engineering', 'evp of engineering']):
            role_tupples.append((T.ROLE_EVP_RND, T.SENIORITY_EVP, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['VP R&D', 'VP RND'],
                                         tokens_case_insensitive=['vp r&d', 'vp rnd', 'vp of r&d', 'vp of rnd']):
            role_tupples.append((T.ROLE_VP_RND, T.SENIORITY_VP, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_sensitive=['VP of QA', 'VP QA', 'VP Quality Assurance'],
                                         tokens_case_insensitive=['vp of qa', 'vp qa', 'vp of quality assurance']):
            role_tupples.append((T.ROLE_VP_QA, T.SENIORITY_VP, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['director of engineering', 'engineering director', 'r&d director', 'director of r&d']):
            role_tupples.append((T.ROLE_DIRECTOR_RND, T.SENIORITY_DIRECTOR, T.AREA_ENGINEERING))
        # Check for Product positions
        elif AcureRateUtils.examine_string(title,
                                           tokens_case_insensitive=['senior development lead']):
            role_tupples.append((T.ROLE_RND, T.SENIORITY_SENIOR, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['product manager', 'product head', 'product lead', 'head of product', 'head of products', 'consumer product', 'consumer products']):
            role_tupples.append((T.ROLE_PRODUCT_HEAD, T.SENIORITY_SENIOR, T.AREA_PRODUCT))
        # Check for IT/DevOps positions
        elif AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['it manager', 'global it', 'dev ops manager', 'deployment manager']):
            role_tupples.append((T.ROLE_IT_HEAD, T.SENIORITY_SENIOR, T.AREA_INFORMATION_TECHNOLOGY))
        # Check for low-level engineering positions
        elif AcureRateUtils.examine_string(title,
                                           tokens_case_insensitive=['software engineer'],
                                           words_case_insensitive=['engineer', 'developer']):
            role_tupples.append((T.ROLE_ENGINEER, T.SENIORITY_NONE, T.AREA_ENGINEERING))
        elif AcureRateUtils.examine_string(title,
                                           tokens_case_insensitive=['qa engineer', 'quality assurance'],
                                           words_case_sensitive=['QA'],
                                           words_case_insensitive=['tester', 'testing']):
            role_tupples.append((T.ROLE_QA, T.SENIORITY_NONE, T.AREA_ENGINEERING))
        else:

            if AcureRateUtils.examine_string(title,
                                             tokens_case_insensitive=['senior vice president', 'senior vp'],
                                             words_case_insensitive=['svp']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_SVP_RND, T.SENIORITY_SVP, area))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_SVP, area))
            elif AcureRateUtils.examine_string(title,
                                             tokens_case_insensitive=['executive vice president', 'executive vp'],
                                             words_case_insensitive=['evp']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_EVP_RND, T.SENIORITY_EVP, area))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_EVP, area))
            elif AcureRateUtils.examine_string(title,
                                             tokens_case_insensitive=['vice president', 'vic president'],
                                             words_case_insensitive=['vp']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_VP_RND, T.SENIORITY_VP, area))
                elif area == T.AREA_SALES:
                    role_tupples.append((T.ROLE_VP_SALES, T.SENIORITY_VP, area))
                elif area == T.AREA_MARKETING:
                    role_tupples.append((T.ROLE_VP_MARKETING, T.SENIORITY_VP, area))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_VP, area))
            elif AcureRateUtils.examine_string(title,
                                             tokens_case_insensitive=['senior director', 'sr. director']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_DIRECTOR_RND, T.SENIORITY_SENIOR_DIRECTOR, area))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_SENIOR_DIRECTOR, area))
            elif AcureRateUtils.examine_string(title,
                                               words_case_insensitive=['director']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_DIRECTOR_RND, T.SENIORITY_DIRECTOR, area))
                elif area:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_DIRECTOR, area))
                elif title.strip().lower() == 'director':
                    role_tupples.append((T.ROLE_BOARD_MEMBER, T.SENIORITY_BOARD, T.AREA_BOARD))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_DIRECTOR, area))
            elif AcureRateUtils.examine_string(title,
                                               tokens_case_insensitive=['team lead', 'team leader', 'manager']):
                if area == T.AREA_ENGINEERING:
                    role_tupples.append((T.ROLE_TEAM_LEAD_RND, T.SENIORITY_SENIOR, area))
                else:
                    role_tupples.append((T.ROLE_UNKNOWN, T.SENIORITY_SENIOR, area))
            elif AcureRateUtils.examine_string(title,
                                             words_case_insensitive=['president']):
                role_tupples.append((T.ROLE_PRESIDENT, T.ROLE_PRESIDENT, T.AREA_GENERAL_AND_ADMINISTRATIVE))

        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['managing partner'],
                                         words_case_insensitive=['partner']):
            role_tupples.append((T.ROLE_BOARD_ADVISOR, T.SENIORITY_BOARD, T.AREA_BOARD))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['board advisor', 'board-advisor', 'board - advisor'],
                                         words_case_insensitive=['advisor']):
            role_tupples.append((T.ROLE_BOARD_ADVISOR, T.SENIORITY_BOARD, T.AREA_BOARD))
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['board chair', 'board chairman', 'chairman of the board', 'board-chairman', 'board - chairman'],
                                         words_case_insensitive=['chairman']):
            role_tupples.append((T.ROLE_BOARD_CHAIR, T.SENIORITY_BOARD, T.AREA_BOARD))
        elif AcureRateUtils.examine_string(title,
                                         words_case_insensitive=['board']):
            role_tupples.append((T.ROLE_BOARD_MEMBER, T.SENIORITY_BOARD, T.AREA_BOARD))

        # Check founders
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['Co Founder', 'Co-Founder' 'Founding Member'],
                                         words_case_insensitive=['Founder', 'CoFounder']):
            role_tupples.append((T.ROLE_FOUNDER, T.SENIORITY_FOUNDER, T.AREA_GENERAL_AND_ADMINISTRATIVE))

        # Check owners
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['Co Owner', 'Co-Owner'],
                                         words_case_insensitive=['Owner', 'CoOwner']):
            role_tupples.append((T.ROLE_OWNER, T.SENIORITY_OWNER, T.AREA_GENERAL_AND_ADMINISTRATIVE))

        # Reconcile special
        if (T.SENIORITY_DIRECTOR, T.SENIORITY_DIRECTOR, T.AREA_UNKNOWN) in role_tupples and (T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING) in role_tupples and (T.ROLE_DIRECTOR_RND, T.SENIORITY_DIRECTOR, T.AREA_ENGINEERING) not in role_tupples:
            role_tupples.append((T.ROLE_DIRECTOR_RND, T.SENIORITY_DIRECTOR, T.AREA_ENGINEERING))
            role_tupples.remove((T.SENIORITY_DIRECTOR, T.SENIORITY_DIRECTOR, T.AREA_UNKNOWN))
            role_tupples.remove((T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING))
        if (T.SENIORITY_VP, T.SENIORITY_VP, T.AREA_UNKNOWN) in role_tupples and (T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING) in role_tupples and (T.ROLE_VP_RND, T.SENIORITY_VP, T.AREA_ENGINEERING) not in role_tupples:
            role_tupples.append((T.ROLE_VP_RND, T.SENIORITY_VP, T.AREA_ENGINEERING))
            role_tupples.remove((T.SENIORITY_VP, T.SENIORITY_VP, T.AREA_UNKNOWN))
            role_tupples.remove((T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING))
        if (T.SENIORITY_SVP, T.SENIORITY_SVP, T.AREA_UNKNOWN) in role_tupples and (T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING) in role_tupples and (T.ROLE_SVP_RND, T.SENIORITY_SVP, T.AREA_ENGINEERING) not in role_tupples:
            role_tupples.append((T.ROLE_SVP_RND, T.SENIORITY_SVP, T.AREA_ENGINEERING))
            role_tupples.remove((T.SENIORITY_SVP, T.SENIORITY_SVP, T.AREA_UNKNOWN))
            role_tupples.remove((T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING))
        if (T.SENIORITY_EVP, T.SENIORITY_EVP, T.AREA_UNKNOWN) in role_tupples and (T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING) in role_tupples and (T.ROLE_EVP_RND, T.SENIORITY_EVP, T.AREA_ENGINEERING) not in role_tupples:
            role_tupples.append((T.ROLE_EVP_RND, T.SENIORITY_EVP, T.AREA_ENGINEERING))
            role_tupples.remove((T.SENIORITY_EVP, T.SENIORITY_EVP, T.AREA_UNKNOWN))
            role_tupples.remove((T.ROLE_RND, T.SENIORITY_UNKNOWN, T.AREA_ENGINEERING))

        return role_tupples

    @staticmethod
    def is_venture_partner(title):
        if AcureRateUtils.examine_string(title,
                                         tokens_case_insensitive=['venture partner', 'vc partner',
                                                                  'general partner', 'partner',
                                                                  'senior advisor', 'managing director']):
            return True

        return False

    @staticmethod
    def is_business(title):
        if title is None:
            return False
        formal_titles_exact_case = [
            "CEO", "C.E.O",
            "COO", "C.O.O",
            "CTO", "C.T.O",
            "CFO", "C.F.O",
            "CIO", "C.I.O",
            "CDO", "C.D.O",
            "CMO", "C.M.O",
            "CRO", "C.R.O",
            "CXO", "C.X.O",
            "CPO", "C.P.O",
            "CCO", "C.C.O",
            "CBO", "C.B.O",
            "VP", "EVP", "SVP",
            "GM"
        ]
        formal_titles = [
            "Chief Executive Officer", "Chief Execution Officer",
            "Chief Operations Officer", "Chief Operating Officer", #"Head of Operations",
            "Chief Technology Officer", "Chief Technologies Officer",
            "Chief Financial Officer",
            "Chief Information Officer",
            "Chief Marketing Officer", #"Head of Marketing",
            "Chief Revenue Officer",
            "Chief Product Officer",
            "Chief Business Officer",
            "Chief Commercial Officer", "Chief Commercials Officer",
            "Chief Customer Officer", "Chief Customers Officer",
            #"R&D Director", "Director of R&D", "Engineering Director", "Director of engineering",
            "Senior VP", "Senior Vice President",
            "Vice President",
            "Development Manager",
            "General Manager",
            "Site Manager",
            #"Senior Director", "Director", "Director of",
            "Managing Partner",
            "General Partner",
            "Partner",
            "President",
            "Chairman of the Board",
            "Chairman",
            "Board Member",
            "Board Observer",
            "Principal",
            "Advisor ", " Advisor"
            "Angel ", " Angel",
            "Investor",
            #"executive",
            "Owner ", " Owner"
            "cofounder", "co-founder", "co founder", "co-leader", "co leader"
            "Founder ", " Founder",
            "Founding ", " Founding"
        ]
        title_lower = title.lower()
        for t in formal_titles_exact_case:
            if t in title:
                return True
        for t in formal_titles:
            if t.lower() in title_lower:
                return True
        return False

    @staticmethod
    def examine_string(string, tokens_case_sensitive=None, tokens_case_insensitive=None, words_case_sensitive=None, words_case_insensitive=None):
        string_lower = string.lower()
        if tokens_case_sensitive is not None:
            for t in tokens_case_sensitive:
                if t in string:
                    return True
        if tokens_case_insensitive is not None:
            for t in tokens_case_insensitive:
                if t.lower() in string_lower:
                    return True
        if words_case_sensitive is not None:
            if AcureRateUtils.contains_words(string, words_case_sensitive):
                return True
        if words_case_insensitive is not None:
            if AcureRateUtils.contains_words(string_lower, [w.lower() for w in words_case_insensitive]):
                return True
        return False

    @staticmethod
    def get_domain(website):
        if website is None:
            return None
        parsed_uri = urlparse(website)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        domain = domain.replace("://www.", "://")
        try:
            domain = domain[domain.index("://") + 3:-1]
        except ValueError:
            domain = website
        return domain

    @staticmethod
    def get_url_last_path_element(url):
        # Extract the last path element of a url. Examples:
        # (a) https://www.crunchbase.com/organization/powermat  --> powermat
        # (b) https://www.crunchbase.com/organization/powermat/  --> powermat
        # (c) https://www.crunchbase.com/organization/powermat?utm_source=odm_csv&... --> powermat
        if url is None or len(url.strip()) == 0:
            return None
        if url.rfind("/") == len(url)-1:
            url = url[-1]
        k = url.find("?")
        if k > 0:
            url = url[:k]
        k = url.rfind("/")
        new_string = url[k + 1:]
        return new_string

    regions = {
       'il': ['israel', 'tel-aviv', 'tel aviv', 'jerusalem', 'haifa', 'hadera', 'herzliya', 'modiin'],
       'us_east': ['new york', 'new-york', 'boston', 'toronto', 'miami', 'albany'],
       'us_west': ['sunnyvale', 'newport beach', 'foster city', 'san antonio', 'menlo park', 'francescas',
                   'san francisco', 'san francisco bay area', 'beaverton', 'seattle', 'los altos', 'palo alto',
                   'mountain view', 'portland', 'silicon valley', 'los angeles', 'mills valley', 'san marcos'],
       'oceania': ['melbourne', 'sydney', 'australia', 'new-zealand', 'oakland'],
       'eu': ['london', 'paris', 'munich', 'zurich', 'milan', 'gibraltar'],
       'other': ['cyprus']
    }

    @staticmethod
    def geo_tagging_from_location_str(location_str):
        location_str_lower = location_str.lower()
        for region_name, region_tags in AcureRateUtils.regions.items():
            for t in region_tags:
                if t in location_str_lower:
                    return region_name
        return None

    @staticmethod
    def get_now_as_str():
        from time import gmtime, strftime
        str = strftime("%Y-%m-%d %H:%M:%S")
        return str

    def tail(f, n, offset=None):
        """Reads a n lines from f with an offset of offset lines.  The return
        value is a tuple in the form ``(lines, has_more)`` where `has_more` is
        an indicator that is `True` if there are more lines in the file.
        """
        avg_line_length = 74
        to_read = n + (offset or 0)

        while 1:
            try:
                f.seek(-(avg_line_length * to_read), 2)
            except IOError:
                # woops.  apparently file is smaller than what we want
                # to step back, go to the beginning instead
                f.seek(0)
            pos = f.tell()
            lines = f.read().splitlines()
            if len(lines) >= to_read or pos == 0:
                return lines[-to_read:offset and -offset or None], \
                       len(lines) > to_read or pos > 0
            avg_line_length *= 1.3