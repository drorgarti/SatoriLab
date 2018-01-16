class P:

    DEDUCED = 'deduced'

    AID = '_aid'
    USER_ID = 'user_id'

    MATCH_SCORE = 'match_score'

    ATTRIBUTION_ID = "attribution_id"
    ATTRIBUTION_NAME = "attribution_name"

    # Administrative information
    FIRST_NAME = 'first_name'
    MIDDLE_NAME = 'middle_name'
    LAST_NAME = 'last_name'
    FULL_NAME = 'full_name'
    CNAM = 'cnam'
    SUFFIX = 'suffix'
    PREFIX = 'prefix'
    EMAIL = 'email'
    EMAILS = 'emails'
    PHONE = 'phone'
    PHONES = 'phones'
    GENDER = 'gender'
    SHORT_DESCRIPTION = 'short_description'
    DESCRIPTION = 'description'
    DIRECT_MANAGER = 'direct_manager'

    # Usernames
    USERNAMES = 'usernames'
    USERNAME_SITE = 'username_site'
    USERNAME_VALUE = 'username_value'

    # Education
    EDUCATIONS = 'educations'
    EDUCATION_DEGREE = 'education_degree'
    EDUCATION_SUBJECT = 'education_subject'
    EDUCATION_INSTITUTE = 'education_institute'
    EDUCATION_YEARS = 'education_years'

    # Jobs
    JOBS = 'jobs'
    JOB_TITLE = 'job_title'
    JOB_NAME = 'job_name'

    # Job Roles
    JOB_ROLES = 'job_roles'
    JOB_ROLE = 'job_role'
    JOB_SENIORITY = 'job_seniority'
    JOB_AREA = 'job_area'

    JOB_STARTED_BC = 'start_date'  # Backward compatibility
    JOB_ENDED_BC = 'end_date'      # Backward compatibility
    JOB_STARTED = 'started_on'
    JOB_ENDED = 'ended_on'
    JOB_CURRENT = 'job_current'
    JOB_PRIMARY = 'primary_job'
    PRIMARY_ROLE = 'primary_role'

    # References/Referrals
    REFERER_AID = 'referer_aid'
    REFERER_NAME = 'referer_name'
    REFERER_REVIEW = 'referer_review'
    REFERER_SOURCE = 'referer_source'
    REFERER_ANGELLIST_URL = 'referer_angellist_url'
    REFERENCES = 'references'

    # Advisory Jobs
    ADVISORY_JOBS = 'advisory_jobs'

    DOB = 'dob'
    ACCREDITED_JOBS = 'accredited_jobs'
    ACCREDITED_JOBS_2 = 'accredited_jobs_2'
    ACCREDITED_COMPANIES = 'accredited_companies'
    UNRECOGNIZED_COMPANIES = 'unrecognized_companies'
    LOCATIONS = 'location'
    GEO_TAG = 'geo_tag'
    WEBSITE = 'website'

    # CrunchBase Related
    CB_PERMALINK = 'cb_permalink'
    CB_URL = 'cb_url'

    # Bloomberg Related
    BLOOMBERG_ID = 'bloomberg_id'
    BLOOMBERG_URL = 'bloomberg_url'

    # Images
    PHOTOS = 'photos'
    PHOTO_SOURCE = 'source'
    PHOTO_URL = 'url'
    PHOTO_SELECTED = 'photo_selected'

    # Socials Related
    GOOGLEPLUS_URL = 'googleplus_url'
    FACEBOOK_URL = 'facebook_url'
    TWITTER_URL = 'twitter_url'
    GRAVATAR_URL = 'gravatar_url'
    FOURSQUARE_URL = 'foursquare_url'
    PINTEREST_URL = 'pinterest_url'
    KLOUT_URL = 'klout_url'
    TWITTER_SCREEN_NAME = 'twitter_screen_name'

    TWITTER_ACTIVITY = 'twitter_activity'

    TWITTER_FOLLOWERS_COUNT = 'twitter_followers_count'
    TWITTER_FOLLOWERS = 'twitter_followers'

    TWITTER_FRIENDS_COUNT = 'twitter_friends_count'  # Following
    TWITTER_FRIENDS = 'twitter_friends'

    TWITTER_LISTED_COUNT = 'twitter_listed_count'
    TWITTER_FAVOURITES_COUNT = 'twitter_favourites_count'  # Likes
    TWITTER_STATUSES_COUNT = 'twitter_statuses_count'  # tweets
    TWITTER_ACCOUNT_CREATION_DATE = 'twitter_account_creation_date'

    LINKEDIN_URL = 'linkedin_url'
    CRUNCHBASE_URL = 'crunchbase_url'
    ANGELLIST_URL = 'angellist_url'
    ANGELLIST_ID = 'angellist_id'
    BLOG_URL = 'blog_url'

    # Related URLs
    RELATED_URL_SOURCE = 'related_url_source'
    RELATED_URL_VALUE = 'related_url_value'
    RELATED_URL_DISPLAY = 'related_url_display'
    RELATED_URLS = 'related_urls'

    # Business related
    CEO = 'ceo'
    FOUNDER = 'founder'
    EXITS = 'exits'

    INVESTOR = 'investor'
    INVESTOR_REASON = 'investor_reason'
    INVESTMENTS = 'investments'
    INVESTMENT_CATEGORIES = 'categories'
    RELATED_INVESTORS = 'related_investors'
    RELATED_VCS = 'related_vcs'

    BUSINESS = 'business'
    BUSINESS_REASON = 'business_reason'

    NUMBER_MATCHES = 'number_matches'

class R:

    WORKED_AT = "worked_at"
    ADVISOR_AT = "advisor_at"
    CONTACT_OF = "contact_of"
    INVESTOR_AT = "investor_at"
    FOUNDER_OF = "founder_of"

    INVESTOR_BC = "investor"  # backward compatability
    FOUNDER_OF_BC = "founder"    # backward compatability
    ADVISOR_AT_BC = "board-member"  # backward compatability
    PAST_TEAM_BC = "past-team"  # backward compatability


class C:
    RANGE_1_10 = '1-10'
    RANGE_10_100 = '10-100'
    RANGE_100_1000 = '100-1000'
    RANGE_1000_10000 = '1000-10000'
    RANGE_10000_100000 = '10000-100000'

    DEDUCED = 'deduced'

    # Administrative information
    NAME = 'name'
    ALIASES = 'aliases'
    WEBSITE = 'website'
    DOMAIN = 'domain'
    EMAIL_DOMAINS = 'email_domains'
    EMAIL_FORMATS = 'email_formats'
    STATS = 'stats'
    ADDRESSES = 'addresses'
    PHONES = 'phones'
    HEADQUARTERS = 'headquarters'
    DESCRIPTION = 'description'
    SHORT_DESCRIPTION = 'short_description'
    ORGANIZATION_TYPE = 'organization_type'
    STOCK_SYMBOL = 'stock_symbol'
    KEYWORDS = 'keywords'
    EMPLOYEES_NUMBER = 'employees_number'
    EMPLOYEES_RANGE = 'employees_range'
    PRIMARY_ROLE = 'primary_role'
    SECTOR = 'sector'
    BUSINESS_MODEL = 'business model'
    FUNDING_STAGE = 'funding_stage'
    PRODUCT_STAGE = 'product_stage'

    # Organization types
    ORGANIZATION_TYPE_COMPANY = 'company'
    ORGANIZATION_TYPE_GOVERNMENT = 'government'
    ORGANIZATION_TYPE_MILITARY = 'military'
    ORGANIZATION_TYPE_VENTURE_CAPITAL = 'venturecapital'
    ORGANIZATION_TYPE_ACADEMY = 'academy'
    ORGANIZATION_TYPE_SCHOOL = 'school'

    # Images
    LOGOS = 'logos'
    LOGO_SOURCE = 'source'
    LOGO_URL = 'url'  # TODO: rename this to logo_url - may be very painful on system to refactor....
    LOGO_SELECTED = 'logo_selected'
    IMAGE_URL = 'image_url'

    # Socials Related
    FACEBOOK_URL = 'facebook_url'
    TWITTER_URL = 'twitter_url'
    TWITTER_SCREEN_NAME = 'twitter_screen_name'
    LINKEDIN_URL = 'linkedin_url'
    CRUNCHBASE_URL = 'crunchbase_url'
    CRUNCHBASE_PERMALINK = 'cb_permalink'
    ANGELLIST_URL = 'angellist_url'
    STARTUPNATIONCENTRAL_URL = 'startupnationcentral_url'
    BLOOMBERG_URL = 'bloomberg_url'

    # Acquisition Related
    ACQUIRED = 'acquired'
    ACQUIRED_BY = 'acquired_by'
    ACQUIRED_PRICE = 'acquiry_price'
    ACQUIRED_METHOD = 'acquiry_method'
    ACQUIRED_DATE = 'acquiry_date'

    ACQUISITIONS = 'acquisitions'

    # Related People
    FOUNDERS = 'founders'
    TEAM = 'team'
    ADVISORS = 'advisors'
    INVESTORS = 'investors'
    CATEGORIES = 'categories'
    MARKETS = 'markets'
    FOUNDING_YEAR = 'founding_year'
    RELATED_INVESTORS = 'related_investors'
    RELATED_VCS = 'related_vcs'

    # Investment company information
    INVESTMENT_COMPANY_TYPE = 'investment_company_type'
    INVESTMENTS_RANGE = 'investments_range'
    INVESTMENT_REGIONS = 'investment_regions'
    TOTAL_FUNDING = 'total_funding'
    INVESTMENT_TYPES = 'investment_types'  # pre-seed, seed, seed-extension, series a, series b, ....
    UNRECOGNIZED_PEOPLE = 'unrecognized_people'
    UNRECOGNIZED_COMPANIES = 'unrecognized_companies'
    PORTFOLIO_COMPANIES = 'portfolio_companies'

class I:

    STAGE_PRE_SEED = 'pre-seed'
    STAGE_SEED = 'seed'
    STAGE_SERIES_A = 'series-a'
    STAGE_SERIES_B = 'series-b'
    STAGE_SERIES_C = 'series-c'
    STAGE_SERIES_D = 'series-d'

    APPROACH_INITIAL_CUSTOMERS = 'initial-customers'
    APPROACH_GREAT_TEAM_2_TIME = 'great-team-2nd-time'
    APPROACH_TECHNOLOGY_PROOF = 'technology-proof'
    APPROACH_PROTOTYPE = 'prototype'
    APPROACH_MIN_300K_YEARLY_REV = 'min-300k-yearly-rev'
    APPROACH_MIN_1M_YEARLY_REV = 'min-1m-yearly-rev'
    APPROACH_MIN_5M_YEARLY_REV = 'min-5m-yearly-rev'

    ENTITY_ANGEL = 'angel'
    ENTITY_ANGEL_CLUB = 'angel-club'
    ENTITY_MICRO_VC = 'micro-vc'
    ENTITY_VC = 'vc'
    ENTITY_CORP_VC = 'corp-vc'
    ENTITY_INCUBATOR = 'incubator'
    ENTITY_ACCELERATOR = 'accelerator'
    ENTITY_FAMILY_OFFICE = 'family-office'
    ENTITY_GROWTH_FUND = 'growth-fund'


class T:

    # ------------------------
    # Job roles
    # ------------------------

    # Roles - C-Level
    ROLE_CEO = 'CEO'
    ROLE_CFO = 'CFO'
    ROLE_COO = 'COO'
    ROLE_CXO = 'CXO'
    ROLE_CDO = 'CDO'
    ROLE_CMO = 'CMO'
    ROLE_CRO = 'CRO'
    ROLE_CTO = 'CTO'
    ROLE_CIO = 'CIO'
    ROLE_CISO = 'CISO'
    ROLE_CPO = 'CPO'
    ROLE_CCO = 'CCO'
    ROLE_CSO = 'CSO'
    ROLE_CBO = 'CBO'

    # Roles - Engineering
    ROLE_EVP_RND = 'EVP R&D'
    ROLE_SVP_RND = 'SVP R&D'
    ROLE_VP_RND = 'VP R&D'
    ROLE_VP_QA = 'VP QA'
    ROLE_DIRECTOR_RND = 'Director of R&D'
    ROLE_TEAM_LEAD_RND = 'R&D Team Lead'
    ROLE_CHIEF_SCIENTIST = 'Chief Scientist'
    ROLE_CHIEF_ARCHITECT = 'Chief Architect'
    ROLE_SENIOR_ARCHITECT = 'Senior Architect'
    ROLE_ARCHITECT = 'Architect'
    ROLE_ENGINEER = 'Engineer'

    ROLE_QA_HEAD = 'QA Manager'
    ROLE_QA_ENGINEER = 'QA Engineer'
    ROLE_QA = 'QA'

    # Sales
    ROLE_VP_SALES = "Head of Sales"

    # Marketing
    ROLE_VP_MARKETING = "VP Marketing"

    # Roles - Product
    ROLE_VP_PRODUCT = "VP Product"
    ROLE_PRODUCT_HEAD = 'Head of Product'

    # Roles - IT/DevOps
    ROLE_IT_HEAD = 'IT Manager'
    ROLE_DEV_OPS_HEAD = 'DevOps Manager'

    # Special Roles
    ROLE_PRESIDENT = 'President'
    ROLE_OFFICER = 'Officer'

    # Other
    ROLE_RND = 'RND'
    ROLE_SITE_MANAGER = 'Site Manager'
    ROLE_GENERAL_MANAGER = 'General Manager'

    # Board positions: Chairman, Board Member, Board Advisor
    ROLE_BOARD_CHAIR = 'Board Chair'
    ROLE_BOARD_MEMBER = 'Board Member'
    ROLE_BOARD_ADVISOR = 'Board Advisor'

    # Role - Founder / Owner
    ROLE_FOUNDER = 'Founder'
    ROLE_OWNER = 'Owner'

    ROLE_UNKNOWN = 'Unknown'

    # Job Seniorities
    SENIORITY_FOUNDER = "Founder"
    SENIORITY_OWNER = "Owner"
    SENIORITY_CLEVEL = "C-Level"
    SENIORITY_BOARD = "Board"
    SENIORITY_EVP = 'Executive Vice President'
    SENIORITY_SVP = 'Senior Vice President'
    SENIORITY_VP = 'Vice President'
    SENIORITY_SENIOR_DIRECTOR = "Senior Director"
    SENIORITY_DIRECTOR = 'Director'  # Director - managing few groups.
    SENIORITY_SENIOR = 'Senior'      # Team Lead, IT Manager, Group Manager, etc.
    SENIORITY_NONE = 'Not Senior'    # Developer, Engineer, etc.
    SENIORITY_UNKNOWN = 'Unknown'

    # Job Areas
    #EXECUTIVE = 'Executive'
    AREA_BOARD = 'Board'
    AREA_GENERAL_AND_ADMINISTRATIVE = 'G&A'
    AREA_COMMUNICATIONS = 'Communications'
    AREA_CONSULTING = 'Consulting'
    AREA_CUSTOMER_SERVICE = 'Customer Service'
    AREA_EDUCATION = 'Education'
    AREA_ENGINEERING = 'Engineering'
    AREA_FINANCE = 'Finance'
    AREA_HEALTH_PROFESSIONAL = 'Health Professional'
    AREA_HUMAN_RESOURCES = 'Human Resources'
    AREA_INFORMATION_TECHNOLOGY = 'Information Technology'
    AREA_LEGAL = 'Legal'
    AREA_MARKETING = 'Marketing'
    AREA_OPERATIONS = 'Operations'
    AREA_PRODUCT = 'Product'
    AREA_PUBLIC_RELATIONS = 'Public Relations'
    AREA_REAL_ESTATE = 'Real Estate'
    AREA_RECRUITING = 'Recruiting'
    AREA_RESEARCH = 'Research'
    AREA_SALES = 'Sales'
    AREA_BUSINESS_DEVELOPMENT = 'Business Development'

    AREA_UNKNOWN = 'Unknown'

    TITLE_UNKNOWN = 'Unknown'

    ROLE_ENTITY_UNKNOWN = {P.JOB_TITLE: TITLE_UNKNOWN, P.JOB_ROLE: SENIORITY_UNKNOWN, P.JOB_AREA: AREA_UNKNOWN}


class G:

    LABEL_PERSON = 'Person'
    LABEL_COMPANY = 'Company'
    LABEL_USER = 'User'
    LABEL_EDUCATION = 'Education'
    LABEL_CEO = 'Ceo'

    RELATION_LABEL_IN_CLIQUE = 'IN_CLIQUE'
    RELATION_LABEL_INVESTS_IN = 'INVESTS_IN'
    RELATION_LABEL_CONTACT = 'CONTACT'
    RELATION_LABEL_LINKEDIN_FRIEND = 'LINKEDIN_FRIEND'
    RELATION_LABEL_ACQUIRED_BY = 'ACQUIRED_BY'
    RELATION_LABEL_EMPLOYEE_OF = 'EMPLOYEE_OF'
    RELATION_LABEL_ADVISOR_AT = 'ADVISOR_AT'
    RELATION_LABEL_FOUNDER_OF = 'FOUNDER_OF'
    RELATION_LABEL_WORKED_WITH = 'WORKED_WITH'
    RELATION_LABEL_STUDIED_AT = 'STUDIED_AT'
    RELATION_LABEL_TWITTER_FRIEND = 'TWITTER_FRIEND'
    RELATION_LABEL_REFERENCED = 'REFERENCED'

class LISTS:

    TRIGGERING_PROPERTIES = [P.FULL_NAME, C.NAME, C.DOMAIN]
