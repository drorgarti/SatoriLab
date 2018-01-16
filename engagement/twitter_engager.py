import tweepy
import time

from engagement.engager import Engager
from engagement.engagement_exception import EngagementException
from entities.acurerate_attributes import P, C
from entities.acurerate_job import AcureRateJob
from entities.acurerate_person import AcureRatePerson


class TwitterEngager(Engager):

    TWITTER_BASE_URL = 'http://twitter.com'

    EXTRACT_FF = False  # Should extract Friends/Followers ?

    CONSUMER_KEY = "POyfeLUUTusc4pxGktIn4wKm3"  # "DC0sePOBbQ8bYdC8r4Smg"
    CONSUMER_SECRET = "TmdfHSFjaEcIOAp601RubR936Jk8U4Kqn5whUFCJPIkZlnMTQv"

    cnsOauthToken = "4120134688-VsmFvqVhW5KHBtZlOJBsXJoquxpiAAswpIh9EXa"
    cnsOauthTokenSecret = "fVT4GNzFJYOidoPqOAFkxxjtgoK15BqNRIkop21l91V0H"


    def __init__(self):
        super().__init__()
        try:
            # Authenticate with Twitter
            auth = tweepy.OAuthHandler(self.CONSUMER_KEY, self.CONSUMER_SECRET)
            auth.set_access_token(self.cnsOauthToken, self.cnsOauthTokenSecret)
            self._api = tweepy.API(auth)
        except Exception as e:
            self.logger.error('Failed to authenticate with Twitter: %s', e)
        pass

    def __str__(self):
        return 'Twitter Engager'

    def __repr__(self):
        return 'Twitter Engager'

    def get_provider_name(self):
        return 'Twitter'

    def get_short_symbol(self):
        return 'tw'

    def get_api_key(self):
        return TwitterEngager.CONSUMER_KEY

    def set_enrich_key(self):
        # TODO: return twitter handle
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson':
            if P.TWITTER_URL in self.enriched_entity.deduced:
                self.enrich_key = self.enriched_entity.deduced[P.TWITTER_URL]
            else:
                self.enrich_key = self.enriched_entity.deduced[P.FULL_NAME]
        else:
            self.enrich_key = self.enriched_entity.deduced[C.TWITTER_URL]

    def enrich_person(self):

        # Extract Twitter screen name
        screen_name = self.enriched_entity.deduced.get(P.TWITTER_SCREEN_NAME, None)
        if not screen_name:
            url = self.enrich_key
            screen_name = self._extract_screenname_from_url(url)
        if not screen_name:  # If no screenname, search with Twitter using full name
            # Get all my job information to cross it against matches
            query = self.enriched_entity.deduced[P.FULL_NAME]
            possible_users = [user for user in tweepy.Cursor(self._api.search_users, q=query).items(10)]  # TODO: why 10?
            for u in possible_users:
                job = AcureRateJob.attempt_parse(u.description)
                if self.enriched_entity.fuzzy_match_on_jobs(job):
                    screen_name = u.screen_name
                    self.logger.info('Located Twitter screen_name from %s matches. User = %s', len(possible_users), str(u))
                    break

        if not screen_name:
            raise EngagementException('Unable to enrich via Twitter. No twitter url/screenname.')

        try:
            # Keep Screen Name
            self.set_data(P.TWITTER_SCREEN_NAME, screen_name)

            # Pull info from Twitter
            user = self._api.get_user(screen_name)

            # Get user information
            self.set_data(P.FULL_NAME, user.name)
            self.set_data(P.TWITTER_FOLLOWERS_COUNT, user.followers_count)
            self.set_data(P.TWITTER_FRIENDS_COUNT, user.friends_count)
            self.set_data(P.TWITTER_LISTED_COUNT, user.listed_count)
            self.set_data(P.TWITTER_FAVOURITES_COUNT, user.favourites_count)
            self.set_data(P.TWITTER_STATUSES_COUNT, user.statuses_count)
            self.set_data(P.TWITTER_ACCOUNT_CREATION_DATE, str(user.created_at))

            # Get description
            self.set_data(P.SHORT_DESCRIPTION, user.description)  # TODO: need to deal with URLs (grab them too)

            # Is Investor
            # TODO: refine this. We cannot rely only on the word 'investment'. Use NLTK.
            desc = user.description.lower()
            if 'investment' in desc or 'investor' in desc or 'investing' in desc:
                self.set_data(P.INVESTOR, True)
                self.set_data(P.INVESTOR_REASON, 'Twitter: %s:' % user.description)

            # Get location
            self.add_data(P.LOCATIONS, user.location)

            # Get photo
            self.add_data(P.PHOTOS, {P.PHOTO_URL: user.profile_image_url, P.PHOTO_SOURCE: 'twitter'})

            # Assimilate the display urls into the description
            desc = self._assemble_description(user.description, user.entities['description']['urls'])
            if desc:
                self.set_data(P.SHORT_DESCRIPTION, desc)  # TODO: need to deal with URLs (grab them too)

            # Get all the urls a person may add to his twitter profile
            the_urls = set()
            if 'description' in user.entities:
                for url in user.entities['description'].get('urls', []):
                    the_urls.add(url['expanded_url'])
            if 'url' in user.entities:
                for url in user.entities['url'].get('urls', []):
                    the_urls.add(url['expanded_url'])
            for url in the_urls:
                self.add_data(P.RELATED_URLS, {P.RELATED_URL_SOURCE: 'Twitter', P.RELATED_URL_VALUE: url})

            # user.entities['url']

            # Get "followers" (those who 'stock' a person) and "friends" (following) - person chose to do it
            if TwitterEngager.EXTRACT_FF:
                paged_users = self._get_followers(screen_name)
                self.set_data(P.TWITTER_FOLLOWERS, paged_users)
                paged_users = self._get_friends(screen_name)
                self.set_data(P.TWITTER_FRIENDS, paged_users)

        except Exception as e:
            self.logger.error('Error raised during enrichment via twitter. %s', e)

        return [P.TWITTER_SCREEN_NAME, P.DESCRIPTION]

    def _extract_screenname_from_url(self, url):
        url = url.replace('https://', 'http://')
        base = TwitterEngager.TWITTER_BASE_URL + '/'
        if not url.startswith(base):
            return None
        return url[len(base):]


    def _get_followers(self, user_id):
        paged_users = []
        for page in tweepy.Cursor(self._api.followers, id=user_id, count=100).pages():
            time.sleep(5)
            paged_users.append([user.screen_name for user in page])
        return paged_users


    def _get_friends(self, user_id):
        paged_users = []
        for page in tweepy.Cursor(self._api.friends, id=user_id, count=100).pages():
            time.sleep(5)
            paged_users.append([user.screen_name for user in page])
        return paged_users


    def _assemble_description(self, description_template, urls):
        temp = description_template
        offset = 0
        try:
            for url in urls:
                i0 = int(url['indices'][0]) - offset
                i1 = int(url['indices'][1]) - offset
                temp = temp[:i0] + url['display_url'] + temp[i1:]
                offset += len(url['url']) - len(url['display_url'])
        except Exception as e:
            temp = None
        return temp
        pass



