from engagement.engager import Engager
from engagement.engagement_exception import EngagementException

from entities.acurerate_attributes import P, C

from engagement.crunchbase_engager import CrunchBaseEngager
from selenium import webdriver

from utils.acurerate_utils import AcureRateUtils
from utils.email_util import EmailUtil


class CrunchBaseBotEngager(Engager):

    def __init__(self):
        super().__init__()

    def __repr__(self):
        return "CrunchBaseBot Engager"

    def get_provider_name(self):
        return "CrunchBaseBot"

    def get_short_symbol(self):
        return "cbb"

    def set_enrich_key(self):
        t = self.enriched_entity.__class__.__name__
        if t == 'AcureRatePerson':
            if P.CB_PERMALINK in self.enriched_entity.deduced:
                self.enrich_key = self.enriched_entity.deduced[P.CB_PERMALINK]
            elif P.FULL_NAME in self.enriched_entity.deduced:
                name = self.enriched_entity.deduced[P.FULL_NAME]
                self.enrich_key = CrunchBaseEngager.formalize_permalink(name)
            else:
                raise EngagementException("CrunchBaseBot - cannot engage - cannot generate enrich key for person. No permalink or name", t)
        elif t == 'AcureRateCompany':
            if C.CRUNCHBASE_PERMALINK in self.enriched_entity.deduced:
                self.enrich_key = self.enriched_entity.deduced[C.CRUNCHBASE_PERMALINK]
            elif C.NAME in self.enriched_entity.deduced:
                name = self.enriched_entity.deduced[C.NAME]
                self.enrich_key = CrunchBaseEngager.formalize_permalink(name)
            else:
                raise EngagementException("CrunchBaseBot - cannot engage - cannot generate enrich key for company. No permalink or name", t)
        else:
            raise EngagementException("CrunchBaseBot - cannot engage - cannot generate enrich key. Entity type: %s", t)

    def is_run_succesful(self, msg):
        return 'Failed to enrich' not in msg

    def enrich_person(self):
        permalink = self.enrich_key
        url = 'https://www.crunchbase.com/person/%s#/entity' % permalink

        #driver = webdriver.Firefox()
        driver = webdriver.Chrome(r'C:\Python353\browser_drivers\chromedriver')
        #driver.set_window_size(1120, 550)
        driver.implicitly_wait(11)  # seconds
        try:
            # Activate the driver
            driver.get(url)

            # If we got to here, keep the permalink and URL
            self.set_data(P.CB_PERMALINK, permalink)
            self.set_data(P.CRUNCHBASE_URL, url)

            # Get person name
            try:
                full_name = driver.find_element_by_id('profile_header_heading').text
                f, m, l = AcureRateUtils.tokenize_full_name(full_name)
                self.set_data(P.FIRST_NAME, f)
                self.set_data(P.LAST_NAME, l)
                if m:
                    self.set_data(P.MIDDLE_NAME, m)
                driver.implicitly_wait(2)  # seconds
            except:
                s = "Failed to enrich %s. Unable to locate name entity in page - %s - something went awry... dumping this crawl." % (permalink, url)
                raise EngagementException(s)

            # Get primary role
            try:
                content = driver.find_element_by_xpath('//dt[text()="Primary Role"]')
                role_str = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(P.PRIMARY_ROLE, role_str.replace('\n', ' '))
            except:
                pass

            # Get photo
            try:
                content = driver.find_element_by_class_name('logo-links-container')
                photo_url = content.find_element_by_css_selector("div > img").get_attribute("src")
                if "cb-default" not in photo_url:
                    self.add_data(P.PHOTOS, {P.PHOTO_URL: photo_url, P.PHOTO_SOURCE: 'crunchbase'})
            except:
                pass

            # Get dob
            try:
                content = driver.find_element_by_xpath('//dt[text()="Born:"]')
                dob = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(P.DOB, dob)
            except:
                pass

            # Get gender
            try:
                content = driver.find_element_by_xpath('//dt[text()="Gender:"]')
                gender = content.find_element_by_xpath("following-sibling::*[1]").text
                self.add_data(P.GENDER, gender)
            except:
                pass

            # Get location
            try:
                content = driver.find_element_by_xpath('//dt[text()="Location:"]')
                location = content.find_element_by_xpath("following-sibling::*[1]").text
                if location != "Unknown":
                    self.add_data(P.LOCATIONS, location)
            except:
                pass

            # Get web-site
            try:
                content = driver.find_element_by_xpath('//dt[text()="Website:"]').find_element_by_xpath("following-sibling::*[1]")
                website_url = content.find_element_by_css_selector('a').get_attribute("href")
                self.set_data(P.WEBSITE, website_url)
            except:
                pass

            # Get socials
            try:
                content = driver.find_element_by_xpath('//dt[text()="Social: "]').find_element_by_xpath("following-sibling::*[1]")
                social_links_elems = content.find_elements_by_tag_name('a')
                for e in social_links_elems:
                    social_type = e.get_attribute('data-icons')  # "facebook", "twitter", "linkedin", etc.
                    social_link = e.get_attribute('href')
                    if social_type == 'facebook':
                        self.set_data(P.FACEBOOK_URL, social_link)
                    elif social_type == 'twitter':
                        self.set_data(P.TWITTER_URL, social_link)
                    elif social_type == 'linkedin':
                        self.set_data(P.LINKEDIN_URL, social_link)
            except Exception as e:
                print(e)

            # Get person details (description)
            try:
                person_details_elem = driver.find_element_by_id('description')
                person_details_str = person_details_elem.text
                self.set_data(P.DESCRIPTION, person_details_str)
            except Exception as e:
                print(e)

            # Get current jobs
            try:
                for row in driver.find_elements_by_css_selector(".experiences .current_job"):
                    title = row.find_element_by_tag_name('h4').text
                    company = row.find_element_by_tag_name('h5').text
                    current_job = {P.JOB_CURRENT: True, P.JOB_TITLE: title, P.JOB_NAME: company}
                    self.add_data(P.JOBS, current_job)
            except Exception as e:
                print(e)

            # Get past jobs
            try:
                past_job_section = driver.find_element_by_css_selector(".experiences .past_job")
                for row in past_job_section.find_elements_by_css_selector(".info-row")[1:-1]:
                    cols = row.find_elements_by_css_selector(".cell")
                    started = cols[0].text
                    ended = cols[1].text
                    title = cols[2].text
                    company = cols[3].text
                    past_job = {P.JOB_STARTED: started, P.JOB_ENDED: ended, P.JOB_TITLE: title, P.JOB_NAME: company}
                    self.add_data(P.JOBS, past_job)
            except Exception as e:
                print(e)

            # Get advisory roles
            try:
                advisory_roles_section = driver.find_element_by_css_selector(".advisory_roles")
                for row in advisory_roles_section.find_elements_by_css_selector("li .info-block"):
                    company = row.find_element_by_tag_name('h4').text
                    role_started = row.find_elements_by_css_selector('h5')
                    role = role_started[0].text
                    started = role_started[1].text
                    advisory_job = {P.JOB_TITLE: role, P.JOB_NAME: company}
                    if started.strip() != '':
                        advisory_job[P.JOB_STARTED] = started
                    self.add_data(P.ADVISORY_JOBS, advisory_job)
            except Exception as e:
                print(e)

            # Get investments
            try:
                investments = []
                investors_tables = driver.find_elements_by_css_selector(".table.investors")
                if len(investors_tables) > 0:
                    investors_rows_elements = investors_tables[0].find_elements_by_tag_name("tr")
                    for investor_element in investors_rows_elements[1:]:  # we're skipping the header line
                        txt = investor_element.text
                        # We care only about personal investments, so we go in only if there's anywhere seed investment
                        if 'personal investment' in txt.lower():
                            cols = investor_element.find_elements_by_tag_name('td')
                            if cols[3].text == 'Personal Investment':
                                investments.append((cols[0].text, cols[1].text, cols[2].text))
                    self.set_data(P.INVESTMENTS, investments)
            except Exception as e:
                print(e)

            # Get education
            try:
                content = driver.find_element_by_class_name('education')
                education_elements = content.find_elements_by_css_selector("li > div")
                ed = {}
                for elem in education_elements:
                    institute_name = elem.find_element_by_css_selector('h4 > a').text
                    if institute_name != '':
                        ed[P.EDUCATION_INSTITUTE] = institute_name
                    degree = elem.find_element_by_css_selector('h5').text
                    if degree != '':
                        ed[P.EDUCATION_DEGREE] = degree
                    years = elem.text.replace(institute_name, '').replace(degree, '').strip()
                    if years != '':
                        ed[P.EDUCATION_YEARS] = years
                    self.add_data(P.EDUCATIONS, ed)
            except:
                pass

        except Exception as e:
            raise e

        driver.close()
        return [P.FULL_NAME]

    def enrich_company(self):

        company_name = self.enriched_entity.deduced.get(C.NAME, None)
        if company_name is None:
            self.logger.warning('Unable to enrich company. No name detected in entity: %s', self.enriched_entity)
            return

        # If there's a permalink, use it, otherwise try creating one
        if C.CRUNCHBASE_URL in self.enriched_entity.deduced:
            url = self.enriched_entity.deduced[C.CRUNCHBASE_URL]
            if url.find('/organization') == 0:
                url = 'https://www.crunchbase.com' + url
            permalink = AcureRateUtils.get_url_last_path_element(url)
        else:
            permalink = self.enriched_entity.deduced.get(C.CRUNCHBASE_PERMALINK,
                                                         CrunchBaseEngager.formalize_permalink(company_name))
            url = 'https://www.crunchbase.com/organization/%s#/entity' % permalink

        #driver = webdriver.Firefox()
        driver = webdriver.Chrome(r'C:\Python353\browser_drivers\chromedriver')
        driver.implicitly_wait(20)  # seconds
        try:
            # Activate the driver
            driver.get(url)

            # If we got to here, keep the permalink
            self.set_data(C.CRUNCHBASE_PERMALINK, permalink)

            # Get company name
            try:
                name = driver.find_element_by_id('profile_header_heading').text
                self.set_data(C.NAME, name)
                driver.implicitly_wait(2)  # seconds
            except:
                # TODO: there should be a smarter way to understand we got 404...
                s = "Failed to enrich %s. Unable to locate name entity in page - %s - something went awry... dumping this crawl." % (company_name, url)
                raise EngagementException(s)

            # Get company logo
            try:
                content = driver.find_element_by_class_name('logo-links-container')
                logo_url = content.find_element_by_css_selector("div > img").get_attribute("src")
                self.add_data(C.LOGOS, {C.LOGO_URL: logo_url, C.LOGO_SOURCE: 'crunchbase'})
            except:
                pass

            # Get overview stats (acquisitions, total funds, etc.)
            try:
                stats = driver.find_element_by_class_name('overview-stats').text
                if stats.strip() != "":
                    self.set_data(C.STATS, stats)
                    stats_lower = stats.replace('\n', ' ').lower()
                    if 'acquired by' in stats_lower and stats_lower.find(' on ') > 0:
                        acquiring_company = stats[stats_lower.find('acquired by')+12:stats_lower.find(' on ')]
                        self.set_data(C.ACQUIRED_BY, acquiring_company)
                        #tokens = stats.split('\n')
                        #self.set_data(C.ACQUIRED_BY, tokens[2])

            except:
                pass

            # Get headquarters
            try:
                content = driver.find_element_by_xpath('//dt[text()="Headquarters:"]')
                headquarters = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(C.HEADQUARTERS, headquarters)
            except:
                pass

            # Get description
            try:
                content = driver.find_element_by_xpath('//dt[text()="Description:"]')
                description = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(C.DESCRIPTION, description)
            except:
                pass

            # Get founders
            try:
                founders = []
                content = driver.find_element_by_xpath('//dt[text()="Founders:"]').find_element_by_xpath("following-sibling::*[1]")
                founders_elements = content.find_elements_by_css_selector('a')
                for f in founders_elements:
                    name = f.get_attribute("data-name")
                    permalink = f.get_attribute("data-permalink")
                    image = f.get_attribute("data-image")
                    founders.append(name)
                    #founders.append((name, permalink, image))
                self.set_data(C.FOUNDERS, founders)
            except Exception as e:
                print(e)

            # Get categories
            try:
                content = driver.find_element_by_xpath('//dt[text()="Categories:"]')
                categories = content.find_element_by_xpath("following-sibling::*[1]").text
                for c in categories.split(","):
                    self.add_data(C.CATEGORIES, c)
            except:
                pass

            # Get web-site
            try:
                content = driver.find_element_by_xpath('//dt[text()="Website:"]').find_element_by_xpath("following-sibling::*[1]")
                website_url = content.find_element_by_css_selector('a').get_attribute("href")
                self.set_data(C.WEBSITE, website_url)
            except:
                pass

            # Get socials
            try:
                content = driver.find_element_by_xpath('//dt[text()="Social: "]').find_element_by_xpath("following-sibling::*[1]")
                social_links_elems = content.find_elements_by_tag_name('a')
                for e in social_links_elems:
                    social_type = e.get_attribute('data-icons')  # "facebook", "twitter", etc.
                    social_link = e.get_attribute('href')
                    if social_type == 'facebook':
                        self.set_data(C.FACEBOOK_URL, social_link)
                    elif social_type == 'twitter':
                        self.set_data(C.TWITTER_URL, social_link)
            except Exception as e:
                print(e)

            # Get founding year
            try:
                content = driver.find_element_by_xpath('//dt[text()="Founded:"]')
                founding_year = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(C.FOUNDING_YEAR, founding_year)
            except:
                pass

            # Get contact email - for emails-domain info
            try:
                content = driver.find_element_by_xpath('//dt[text()="Contact:"]')
                contact_info = content.find_element_by_xpath("following-sibling::*[1]").text
                tokens = contact_info.split(' ')  # contact info may be structured:  email@support.domain | Telephone
                email_domain = EmailUtil.get_email_domain_part(tokens[0])
                if email_domain and len(email_domain) > 0:
                    self.add_data(C.EMAIL_DOMAINS, email_domain)
            except:
                pass

            # Get aliases
            try:
                content = driver.find_element_by_xpath('//dt[text()="Aliases:"]')
                aliases = content.find_element_by_xpath("following-sibling::*[1]").text
                for a in aliases.split(", "):
                    self.add_data(C.ALIASES, a)
            except:
                pass

            # Get company type
            try:
                content = driver.find_element_by_xpath('//dt[text()="Type:"]')
                type_str = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(C.INVESTMENT_COMPANY_TYPE, type_str)
            except:
                pass

            # Get sectors (in case it's investor company)
            try:
                content = driver.find_element_by_xpath('//dt[text()="Sectors:"]')
                sectors_str = content.find_element_by_xpath("following-sibling::*[1]").text
                for c in sectors_str.split(", "):
                    self.add_data(C.CATEGORIES, c)
            except:
                pass

            # Get Investment Size (in case it's investor company)
            try:
                content = driver.find_element_by_xpath('//dt[text()="Investment Size:"]')
                investments_size_str = content.find_element_by_xpath("following-sibling::*[1]").text
                self.set_data(C.INVESTMENTS_RANGE, investments_size_str.replace(" ",""))
            except:
                pass

            # Get investments regions (in case it's investor company)
            try:
                content = driver.find_element_by_xpath('//dt[text()="Regions:"]')
                investments_regions_str = content.find_element_by_xpath("following-sibling::*[1]").text
                for r in investments_regions_str.split(", "):
                    self.add_data(C.INVESTMENT_REGIONS, r)
            except:
                pass

            # Get employees range
            try:
                content = driver.find_element_by_xpath('//dt[text()="Employees:"]')
                employees_range_str = content.find_element_by_xpath("following-sibling::*[1]").text
                i = employees_range_str.find('None found')
                if i < 0:
                    self.set_data(C.EMPLOYEES_RANGE, employees_range_str.replace(" ", ""))
                elif i > 0:
                    self.set_data(C.EMPLOYEES_RANGE, employees_range_str.replace(" ", "")[:employees_range_str.find("|")-1])
            except:
                pass

            # Get investors
            try:
                investors = []
                investors_tables = driver.find_elements_by_css_selector(".table.investors")
                if len(investors_tables) > 0:
                    investors_rows_elements = investors_tables[0].find_elements_by_tag_name("tbody")
                    for investor_element in investors_rows_elements:  # skip the header row of the table
                        rows = investor_element.find_elements_by_tag_name("tr")
                        for row in rows:
                            cols = row.find_elements_by_tag_name("td")
                            investor_permalink = ''
                            if len(cols) == 3:
                                investor_name = cols[0].text
                                round = cols[1].text
                                partner = cols[2].text
                                investor_permalink = cols[0].find_element_by_class_name("follow_card").get_attribute('data-permalink')
                            elif len(cols) == 2:
                                round = cols[0].text
                                partner = cols[1].text
                            if "/organization" in investor_permalink:
                                investor_type = "organization"
                            else:
                                investor_type = "person"
                            if 'Seed' in round or 'Angel' in round:
                                str = "%s / %s" % (partner, round)
                                investors.append((investor_name, investor_type, str))
                            else:
                                str = "%s / %s" % (partner, round)
                                investors.append((investor_name, investor_type, str))
                    if len(investors) > 0:
                        self.set_data(C.INVESTORS, investors)
            except Exception as e:
                print(e)

            # TODO: get Acquisitions

            # Get current team
            current_team = []
            try:
                people_table = driver.find_elements_by_class_name('people')
                if len(people_table) > 0:
                    # TODO: get the person title - we don't want developers here...
                    people_rows_element = people_table[1].find_elements_by_css_selector("li")
                    for person in people_rows_element:
                        name_element = person.find_element_by_css_selector("h4 a")
                        name = name_element.get_attribute('data-name')
                        permalink = name_element.get_attribute('data-permalink')
                        title_element = person.find_element_by_css_selector("h5")
                        title = title_element.text
                        image = person.find_element_by_css_selector("span a img").get_attribute("src")
                        current_team.append(name)
                        #current_team.append((name, permalink, title, image))
            except Exception as e:
                print(e)

            # Get past team
            try:
                people_table = driver.find_elements_by_class_name('past_people')
                if len(people_table) > 0:
                    # TODO: get the person title - we don't want developers here...
                    people_rows_element = people_table[0].find_elements_by_css_selector("li")
                    for person in people_rows_element:
                        name_element = person.find_element_by_css_selector("h4 a")
                        name = name_element.get_attribute('data-name')
                        #permalink = name_element.get_attribute('data-permalink')
                        #title_element = person.find_element_by_css_selector("h5")
                        #title = title_element.text
                        #image = person.find_element_by_css_selector("span a img").get_attribute("src")
                        current_team.append(name)
                        #current_team.append((name, permalink, title, image))
            except Exception as e:
                print(e)

            # Store past & current team
            if len(current_team) > 0:
                self.set_data(C.TEAM, current_team)

            # Get board members and advisors
            try:
                advisors = []
                advisors_table = driver.find_elements_by_css_selector('.base.no-data.advisors')
                if len(advisors_table) == 0:
                    advisors_table = driver.find_elements_by_css_selector('.base.advisors')
                    if len(advisors_table) > 0:
                        advisors_rows_elements = advisors_table[0].find_elements_by_css_selector("h4 a")
                        for advisor_element in advisors_rows_elements:
                            name = advisor_element.get_attribute('data-name')
                            permalink = advisor_element.get_attribute('data-permalink')
                            # TODO: check that investors is person and not organization
                            advisors.append(name)
                            #advisors.append((name, permalink))
                    if len(advisors) > 0:
                        self.set_data(C.ADVISORS, advisors)
            except Exception as e:
                print(e)

        except Exception as e:
            raise e

        driver.close()
        return [C.NAME]

