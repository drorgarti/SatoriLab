import datetime
from dateutil.parser import parse

from entities.acurerate_attributes import P, C, T
from utils.acurerate_utils import AcureRateUtils


class AcureRateJob:
    MIN_YEAR = 1970
    job_name = None
    job_roles = None

    def __init__(self):
        pass

    def __str__(self):
        num_roles = len(self.job_roles) if self.job_roles else 0
        if self.job_name:
            the_name = '%s (num roles: %d)' % (self.job_name, num_roles)
        else:
            the_name = '<no job name>'
        return the_name

    def __repr__(self):
        num_roles = len(self.job_roles) if self.job_roles else 0
        if self.job_name:
            the_name = '%s (num roles: %d)' % (self.job_name, num_roles)
        else:
            the_name = '<no job name>'
        return the_name

    @staticmethod
    def reconstruct(data):
        p = AcureRateJob()
        for k, v in data.items():
            setattr(p, k, v)
        return p

    @staticmethod
    def attempt_parse(description):
        # TODO: implement using NLTK
        roles = AcureRateUtils.normalized_titles(description)
        j = AcureRateJob()
        setattr(j, 'job_name', 'unknown') # Use __init__ instead...
        job_roles = []
        for normalized_title, seniority, area in roles:
            job_roles.append({'job_role': normalized_title, 'job_seniority': seniority, 'job_area': area})
        setattr(j, 'job_roles', job_roles)
        return j

    @staticmethod
    def get_year(date_str):
        """ Parse year from string.
        It could be of format yyyy-mm-dd or yy-mm-dd or yyyy-mm or yyyy

        returns: year parsed or None if unable to parse
        """
        try:
            parsed_date = parse(date_str)
        except Exception as e:
            return None
        return parsed_date.year

    @staticmethod
    def valid_year(year_str):
        current_year = datetime.datetime.now().year
        # Should be a 4 digit number from 1970
        valid = True
        try:
            if len(year_str) != 4:
                return False
            years = int(year_str)
            if years < AcureRateJob.MIN_YEAR or years > current_year:
                return False
        except:
            valid = False
        return valid

    def name(self):
        if self.job_name:
            return self.job_name
        else:
            'unknown'

    def areas(self):
        jobs_areas, jobs_roles, jobs_seniorities = self.information()
        return jobs_areas

    def seniorities(self):
        jobs_areas, jobs_roles, jobs_seniorities = self.information()
        return jobs_seniorities

    def roles(self):
        jobs_areas, jobs_roles, jobs_seniorities = self.information()
        return jobs_roles

    def information(self):
        jobs_areas = set()
        jobs_roles = set()
        jobs_seniorities = set()
        if self.job_roles:
            for job_role in self.job_roles:
                if 'job_area' in job_role and job_role['job_area'] and job_role['job_area'] != T.AREA_UNKNOWN:
                    jobs_areas.add(job_role['job_area'])
                if 'job_role' in job_role and job_role['job_role'] and job_role['job_role'] != T.ROLE_UNKNOWN:
                    jobs_roles.add(job_role['job_role'])
                if 'job_seniority' in job_role and job_role['job_seniority'] and job_role['job_seniority'] != T.SENIORITY_UNKNOWN:
                    jobs_seniorities.add(job_role['job_seniority'])
        return jobs_areas, jobs_roles, jobs_seniorities

    def is_founder(self):
        if self.job_roles:
            for role in self.job_roles:
                if P.JOB_TITLE in role and role[P.JOB_ROLE] == T.ROLE_FOUNDER:
                    return True
        return False

    def is_board(self):
        return True if self.board_role() else False

    def board_role(self):
        if self.job_roles:
            for role in self.job_roles:
                if P.JOB_TITLE in role and role[P.JOB_ROLE] in [T.ROLE_BOARD_MEMBER, T.ROLE_BOARD_ADVISOR, T.ROLE_BOARD_CHAIR]:
                    return role
        return None

    def get_start_year(self):
        min_year, max_year = self.get_start_end_years()
        return min_year

    def get_end_year(self):
        min_year, max_year = self.get_start_end_years()
        return max_year

    def get_working_years(self):
        min_year, max_year = self.get_start_end_years()
        if min_year and not max_year:
            return 2
        if max_year and not min_year:
            return 2
        return max_year - min_year if min_year and max_year else None

    # Iterate over all roles and return a (start_year, end_year) tuple
    def get_start_end_years(self):
        min_year = None
        max_year = None
        # Iterate over all roles of the job
        try:
            for role in self.job_roles:
                if P.JOB_STARTED in role and (not min_year or int(role[P.JOB_STARTED]) < min_year):
                    min_year = int(role[P.JOB_STARTED])
                if P.JOB_ENDED in role and (not max_year or int(role[P.JOB_ENDED]) > max_year):
                    max_year = int(role[P.JOB_ENDED])
                if P.JOB_CURRENT in role and (not max_year or int(role[P.JOB_CURRENT]) > max_year):
                    max_year = int(role[P.JOB_CURRENT])
        except:
            pass
        return min_year, max_year

    seniority_table = {
        C.RANGE_1_10: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL,
                       T.SENIORITY_SVP, T.SENIORITY_VP, T.SENIORITY_DIRECTOR, T.ROLE_SITE_MANAGER],
        C.RANGE_10_100: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL,
                         T.SENIORITY_SVP, T.SENIORITY_VP],
        C.RANGE_100_1000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL,
                           T.SENIORITY_SVP, T.SENIORITY_VP],
        C.RANGE_1000_10000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL,
                             T.SENIORITY_VP, T.SENIORITY_SVP, T.SENIORITY_EVP],
        C.RANGE_10000_100000: [T.ROLE_PRESIDENT, T.ROLE_OFFICER, T.SENIORITY_FOUNDER, T.SENIORITY_BOARD, T.SENIORITY_CLEVEL,
                               T.SENIORITY_VP, T.SENIORITY_SVP, T.SENIORITY_EVP]}

    def is_senior(self, employees_range):

        if not self.job_roles:
            return False

        if not employees_range:
            employees_range = C.RANGE_1_10

        # Iterate over roles, see if one of them was a senior role
        for role in self.job_roles:
            if P.JOB_SENIORITY in role and role[P.JOB_SENIORITY] in AcureRateJob.seniority_table[employees_range]:
                return True
            if P.JOB_ROLE in role and role[P.JOB_ROLE] in AcureRateJob.seniority_table[employees_range]:
                return True
        return False
