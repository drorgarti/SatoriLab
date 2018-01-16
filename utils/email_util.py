class EmailUtil(object):

    @staticmethod
    def all_numeric(input):
        return all(char.isdigit() for char in input)

    @staticmethod
    def email_as_key(email):
        # Normalize an email so all of its variances will match when used as key
        #    *** Assumes email is valid! ***
        # 1. move to lower-case
        # 2. in-case of gmail, remove '.' and '+' suffixes
        tokens = list(EmailUtil.get_email_local_and_domain_parts(email.lower()))
        if tokens[1] in ['gmail.com', 'googlemail.com']:
            tokens[0] = tokens[0].replace('.', '')
            # TODO: remove '+' suffix
        return "@".join(tokens)

    # This assumes email address is a valid one
    @staticmethod
    def is_private(email):
        if not EmailUtil.is_valid(email):
            return False
        domain = EmailUtil.get_email_domain_part(email)

        # TODO: academy domains...

        return domain in ["gmail.com", "googlemail.com",
                          "yahoo.com", "yahoo.co.uk",
                          "hotmail.com", "hotmail.it", "aol.com", "msn.com",
                          "icloud.com", "me.com", "mac.com",
                          "hotmail.co.il", "walla.co.il", "walla.com",
                          "rocketmail.com",
                          "zahav.net.il", "smile.net.il", "bezeqint.net", "012.net.il", "barak.net.il", "netvision.net.il"]

    @staticmethod
    def is_academic(email):
        # TODO: check this project - https://github.com/Hipo/university-domains-list
        if '.ac.il' or '.edu' in email:
            return True
        return False

    @staticmethod
    def is_invalid(email):
        return not EmailUtil.is_valid(email)

    @staticmethod
    def get_email_domain_part(email):
        tokens = EmailUtil.get_email_local_and_domain_parts(email)
        return tokens[1]

    @staticmethod
    def get_email_local_part(email):
        tokens = EmailUtil.get_email_local_and_domain_parts(email)
        return tokens[0]

    @staticmethod
    def get_email_local_and_domain_parts(email):
        i = email.find("@")
        if i == -1:
            raise Exception("EmailUtil::get_email_local_and_domain_parts: No @ found in email line")
        local_part = email[:i]
        domain_part = email[i+1:]
        return local_part, domain_part

    @staticmethod
    def is_valid(email):

        # TODO: replace entire validation with a decent library the does much more that this

        if email is None:
            return False
        if email.strip() == "":
            return False
        if email.count('@') != 1:
            return False

        # Remove all the info@... support@... and others
        service_emails = ["careers", "info", "infos", "support", "helpdesk", "sales", "help", "security",
                          "job", "jobs", "jobs-il", "jobs-n", "jobs1", "jobs2", "jobs11"
                          "contact", "hr", "hr4u",
                          "webmaster", "mailer-daemon"]
        suffix = email[:email.find("@")]
        if suffix in service_emails:
            return False

        if "reply-" in suffix:
            return False

        if len(suffix) > 40:
            return False

        # Is suffix only numeric?
        if EmailUtil.all_numeric(suffix):
            return False

        # Check domain
        # TODO: make this configurable, in file
        domain = email[email.find("@")+1:]
        if "support" in domain or\
                        ".evernote." in domain or\
                        "follow.cc" in domain or\
                        "followup.cc" in domain or\
                        "posts.mobilize.io" in domain:
                        # "nisha.co.il" in domain or\
                        # "ethosia.com" in domain or\
                        # "nisha-hr.com" in domain:
            return False

        # bla bla...
        return True

    @staticmethod
    def get_preferred_email_from_list(emails):
        if emails is None or len(emails) == 0:
            return None
        if all(EmailUtil.is_invalid(e) for e in emails):
            return None
        # Search for the first private email
        for e in emails:
            if EmailUtil.is_private(e):
                return e
        return emails[0]

