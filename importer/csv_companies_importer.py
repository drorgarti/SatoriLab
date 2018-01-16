from importer.csv_importer import CSVImporter
from enrichment.enrichment_service import EnrichmentData, EnrichmentBehavior, EnrichmentSource
from entities.acurerate_attributes import C


class CSVCompaniesImporter(CSVImporter):

    # define mapping from CB excel columns to AcureRate fields
    crunchbase_mapping = {
        "primary_role": C.ORGANIZATION_TYPE,
        "name": C.NAME,
        "crunchbase_url": C.CRUNCHBASE_URL,
        "homepage_domain": C.DOMAIN,
        "homepage_url": C.WEBSITE,
        "stock_symbol": C.STOCK_SYMBOL,
        "short_description": C.DESCRIPTION,
        "profile_image_url": C.IMAGE_URL,  # TODO: We may want to change this mapping to use the LOGO.*
        "facebook_url": C.FACEBOOK_URL,
        "twitter_url": C.TWITTER_URL,
        "linkedin_url": C.LINKEDIN_URL
        # TODO: grab also location fields - need to later digest them
    }

    angellist_mapping = {
        "Name": C.NAME,
        "Pitch": C.SHORT_DESCRIPTION,
        "Angellist URL": C.ANGELLIST_URL,
        "Location": C.HEADQUARTERS,
        "Market": C.MARKETS,
        "Website": C.DOMAIN,
        "Num Employees": C.EMPLOYEES_RANGE,
        # "Stage": C.INVESTMENT_STAGE,
        "Total": C.TOTAL_FUNDING,
        #"Raised": C.INVESTMENT_RAISED,
        "Image URL": C.LOGO_URL
    }

    def __init__(self, path, encoding, source, attribution_id=None, attribution_name=None, mapping=None, test_import=False, logger=None):
        super().__init__(path, encoding, source, attribution_id, attribution_name, mapping, test_import, logger)

    def columns_mapping(self):
        if self.mapping:
            return self.mapping
        else:
            return CSVCompaniesImporter.crunchbase_mapping

    def handle_row(self, mapped_row, raw_row, row_number):
        super().handle_row(mapped_row, raw_row, row_number)

        if 'name' not in mapped_row or mapped_row['name'] is None:
            self.logger.warning('Could not find company name field in row (row: %s)', mapped_row)
            return False
        if 'domain' not in mapped_row or mapped_row['domain'] is None or mapped_row['domain'].strip() == '':
            self.logger.warning('Could not find company domain field in row (row: %s)', mapped_row)
            return False
        ed = []
        name = None
        key = None
        for k, v in mapped_row.items():
            if v == '' or v == '-':
                continue
            if k == 'name':
                ed.append(EnrichmentData(k, v, 'override'))
                name = v
            elif k == 'stock_symbol':
                if v != ":":
                    ed.append(EnrichmentData(k, v, 'override'))
            elif k == C.DOMAIN:
                key = v
            else:
                ed.append(EnrichmentData(k, v, 'override'))
        if key is None or name is None:
            self.logger.warning('Could not find company name or domain fields in row (row: %s)', mapped_row)
            return False

        if not self.test_import:
            self.logger.info('Row %d - key_domain: %s. Sending to enrichment...', row_number, key)
            source = EnrichmentSource(source_type=self.source, source_key='%s %s' % (self.attribution_id, key.lower()))
            behavior = EnrichmentBehavior()
            self.es.enrich_company(enrichment_key=key, enrichment_data=ed, enrichment_source=source, enrichment_behavior=behavior)
        else:
            self.es.check_company(name, key)

        return True