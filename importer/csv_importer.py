import csv
import codecs
import sys, traceback
import os.path
import logging
from SatoriConfig import GeneralConfig
from enrichment.enrichment_service import EnrichmentService
from utils.acurerate_utils import AcureRateUtils


class CSVImporter:

    def __init__(self, path, encoding, source, attribution_id, attribution_name=None, providers=None, mapping=None, test_import=False, logger=None):
        # Set up logger
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        # TODO: handle cases where we don't want to override
        file_path = '%s\importer.log' % GeneralConfig.LOGS_FOLDER
        if GeneralConfig.LOGS_APPEND:
            handler = logging.FileHandler(file_path, mode='a', encoding='utf-8')  # append
        else:
            handler = logging.FileHandler(file_path, mode='w', encoding='utf-8')  # override
        handler.setLevel(logging.INFO)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Set the providers
        self.providers = providers

        # Add the handlers to the logger
        self.logger.addHandler(handler)
        self.logger.info('-=' * 50)
        self.logger.info('Importer started')
        self.logger.info('-=' * 50)


        # Set up Enrichment Service
        self.es = EnrichmentService.singleton()
        self.eps = self.es.get_providers()
        self.path = path
        self.encoding = encoding
        self.source = source
        self.attribution_id = attribution_id
        self.attribution_name = attribution_name
        self.mapping = mapping
        self.num_rows_handled = 0

        self.test_import = test_import

    def columns_mapping(self):
        return self.mapping

    def import_now(self):
        try:
            self.import_entries()
        except Exception as e:
            self.logger.error('Failed to import contacts', exc_info=True)

    def import_entries(self):

        # Check that file exists
        if not os.path.isfile(self.path):
            self.logger.error('Could not locate file (%s)', self.path)
            return

        # TODO: Check what file it is (Google/Outlook) - check the file was not violated - header file exists

        self.num_rows_handled = 0
        self.num_rows_succesfully_handled = 0
        csv_reader = csv.DictReader(codecs.open(self.path, 'r', self.encoding))
        for csv_row in csv_reader:
            # TODO: remove this from here, it should not be in base class, but specific to contacts importing
            fixed_csv_row = {}
            for k, v in csv_row.items():
                if k is None:
                    continue
                k = 'First Name' if 'First Name' in k else k
                fixed_csv_row[k] = v
            # If there's a mapping defined, use it
            if self.columns_mapping():
                row = {}
                AcureRateUtils.dict2dict(fixed_csv_row, row, self.columns_mapping())
            else:
                row = fixed_csv_row
            # Check if row should be ignored all-together (check all fields, not only those mapped)
            if self.handle_row(row, fixed_csv_row, self.num_rows_handled+1):
                self.num_rows_succesfully_handled += 1
            self.num_rows_handled += 1
            #if self.num_rows_handled % 1000 == 0:
            if True:
                self.logger.info('Done importing %d rows...', self.num_rows_handled)
        self.logger.info('Done importing all rows. Total: %d / Successful: %d', self.num_rows_handled, self.num_rows_succesfully_handled)

    def handle_row(self, mapped_row, raw_row, row_number):
        pass

    def rows_handled(self):
        return self.num_rows_handled
