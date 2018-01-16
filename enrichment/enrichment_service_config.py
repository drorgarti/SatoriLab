from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY, DEFAULT_SERIALIZER,
    RPC_EXCHANGE_CONFIG_KEY, SERIALIZER_CONFIG_KEY)


class EnrichmentServiceConfig:

    RAND_LOW = 0.2  # Measured in Seconds
    RAND_HIGH = 1   # Measured in Seconds
    THROTTLE = True

    LOGGER_NAME = 'enrichment_service_logger'
    LOGFILE_NAME = 'enrichment.log'

    # Todo: should I be setting the other configurations?
    AMQP_CONFIG = {AMQP_URI_CONFIG_KEY: 'amqp://naqvsuhj:qDm_j7eCq0WyrwRS_TaO-Cm2bWn6vC0F@zebra.rmq.cloudamqp.com/naqvsuhj',
                   SERIALIZER_CONFIG_KEY: 'json'}

    def __init__(self):
        pass
