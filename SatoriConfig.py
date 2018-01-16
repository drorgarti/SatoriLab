from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY, DEFAULT_SERIALIZER,
    RPC_EXCHANGE_CONFIG_KEY, SERIALIZER_CONFIG_KEY)


class GeneralConfig:

    AMQP_CONFIG = {AMQP_URI_CONFIG_KEY: 'amqp://naqvsuhj:qDm_j7eCq0WyrwRS_TaO-Cm2bWn6vC0F@zebra.rmq.cloudamqp.com/naqvsuhj',
                   SERIALIZER_CONFIG_KEY: 'json'}
    UPLOAD_FOLDER = r'f:\temp\AcureRate\Contact Files\uploads'
    CACHE_FOLDER = r'F:\temp\AcureRate\Satori Lab\cache'
    REQUESTS_CACHE_DB = 'acurerate-cache'
    LOGS_FOLDER = r'f:\temp\AcureRate\Satori Lab\logs'
    LOGS_APPEND = False
