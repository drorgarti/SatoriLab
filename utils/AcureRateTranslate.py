import os
from google.cloud import translate


class AcureRateTranslate(object):

    def __init__(self):
        pass

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

    # [END translate]
