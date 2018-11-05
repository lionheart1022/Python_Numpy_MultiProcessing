# # -*- coding: utf-8 -*-
# """
# Created on Tue Oct 23 08:51:23 2018
#
# @author: seniortasse
# """
#
import config
from readingComprehensionAssistant import TextComprehension
from flask import Flask
from flask import jsonify

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    """
    index endpoint
    """
    return 'Please go to api endpoint to read text. ' \
           'When you input text in endpoint, you have to replace space with _.'


@app.route('/<text>', methods=['GET'])
def display(text):
    result = {}
    if '_' in text:
        text = text.replace('_', ' ')
    comprehension = TextComprehension(text, config.PARSE['topcontexts'], config.PARSE['phraseLength'])
    keywords = comprehension.findContext()
    length = len(keywords)

    for i in range(1, length+1):
        for key in keywords[i-1]:
            result_value = {}
            for sub_key in keywords[i-1][key]:
                sub_key_value = {"keyword_location": [str(keywords[i-1][key][sub_key]['keyword_location'][0])],
                                 "keyword_text": str(keywords[i-1][key][sub_key]['keyword_text']),
                                 "keyword_phrase_id": str(keywords[i-1][key][sub_key]['keyword_phrase_id'])}
                result_value.update({str(sub_key): sub_key_value})

            formatted_value = {str(key): result_value}
            result.update({'TOP CONTEXT {}'.format(i): formatted_value})

    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5302')
