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
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

app = Flask(__name__)

con = connect(dbname=config.DATABASE['dbname'],
              user=config.DATABASE['user'],
              password=config.DATABASE['password'])
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


@app.route('/', methods=['GET'])
def index():
    """
    index endpoint
    """
    return 'Please go to api endpoint to read text. ' \
           'When you input text in endpoint, you have to replace space with _.'


@app.route('/<text>', methods=['GET'])
def display(text):
    if '_' in text:
        text = text.replace('_', ' ')

    TextComprehension(text, config.PARSE['phraseLength'])

    cur = con.cursor()

    strSQL = 'SELECT * FROM input_text_keywords'
    cur.execute(strSQL)
    result_keyword = cur.fetchall()

    strSQL = 'SELECT * FROM input_text_context_identifier'
    cur.execute(strSQL)
    result_identifier = cur.fetchall()

    result = {'input_text_keywords': [{"input_text_id": keyword[0], "context_id": keyword[1],
                                       "keyword_id": keyword[2], "keyword_location": keyword[3],
                                       "keyword_text": keyword[4], "phrase_id": keyword[5]}
                                      for keyword in result_keyword],
              'input_text_context_identifier': [{"input_text_id": identifier[0], "context_id": identifier[1],
                                                 "context_weight": int(identifier[2])}
                                                for identifier in result_identifier]}

    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5300')
