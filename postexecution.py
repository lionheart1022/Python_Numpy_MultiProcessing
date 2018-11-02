# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 08:37:16 2018

@author: seniortasse

ANALYZE THE DATA in context table, document table, phrase table, phrase origin table and
phrase meaning table.
Calculate the phrase vector space and distances between phrase-context and phrase-phrase.
"""


import config
from contextionaryAnalytics import WordVectorSpace
from readingComprehensionAssistant import TextComprehension

phraseSpace = WordVectorSpace(config.PARSE['distancePercentile'], config.PARSE['bondingIndexPercentile'])

text = "mathematics is a form of knowledge."
comprehension = TextComprehension(text, config.PARSE['topcontexts'])
keywords = comprehension.findContext()
print(keywords)
