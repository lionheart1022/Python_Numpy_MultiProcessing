# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 08:51:23 2018

@author: seniortasse
"""

import config
from readingComprehensionAssistant import TextComprehension

text = "Achille heel was hit during the trojan war"
print(text)
test1 = TextComprehension(text, config.PARSE['phraseLength'])


text = "Dance is my passion. I am a great dancer and can move on any style of music."
print(text)
test2 = TextComprehension(text, config.PARSE['phraseLength'])


text = "A vector space is a mathematical object that is pretty abstract."
print(text)
test3 = TextComprehension(text, config.PARSE['phraseLength'])