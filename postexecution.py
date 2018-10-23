# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 08:37:16 2018

@author: seniortasse
"""

"""
NEW PARAMETERS
"""
distancePercentile=10
bondingIndexPercentile=90
"""
ANALYZE THE DATA in context table, document table, phrase table, phrase origin table and
phrase meaning table.
Calculate the phrase vector space and distances between phrase-context and phrase-phrase.
"""
from contextionaryAnalytics import WordVectorSpace
phraseSpace=WordVectorSpace(distancePercentile,bondingIndexPercentile)