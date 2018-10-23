# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 21:41:59 2018

@author: seniortasse

As a first step, we need to establish a connection to the "contextionary" database
"contextionary" database is a database of phrases and their contexts organized into multiple
tables including:
"""

import config
from psycopg2 import connect 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

con = connect(dbname=config.DATABASE['dbname'],
              user=config.DATABASE['user'],
              password=config.DATABASE['password'])
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


class TextComprehension(object):

    def __init__(self, text, phraseMaxLength=config.PARSE['phraseLength']):
        
        from textProcessing import TextProcessor
        
        self.text = text
        self.phraseMaxLength = phraseMaxLength
        self.textProcessor = TextProcessor(self.text, self.phraseMaxLength)
        self.phraseCount = self.textProcessor.getPhraseCount()
        self.wordOrderedList = self.textProcessor.getWordOrderedList()
        print(self.wordOrderedList)
        self.contextMatch = dict()
        self.contextAndKeywords = dict()
        
        self.findContextAndKeywords()

    def findContextAndKeywords(self):
        self.findContext()
        self.findKeywords()
        
        print(self.contextAndKeywords)
        
        return self.contextAndKeywords

    def findContext(self):
        
        cur = con.cursor()
        cur.execute(""" SELECT "context_id" FROM context;""")
        contextIDList = cur.fetchall()
        
        topContexts=[]
        contextRanking=[]
        
        for contextID in contextIDList:
            self.contextMatch.update({contextID[0]: 0})
        
        for phraseLength in range(1, self.phraseMaxLength+1):
            for phrase in self.phraseCount[phraseLength]:
                cur.execute(""" SELECT "phrase_id" FROM phrase WHERE "phrase_text" = %s; """, ([phrase]))
                phraseID = cur.fetchall()
                if phraseID:
                    for contextID in contextIDList:
                        cur.execute("""SELECT "phrase_weight" FROM "phrase_weight_by_context" WHERE "phrase_id" = %s AND "context_id"=%s; """, [phraseID[0][0],contextID[0]])
                        weight = cur.fetchall()
                        if weight:
                            self.contextMatch[contextID[0]] = self.contextMatch.get(contextID[0], 0) + weight[0][0] * self.phraseCount[phraseLength][phrase]
        
        print(self.contextMatch)  

        cur = con.cursor()
        cur.execute("""DELETE FROM "input_text_context_identifier";""")
        cur = con.cursor()
        cur.execute("""DELETE FROM "input_text_word_position";""")
        cur = con.cursor()
        cur.execute("""DELETE FROM "input_text_keywords";""")

        # set input text ID
        inputTextID = 1
        
        # sort all weights (low to high)
        weightsList = [float(self.contextMatch[key]) for key in self.contextMatch.keys()]
        weightsList = list(set(weightsList))
        weightsList.sort()
        
        # subset contextMatch dictionary to only include contexts with large weights
        likelyContextMatch = {key:float(self.contextMatch[key]) for key in self.contextMatch.keys() if float(self.contextMatch[key]) >= min(weightsList[-3:])}
        
        for contextID in likelyContextMatch.keys():
            
            # update --input text context identifier-- table
            cur = con.cursor()
            cur.execute(""" INSERT INTO "input_text_context_identifier" 
                       ("input_text_id", "context_id", "context_weight") 
                       VALUES (%s,%s,%s); """, [inputTextID, contextID, likelyContextMatch[contextID]])
            topContexts.append(contextID)
            contextRanking.append(likelyContextMatch[contextID])

        self.contextAndKeywords.update({"Top contexts": topContexts})
        self.contextAndKeywords.update({"Context ranking": contextRanking})
    
    def findKeywords(self):

        keywordContext=[]        
        keywordOrder=[]
        keywordLocation=[]
        keywordPhraseID=[]

        # set input text ID
        inputTextID = 1

        for i in range(len(self.wordOrderedList)):
            
            # update --input text word position-- table
            cur = con.cursor()
            cur.execute(""" INSERT INTO "input_text_word_position" ("input_text_id", "word_position", "word_text") VALUES (%s,%s,%s); """, [inputTextID, i+1, self.wordOrderedList[i]])

        # get contexts from --input text context identifier--
        cur = con.cursor()
        cur.execute(""" SELECT "context_id" from "input_text_context_identifier"; """)
        identifiedContextID = cur.fetchall()
        
        # extract all phrases from --phraseCount--
        phrasesList = []
        for key1, val in self.phraseCount.items():
            subdict = val
            for key2 in subdict.keys():
               phrasesList.append(key2)
        
        for contextID in identifiedContextID:
            keywordID = 0
            # get all --Phrase ID-- for a given context semantic field
            cur = con.cursor()
            cur.execute(""" SELECT "phrase_id" from "context_phrase" WHERE "context_id" = %s; """, [contextID[0]])
            csfPhraseIDList = cur.fetchall()
            for phraseID in csfPhraseIDList:
                # get corresponding phrase text
                cur = con.cursor()
                cur.execute(""" SELECT "phrase_text" from "phrase" WHERE "phrase_id" = %s; """, [phraseID[0]])
                phraseText = cur.fetchone()
                if phraseText[0] in phrasesList:
                    # if --phraseText-- is in input text, assign keywordID
                    keywordID += 1
                    keywordText = phraseText[0]
                    from nltk.tokenize import word_tokenize
                    keywordWords = word_tokenize(keywordText)
                    phraseLength = len(keywordWords)
                    phraseWordsPosition = dict()
                    for i in range(phraseLength):
                        # create --phraseWordsPosition-- dictionary which identifies the word position
                        # in the input text corresponding to each word in --keywordText--
                        # {key:value} = {keywordWords index: input text word position}
                        word = keywordWords[i]
                        cur = con.cursor()
                        cur.execute(""" SELECT "word_position" FROM "input_text_word_position" WHERE "word_text" = %s; """, [word])
                        position = cur.fetchall()
                        position = [x[0] for x in position]
                        phraseWordsPosition[i] = position
                    # create --keywordLocation-- list which identifies the location of the keyword in the input text
                    keywordLocation = []
                    independentWordsPosition = phraseWordsPosition[0]
                    for independentWordPosition in independentWordsPosition:
                        keywordLocation_temp = [independentWordPosition]
                        i = 1
                        while i < phraseLength and independentWordPosition + i in phraseWordsPosition[i]:
                            try:
                                keywordLocation_temp.extend([independentWordPosition + i])
                                i += 1
                            except KeyError:
                                break
                        if len(keywordLocation_temp) == phraseLength:
                            keywordLocation.append(keywordLocation_temp)
                    cur = con.cursor()
                    cur.execute(""" INSERT INTO "input_text_keywords" ("input_text_id", "context_id", "keyword_id", "keyword_location", "keyword_text", "phrase_id") VALUES (%s,%s,%s,%s,%s,%s); """, [inputTextID, contextID[0], keywordID, keywordLocation, keywordText, phraseID[0]])
                    keywordContext.append(contextID[0])      
                    keywordOrder.append(keywordID)
                    keywordLocation.append(keywordLocation)
                    keywordPhraseID.append(phraseID[0])

        self.contextAndKeywords.update({"Keyword context": keywordContext})
        self.contextAndKeywords.update({"Keyword order": keywordOrder})
        self.contextAndKeywords.update({"Keyword location": keywordLocation})
        self.contextAndKeywords.update({"Keyword phrase ID": keywordPhraseID})
    
    def __str__(self):
        return "I am the Phrase class"
