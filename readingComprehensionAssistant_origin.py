# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 21:41:59 2018

@author: seniortasse
"""

import config
import time
from psycopg2 import connect 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

con = connect(dbname=config.DATABASE['dbname'],
              user=config.DATABASE['user'],
              password=config.DATABASE['password'])
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


class TextComprehension(object):

    def __init__(self, text, topCount=3,phraseMaxLength=5):
        
        from textProcessing import TextProcessor
        
        self.text = text
        self.topCount=topCount
        self.phraseMaxLength = phraseMaxLength
        self.textProcessor = TextProcessor(self.text, self.phraseMaxLength)
        
        self.phraseCount = self.textProcessor.getPhraseCount()
        self.phraseList = self.textProcessor.getWordOrderedList()
        #print(self.phraseList)
        #print(self.phraseCount)
        self.topContexts = dict()
        self.input_text_keywords=[]
        
        #self.findContext2()
        
        
    def findContext(self):
        
        import numpy as np
        #from numpy import genfromtxt

        start_time = time.time()
        phraseWeightByContextMatrix = np.genfromtxt('phraseWeightByContextMatrix.csv', delimiter=',')
        end_time = time.time()
        print("time to read from phrase weight matrix csv file: %s" % (str(end_time - start_time)))
        print("")
        print("phrase weight by context matrix")
        print(phraseWeightByContextMatrix)
        
        phraseArraySize=len(phraseWeightByContextMatrix)
        
        phraseCountArray=np.zeros(phraseArraySize)
         
        cur = con.cursor()
        start_time = time.time()
        for phraseLength in range(1, self.phraseMaxLength+1):
            for phrase in self.phraseCount[phraseLength]:
                
                cur.execute(""" SELECT "phrase_index" FROM phrase WHERE "phrase_text" = %s; """, ([phrase]))
                phrase_index = cur.fetchall()
                if phrase_index:
                    phraseCountArray[phrase_index]=self.phraseCount[phraseLength][phrase]
                    #print("phrase: %s, count: %s" %(phrase,self.phraseCount[phraseLength][phrase]))
        end_time = time.time()
        print("time to execute for loop and get phrase_index from table phrase: %s" % (str(end_time - start_time)))
        print("")
        print("input text phrase count")
        print(phraseCountArray)
        
        textWeights=np.dot(phraseWeightByContextMatrix.transpose(),phraseCountArray.transpose())
        print("")
        print("input text weights by context. Total independent context is %s" %len(textWeights))
        print(textWeights)
        
        
        topRCIndex=list(sorted(range(len(textWeights)), key=lambda i: textWeights[i])[-self.topCount:])
        
        print(topRCIndex)
        
        start_time = time.time()
        for i in range(self.topCount):
            rcindex=topRCIndex[i]
            #print(rcindex)
            cur.execute(""" SELECT "context_id" FROM context WHERE "rcindex" = %s; """, ([rcindex]))
            contextID = cur.fetchall()
            self.topContexts.update({contextID[0][0]: textWeights[topRCIndex[i]]})
        print("")
        print("list of top contexts with corresponding weights:")
        print(self.topContexts)
        end_time = time.time()
        print("time to execute for loop and get top contexts from context table: %s" % (str(end_time - start_time)))
        
        
        
        # extract all likelihood scores from dictionary (one for each context) and store in list
        allLHScores = [v for k,v in self.topContexts.items()]
        
        # sort all likelihood scores (largest to smallest)
        allLHScores.sort(reverse=True)
        
         # get list of all input text subphrases
        # for example, if input text is "water polo", then inputTextSubphrases = ["water", "polo", "water polo"]
        inputTextSubphrases = [v for k,v in self.phraseCount.items()]
        inputTextSubphrases = [i for d in inputTextSubphrases for i in list(d.keys()) ]
        
        # create kewordLocationDict dictionary {<subphrase length> : {<subphrase location> : <subphrase text>}}
        # for example, if the input text is "a b a b" and self.phraseMaxLength = 5, then
        # kewordLocationDict = 
        #    {1: {1: 'a', 2: 'b', 3: 'a', 4: 'b'},
        #     2: {1: 'a b', 2: 'b a', 3: 'a b'},
        #     3: {1: 'a b a', 2: 'b a b'},
        #     4: {1: 'a b a b'}}
        kewordLocationDict = dict()
        start_time = time.time()
        maxLength = min([len(self.phraseList), self.phraseMaxLength])
        for length in range(1, maxLength+1):
            location_id = 0
            nGramLocationDict = dict()
            for word_index, word in enumerate(self.phraseList):
                location_id += 1
                if word_index+length <= len(self.phraseList):
                    nGramLocationDict.update({location_id: " ".join(self.phraseList[word_index: (word_index+length)])})  
            kewordLocationDict.update({length:nGramLocationDict})
        end_time = time.time()
        print("Time to update keyword location dictionary: %s" % (str(end_time - start_time)))
        
        # get sorted list of *unique* top likelihood scores (largest to smallest)
        # for example, if self.topContexts = {1: Decimal('3'), 2: Decimal('5'), 7: Decimal('3')}, then topNLHScore = [5,3]
        topNLHScore = list(set(allLHScores[0:self.topCount]))
        topNLHScore.sort(reverse=True)
        
        input_text_keywords = []
        start_time = time.time()
        for likScore in topNLHScore: # for each unique top likelihood score:
            
            # get list of top contexts whose likelihood score is likScore
            contextIDs = [k for k,v in self.topContexts.items() if v == likScore]
            
            for contextID in contextIDs: # for each ordered top context:
                
                keywordDict = dict()
                kw_id = 0
                
                for kwText in inputTextSubphrases: # for each subphrase of input text:
                    
                    # get phrase ID
                    cur = con.cursor()
                    cur.execute(""" SELECT "phrase_id" FROM phrase WHERE "phrase_text" = %s; """, ([kwText]))
                    phraseID = cur.fetchall()
                    
                    if phraseID: # if input text subphrase is in contextionary...
                        
                        cur.execute(""" SELECT exists (SELECT 1 FROM "context_phrase" WHERE "context_id" = %s AND "phrase_id" = %s); """, ([contextID, phraseID[0]]))
                        isContextPhrase = cur.fetchall()
                        
                        if isContextPhrase[0][0]: # ...and if input text subphrase is a context phrase:
                            
                            kw_id += 1
                            
                            phraseLength = len(kwText.split())
                            nGramLocationDict = kewordLocationDict[phraseLength]
                            startIndexList = [k for k,v in nGramLocationDict.items() if v == kwText]
                            
                            keywordLocation = []
                            
                            for startIndex in startIndexList:
                                keywordLocation.append(set(range(startIndex, startIndex+phraseLength)))
                                
                            # store keyword attributes in keywordDict
                            keywordDict.update({kw_id: {"keyword_location": keywordLocation, 
                                                        "keyword_text": kwText, 
                                                        "keyword_phrase_id": phraseID[0][0]}})
                
                # store keywordDict in contextDict
                contextDict = {contextID: keywordDict}
                
                # append contextDict to input_text_keywords
                input_text_keywords.append(contextDict)
        end_time = time.time()
        print("Time to update input test keyword list: %s" % (str(end_time - start_time)))
        
        print("")
        print("ordered list of contexts with keywords")
        print(input_text_keywords)
        return(input_text_keywords)
         

    
    def __str__(self):
        return "I am the Phrase class"
