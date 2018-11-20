# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 21:41:59 2018

@author: seniortasse
"""

import config
import pandas as pd
from psycopg2 import connect 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

con = connect(host=config.DATABASE['host'],
              dbname=config.DATABASE['dbname'],
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
        self.topContexts = dict()
        self.input_text_keywords=[]
        
        #self.findContext()

    def findContext(self):
        
        cur = con.cursor()
        cur.execute(""" SELECT "context_id" FROM context;""")
        contextIDList = cur.fetchall()

        # initialize topContextsDict dictionary {<context id> : [<weighted sum>, <context level>, <context id>]}
        topContextsDict = dict()
        for contextID in contextIDList:
            topContextsDict.update({contextID[0]: list()})
        
        # calculate weighted sum for each context and store in topContextsDict (along with corresponding context level and context id)
        for contextID in contextIDList:
            cur.execute("""SELECT "context_level" FROM "context" WHERE "context_id" = %s; """, [contextID[0]])
            contextLevel = cur.fetchall()
            weightedSum = 0
            for phraseLength in range(1, self.phraseMaxLength+1):
                for phrase in self.phraseCount[phraseLength]:
                    cur.execute(""" SELECT "phrase_id" FROM phrase WHERE "phrase_text" = %s; """, ([phrase]))
                    phraseID = cur.fetchall()
                    if phraseID:
                        cur.execute("""SELECT "phrase_weight" FROM "phrase_weight_by_context" WHERE "phrase_id" = %s AND "context_id"=%s; """, [phraseID[0][0],contextID[0]])
                        weight = cur.fetchall()
                        if weight:
                            weightedSum = weightedSum + float(weight[0][0]) * self.phraseCount[phraseLength][phrase]
            topContextsDict[contextID[0]].extend([weightedSum, contextLevel[0][0], contextID[0]])
        
        # convert topContextsDict to dataframe and sort (in descending order) by weighted sum, context level, and context id;
        # for example, topContextsDataframe = 
        #     weighted_sum  context_level  context_id
        # 6      300              3           6
        # 2      200              2           2
        # 1      200              1           1
        # 7      100              3           7
        # 5      100              3           5
        # 4      100              3           4
        # 3      100              2           3
        topContextsDataframe = pd.DataFrame.from_dict(topContextsDict, orient = 'index', columns = ['weighted_sum', 'context_level', 'context_id'])
        topContextsDataframe = topContextsDataframe.sort_values(by = ['weighted_sum', 'context_level', 'context_id'], ascending = [False, True, False])

        """
        START FROM HERE
        
        TASK 1: PROVIDED AND INPUT DICTIONARY CALLED self.topContexts, REFINE THE DICTIONARY TO KEEP ONLY THE TOP CONTEXTS
        BY VALUE.
        
        For example,a potential content of self.topContexts dictionary is:
        
        self.topContexts=
        {1: Decimal('0.18396226415094338'), 2: Decimal('0.18840579710144928'), 
        3: Decimal('0.17567567567567569'), 4: Decimal('0.17105263157894735'),
        5: Decimal('0.0'), 6: Decimal('0.0'), 7: Decimal('0.20967741935483869'),
        8: Decimal('0.0'), 9: Decimal('0.17567567567567569'), 10: Decimal('0.0'), 
        11: Decimal('0.0'), 12: Decimal('0.17105263157894735'), 13: Decimal('0.0'), 14: Decimal('0.0')}
        
        The key (integer) represents the context_id. The value (float) is proportional to the
        likelihood of the input text to belong to the context in the key.
        
        Assuming topCount=3 (or X), reduce the dictionary size to only the top 3 (or X) contexts:
        
        self.topContexts=
        {1: Decimal('0.18396226415094338'), 2: Decimal('0.18840579710144928'), 
         7: Decimal('0.20967741935483869')}
        
        """
        
        # if self.topCount > n contexts in contextionary, set self.topCount = n 
        nContexts = len(contextIDList)
        if self.topCount > nContexts:
            self.topCount = nContexts
        
        # select the top contexts from topContextsDataframe and store in self.topContexts;
        # note that this disctionary preserves the sorting logic of topContextsDataframe;
        # for example, if self.topCount = 3, then self.topContexts = 
        #     {6: 300,
        #      2: 200,
        #      1: 200}
        
        topContextsSeries = topContextsDataframe.iloc[0:self.topCount]['weighted_sum']
        self.topContexts = pd.Series.to_dict(topContextsSeries)

        
        """
        
        
        TASK 2: CREATE A LIST THAT WILL ARRANGE THE CONTEXTS IN ORDER AND FOR EACH CONTEXT, 
        ATTACH A DICTIONARY PRESENTING A KEYWORD ID AS THE KEY AND SOME KEYWORD DATA AS VALUE.
        
        To continue with the same above example, the list input_text_keywords will have 3 dictionaries
        with keys 7 (top context), 2 (second top context) and 1 (third top context).
        The context dictionary for context 7 below shows that the input test has 3 keywords identified 1, 2 and 3
        with value a dictionary of the properties of each of the identified keywords.
        
        Note that keyword_phrase_id can be pulled from the phrase table WHERE phrase_text=keyword_text.
        
        
        "input_text_keywords":
        [{7: 
            {
           1:
           {"keyword_location": "{{1,2}}", 
          "keyword_text": "water polo", 
          "keyword_phrase_id": 171711},
           2: {
          "keyword_location": "{{1}}", 
          "keyword_text": "water", 
          "keyword_phrase_id": 29805},
           3: {
          "keyword_location": "{{2}}", 
          "keyword_text": "polo", 
          "keyword_phrase_id": 119472}
           }}
           ,
         {2: 
             {
           1: {
          "keyword_location": "{{1}}", 
          "keyword_text": "water", 
          "keyword_phrase_id": 29805}
           }}
           ,
        {1: 
             {
           3: {
          "keyword_location": "{{2}}", 
          "keyword_text": "polo", 
          "keyword_phrase_id": 119472}
           }}
        ]
        
        """
        
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
        maxLength = min([len(self.phraseList), self.phraseMaxLength])
        for length in range(1, maxLength+1):
            location_id = 0
            nGramLocationDict = dict()
            for word_index, word in enumerate(self.phraseList):
                location_id += 1
                if word_index+length <= len(self.phraseList):
                    nGramLocationDict.update({location_id: " ".join(self.phraseList[word_index: (word_index+length)])})  
            kewordLocationDict.update({length:nGramLocationDict})
        
        input_text_keywords = []
        for contextID in self.topContexts.keys(): # for each ordered top context:
                
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
                            
                        
            
        
            
        """
        TASK 3: RETURN THE LIST input_text_keywords (already done below)
    
           
        """
    
        return(input_text_keywords)

    
    def __str__(self):
        return "I am the Phrase class"
