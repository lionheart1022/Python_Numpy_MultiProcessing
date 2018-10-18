# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 22:05:28 2018

@author: seniortasse
"""


class TextProcessor(object):

    def __init__(self, text, phraseMaxLength):

        self.text = text
        self.phraseMaxLength = phraseMaxLength
        self.clauses = []
        self.phraseCount = dict()
        self.wordOrderedList = []
        self.size = dict()

        """
        As a first step once we receive a text, we will break it into clauses using
        the method breakTextIntoClauses.
        """
        self.breakTextIntoClauses()
        
        """
        As a second step, we will break the clauses into phrases of various lengths.
        """
        self.breakClausesIntoPhrases()

    def getWordOrderedList(self):
        return self.wordOrderedList
    
    def getSize(self):
        return self.size
    
    def getClauses(self):
        return self.clauses
    
    def getPhraseCount(self):
        return self.phraseCount
    
    def breakTextIntoClauses(self):
        
        """
        All commas and semi-columns that usually separate clauses will be replaced
        with dots for convenience
        """
        modifiedText = self.text.replace(",", ". .")
        modifiedText = modifiedText.replace(";", ". .")
        modifiedText = modifiedText.replace("(", ". .")
        
        """
        A very useful tool of Python is the sentence tokenizer that splits
        a text into its various sentences and save them into a list of sentences.
        """
        from nltk.tokenize import sent_tokenize
        self.clauses = sent_tokenize(modifiedText)

    def breakClausesIntoPhrases(self):
        
        """
        A very useful tool of Python is the word tokenizer that splits
        a text into its words and save them into a list of words.
        The list of words will be stored into --wordSplit-- variable.
        """
    
        from nltk.tokenize import word_tokenize
        import re
        
        """
        We define a dictionary {phraselength:phrases} that will store for each
        phraselength the list of phrases that have that length.
        """
        phraseSplit = dict()
        
        for length in range(1, self.phraseMaxLength+1):
             phraseSplit.update({length: []})

        """
        We now look at each of the clauses one by one.
        """
        for clause in self.clauses:
            
            """
            All the words of the clause are stored in a list
            """
            wordSplit = []
            
            wordSplit = word_tokenize(clause)

            wordSplit = [w for w in wordSplit if re.match("^[A-Za-z0-9_-]*$", w) and not w.startswith('-') and not w.endswith('-') and not w.startswith('_') and not w.endswith('_')]

            self.wordOrderedList.extend(wordSplit)
        
            
            """
            All words that start with a capital letter and contain only 1 capital letter will be
            lowered. 
            We transform the text into lower cases to make our analysis non case
            sensitive. Though special words with a capital letter not at the beginning of the word will be left untouched
            and will remain case sensitive.
            """

            for wordindex in range(len(wordSplit)):
                word = wordSplit[wordindex]
                if word[0].isupper() and sum(1 for c in word if c.isupper()) == 1:
                    word = word.lower()
                    wordSplit[wordindex] = word

            from collections import Counter
            
            """
            We list for each phrase length all the phrases of the clause
            that have that length
            """
            for length in range(1, self.phraseMaxLength+1):
                for i in range(len(wordSplit)):
                    if i < len(wordSplit)-(length-1):
                        s = wordSplit[i]
                        
                        if length > 1:
                            for l in range(1, length):
                                s += " " + wordSplit[i+l]
                        
                        # If special symbol in the beginning
                        if not s[0].isalnum():
                            i = 0
                            while not s[0].isalnum():
                                i += 1
                                if i == len(s):
                                    break
                            s1 = s[i:]
                        else:
                            s1 = s
                        
                        # If special symbol in the end
                        if not s[-1].isalnum():
                            i = 0
                            while not s[-i].isalnum():
                                i += 1
                                if i == len(s):
                                    break
                            s2 = s1[:-i]
                        else:
                            s2 = s1

                        phraseSlice = s2
                        """
                        We add the phrase into the phrases list
                        """
                        phraseSplit[length].append(phraseSlice)
                                                                     
        
        """
        Once all the phrases have been listed, we count their number of occurences
        in the document and we record that into the --phraseCounter-- dictionary.
        We also update the --phraseLengthCounter-- dictionary that tells us for each
        phrase length how many phrases in total exist.
        """
        for length in range(1, self.phraseMaxLength+1):
            self.size.update({length: len(phraseSplit[length])})
            counter = Counter()
            counter.update(phraseSplit[length]) 
            self.phraseCount.update({length: dict(counter)})

    def __str__(self):    
        
        return "I am the Document class"
