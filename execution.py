#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 14 20:10:12 2017

@author: seniortasse

"""
"""
NOTE:
    The space below is dedicated to test our code by creating objects, calling methods,
    printing relevant outputs.
"""

"""
Database is a class from the module contextionaryDatabase. The class Database helps create a postgreSQL
database for the program, called "contextionary". The "contextionary" database contains 8 tables as of now.
"""

from contextionaryDatabase import Database
import os

libraryName = "Human activity"
projectPath = os.getcwd()
phraseLength = 2
createDatabase = 1
db = Database(libraryName, phraseLength, projectPath, createDatabase)


"""
from contextionaryAnalytics import WordVectorSpace
vectorSpace=WordVectorSpace()
"""

"""
from readingComprehensionAssistant import TextComprehension

text="Pope and catholic church"
textComprehension=TextComprehension(text)
#textComprehension.findContext()
"""



"""
Personal note:
SELECT 
    pg_terminate_backend(pid) 
FROM 
    pg_stat_activity 
WHERE 
    -- don't kill my own connection!
    pid <> pg_backend_pid()
    -- don't kill the connections to other databases
    AND datname = 'contextionary'
"""
