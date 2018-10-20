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
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
from multiprocessing import Process


libraryName = "Human activity"
projectPath = os.getcwd()
phraseLength = 2
password = 'postgres'
dbname = 'contextionary'
usr = 'postgres'

createDatabase = 1
db = Database(libraryName, phraseLength, projectPath, createDatabase)

libraryFolderPath = db.libraryFolderPath


def add_document_process(filepath):
    db.add_document(filepath)


file_list = []

for root, dirs, files in os.walk(libraryFolderPath):
    rootdirname = Path(root).parts[-1]

    con = connect("dbname=contextionary user=postgres password=%s" % password)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute(""" SELECT count(*) FROM context WHERE "context_name" = %s; """,
                ([rootdirname]), )
    dircount = cur.fetchone()

    cur.execute(""" SELECT "context_children_id" FROM context WHERE "context_name" = %s; """,
                ([rootdirname]), )
    childlist = cur.fetchone()

    try:
        if (dircount[0] > 0) and (childlist[0] == '0'):
            file_list.extend([[files, root]])

    finally:
        cur.close()
        con.close()

if __name__ == '__main__':

    if file_list:
        print("Updating documents.....")

        processes = []
        for files in file_list:
            for file in files[0]:
                p = Process(target=add_document_process, args=(files[1] + '/' + file,))
                processes.append(p)
        for p in processes:
            p.start()
            p.join()
