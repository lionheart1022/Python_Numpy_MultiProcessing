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
import multiprocessing
from multiprocessing import Process
from threading import Thread
from queue import Queue, Empty
import platform


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


class AddDocumentThread(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                filepath = self.queue.get_nowait()
                add_document_process(filepath)
            except Empty:
                break
        return


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

        q = Queue()
        for files in file_list:
            for file in files[0]:
                if 'Linux' in platform.platform():
                    q.put(files[1] + '/' + file)
                else:
                    q.put(files[1] + '\\' + file)

        new_threads = []
        t = AddDocumentThread(q)
        t.start()
        new_threads.append(t)
        for thread in new_threads:
            thread.join()
