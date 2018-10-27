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
from threading import Thread, Event
from queue import Queue, Empty
import platform
import time
from random import shuffle
import config


libraryName = "Human activity"
projectPath = os.getcwd()

start_time = time.time()
createDatabase = 0
db = Database(libraryName, config.PARSE['phraseLength'], projectPath, createDatabase)

libraryFolderPath = db.libraryFolderPath

stop_event = Event()


def time_variable_process(t_var):
    result_time = None
    if 'seconds' in t_var:
        result_time = int(t_var.split(' seconds')[0])
    if 'minutes' in t_var:
        result_time = int(t_var.split(' minutes')[0]) * 60
    if 'hours' in t_var:
        result_time = int(t_var.split(' hours')[0]) * 3600
    if 'days' in t_var:
        result_time = int(t_var.split(' days')[0]) * 3600 * 24

    return result_time


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
                if stop_event.is_set():
                    break
            except Empty:
                print("Execution Time:", str(time.time() - start_time))
                os._exit(10)
        return


file_list = []

for root, dirs, files in os.walk(libraryFolderPath):
    rootdirname = Path(root).parts[-1]

    con = connect(dbname=config.DATABASE['dbname'],
                  user=config.DATABASE['user'],
                  password=config.DATABASE['password'])
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


def main():
    if file_list:
        print("Updating documents.....")
        random_ordered_list = []

        for files in file_list:
            for file in files[0]:
                if 'Linux' in platform.platform():
                    random_ordered_list.append(files[1] + '/' + file)
                else:
                    random_ordered_list.append(files[1] + '\\' + file)

        shuffle(random_ordered_list)

        while float(time.time() - start_time) <= float(time_variable_process(config.PARSE['executionTime'])):
            q = Queue()
            for rand_file in random_ordered_list:
                q.put(rand_file)

            new_threads = []
            t = AddDocumentThread(q)
            new_threads.append(t)
            t.start()

            for t in new_threads:
                t.join(float(time_variable_process(config.PARSE['executionTime'])) - float(time.time() - start_time))
        else:
            stop_event.set()
            print("Timed Out")


if __name__ == '__main__':
    main()
    end_time = time.time()
    print("Execution Time:", str(end_time - start_time))
