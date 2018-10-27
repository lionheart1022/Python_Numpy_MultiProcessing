# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 08:37:16 2018

@author: seniortasse

ANALYZE THE DATA in context table, document table, phrase table, phrase origin table and
phrase meaning table.
Calculate the phrase vector space and distances between phrase-context and phrase-phrase.
"""

import config
import numpy as np
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import multiprocessing
from multiprocessing import Process
from threading import Thread, Event
from queue import Queue, Empty
from contextionaryAnalytics import WordVectorSpace
phraseSpace = WordVectorSpace(config.PARSE['distancePercentile'], config.PARSE['bondingIndexPercentile'])


con = connect(dbname=config.DATABASE['dbname'],
              user=config.DATABASE['user'],
              password=config.DATABASE['password'])
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = con.cursor()
cur.execute("""DELETE FROM "phrase_distance_to_context";""")

p = len(phraseSpace.phrases)
c = len(phraseSpace.contexts)

phraseSpace.distanceToContextMatrix = np.zeros((p, c))


def add_distance_to_context(phrase_id, phrase_vector, i):
    phraseSpace.buildDistanceToContextMatrix(phrase_id, phrase_vector, i)


class AddDocumentThread(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                phrase_id = self.queue.get_nowait()[0]
                phrase_vector = self.queue.get_nowait()[1]
                i_var = self.queue.get_nowait()[2]
                add_distance_to_context(phrase_id, phrase_vector, i_var)
            except Empty:
                break
        return


def main():
    q = Queue()

    for phrase_id in phraseSpace.phrases.keys():
        i_var = phraseSpace.phrases[phrase_id].getIndex()
        phraseVector = phraseSpace.phraseVectorSpaceMatrix[i_var]
        phraseVector.shape = (phraseVector.size, 1)
        q.put(phrase_id, phraseVector, i_var)

    new_threads = []
    for count in range(multiprocessing.cpu_count()):
        t = AddDocumentThread(q)
        new_threads.append(t)
        t.start()

    for t in new_threads:
        t.join()


if __name__ == '__main__':
    main()

    for contextID in phraseSpace.contexts.keys():

        context = phraseSpace.contexts[contextID]
        j = context.getRCIndex()

        distance = []
        for phraseID in phraseSpace.phrases.keys():
            phrase = phraseSpace.phrases[phraseID]
            i = phrase.getIndex()

            """
            if the phrase exists in the context
            """
            if phrase.getPhraseCountPerContext()[contextID] > 0:
                distance.append(phraseSpace.distanceToContextMatrix[i, j])

        if not distance:
            context.setLexicalSetBoundary(0)
        else:
            boundary = np.percentile(distance, phraseSpace.distancePercentile)
            context.setLexicalSetBoundary(boundary)

    print("create context lexical set....")
    phraseSpace.createContextLexicalSet()
    print("create phrase lexical set....")
    phraseSpace.createPhraseLexicalSet()

    print("build phrase weight by context matrix....")
    phraseSpace.buildPhraseWeightByContextMatrix()
