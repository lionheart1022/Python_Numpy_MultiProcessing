# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 15:41:44 2017

@author: GTasse
"""

"""
NOTE:
In the future, we want to be able to change the context of a document as an additional action.
The action will update the document table and the phrase meaning table (count per context will reduce
for old context and count per context will increase for new context).
"""

password = 'postgres'
dbname = 'contextionary'
usr = 'postgres'

"""
The document class helps collect and file documents in the document table
according to their context. Each document is assigned to a context.
Each phrase of a document is counted and the count is updated in the phrase origin table.
The phrase count per context is updated in the phrase meaning table.
"""
import os
import csv
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path


class Document(object):
    def __init__(self, doc_id, documentlocation, phraseMaxLength, contextname):

        import os.path
        from textProcessing import TextProcessor

        print("Initializing Document class.....")

        """
        To initialize the Document class, we need the text of the document and the
        number of words a phrase should contain at maximum.
        --text-- and --phraseMaxLength-- will represent the two arguments needed
        to construct the class
        """
        self.documentlocation = documentlocation
        file = open(self.documentlocation, "r", encoding="UTF-8-sig")
        self.text = file.read()
        file.close()
        self.filename = os.path.basename(self.documentlocation)
        self.phraseMaxLength = phraseMaxLength
        self.doc_id = doc_id
        self.contextname = contextname
        self.phraseTable = []

        """
        Once we have a text, we want to split it in clauses. A sentence is usually divided
        in many clauses often separated by commas.
        --clauses-- will be the list of all the clauses of the text, using the comma and
        the dot as separators.
        Each clause will be made of phrases. A phrase can be 1 word, a chain of multiple
        words (2, 3, 4, 5, .....).
        We want to record the phrases of a document into a dictionary. Each key of the
        dictionary will represent the length of the phrase.
        For example, in the following text: 
        "Always forgive your enemies; nothing annoys them so much.",
        we have 1 sentence made of 2 clauses. The first clause has 4 words and the second
        clause has 5 words.
        The first clause has 3 phrases of 2-words ("Always forgive", "forgive your", "your enemies").
        The second clause has 2 phrases of 4 words ("nothing annoys them so", "annoys them so much").
        We will then define --phraseCounter-- dictionary that will tell us how many times
        a phrase of a given length appears in the document.
        We will also define another dictionary --phraseLengthCounter-- that will count for each
        phrase length the total number of phrases that exist in the document.
        """

        self.textProcessor = TextProcessor(self.text, self.phraseMaxLength)

    def getText(self):

        return self.text

    def getFileName(self):

        return self.filename

    def updatePhraseTables(self):

        con = connect("dbname=%s user=%s password=%s" % (dbname, usr, password))
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            for length in range(1, self.phraseMaxLength + 1):
                for phrase in self.textProcessor.getPhraseCount()[length].keys():
                    self.phraseTable.append([phrase, length, self.textProcessor.getPhraseCount()[length][phrase]])

                    # Update phrase table (if there is no such phrase in the table and new document is created)

                    cur.execute('''SELECT "phrase_id" FROM phrase WHERE "phrase_text" = %s;''', (phrase,))
                    phrase_id = cur.fetchone()

                    if not phrase_id:
                        cur.execute(
                            '''
                              INSERT INTO phrase ("phrase_text", "phrase_length") 
                              VALUES (%s, %s)
                            ''', (phrase, length))

                        cur.execute('''SELECT "phrase_id" FROM phrase WHERE "phrase_text" = %s;''', (phrase,))
                        phrase_id = cur.fetchone()

                    # Update phrase origin and phrase meaning

                    self.updatePhraseOrigin(phrase_id, phrase, length)
                    self.updatePhraseMeaning(phrase_id)

        finally:
            cur.close()
            con.close()
            print('table phrase updated')

    def updatePhraseOrigin(self, phrase_id, phrase, length):

        con = connect("dbname=%s user=%s password=%s" % (dbname, usr, password))
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute('''SELECT "phrase_id" FROM "phrase_origin"
                    WHERE "phrase_id" = %s AND "document_id" = %s''',
                    (phrase_id,
                     self.doc_id
                     ))
        exists = cur.fetchone()

        if not exists:
            cur.execute('''
                        INSERT INTO "phrase_origin" 
                        ("phrase_id", "document_id", "phrase_count_per_document") 
                        VALUES (%s, %s, %s)
                        ''', (phrase_id,
                              self.doc_id,
                              self.textProcessor.getPhraseCount()[length][phrase])
                        )

        cur.close()
        con.close()
        print('table phrase origin updated')

    def updatePhraseMeaning(self, phr_id):

        con = connect("dbname=%s user=%s password=%s" % (dbname, usr, password))
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute("""SELECT "context_id" FROM document WHERE "document_id" = %s;  """, self.doc_id)
        cont_id = cur.fetchone()

        # check if exists in phrase meaning
        cur.execute('''SELECT "phrase_id" FROM "phrase_meaning" WHERE 
                        "phrase_id" = %s 
                        AND "context_id" = %s'''
                    , (phr_id, cont_id[0]))

        exist = cur.fetchone()

        # not exists
        if not exist:
            cur.execute('''
                        INSERT INTO "phrase_meaning" 
                        ("phrase_id" , 
                         "context_id" , 
                         "phrase_count_per_context") 
                        VALUES (%s, %s, 0)''', (phr_id, cont_id[0]))

            if phr_id == 1:
                print("first phrase is not yet into phrase meaning table. Just added now at context ID")
                print(cont_id[0])

        cur.execute('''SELECT "phrase_count_per_context" FROM "phrase_meaning" WHERE 
                "phrase_id" = %s 
                AND "context_id" = %s'''
                    , (phr_id, cont_id[0]))
        cont_old_count = cur.fetchone()

        if phr_id == 1:
            print("first phrase old count was:")
            print(cont_old_count[0])

        cur.execute('''SELECT "phrase_count_per_document"
                                    FROM "phrase_origin"
                                    WHERE "phrase_id" = %s AND
                                    "document_id" = %s'''
                    , (phr_id, self.doc_id))
        doc_count = cur.fetchone()

        if phr_id == 1:
            print("first phrase doc count was:")
            print(doc_count[0])

        cont_new_count = cont_old_count[0] + doc_count[0]

        if phr_id == 1:
            print("first phrase new count is:")
            print(cont_new_count)

        cur.execute('''
                UPDATE "phrase_meaning"
                SET 
                "phrase_count_per_context" = %s WHERE  "phrase_id" = %s 
                        AND "context_id" = %s
                ''', (cont_new_count, phr_id, cont_id[0])
                    )

        cur.close()
        con.close()
        print('table phrase meaning updated')

    def getPhraseTable(self):
        return self.phraseTable

    def getContext(self):
        """
        - Return document context name
        """
        return self.contextname

    def setID(self, doc_ID):
        self.doc_ID = doc_ID

    def getID(self):
        return self.doc_id

    def __str__(self):

        return "I am the Document class"


class Database(object):
    def __init__(self, libraryName, phraseMaximumLength, projectPath, createDatabase=0):

        print("Initializing language universe class.....")

        """
        Define variables
        """

        """
        1- the --libraryName-- is the name of the root context
        2- the -- phraseMaximumLength-- is the maximum number of words a phrase can contain 
        3- the --projectPath-- is the folder path to the project
        4- The --libraryFolderPath-- is the folder path to the library
        text documents (.txt files) and is created from the library name.
        5- the -documents- is the list of all documents
        """

        self.usr = 'postgres'
        self.password = password
        self.dbname = 'contextionary'

        self.libraryName = libraryName
        self.phraseMaximumLength = phraseMaximumLength
        self.projectPath = projectPath
        self.libraryFolderPath = self.createLibraryContextPath()
        self.documents = []

        if createDatabase == 1:
            self.drop()
            self.create()
            self.create_tables()
            self.add_contexts()

        # self.add_documents()

    def create(self):

        con = connect(user=self.usr, host='localhost', password=self.password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            cur.execute('CREATE DATABASE ' + self.dbname)
        finally:
            cur.close()
            con.close()
            print('Database %s created.' % self.dbname)

    def createLibraryContextPath(self):

        for root, dirs, files in os.walk(self.projectPath):
            if Path(root).parts[-1] == self.libraryName:
                return root
        return None

    def create_tables(self):

        con = connect("dbname=" + self.dbname + " user=" + self.usr + " password=" + self.password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        try:

            cur.execute('''CREATE TABLE context (
                        "context_id" serial PRIMARY KEY, 
                        "context_immediate_parent_id" bigint, 
                        "context_name" varchar(255), 
                        "context_children_id" text, 
                        "context_picture" varchar(255),
                        "context_level" integer)''')

            cur.execute('''CREATE TABLE document (
                        "document_id" serial PRIMARY KEY, 
                        "document_title" varchar(255), 
                        "context_id" bigint references context("context_id"), 
                        "document_content" text,
                        "document_path" text)''')

            cur.execute('''CREATE TABLE phrase (
                        "phrase_id" serial PRIMARY KEY, 
                        "phrase_text" varchar(255), 
                        "phrase_length" smallint, 
                        "red_flag" smallint)''')

            cur.execute('''CREATE TABLE "phrase_origin" (
                        "phrase_id" bigint references phrase("phrase_id"),
                        "document_id" bigint references document("document_id"), 
                        "phrase_count_per_document" integer, 
                        PRIMARY KEY ("phrase_id", "document_id"))''')

            cur.execute('''CREATE TABLE "phrase_meaning" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "context_id" bigint references context("context_id"), 
                        "phrase_count_per_context" integer, 
                        "phrase_picture" varchar(255),
                        "en_meaning" text,
                        "fr_meaning" text,
                        "es_meaning" text,
                        "hi_meaning" text,
                        "zh_meaning" text,
                        "fr_meaning_translation" varchar(255),
                        "es_meaning_translation" varchar(255),
                        "hi_meaning_translation" varchar(255),
                        "zh_meaning_translation" varchar(255),
                        "fr_phrase_translation" varchar(255),
                        "es_phrase_translation" varchar(255),
                        "hi_phrase_translation" varchar(255),
                        "zh_phrase_translation" varchar(255),
                        "phrase_part_of_speech" varchar(255),
                        PRIMARY KEY ("phrase_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "phrase_vector_space" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "context_id" bigint references context("context_id"), 
                        "phrase_relative_frequency" decimal, 
                        PRIMARY KEY ("phrase_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "context_axis" (
                        "context_id" bigint references context("context_id"),
                        "independent_context_id" bigint references context("context_id"), 
                        "axis_coordinate" decimal, 
                        PRIMARY KEY ("context_id", "independent_context_id")
                        )''')

            cur.execute('''CREATE TABLE "phrase_distance_to_context" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "context_id" bigint references context("context_id"), 
                        "phrase_distance_to_context" decimal, 
                        PRIMARY KEY ("phrase_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "phrase_weight_by_context" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "context_id" bigint references context("context_id"), 
                        "phrase_weight" decimal, 
                        PRIMARY KEY ("phrase_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "context_phrase" (                         
                        "context_id" bigint references context("context_id"), 
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "work_order" bigint, 
                        PRIMARY KEY ("context_id","phrase_id")
                        )''')

            cur.execute('''CREATE TABLE "related_phrase" (
                        "context_id" bigint references context("context_id"), 
                        "context_phrase_id" bigint references phrase("phrase_id"), 
                        "related_phrase_id" bigint references phrase("phrase_id"), 
                        "phrase_bonding_index" decimal, 
                        PRIMARY KEY ("context_id", "context_phrase_id","related_phrase_id")
                        )''')

            cur.execute('''CREATE TABLE "shared_word" (
                        "long_phrase_id" bigint references phrase("phrase_id"), 
                        "sibling_id" bigint references phrase("phrase_id"), 
                        "shared_word" varchar(255), 
                        "shared_word_position_in_long_phrase" bigint, 
                        "shared_word_position_in_sibling" bigint, 
                        PRIMARY KEY ("long_phrase_id", "sibling_id","shared_word")
                        )''')

            cur.execute('''CREATE TABLE "context_spelling_similarity" (
                        "context_id" bigint references context("context_id"), 
                        "similar_spelling_context_id" bigint references context("context_id"), 
                        "similarity_index" bigint,
                        PRIMARY KEY ("context_id", "similar_spelling_context_id")
                        )''')

            cur.execute('''CREATE TABLE "phrase_spelling_similarity" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "similar_spelling_phrase_id" bigint references phrase("phrase_id"), 
                        "similarity_index" bigint,
                        PRIMARY KEY ("phrase_id", "similar_spelling_phrase_id")
                        )''')

            cur.execute('''CREATE TABLE "phrase_frequency_and_distance" (
                        "phrase_id" bigint references phrase("phrase_id"), 
                        "context_id" bigint references context("context_id"), 
                        "phrase_relative_frequency" decimal, 
                        "phrase_distance_to_context" decimal, 
                        "phrase_difficulty" integer, 
                        PRIMARY KEY ("phrase_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "input_text_context_identifier" (
                        "input_text_id" bigint,
                        "context_id" bigint, 
                        "context_weight" decimal, 
                        PRIMARY KEY ("input_text_id", "context_id")
                        )''')

            cur.execute('''CREATE TABLE "input_text_word_position" (
                        "input_text_id" bigint,
                        "word_position" bigint, 
                        "word_text" varchar(255), 
                        PRIMARY KEY ("input_text_id", "word_position")
                        )''')

            cur.execute('''CREATE TABLE "input_text_keywords" (
                        "input_text_id" bigint, 
                        "context_id" bigint, 
                        "keyword_id" bigint, 
                        "keyword_location" varchar(255), 
                        "keyword_text" varchar(255), 
                        "phrase_id" bigint, 
                        PRIMARY KEY ("input_text_id", "context_id", "keyword_id")
                        )''')

        finally:
            cur.close()
            con.close()
            print('tables created')

    def add_contexts(self):

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:

            data_list = []
            completed_list = []

            file = 'Context list.csv'
            with open(file) as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                for row in reader:
                    data_list.append(row)

            context_children_id_list1 = []
            context_children_id_list2_all = []

            self.get_parent_id(data_list, context_children_id_list1, completed_list)

            self.get_sub_child_id(data_list, context_children_id_list1, context_children_id_list2_all, completed_list)

            self.get_last_child_id(data_list, context_children_id_list2_all, completed_list)

            for c_list in completed_list:
                context_id = c_list[0]
                context_immediate_parent_id = c_list[1]
                context_name = c_list[2]
                context_children_id = c_list[3]
                context_picture = c_list[4]
                context_level = c_list[5]
                cur.execute('''insert into context 
                                ("context_id", "context_immediate_parent_id", "context_name", "context_children_id", 
                                "context_picture","context_level") 
                                VALUES (%s, %s, %s, %s, %s, %s)''', (
                    context_id,
                    context_immediate_parent_id,
                    context_name, context_children_id, context_picture, context_level))
        finally:
            cur.close()
            con.close()

    def get_parent_id(self, data_list, context_children_id_list, completed_list):
        for data in data_list:
            context_id = int(data[0])
            context_immediate_parent_id = int(data[2])
            context_name = data[1]
            context_picture = '{}-{}.jpg'.format(str(context_id), context_name)

            if context_immediate_parent_id == -1:
                context_level = 1
                for sub_data in data_list:
                    if context_id == int(sub_data[2]):
                        context_children_id_list.append(sub_data[0])
                context_children_id = ','.join(context_children_id_list)
                completed_list.append([context_id, context_immediate_parent_id, context_name,
                                       context_children_id, context_picture, context_level])

    def get_sub_child_id(self, data_list, context_children_id_list, context_child_id_total, completed_list):
        for data in data_list:
            for context_child_id in context_children_id_list:
                context_id = int(data[0])
                context_immediate_parent_id = int(data[2])
                context_name = data[1]
                context_picture = '{}-{}.jpg'.format(str(context_id), context_name)
                context_level = 2

                context_children_id_list2 = []

                if context_id == int(context_child_id):
                    for sub_data in data_list:
                        if context_id == int(sub_data[2]):
                            context_children_id_list2.append(sub_data[0])
                    context_children_id = ','.join(context_children_id_list2)
                    completed_list.append([context_id, context_immediate_parent_id, context_name,
                                           context_children_id, context_picture, context_level])
                context_child_id_total.extend(context_children_id_list2)

    def get_last_child_id(self, data_list, context_children_id_list, completed_list):
        for data in data_list:
            for context_child_id in context_children_id_list:
                context_id = int(data[0])
                context_immediate_parent_id = int(data[2])
                context_name = data[1]
                context_picture = '{}-{}.jpg'.format(str(context_id), context_name)
                context_level = 3
                context_children_id = '0'
                if context_id == int(context_child_id):
                    completed_list.append([context_id, context_immediate_parent_id, context_name,
                                           context_children_id, context_picture, context_level])

    def add_document(self, file_path):
        name = Path(file_path).parts[-1]
        temp_name = '/' + name
        root = file_path.split(temp_name)[0]
        rootdirname = Path(root).parts[-1]
        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        doc_path = root.split('Context tree/')[1] + '/' + name
        cur.execute(""" SELECT count(*) FROM document WHERE "document_path" = %s; """,
                    ([doc_path]), )
        docpathcount = cur.fetchone()

        try:
            if name.endswith(".txt") and (docpathcount[0] == 0):
                dummytitle = name
                cur.execute(
                    '''
                    INSERT INTO document 
                    ("document_title", "context_id", "document_content", "document_path") 
                    VALUES (%s, %s, %s, %s)
                    ''', (dummytitle, 1, 1, doc_path))

                cur.execute("""SELECT "document_id" FROM document WHERE "document_title"=%s;""",
                            ([dummytitle]), )
                doc_id = cur.fetchone()

                filelocation = os.path.join(root, name)
                document = Document(doc_id, filelocation, self.phraseMaximumLength, rootdirname)
                self.documents.append(document)

                b = document.getContext()
                cur.execute("""SELECT "context_id" FROM context WHERE "context_name" = %s;  """, ([b]), )
                cont_id = cur.fetchone()

                # update entries in document table
                cur.execute(
                    '''
                    UPDATE document SET
                    "document_title" = %s, 
                    "context_id" = %s,
                    "document_content" = %s,
                    "document_path" = %s
                    WHERE "document_id" = %s; 
                    ''', (name, cont_id[0], document.getText(), doc_path, doc_id))

                # update entries in phrase table, phrase origin table and phrase meaning table
                document.updatePhraseTables()
                print('table document updated')
        finally:
            cur.close()
            con.close()

    def delete_entry(self):

        s1 = '''Please enter the table for entry deletion:
        1 - Context
        2 - Document'''
        print(s1)
        table = input()

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        try:
            print('Please enter ID for deletion')
            ID_for_del = input()

            if table == '1':
                self.delete_context(ID_for_del)

            if table == '2':
                self.delete_document(ID_for_del)

        finally:
            cur.close()
            con.close()
            print('The entry was deleted')

    def delete_context(self, cont_for_del):

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute('''SELECT "document_id" FROM document WHERE "context_id" = %s;''', cont_for_del)
        doc_for_del = cur.fetchall()

        for dfd in doc_for_del:
            self.delete_document(dfd)

        cur.execute('DELETE FROM context WHERE "context_id" = %s;', cont_for_del)

        cur.close()
        con.close()

    def delete_document(self, doc_id_for_del):

        con = connect("dbname=contextionary user=postgres password='password'")
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        """
        Get the context ID for the document to delete
        """
        cur.execute("""SELECT "context_id" FROM document WHERE "document_id" = %s;  """, doc_id_for_del)
        cont_id = cur.fetchone()

        try:

            """
            Get the list of phrases of the document to delete.
            All these phrases-document have to be deleted from the phrase origin table
            And the phrases-context count per context have to be updated in the phrase meaning table
            """
            cur.execute('''SELECT "Phrase ID" FROM "phrase origin" WHERE "document_id" = %s;''', doc_id_for_del, )
            phrases_for_del = cur.fetchall()

            for phr in phrases_for_del:

                """
                Record the phrase count per document to delete
                """
                cur.execute(
                    '''SELECT "phrase_count_per_document"
                       FROM "phrase_origin"
                       WHERE "phrase_id" = %s 
                       AND "document_id" = %s
                    ''', (phr, doc_id_for_del))
                doc_count = cur.fetchone()

                """
                Recall the phrase count per context. This count needs to be
                reduced by the phrase count per document.
                """
                cur.execute(
                    '''SELECT "phrase_count_per_context" 
                       FROM "phrase_meaning" 
                       WHERE "phrase_id" = %s 
                       AND "context_id" = %s
                    ''', (phr, cont_id[0]))
                cont_old_count = cur.fetchone()

                """
                Calculate the new phrase count per context and update the count into
                the phrase meaning table.
                """
                cont_new_count = cont_old_count[0] - doc_count[0]

                cur.execute('''
                        UPDATE "phrase_meaning"
                        SET "phrase_count_per_context" = %s
                        WHERE "phrase_id" = %s 
                        AND "context_id" = %s
                        ''', (cont_new_count, phr, cont_id))

                """
                Delete the phrase-document to delete entry from the phrase origin table
                """
                self.delete_phrase_origin(int(phr[0]), doc_id_for_del)

                """
                Delete the phrase-context entry from the phrase meaning table
                only if the phrase count per context is equal to 0.
                """
                cur.execute(
                    '''SELECT "phrase_id" 
                       FROM "phrase_meaning" 
                       WHERE "context_id"=%s 
                       AND "phrase_count_per_context"=0
                    ''', cont_id)
                phrase_meaning_for_del = cur.fetchall()

                for phrm_for_del in phrase_meaning_for_del:
                    self.delete_phrase_meaning(phrm_for_del, cont_id)

                """
                Delete phrase from phrase table in case the phrase does not exist anymore in
                the phrase origin table
                """
                cur.execute('''SELECT "phrase_id" FROM "phrase_origin" WHERE "phrase_id"=%s''', phr)
                exist = cur.fetchone()
                if not exist:
                    cur.execute('''DELETE FROM phrase WHERE "phrase_id"=%s ''', phr)

            """
            The document can now be deleted from the document table
            """
            cur.execute('DELETE FROM document WHERE "document_id" = %s;', doc_id_for_del, )
        finally:
            cur.close()
            con.close()

    def delete_phrase(self, phrase_id_for_del):

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute('DELETE FROM phrase WHERE "phrase_id" = %s;', ([phrase_id_for_del]))

        cur.close()
        con.close()

    def delete_phrase_origin(self, phrase_id_for_del, doc_id_for_del):

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute('DELETE FROM "phrase_origin" WHERE "phrase_id" = %s AND "document_id" = %s;',
                    (phrase_id_for_del, doc_id_for_del))

        cur.close()
        con.close()

    def delete_phrase_meaning(self, phrase_id_for_del, cont_id_for_del):

        con = connect("dbname=contextionary user=postgres password=%s" % password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute('DELETE FROM "phrase_meaning" WHERE "phrase_id" = %s AND "context_id" =%s;',
                    (phrase_id_for_del, cont_id_for_del))

        cur.close()
        con.close()

    def drop(self):

        con = connect(user=self.usr, host='localhost', password=self.password)
        dbname = self.dbname
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            cur.execute('''SELECT pg_terminate_backend(pg_stat_activity.pid)
                         FROM pg_stat_activity
                         WHERE pg_stat_activity.datname = 'contextionary'
                         AND pid <> pg_backend_pid()''')
            cur.execute('DROP DATABASE ' + dbname)
        finally:
            cur.close()
            con.close()
            print('Database deleted')

    def change_context(self):

        print('Please enter document number')
        doc_n = input()

        print('Please provide the new context name')
        new_cont_name = input()

        con = connect("dbname=" + self.dbname + " user=" + self.usr + " password=" + self.password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            cur.execute('''SELECT "context_id" FROM "context" WHERE "context_name" = %s''', ([new_cont_name]))
            new_cont_id = cur.fetchone()
            print(new_cont_id[0])

            cur.execute('''SELECT "context_id" FROM "document" WHERE "document_id" = %s''', ([doc_n]))
            old_cont_id = cur.fetchone()

            print('new_cont_id = ', new_cont_id[0], ' old_cont_id = ', old_cont_id[0])

            if new_cont_id[0] == old_cont_id[0]:
                print('Context name is the same. No changes required.')
            else:

                cur.execute('''SELECT "phrase_id" FROM "phrase_origin" WHERE "document_id" = %s''', doc_n)
                phr_in_doc = cur.fetchall()

                for phr in phr_in_doc:
                    cur.execute('''
                            UPDATE "phrase_meaning" 
                            SET "phrase_count_per_context" = "phrase_count_per_context" 
                            - (SELECT "phrase_count_per_document"
                            FROM "phrase_origin" 
                            WHERE ("phrase_id" = %s AND "document_id" = %s))
                            WHERE "phrase_id" = %s AND "context_id" = %s
                            '''
                                , (phr, doc_n, phr, old_cont_id[0])
                                )
                    cur.execute(
                        '''DELETE FROM "phrase_meaning" 
                           WHERE "phrase_count_per_context" <= 0 OR "phrase_count_per_context" IS NULL''')

                    cur.execute(
                        '''UPDATE "document" SET "context_id" = %s
                           WHERE "document_id" = %s''', (new_cont_id, doc_n))

                    cur.execute('''SELECT count(*) FROM "phrase_meaning" WHERE
                            "phrase_id" = %s
                            AND "context_id" = %s''', (phr, new_cont_id))
                    exist = cur.fetchone()

                    if exist[0] == 0:
                        cur.execute(
                            '''INSERT INTO "phrase_meaning" 
                                ("phrase_id", "context_id", "phrase_count_per_context")
                                VALUES (%s, %s, (SELECT SUM("phrase_count_per_document") 
                                FROM "phrase_origin" 
                                WHERE ("phrase_id" = %s) 
                                AND ("document_id" IN (SELECT "document_id" FROM "document" WHERE "dontext_id" = %s)
                                )))''', (phr, new_cont_id, phr, new_cont_id))

                    else:
                        cur.execute('''
                            UPDATE "phrase_meaning" 
                            SET "phrase_count_per_context" = (SELECT SUM("phrase_count_per_document") 
                            FROM "phrase_origin" 
                            WHERE "phrase_id" = %s AND ("document_id" IN (SELECT "document_id" FROM "document" WHERE "context_id" = %s)))
                            WHERE "phrase_id" = %s AND "context_id" = %s
                            ''', (phr, new_cont_id[0], phr, new_cont_id[0]))

                print('Document context was changed')

        finally:
            cur.close()
            con.close()

    def __str__(self):

        return "I am the Database class"
