#!/usr/bin/env python3

import csv
import hashlib
import sqlite3
import urllib
import urllib.parse

import ModsTransformer as MT


class MSVUUtilities:
    def __init__(self):
        self.conn = sqlite3.connect('msvu.db')
        self.conn.row_factory = sqlite3.Row
        self.fields = ['PID', 'model', 'RELS_EXT_isMemberOfCollection_uri_ms', 'RELS_EXT_isPageOf_uri_ms']
        self.objectStore = '/usr/local/fedora/data/objectStore/'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore/'
        self.rels_map = {'isMemberOfCollection': 'collection_pid',
                         'isMemberOf': 'collection_pid',
                         'hasModel': 'content_model',
                         'isPageOf': 'page_of',
                         'isSequenceNumber': 'sequence',
                         'isConstituentOf': 'constituent_of'
                         }
        self.mt = MT.ModsTransformer()

    # Adds node_id to table
    def add_node_ids(self, table, csv_file):
        cursor = self.conn.cursor()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                command = f"update {table} set node_id = '{row['ID']}' where pid = '{row['PID']}'"
                cursor.execute(command)
        self.conn.commit()

    # Identifies object and datastream location within Fedora objectStores and datastreamStore.
    def dereference(self, identifier: str) -> str:
        # Replace '+' with '/' in the identifier
        slashed = identifier.replace('+', '/')
        full = f"info:fedora/{slashed}"

        # Generate the MD5 hash of the full string
        hash_value = hashlib.md5(full.encode('utf-8')).hexdigest()

        # Pattern to fill with hash (similar to the `##` placeholder)
        subbed = "##"

        # Replace the '#' characters in `subbed` with the corresponding characters from `hash_value`
        hash_offset = 0
        pattern_offset = 0
        result = list(subbed)

        while pattern_offset < len(result) and hash_offset < len(hash_value):
            if result[pattern_offset] == '#':
                result[pattern_offset] = hash_value[hash_offset]
                hash_offset += 1
            pattern_offset += 1

        subbed = ''.join(result)
        # URL encode the full string, replacing '_' with '%5F'
        encoded = urllib.parse.quote(full, safe='').replace('_', '%5F')
        return f"{subbed}/{encoded}"

    # Gets all pages from book
    def get_pages(self, table, book_pid):
        cursor = self.conn.cursor()
        command = f"SELECT PID from {table} where page_of = '{book_pid}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    # Gets all books in the repository.
    def get_books(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}' AND CONTENT_MODEL = 'islandora:bookCModel' "
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    # Processes CSV returned from direct objectStore harvest
    def process_clean_institution(self, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists MSVU(
            pid TEXT PRIMARY KEY,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT
            )""")
        self.conn.commit()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                collection = row['collection_pid']
                page_of = row['page_of']
                if not page_of:
                    page_of = ' '
                constituent_of = row['constituent_of']
                if not constituent_of:
                    constituent_of = ' '
                try:
                    command = f"INSERT OR REPLACE INTO  MSVU VALUES('{row['pid']}', '{row['content_model']}', '{collection}','{page_of}', '{row['sequence']}','{constituent_of}')"
                    cursor.execute(command)
                except sqlite3.Error:
                    print(command)
                    print(row['pid'])
        self.conn.commit()

    # Get all collection contents within namespace
    def get_collection_content_pids(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT * from {table} where collection_pid = '{collection}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row['pid'])
        return pids

    # Get all collection details within namespace
    def get_collection_details(self, table):
        command = f"SELECT * from {table} where content_model = 'islandora:collectionCModel'"
        return self.get_details(command)

    # Get collection member's details.
    def get_collection_member_details(self, table, collections):
        collections = f"({', '.join([repr(item) for item in collections])})"
        command = f"SELECT * FROM {table} WHERE collection_pid IN {collections}"
        return self.get_details(command)

    # Gets all books in repository
    def get_book_details(self, table):
        command = f"Select * from {table} where content_model = 'islandora:bookCModel'"
        return self.get_details(command)

    # Gets all pages in repository
    def get_page_details(self, table):
        command = f"Select * from {table} where content_model = 'islandora:pageCModel'"
        return self.get_details(command)

    # Utility function to prepare database selections for workbenchl
    def get_details(self, command):
        cursor = self.conn.cursor()
        details = []
        for row in cursor.execute(command):
            keys = row.keys()
            line = {}
            for key in keys:
                line[key] = (row[key])
            cleaned_line = self.map_worksheet_values(line)
            details.append(cleaned_line)
        return details

    # Map D7 values to D10
    def map_worksheet_values(self, line):
        map = {
            'content_model': 'field_model',
            'pid': 'field_pid',
            'collection_pid': 'field_member_of',
            'page_of': 'field_member_of',
        }
        content_map = {
            'islandora:collectionCModel': 'collection',
            'islandora:sp_large_image_cmodel': 'image',
            'islandora:sp-audioCModel': 'audio',
            'islandora:pageCModel': 'page',
            'islandora:bookCModel': 'Paged Content',
            'islandora:compoundCModel': 'Compound Object',
            'islandora:sp_pdf': 'Digital Document',
            'islandora:sp_basic_image': 'image',
            'islandora:newspaperCModel': 'newspaper',
            'islandora:newspaperIssueCModel': 'Publication Issue',
            'islandora:newspaperPageCModel': 'page',
            'islandora:oralhistoriesCModel': 'Compound Object',
            'islandora:sp_videoCModel': 'Video',
            'ir:thesisCModel': 'Digital Document',
            'islandora:rootSerialCModel': 'Compound Object',
            'islandora:intermediateCModel': 'Compound Object',
            'ir:citationCModel': 'Citation'
        }

        cleaned_line = {}
        for key, value in line.items():
            if key in map:
                if value.strip():
                    cleaned_line[map[key]] = value

        cleaned_line['field_model'] = content_map[cleaned_line['field_model']]
        return cleaned_line

    # Get all content models from map
    def get_collection_pid_model_map(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}'"
        map = {}
        for row in cursor.execute(command):
            map[row[0]] = row[1]
        return map

    def get_subcollections(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}' AND CONTENT_MODEL = 'islandora:collectionCModel' "
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    def get_collection_recursive_pid_model_map(self, table, collection_pid):
        descendants = {}
        cursor = self.conn.cursor()
        command = f"select PID, CONTENT_MODEL from {table} where COLLECTION_PID = '{collection_pid}'"
        child_collections = []
        books = []
        for row in cursor.execute(command):
            if row['content_model'] in ['islandora:collectionCModel', 'islandora:bookCModel']:
                child_collections.append(row['PID'])
                descendants[row['PID']] = row['content_model']
            else:
                descendants[row['PID']] = row['content_model']
        while child_collections:
            child_collection = child_collections.pop(0)
            command = f"select PID, CONTENT_MODEL from {table} where COLLECTION_PID = '{child_collection}' or page_of = '{child_collection}' "
            for row in cursor.execute(command):
                if row['content_model'] in ['islandora:collectionCModel', 'islandora:bookCModel']:
                    child_collections.append(row['PID'])
                    descendants[row['PID']] = row['content_model']
                else:
                    descendants[row['PID']] = row['content_model']
        return descendants

    def extract_from_mods(self, pid):
        cursor = self.conn.cursor()
        command = f"SELECT MODS from MSVU where PID = '{pid}'"
        result = cursor.execute(command).fetchone()
        mods = result['MODS']
        if mods is not None and len(mods) < 10:
            return {}
        return self.mt.extract_from_mods(mods)

    # Get node_id associated with pid.
    def get_nid_from_pid(self, table, pid):
        cursor = self.conn.cursor()
        command = f"SELECT node_id from {table} where PID = '{pid}'"
        result = cursor.execute(command).fetchone()
        return result['node_id'] if result is not None else ''




if __name__ == '__main__':
    MU = MSVUUtilities()

    MU.add_node_ids('msvu', 'inputs/nid-pid.csv')

