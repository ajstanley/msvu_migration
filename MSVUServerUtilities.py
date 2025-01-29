# Utility class for functions to be run on the server

import FoxmlWorker as FW
import MSVUUtilities as MU
from pathlib import Path
from urllib.parse import unquote
import csv

class MSVUServerUtilities:
    def __init__(self, namespace):
        self.namespace = namespace
        self.objectStore = '/home/alan/data/objectStore'
        self.datastreamStore = '/home/alan/data/datastreamStore'
        self.staging_dir = 'staging'
        self.mu = MU.MSVUUtilities()


    # Retrieves FOXml object store with pid
    def get_foxml_from_pid(self, pid):
        foxml_file = self.mu.dereference(pid)
        foxml = f"{self.objectStore}/{foxml_file}"
        try:
            return FW.FWorker(foxml)
        except:
            print(f"No results found for {pid}")

    # Gets PIDS, filtered by namespace directly from objectStore
    def get_pids_from_objectstore(self, namespace=''):
        wildcard = '*/*'
        if namespace:
            wildcard = f'*/*{namespace}*'
        pids = []
        for p in Path(self.objectStore).rglob(wildcard):
            pid = unquote(p.name).replace('info:fedora/', '')
            pids.append(pid)
        return pids

    # Gets all dc datastream from objectstore
    s
    def get_all_dc(self, table):
        cursor = self.mu.conn.cursor()
        statement = f"select pid from {table}"
        headers = 'pid', 'dublin_core'
        csv_file_path = f"{self.staging_dir}/{table}_dc.csv"
        with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=headers)  # Pass the file object here
            writer.writeheader()
            for row in cursor.execute(statement):
                pid = row['pid']
                foxml_file = self.mu.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                try:
                    fw = FW.FWorker(foxml)
                except:
                    print(f"No record found for {pid}")
                    continue
                dc = fw.get_dc()
                writer.writerow({'pid': pid, 'dublin_core': dc})





