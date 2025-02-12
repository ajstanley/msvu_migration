# Utility class for functions to be run on the server
import csv
import shutil
from pathlib import Path
from urllib.parse import unquote

import FoxmlWorker as FW
import MSVUUtilities as MU


class MSVUServerUtilities:
    def __init__(self, namespace):
        self.namespace = namespace
        self.objectStore = '/home/alan/data/objectStore'
        self.datastreamStore = '/home/alan/data/datastreamStore'
        self.staging_dir = 'staging'
        self.mu = MU.MSVUUtilities()
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "text/xml": ".xml",
                        "text/plain": ".txt",
                        "application/pdf": ".pdf",
                        "application/xml": ".xml",
                        "audio/x-wav": ".wav",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                        "application/octet-stream": ".bib",
                        "audio/mpeg": ".mp3",
                        "video/mp4": ".mp4",
                        "video/x-m4v": ".m4v",
                        "audio/vnd.wave": '.wav'
                        }

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
    def get_all_dc(self):
        cursor = self.mu.conn.cursor()
        statement = f"select pid from {self.namespace}"
        headers = 'pid', 'dublin_core'
        csv_file_path = f"{self.staging_dir}/{self.namespace}_dc.csv"
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

    def stage_files(self, pids, datastreams):
        for pid in pids:
            nid = self.mu.get_nid_from_pid(self.namespace, pid)
            if nid == '':
                continue
            fw = self.get_foxml_from_pid(pid)
            all_files = fw.get_file_data()
            for datastream in datastreams:
                if datastream in all_files:
                    file_info = all_files[datastream]
                    source = f"{self.datastreamStore}/{self.mu.dereference(file_info['filename'])}"
                    extension = self.mimemap[file_info['mimetype']]
                    destination = f"{self.staging_dir}/{nid}_{datastream}{extension}"
                    shutil.copy(source, destination)
                    print(f"{nid} {pid} {destination}")
                else:
                    print(f"Datastream not found for {nid}")

    # Builds record directly from objectStore
    def build_record_from_pids(self, namespace, output_file):
        pids = self.get_pids_from_objectstore(namespace)
        headers = ['pid',
                   'content_model',
                   'collection_pid',
                   'page_of',
                   'sequence',
                   'constituent_of']

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for pid in pids:
                foxml_file = self.mu.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = FW.FWorker(foxml)
                if fw.get_state() != 'Active':
                    continue
                relations = fw.get_rels_ext_values()
                row = {}
                row['pid'] = pid
                for relation, value in relations.items():
                    if relation in self.mu.rels_map:
                        row[self.mu.rels_map[relation]] = value
                writer.writerow(row)


MS = MSVUServerUtilities('MSVU')