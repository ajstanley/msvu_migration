#!/usr/bin/env python3

import csv
import time

import FoxmlWorker as FW
import MSVUUtilities as MU


class MSVUProcessor:

    def __init__(self, namespace):
        self.objectStore = '/home/alan/data/objectStore'
        self.datastreamStore = '/home/alan/data/datastreamStore'
        self.stream_map = {
            'islandora:sp_pdf': ['OBJ', 'PDF'],
            'islandora:sp_large_image_cmodel': ['OBJ'],
            'islandora:sp_basic_image': ['OBJ'],
            'ir:citationCModel': ['FULL_TEXT'],
            'ir:thesisCModel': ['OBJ', 'PDF', 'FULL_TEXT'],
            'islandora:sp_videoCModel': ['OBJ', 'PDF'],
            'islandora:newspaperIssueCModel': ['OBJ', 'PDF'],
            'islandora:sp-audioCModel': ['OBJ'],
        }
        self.mu = MU.MSVUUtilities()
        self.namespace = namespace
        self.export_dir = '/opt/islandora/msvu_migration'
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
        self.fieldnames = ['id', 'title', 'parent_id', 'field_member_of', 'field_edtf_date_issued', 'field_abstract',
                           'field_genre', 'field_subject', 'field_geographic_subject', 'field_physical_description',
                           'field_extent', 'field_resource_type', 'field_linked_agent', 'field_pid',
                           'field_related_item', 'field_edtf_date_other', 'field_edtf_copyright_data', 'field_issuance',
                           'field_location', 'field_publisher', 'field_edition', 'field_access_condition',
                           'field_model', 'field_edtf_date_created', 'file', 'field_subtitle', 'field_identifier',
                           'field_alternative_title']
        self.start = time.time()

    def get_foxml_from_pid(self, pid):
        foxml_file = self.ca.dereference(pid)
        foxml = f"{self.objectStore}/{foxml_file}"
        try:
            return FW.FWorker(foxml)
        except:
            print(f"No results found for {pid}")

    # Prepares workbench sheet for collection structure

    def prepare_collection_worksheet(self, output_file):
        collection_pids = self.mu.get_collection_pids(self.namespace)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            rows = []
            for entry in collection_pids:
                pid = entry.get('field_pid')
                if 'msvu' not in entry.get('field_member_of'):
                    continue
                mods = self.mu.extract_from_mods(pid)
                row = {'id': pid}
                all_fields = entry | mods
                for key, value in all_fields.items():
                    if type(value) is list:
                        value = '|'.join(value)
                    row[key] = value
                rows.append(row)
            processed = ['msvu:root', 'islandora:root']
            while rows:
                for row in rows:
                    if row.get('field_member_of') in processed:
                        writer.writerow(row)
                        rows.remove(row)
                        processed.append(row.get('id'))

            # Prepares ingest worksheets per collections

    def prepare_collection_member_worksheet(self, collections, output_file):
        details = self.mu.get_collection_member_details('msvu', collections)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.mu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                writer.writerow(row)

    def prepare_book_worksheet(self, output_file):
        details = self.mu.get_book_details('msvu')
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.mu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                node_id = self.mu.get_nid_from_pid('msvu', row['field_member_of'])
                if node_id:
                    row['field_member_of'] = node_id
                    writer.writerow(row)
    def prepare_page_worksheet(self, output_file):
        details = self.mu.get_page_details('msvu')
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.mu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                node_id = self.mu.get_nid_from_pid('msvu', row['field_member_of'])
                row['id'] = row['field_pid']
                if node_id:
                    row['field_member_of'] = node_id
                    writer.writerow(row)


MP = MSVUProcessor('msvu')

MP.prepare_page_worksheet('workbench_sheets/pages.csv')
