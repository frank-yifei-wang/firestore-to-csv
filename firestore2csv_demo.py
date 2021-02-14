# Demo of extracting Firestore data into CSV file using a paginated algorithm (can prevent 503 timeout error for large dataset)
# 2021/02/13: Frank Yifei Wang. frank-yifei-wang@github.com. frank.yifei.wang@outlook.com
# %%
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
import csv
import os
import shutil

# %%
def firestore_to_csv_paginated(db, coll_to_read, fields_to_read, csv_filename='extract.csv', max_docs_to_read=-1, write_headers=True):
    """ Extract Firestore collection data and save in CSV file
    Args:
        db: Firestore database object
        coll_to_read: name of collection to read from in Unicode format (like u'CollectionName')
        fields_to_read: fields to read (like ['FIELD1', 'FIELD2']). Will be used as CSV headers if write_headers=True
        csv_filename: CSV filename to save
        max_docs_to_read: max # of documents to read. Default to -1 to read all
        write_headers: also write headers into CSV file
    """

    # Check input parameters
    if (str(type(db)) != "<class 'google.cloud.firestore_v1.client.Client'>") or (type(coll_to_read) is not str) or not (isinstance(fields_to_read, list) or isinstance(fields_to_read, tuple) or isinstance(fields_to_read, set)):
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Unexpected parameters: \n\tdb = {db} \n\tcoll_to_read = {coll_to_read} \n\tfields_to_read = {fields_to_read}')
        return

    # Read Firestore collection and write CSV file in a paginated algorithm
    page_size = 1000   # Preset page size (max # of rows per batch to fetch/write at a time). Adjust in your case to avoid timeout in default 60s
    total_count = 0
    coll_ref = db.collection(coll_to_read)
    docs = []
    cursor = None
    try:
        # Open CSV file and write header if required
        print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started processing collection {coll_to_read}...')
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_to_read, extrasaction='ignore', restval='Null')
            if write_headers: 
                writer.writeheader()
                print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV headers: {str(fields_to_read)} \n---')
        
            # Append each page of data fetched into CSV file
            while True:
                docs.clear()    # Clear page
                count = 0       # Reset page counter

                if cursor:      # Stream next page starting from cursor
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').start_after(cursor).stream()]
                else:           # Stream first page if cursor not defined yet
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').stream()]
            
                print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started writing CSV row {total_count+1}...')    # +1 as total_count starts at 0
                for doc in docs:
                    doc_dict = doc.to_dict()
                    
                    # Process columns (e.g. add an id column)
                    doc_dict['FIRESTORE_ID'] = doc.id   # Capture doc id itself. Comment out if not used
                    
                    # Process rows (e.g. convert all date columns to local time). Comment out if not used
                    for header in doc_dict.keys():
                        if (header.find('DATE') >= 0) and (doc_dict[header] is not None) and (type(doc_dict[header]) is not str):
                            try:
                                doc_dict[header] = doc_dict[header].astimezone()
                            except Exception as e_time_conv:
                                print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in converting timestamp of {doc.id} in {doc_dict[header]}', e_time_conv)

                    # Write rows but skip certain rows. Comment out "if" and unindent "write" and "count" lines if not used
                    if ('TO_SKIP' not in doc_dict.keys()) or (('TO_SKIP' in doc_dict.keys()) and (doc_dict['TO_SKIP'] is not None) and (doc_dict['TO_SKIP'] != 'VALUE_TO_SKIP')):
                        writer.writerow(doc_dict)
                        count += 1

                # Check if finished writing last page or exceeded max limit
                total_count += count                # Increment total_count
                if len(docs) < page_size:           # Break out of while loop after fetching/writing last page (not a full page)
                    break
                else:
                    if (max_docs_to_read >= 0) and (total_count >= max_docs_to_read):
                        break                       # Break out of while loop after preset max limit exceeded
                    else:
                        cursor = docs[page_size-1]  # Move cursor to end of current page
                        continue                    # Continue to process next page

    except Exception as e_read_write:
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in reading Firestore collection / writing CSV file:', e_read_write)
    else:
        print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV file with {total_count} rows of data \n---')

# %%
def copy_file(source, destination):
    """ Copy file to destination folder
    Args:
        source: Source path including filename. Must be raw string like r'my_file.txt'
        destination: Destination folder to copy to. Must be raw string like r'C:\directory\subdirectory' or r'//server/path'
    """
    # Check input parameters
    if (source is None) or (source == '') or (destination is None) or (destination == ''):
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} copy_file() - Blank path: \n\tSource: {source} \n\tDestination: {destination}')
        return

    try:
        print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} copy_file() - Started copying file over... \n\t{source} \n\t{destination}')
        # Make destination folder if not existing
        if not os.path.exists(destination):
            os.mkdir(destination)

        # copy2() preserves file metadata
        shutil.copy2(source, destination)
        print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} copy_file() - Finished copying file \n---')
    except Exception as copy_e:
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} copy_file() - Exception:', copy_e)

# %%
# Connect to Firestore with service account
print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Started connecting to Firestore database...')
firestore_key = r'replace_with_your_firebase_key.json'  # Replace with your Firebase key file
cred = credentials.Certificate(firestore_key)
app = firebase_admin.initialize_app(cred)
db = firestore.client(app=app)
print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Firestore database connected \n---')

# %%
# Specify Firestore schema and extract file info
coll_to_read = u'YourCollection'
csv_headers = [
            'DATE1',
            'DATE2',
            'COL1',
            'COL2'
        ]
csv_filename = r'FirestoreExtract.csv'
csv_filename = csv_filename[0:-4] + datetime.now().strftime("_%Y-%m-%d_%H-%M-%S.csv")   # Timestamp filename in multiple runs. Comment out if not used

# %%
# Extract docs from Firestore
firestore_to_csv_paginated(db, coll_to_read, csv_headers, csv_filename)

# Copy CSV files to destination
copy_file(csv_filename, r'./extracted_csv')
# copy_file(csv_filename, r'C:\directory\subdirectory')
# copy_file(csv_filename, r'//server/path')
# %%
# _ = os.system('pause')    # Uncomment if running in a command window and need to review the messages before exit
