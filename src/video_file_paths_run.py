import json
import datetime
import pandas as pd
from glob import glob 

# mg_db_client 



from pathlib import Path
print("! detecting userId")
userId = Path.home().parts[-1]

'''
Measurement: Video
Fields:
  - filepath
Tags:
  - participantId
  - studyId
'''

db.inventory.insert_one(
    {"tags": {
        "participantID": pID,
        "dataType": dataType,
        "studyName": studyId,
        "userId:" userId,
        "fileName:" fName
        }
    "fields":
        {"filePath": video_file_path}
    })

def
