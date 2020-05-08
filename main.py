# coding:utf-8

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io


def handle(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
          event (dict):  The dictionary with data specific to this type of
          event. The `data` field contains the PubsubMessage message. The
          `attributes` field will contain custom attributes if there are any.
          context (google.cloud.functions.Context): The Cloud Functions event
          metadata. The `event_id` field contains the Pub/Sub message ID. The
          `timestamp` field contains the publish time.
    """

    if data['name'].endswith('.zip'):
        client = storage.Client()
        bucket = client.get_bucket(data['bucket'])

        blob = bucket.blob(data['name'])
        zipbytes = io.BytesIO(blob.download_as_string())

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(contentfilename)
                    blob.upload_from_string(contentfile)
