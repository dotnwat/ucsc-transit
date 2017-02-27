import time
import datetime
import requests
from google.cloud import bigquery

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset('locations')
table = dataset.table('samples')

table.reload()

while True:
    try:
        r = requests.get('http://bts.ucsc.edu:8081/location/get')
        timestamp = datetime.datetime.utcnow()
        rows = []
        for row in r.json():
            rows.append((timestamp, row['id'], row['lat'], \
                    row['lon'], row['type']))
        if len(rows) > 0:
            errors = table.insert_data(rows)
            if errors:
                msg = "error {}".format(errors)
                print msg
            else:
                print 'loaded {} rows'.format(len(rows))
    except Exception as err:
        msg = "error {}".format(err)
        print msg
        continue
    finally:
        time.sleep(2)
