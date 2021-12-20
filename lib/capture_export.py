
import io

def capture_export(conn):
    """Prints a message with the current time"""
    print("capture_export...")
    # create cursor
    cur = conn.cursor()
    # array of file names
    columns = ["id","planter_id","device_identifier","planter_identifier","approved as verification_status","species_id","token_id","time_created"
]
    sql = f"SELECT {','.join(columns)} FROM trees LIMIT 20"
    print("SQL:", sql)
    # execute query
    cur.execute(sql)
    # fetch all rows
    rows = cur.fetchall()
    lines = [','.join(columns)]
    # print rows
    print ("rows:", rows)
    for row in rows:
        print ("row:", row)
        # join row elements with comma
        line = ",".join(str(v) for v in row)
        # add line to lines
        lines.append(line)
    print ("lines:", lines)
    print ("lines length:", len(lines))
    # close connection
    conn.close() 

    print ("upload file")
    import urllib.request as urllib2
    import urllib
    import json
    import pprint
    import datetime
    import requests

    try:
        # convert lines to file like object
        f = io.StringIO("\n".join(lines))
        date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = "capture" + date + ".csv"
        r = requests.post('https://dev-ckan.treetracker.org/api/3/action/resource_create', 
            data={
                "package_id":"7753e581-6e93-4eb2-8dea-4ca31f0c4d24",
                "url": "http://test.com/sample.csv",
                "name": file_name,
                },
            headers={"X-CKAN-API-Key": "270a5f9e-9319-4f5a-983c-1fa50087fa2d"},
            files=[('upload', f)]
            # files={'report.xls': f}
        )
        # read responce from server
        print ("r:", r)
        print ("r.text:", r.text)
    except urllib2.HTTPError as e:
        print('Error code: {0}'.format(e.code))
        print(e.read())
    return "Done"