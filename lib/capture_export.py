
import io
import requests

def capture_export(conn, year_month):
    """Prints a message with the current time"""
    print("capture_export...")
    # check yearMonth (yyyy-mm) format with regex
    import re
    if not re.match(r'^\d{4}-\d{2}$', year_month):
        raise ValueError('yearMonth format error')

    # check if the resource are already in the CKAN
    # get the resource list from CKAN
    response = requests.get("https://dev-ckan.treetracker.org/api/3/action/package_show?id=my_dataset_name20211218160056")
    print ('response:', response)
    package_data = response.json()
    print('pacage data:', package_data)
    resources = package_data['result']['resources']
    # go through the resource list
    for resource in resources:
        # check if the resource is already in the CKAN
        if resource['name'] == f'capture_{year_month}.csv':
            print('resource already in the CKAN')
            raise ValueError(f'resource {year_month} already in the CKAN')

    start_date = year_month + "-01"
    # calculate end_date of the month
    import datetime
    # get the last day of the month
    end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
    print ("to export data from:", start_date, "to:", end_date)

    # create cursor
    cur = conn.cursor()
    # array of file names
    columns = ["id","planter_id","device_identifier","planter_identifier","verification_status","species_id","token_id","time_created"
]
    sql = f"""
        SELECT 
            id,
            planter_id,
            device_identifier,
            planter_identifier,
            CASE
            WHEN active = true AND approved = false THEN 'Awaiting'
            WHEN active = true AND approved = true THEN 'Approved'
            WHEN active = false AND approved = false THEN 'Rejected'
            END as verification_status,
            species_id,
            token_id,
            time_created 
        FROM trees 
        WHERE time_created BETWEEN '{start_date}' and '{end_date}' 
        LIMIT 20;""";

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

    try:
        # convert lines to file like object
        f = io.StringIO("\n".join(lines))
        date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"capture_{year_month}.csv"
        r = requests.post('https://dev-ckan.treetracker.org/api/3/action/resource_create', 
            data={
                "package_id":"7753e581-6e93-4eb2-8dea-4ca31f0c4d24",
                "url": "http://test.com/sample.csv",
                "name": file_name,
                "format": "CSV",
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