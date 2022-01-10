
import io
import requests

def capture_export(conn, date, organization_id, ckan_config):
    """Prints a message with the current time"""
    print("capture_export...")
    # check yearMonth (yyyy-mm) format with regex
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
        raise ValueError('date format error')

    # check organization_id is int
    if not isinstance(organization_id, int):
        raise ValueError('organization_id must be int')

    # check if the resource are already in the CKAN
    # get the resource list from CKAN
    url = f"{ckan_config['CKAN_DOMAIN']}/api/3/action/package_show?id={ckan_config['CKAN_DATASET_NAME']}"
    print("url:", url)
    response = requests.get(url,
        headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']}
    )
    print ('response:', response)
    # throw an error if the resource is not found
    package_data = response.json()
    print('pacage data:', package_data)
    id = package_data['result']['id']
    # e.g. 75aeeb9c-f671-408b-8b08-f24ec0edefb0
    # using Regex to check if the id is in the format of UUID
    import re
    if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', id):
        raise ValueError('id format error:', id)
    resources = package_data['result']['resources']
    # go through the resource list
    for resource in resources:
        # check if the resource is already in the CKAN
        if resource['name'] == f'capture_{date}.csv':
            print('resource already in the CKAN')
            raise ValueError(f'resource {date} already in the CKAN')

    # start_date = date + "-01"
    # # calculate end_date of the month
    # import datetime
    # # get the last day of the month
    # end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
    # print ("to export data from:", start_date, "to:", end_date)

    # create cursor
    cur = conn.cursor('server-side-cursor')
    # array of file names
    columns = ["id","planter_id","device_identifier","planter_identifier","verification_status","species_id","token_id","time_created"
]
    sql = f"""
        SELECT 
            trees.id,
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
        JOIN planter
        ON trees.planter_id = planter.id
        WHERE time_created <= '{date}' 
        AND planter.organization_id IN (
            select entity_id from getEntityRelationshipChildren({organization_id})
        )
        """;

    print("SQL:", sql)
    # execute query
    cur.execute(sql)
    import datetime
    # create a new file called 'temp.csv' in the current directory
    date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    file_name = f"capture_{date}.csv"
    with open(file_name, 'w') as f:
        f.write(f"{','.join(columns)}\n")
        # write the data rows
        # fetch 1000 rows at a time
        rows = cur.fetchmany(1000)
        while rows:
            print ("fetched rows:", len(rows))
            for row in rows:
                # write each row to the file
                f.write(','.join(str(col) for col in row) + '\n')
            # fetch next 1000 rows
            rows = cur.fetchmany(1000)
    # close the connection
    cur.close()
    # close the file
    f.close()
    # upload the file to the CKAN
    url = f"{ckan_config['CKAN_DOMAIN']}/api/action/resource_create"
    print("url:", url)
    # upload 'temp.csv' in current directory to url
    files = {'upload': open(file_name, 'rb')}
    # create a dictionary of the data to be posted
    data = {
        'package_id': id,
        'name': file_name,
        'description': 'capture export',
        'format': 'csv',
        'url_type': 'upload',
        'resource_type': 'file.upload',
        'mimetype': 'text/csv',
        'hash': '',
        'size': 0,
        'cache_url': '',
        'cache_last_updated': None,
        'webstore_last_updated': None,
        'upload': None,
        'webstore_url': None,
    }
    # post the data to the CKAN
    response = requests.post(url,
        headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']},
        files=files,
        data=data
    )
    print ('response:', response)

    # delete the file
    import os
    os.remove(file_name)
    return True

    # # now fetch the data and convert to stream object to be used in the next step
    # data = io.StringIO()
    # data.write(f"{','.join(columns)}\n")
    # for row in cur.fetchone():
    #     data.write(f"{row}\n")                                                                  
    #     data.write(f"{','.join([str(col) for col in row])}\n")  # convert to string
    # # close the cursor  and connection
    # cur.close()
    # # return the data


    # # fetch all rows
    # rows = cur.fetchall()
    # lines = [','.join(columns)]
    # # print rows length
    # print("rows length:", len(rows))
    # for row in rows:
    #     # print ("row:", row)
    #     # join row elements with comma
    #     line = ",".join(str(v) for v in row)
    #     # add line to lines
    #     lines.append(line)
    # # print ("lines:", lines)
    # print ("lines length:", len(lines))
    # # close connection
    # conn.close() 

    # print ("upload file")
    # import urllib.request as urllib2
    # import urllib
    # import json
    # import pprint
    # import datetime

    # try:
    #     # convert lines to file like object
    #     f = io.StringIO("\n".join(lines))
    #     date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    #     file_name = f"capture_{date}.csv"
    #     r = requests.post(f"{ckan_config['CKAN_DOMAIN']}/api/3/action/resource_create", 
    #         data={
    #             "package_id": id,
    #             "url": "http://test.com/sample.csv",
    #             "name": file_name,
    #             "format": "CSV",
    #             },
    #         headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']},
    #         files=[('upload', f)]
    #         # files={'report.xls': f}
    #     )
    #     # read responce from server
    #     print ("r:", r)
    #     print ("r.text:", r.text)
    # except urllib2.HTTPError as e:
    #     print('Error code: {0}'.format(e.code))
    #     print(e.read())
    # return True