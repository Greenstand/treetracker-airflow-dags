
import io
import requests

def grower_export(conn, date, ckan_config):
    """Prints a message with the current time"""
    print("capture_export...")
    # check yearMonth (yyyy-mm) format with regex
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
      msg = f'date format error {date}';
      raise ValueError(msg)
    
    import datetime 
    # create a new file called 'temp.csv' in the current directory
    # date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    file_name = f'grower_{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")}.csv'

    # check if the resource are already in the CKAN
    # get the resource list from CKAN
   # get the resource list from CKAN
    url = f"{ckan_config['CKAN_DOMAIN']}/api/3/action/package_show?id={ckan_config['CKAN_DATASET_NAME_GROWER_DATA']}"
    print("url:", url)
    response = requests.get(url,
        headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']}
    )
    print ('response:', response)
    # throw an error if the resource is not found
    package_data = response.json()
    # print('pacage data:', package_data)
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
        if resource['name'] == file_name:
            print('resource already in the CKAN')
            raise ValueError(f'resource {file_name} already in the CKAN')

    # create cursor
    cur = conn.cursor()
    # array of file names
    columns = ["id","first_name","last_name","email","phone","image_url", "registed_at"]
    sql = f"""
SELECT 
  p.id,
  p.first_name,
  p.last_name,
  p.email,
  p.phone,
  p.image_url,
  created_at as registed_at
FROM
  planter p
LEFT JOIN
  planter_registrations pr
ON pr.planter_id = p.id
WHERE
  pr.created_at < '{date}'
ORDER BY p.id DESC
LIMIT 20;
        """;

    print("SQL:", sql)
    # execute query
    cur.execute(sql)
    # fetch all rows
    rows = cur.fetchall()
    lines = [','.join(columns)]
    # print rows
    print ("rows len:", len(rows))
    for row in rows:
        # join row elements with comma
        line = ",".join(str(v) for v in row)
        # add line to lines
        lines.append(line)
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
        url = f"{ckan_config['CKAN_DOMAIN']}/api/action/resource_create"
        print("url:", url)
        r = requests.post(url, 
            data={
                "package_id": id,
                "url": "http://test.com/sample.csv",
                "name": file_name,
                'description': 'export grower data',
                'format': 'csv',
                'url_type': 'upload',
                'resource_type': 'file.upload',
                'mimetype': 'text/csv',
                'hash': '',
                'size': 0,
                'cache_url': '',
                'cache_last_updated': None,
                'webstore_last_updated': None,
                'webstore_url': None,
              },
            headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']},
            files={'upload': (file_name, f)}
            # files={'report.xls': f}
        )
        # read responce from server
        print ("r:", r)
        print ("r.text:", r.text)
    except urllib2.HTTPError as e:
        print('Error code: {0}'.format(e.code))
        print(e.read())
    return "Done"