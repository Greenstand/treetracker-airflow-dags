
import io
import requests

def grower_export(conn, date):
    """Prints a message with the current time"""
    print("capture_export...")
    # check yearMonth (yyyy-mm) format with regex
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
      msg = f'yearMonth format error {date}';
      raise ValueError(msg)

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
        if resource['name'] == f'grower_{date}.csv':
            print('resource already in the CKAN')
            raise ValueError(f'resource {date} already in the CKAN')

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
        file_name = f"grower_{date}.csv"
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