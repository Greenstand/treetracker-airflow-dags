
import io
import requests
import os
import urllib.request as urllib2

def earning_export(conn, start_date, end_date, ckan_config):
    """Prints a message with the current time"""
    print("earning_export...")
    # check yearMonth (yyyy-mm) format with regex
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_date):
        raise ValueError(f'date format error (yyyy-mm-dd): {start_date}')
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', end_date):
        raise ValueError(f'date format error (yyyy-mm-dd): {start_date}')
    
    # get substring of start_date, for example: 2019-01
    import re
    year_month = re.search(r'^(\d{4})-(\d{2})-\d{2}$', start_date)
    year = year_month.group(1)
    month = year_month.group(2)
    print ('year_month:', year, month)
    file_name = f'earning-{year}-{month}.csv'
    print ('file_name:', file_name)

    # check if the resource are already in the CKAN
    # get the resource list from CKAN
    url = f"{ckan_config['CKAN_DOMAIN']}/api/3/action/package_show?id={ckan_config['CKAN_DATASET_NAME_EARNING_DATA']}"
    print("url:", url)
    response = requests.get(url,
        headers={"X-CKAN-API-Key": ckan_config['CKAN_API_KEY']}
    )
    print ('response:', response)
    # throw an error if the resource is not found
    package_data = response.json()
    print('pacage data:', package_data)
    id = package_data['result']['id']
    print ('id:', id)
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

    # start_date = date + "-01"
    # # calculate end_date of the month
    # import datetime
    # # get the last day of the month
    # end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
    # print ("to export data from:", start_date, "to:", end_date)

    # create cursor
    cur = conn.cursor('server-side-cursor')
    # array of file names
    columns = [
'id',
'worker_id',                 
'contract_id',
'funder_id',                
'amount',                  
'currency',  
'calculated_at',               
'consolidation_rule_id',       
'consolidation_period_start',  
'consolidation_period_end',    
'payment_confirmation_id',     
'payment_system',              
'payment_confirmed_by',        
'payment_confirmation_method',
'paid_at',                     
'payment_confirmed_at',        
'status',                      
'batch_id',]

    sql = f"""
SELECT 
    id,
    worker_id,                 
    contract_id,
    funder_id,                
    amount,                  
    currency,  
    calculated_at,               
    consolidation_rule_id,       
    consolidation_period_start,  
    consolidation_period_end,    
    payment_confirmation_id,     
    payment_system,              
    payment_confirmed_by,        
    payment_confirmation_method,
    paid_at,                     
    payment_confirmed_at,        
    status,                      
    batch_id
FROM earnings.earnings 
WHERE
paid_at > '{start_date} 00:00'
AND paid_at < '{end_date} 23:59'
        """;

    print("SQL:", sql)
    # execute query
    cur.execute(sql)
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

    # convert lines to file like object
    f = io.StringIO("\n".join(lines))
    url = f"{ckan_config['CKAN_DOMAIN']}/api/action/resource_create"
    print("url:", url)
    response = requests.post(url, 
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
    print ("response:", response)
    print ("response.text:", response.text)

    # check response status code is 200
    if response.status_code != 200:
        # print http response body
        print(response.text)
        print ('response:', response)
        # print response as json
        print('json:', response.json())
        raise ValueError('response status code is not 200')
    return True