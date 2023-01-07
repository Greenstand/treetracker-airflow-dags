import json
import psycopg2
import requests


def refresh_country_leader_board(conn):
  # check env
  continents = [
    'Global',
    'Oceania',
    'South America',
    'Africa',
    'Antarctica',
    'Australia',
    'Asia',
    'Europe',
    'North America'
  ]
  # Limit the number of rows returned
  limit = 10
  # put data in dictinary
  continenctsResult = {}

  for continent in continents:
    print ("continent:", continent)
    # get the data from the API
    if continent == 'Global':
        # SQL Query pulls the number of trees planted by region, region id, region name, and centroid
        # Example: (182490, 6632405, 'Sierra Leone', '{"type":"Point","coordinates":[-11.7927124667898,8.56329593037589]}')
        sql = '''select r2.*,region.name, ST_AsGeoJSON(centroid) as centroid from (
        select count(r.region_id) as planted,r.region_id as region_id from (
        select distinct(tree_id),region_id  from public.active_tree_region
        left join region_type on type_id = region_type.id
        where region_type.type = 'country') r
        group by region_id
        order by planted desc
        limit {}) r2
        left join region
        on r2.region_id = region.id'''.format(limit)
    else:
        sql = '''
          select r.*, region.name, region.centroid from (
          select count(distinct(tree_id)) planted, region_id from active_tree_region
          where region_id in (
          --- select records that it's centroid is within the polygon
          select region.id from region 
          join (select id,name,geom from region where type_id in (select id from region_type where type = 'continents') and name = '{}') continent
          on ST_Contains(continent.geom, region.centroid)
          where type_id in (select id from region_type where type = 'country')
          )
          group by region_id
          order by planted desc
          ) r 
          left join region on r.region_id = region.id
        '''.format(continent)


    try:
        # Setup the cursor to execute SQL statements
        cur = conn.cursor()

        # Execute the SQL statement
        cur.execute(sql)
    
        # Get the first country
        row = cur.fetchone()
        print ("The number of rows: ", cur.rowcount)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




    # Create a list to hold the leading countries in JSON format
    leading_countries = []


    # Dictionary key values
    keys = ["planted", "id", "name", "centriod"]


    # Dictionary object that holds the country's information using keys above
    country = {}


    # Create the dictionary object (that will later be formated as JSON) and append it to a list
    while row is not None:
        index = 0
        for column in row:
            country[keys[index]] = column
            index += 1
        leading_countries.append(country.copy())
        row = cur.fetchone()
    # Return the list of country objects
    continenctsResult[continent] = leading_countries

    # reset connection
    cur.close()
    cur = conn.cursor()

  print ("refresh_leader_board", continenctsResult)
  # Step one is to clean up the databse by deleting the old country leader board data.
  sql_data_delete = ''' delete from webmap.config 
                      where name = 'country-leader-board' '''
  try:
      # Setup the cursor to execute SQL statements
      cur = conn.cursor()

      # Execute the SQL statement
      cur.execute(sql_data_delete)
      conn.commit()

  except (Exception, psycopg2.DatabaseError) as error:
      print(error)

  # Next, insert the updated leaderboard data
  sql_data_insert = ''' insert into webmap.config(name,data)
                      values('country-leader-board', %s) '''

  # declare the list of continents
  try:
    json_string = json.dumps(continenctsResult)
    cur.execute(sql_data_insert, (json_string,))
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)

  finally:
      cur.close()
  
  return True
