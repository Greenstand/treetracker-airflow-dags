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
        with top_countries_cte as (
            -- this query will group by country name and sum all the planted trees for the country
            -- there are multiple region ids for a given country name
            select sum(num_trees.planted) as planted, region.name from (
                -- count the number of trees for each region id in a specific continent
                select count(distinct(tree_id)) planted, region_id
                from active_tree_region
                where region_id in (
                    --- select records that it's centroid is within the polygon
                    -- select id of all trees within all countries within the specified continent
                    select region.id
                    from region 
                    join (
                        -- select the geom of all continents
                        select id, name, geom
                        from region
                        where type_id in (
                            select id
                            from region_type
                            where type = 'continents'
                        ) and name = '{}'
                    ) continent
                    on ST_Contains(continent.geom, region.centroid)
                    where type_id in (
                        select id from region_type where type = 'country'
                    )
                )
                group by region_id
                order by planted desc
            ) num_trees
            left join region -- append country name to num_trees table
            on num_trees.region_id = region.id
            group by region.name
            order by planted desc
            limit {}
        ),
        region_cte as (
            -- There are multiple region ids for the same country name, e.g.:
            -- select distinct(id), name from region where name = 'United States' order by id
            -- returns id 6632869, 6632870, 6632871, 6632872, 6632873, 6632874, 6632875, etc. for 'United States'
            -- to fix this, we arbitrarily choose the smallest id & centroid associated with the 'United States'
            -- https://stackoverflow.com/questions/6841605/get-top-1-row-of-each-group
            select distinct(name), id, centroid, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id) AS rn
            from region
            order by name, id, rn
        ), 
        region_name_cte as (
            select * from region_cte
            WHERE rn = 1
        )
        -- add the id & centroid to the number of trees for each country
        select top_countries_cte.planted, top_countries_cte.name, region_name_cte.id, region_name_cte.centroid
        from top_countries_cte
        left join region_name_cte
        on top_countries_cte.name = region_name_cte.name
        order by planted desc
        '''.format(continent,limit)


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