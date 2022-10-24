import psycopg
import os
import json
from dotenv import load_dotenv

# Used for the database connection local environment variable 
load_dotenv()


def getLeaderBoard(region, conn):
    ''' Get the top 10 countries that have planted the most trees from the treetracker database 
    and return a list of dictionary objects with the country details. 
    The region parameter will return the countries by region, if it is blank it will return the countries from all over 
    the world. The conn parameter is the database connection information.'''

    # Connection informaiton to the TreeTracker Database
    conn = conn

    regionName = ''

    match region.upper():
        case 'AFRICA':
            regionName = "Africa"
        case 'ANTARCTICA':
            regionName = 'Antarctica'
        case 'ASIA':
            regionName = 'Asia'
        case 'AUSTRALIA':
            regionName = 'Australia'
        case 'EUROPE':
            regionName = 'Europe'
        case 'NORTHA':
            regionName = 'North America'
        case 'OCEANIA':
            regionName = 'Oceania'
        case 'SOUTHA':
            regionName = 'South America'
        case _:
            regionName = ''

  
    # Limit the number of rows returned
    limit = 10


    if regionName == '':
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
        select r.*, region.name, ST_AsGeoJSON(region.centroid) as centroid  from (
        select count(region.id) as planted, region.id
        from (
            select trees.*  from trees, region c
            where c.name = '{}' and ST_WITHIN(trees.estimated_geometric_location, c.geom)
        ) as trees_in_continent
        LEFT JOIN region
        on ST_WITHIN(trees_in_continent.estimated_geometric_location, region.geom)
        left join region_type
        on region.type_id = region_type.id
        where
        region_type.type = 'country'
        group by region.id
        order by count(region.id) desc
        limit {}
        ) r left join region
        on r.id = region.id'''.format(regionName,limit)

    try:
        # Setup the cursor to execute SQL statements
        cur = conn.cursor()

        # Execute the SQL statement
        cur.execute(sql)
    
        # Get the first country
        row = cur.fetchone()

    except (Exception, psycopg.DatabaseError) as error:
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

    # Close database connection
    conn.close()

    # Return the list of country objects
    return leading_countries


def insertLeaderBoard(leading_countries, conn):
    ''' Take the results from getLeaderBoard and insert that data into the Web.Config 
    database. 
    The leading_countries parameter is the list of dictionary objects from getLeaderBoard.
    The conn parameter is the database connection information.'''

    # Step one is to clean up the databse by deleting the old country leader board data.
    sql_data_delete = ''' delete from webmap.config 
                        where name = 'country-leader-board' '''
    try:
        # Setup the cursor to execute SQL statements
        cur = conn.cursor()

        # Execute the SQL statement
        cur.execute(sql_data_delete)
        conn.commit()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)

    # Next, insert the updated leaderboard data
    sql_data_insert = ''' insert into webmap.config(name,data)
                        values('country-leader-board', %s) '''

    try:
        for row in leading_countries:
            json_string = json.dumps(row)
            cur.execute(sql_data_insert, (json_string,))

        conn.commit()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)

    finally:
        cur.close()


if __name__ == '__main__':

    # Get the local environment variable holding the database connection information.
    DB_URL = os.environ.get('DB_URL')

    # Get the countries from the TreeTracker Database
    with psycopg.connect(DB_URL) as conn:
        leading_countries = getLeaderBoard('', conn)

    # Insert the countries into the Web.Config Database
    with psycopg.connect(DB_URL) as conn:
        insertLeaderBoard(leading_countries, conn)
