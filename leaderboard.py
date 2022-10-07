# Import the PostgreSQL library
import psycopg2

# Import database connection settings
from config import config

# Connect to TreeTracker Database and return connection object
def connect():
    
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


# Create the LeaderBoard
def getLeaderBoard(region):

    # Connect to TreeTracker Database
    conn = connect()

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
        # Setup the cursor to execute select statements
        cur = conn.cursor()

        # Execute the SQL statement
        cur.execute(sql)
    
        # Get the first country
        row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


    # List of dictionary objects to return
    leading_countries = []

    # Dictionary key values
    keys = ['planted', 'id', 'name', 'centriod']

    # Dictionary object that holds the country's information using keys above
    country = {}

    # Create the dictionary object and append it to a list
    while row is not None:
        index = 0
        for column in row:
            country[keys[index]] = column
            index += 1
        leading_countries.append(country.copy())
        row = cur.fetchone()

    for item in leading_countries:
        print(item)

    # Close database connection
    conn.close()

    # Return the list of country objects
    return leading_countries

getLeaderBoard('')
