import json
import psycopg2
import requests


def refresh_country_leader_board(conn, env):
  # check env
  if env != 'dev' and env != 'prod' and env != 'test':
    print ("env must be dev or prod or test")
    return False

  print ("refresh_leader_board")
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
  continents = [
    'Global',
    'Africa',
    'Americas',
    'Asia',
    'Oceania',
  ]

  # put data in dictinary
  continenctsResult = {}

  for continent in continents:
    # get the data from the API
    url = "https://" + env + "-k8s.treetracker.org/query/countries/leaderboard?continent=" + continent
    print ("url:", url)
    response = requests.get(url)
    data = response.json()
    print('data', data)
    continenctsResult[continent] = data

  try:
    json_string = json.dumps(continenctsResult)
    cur.execute(sql_data_insert, (json_string,))
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)

  finally:
      cur.close()
  
  return True