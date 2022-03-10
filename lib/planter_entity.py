
import io
import requests
import psycopg2
import psycopg2.extras

# go though all planters, generate unified name as key, create row in entity, and 
# link the entity with the planter
def planter_entity(conn):
  cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  try:
      cursor.execute("""
        SELECT *
        FROM planter
        JOIN
        (
          SELECT regexp_replace((trim(lower(first_name)) ||  trim(lower(last_name))), '[ .-]', '', 'g') as name_key, count(*)
          FROM planter
          WHERE
          planter.organization_id IN (
            select entity_id from getEntityRelationshipChildren(178)
          )
          GROUP BY name_key
          HAVING count(*) = 1
          ORDER BY name_key
        ) eligible_records
        ON regexp_replace((trim(lower(first_name)) ||  trim(lower(last_name))), '[ .-]', '', 'g') = eligible_records.name_key
        WHERE planter.organization_id IN (
          select entity_id from getEntityRelationshipChildren(178)
        )
        AND person_id IS NULL
      """);
      print("SQL result:", cursor.query)
      for row in cursor:
          #do something with every single row here
          #optionally print the row
          print(row)

          updateCursor = conn.cursor()
          updateCursor.execute("""
            INSERT INTO entity
            (type, first_name, last_name, email, phone)
            values
            ('p', %s, %s, %s, %s)
            RETURNING *
          """, ( row['first_name'], row['last_name'], row['email'], row['phone'] ) );

          personId = updateCursor.fetchone()[0];
          print(personId)
          updateCursor.execute("""
            UPDATE planter
            SET person_id = %s
            WHERE id = %s
          """, (personId, row['id']) )

      conn.commit()
  except Exception as e:
      print("get error when exec SQL:", e)
      print("SQL result:", updateCursor.query)
      raise ValueError('Error executing query')
      return False
  return True
