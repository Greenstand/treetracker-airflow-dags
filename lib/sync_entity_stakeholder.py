
import io
import requests
import psycopg2
import psycopg2.extras

# I will modify online airflow job to add an extra step to insert new stakeholder record pointing to a planter record.
# This step will scan the table entity find all records that misses corresponding recode in stakeholder (by joining stakeholder_uuid), and insert new recodes into stakeholder.
# Also, I will limit the record to freetown scope (by getEntityRelationshipChildren(178) and type is 'p' in entity, means entity of planter/person.
# Every time we generate data into earnings table, I will execute this task prior earnings generation.
def sync_entity_stakeholder(conn, dry_run = True):
  cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  try:
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
      # execute the query
    result = cursor.execute("""
        --- insert into stakeholder.stakeholder
        --- (id, type, first_name, last_name, email, phone, logo_url)
        select e.stakeholder_uuid as id, 'Person' as type, p.first_name as first_name, p.last_name as last_name, p.email as email, p.phone as phone, p.image_url as logo_url from entity e
        inner join planter p
        on e.id = p.person_id
        where e.stakeholder_uuid not in (select id from stakeholder.stakeholder)
        and type = 'p'
        and p.organization_id IN (
            select entity_id from getEntityRelationshipChildren(178)
        )
        """)
    # print the result
    print("the result:", result)
    # print inserted records
    print("inserted records:", cursor.rowcount)

    rows = cursor.fetchall()
    # go through the result and print the records
    for row in rows:
        print(row)

        # get the organization's stakeholder id
        r = cursor.execute("""
            select s.id
            from entity e
            join planter p on e.id = p.person_id
            join entity ee on ee.id = p.organization_id
            join stakeholder.stakeholder s on s.id = ee.stakeholder_uuid
            where e.stakeholder_uuid = %s;
        """, (row['id'],))

        print("executed:", r);

        print("found count:", cursor.rowcount)
        
        # if the result is not 1, then insert a new record
        if cursor.rowcount != 1:
            # throw an error if the record is not found
            raise Exception("stakeholder record for org not found")

        # print the result
        orgs = cursor.fetchall()
        print("the result:", orgs)

        org_id = orgs[0]['id']
        print("the org id:", org_id)

        # insert the new stakeholder record
        r = cursor.execute("""
            insert into stakeholder.stakeholder 
            (id, type, first_name, last_name, email, phone, logo_url, org_name)
            values (%s, %s, %s, %s, %s, %s, %s, '')
        """, (
            row['id'], 
            row['type'], 
            row['first_name'], 
            row['last_name'], 
            row['email'], 
            row['phone'], 
            row['logo_url']))
        # print inserted records
        print("inserted records to stakeholder:", cursor.rowcount)
        
        # insert relation of stakeholder and organization
        r = cursor.execute("""
            insert into stakeholder.stakeholder_relation
            (parent_id, child_id)
            values (%s, %s)
        """, (
            org_id,
            row['id'],
        ))
        print("inserted records to stakeholder_relation:", cursor.rowcount)

    # commit transaction
    if(dry_run == False):
        conn.commit()
        print("transaction committed")
    else:
        print("transaction rollback for dry run")
        conn.rollback()

  except Exception as e:
      print("get error when exec SQL:", e)
      conn.rollback()
      raise ValueError('Error executing query')
      return False
  return True
