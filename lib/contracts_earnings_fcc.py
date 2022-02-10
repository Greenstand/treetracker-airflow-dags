
import io
import requests
import psycopg2
import psycopg2.extras


def contract_earnings_fcc_term(conn, start_date, end_date):
  cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  try:
      # these hard coded values are placeholders for the upcoming contracts system
      freetown_stakeholder_uuid = "2a34fa81-0683-4d25-94b9-24843ceec3c4"
      freetown_base_contract_uuid = "483a1f4e-0c52-4b53-b917-5ff4311ded26"
      freetown_base_contract_consolidation_uuid = "a2dc79ec-4556-4cc5-bff1-2dbb5fd35b51"

      sql = f"""
        SELECT COUNT(tree_id) capture_count,
        person_id,
        stakeholder_uuid,
        MIN(time_created) consolidation_start_date,
        MAX(time_created) consolidation_end_date,
        ARRAY_AGG(tree_id) tree_ids
        FROM (
          SELECT trees.id tree_id, person_id, time_created,
          stakeholder_uuid,
          rank() OVER (
            PARTITION BY person_id
            ORDER BY time_created ASC
          )
          FROM trees
          JOIN planter
          ON trees.planter_id = planter.id
          JOIN entity
          ON entity.id = planter.person_id
          AND earnings_id IS NULL
          AND planter.organization_id IN (
            select entity_id from getEntityRelationshipChildren(178)
          )
          AND time_created >= TO_TIMESTAMP(
            '{start_date}',
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND time_created <  TO_TIMESTAMP(
            '{end_date}',
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND trees.approved = true
          AND trees.active = true
        ) rank
        GROUP BY person_id, stakeholder_uuid
        ORDER BY person_id;
      """

      print("sql to run:", sql)

      cursor.execute(sql);
      print("SQL result:", cursor.query)
      for row in cursor:
          print(row)

          #calculate the earnings based on FCC logic
          multiplier = (row['capture_count'] - row['capture_count'] % 100) / 10 / 100
          if multiplier > 1: 
            multiplier = 1
          print( "multiplier " + str(multiplier) )

          maxPayout = 1200000
          earningsCurrency = 'SLL'
          earnings = multiplier * maxPayout

          updateCursor = conn.cursor()
          updateCursor.execute("""
            INSERT INTO earnings.earnings(
              worker_id,
              contract_id,
              funder_id,
              currency,
              amount,
              calculated_at,
              consolidation_rule_id,
              consolidation_period_start,
              consolidation_period_end,
              status,
              captures_count
              )
            VALUES(
              %s,
              %s,
              %s,
              %s,
              %s,
              NOW(),
              %s,
              %s,
              %s,
              'calculated',
              %s
            )
            RETURNING *
        """, ( row['stakeholder_uuid'],
                freetown_base_contract_uuid,
                freetown_stakeholder_uuid,
                earningsCurrency, 
                earnings,
                freetown_base_contract_consolidation_uuid,
                row['consolidation_start_date'],
                row['consolidation_end_date'],
                row['capture_count']
                ))
          print("SQL result:", updateCursor.query)

          earningsId = updateCursor.fetchone()[0]
          print(earningsId)
          updateCursor.execute("""
            UPDATE trees
            SET earnings_id = %s
            WHERE id = ANY(%s)
          """, 
          (earningsId, 
          row['tree_ids']))

      conn.commit()
  except Exception as e:
      print("get error when exec SQL:", e)
      print("SQL result:", updateCursor.query)
      raise ValueError('Error executing query')
  return True

def contract_earnings_fcc(conn):
  cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  try:
      cursor.execute("""
        SELECT * FROM stakeholder.fcc_tiered_configuration ftc 
        WHERE
          active = TRUE
      """);
      print("SQL result:", cursor.query)
      for row in cursor:
        print(row)
        print(f'fcc term: {row["start_date"]} - {row["end_date"]}')
        contract_earnings_fcc_term(conn, row["start_date"], row["end_date"])
  except Exception as e:
      print("get error when exec SQL:", e)
      raise ValueError('Error executing query')
  return True
