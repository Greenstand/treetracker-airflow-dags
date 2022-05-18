import datetime

def assign_new_trees_to_cluster(conn, dry_run = True):
    print("assign new trees to cluster...")

    # assign current time to variable
    very_start = datetime.datetime.now()
    start = datetime.datetime.now()

    # execute query
    cur = conn.cursor()
    cur.execute("""
      SELECT count(id)
      FROM trees
      WHERE trees.active = true
      AND trees.cluster_regions_assigned = false
    """)
    print("SQL result:", cur.query)
    # get result
    count = cur.fetchone()[0]
    print("count of tree that needs to assign to cluster:", count)
    if(count == 0):
        print("no tree needs to assign to cluster")
        return

    # try/except block to catch errors
    try:
        # begin db transaction with isolation level READ COMMITTED
        # the whole job is in this read committed transaction, so this job
        # will just deal with the tree data that goes into DB before the
        # transaction is started, for the trees that will go into the DB 
        # after this transaction is started, the job will be done in the
        # next round of schedule.
        conn.set_isolation_level(0)
        cur.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")

        start = datetime.datetime.now()
        insertSQL = """
            INSERT INTO tree_region
            (tree_id, zoom_level, region_id)
            SELECT DISTINCT ON (trees.id, zoom_level) trees.id AS tree_id, zoom_level, region.id
            FROM (
                SELECT *
                FROM trees
                WHERE trees.active = true
                AND trees.cluster_regions_assigned = false
                LIMIT 1000
            ) trees
            JOIN region
            ON ST_Contains( region.geom, trees.estimated_geometric_location)
            JOIN region_zoom
            ON region_zoom.region_id = region.id
            ORDER BY trees.id, zoom_level, region_zoom.priority DESC
        """
        # print("insertSQL:", insertSQL)
        cur.execute(insertSQL)
        print("SQL result:", cur.query)

        # update all trees that are assigned to cluster
        updateSQL = """
            UPDATE trees
            SET cluster_regions_assigned = true
            FROM tree_region
            WHERE tree_region.tree_id = trees.id
            AND cluster_regions_assigned = false
        """
        # print("updateSQL:", updateSQL)
        cur.execute(updateSQL)
        print("SQL result:", cur.query)

        # print time elapsed
        print("time elapsed:", datetime.datetime.now() - start)
        start = datetime.datetime.now()

        print("update metiarialized views...")
        updateMaterializedViewSQL = """
            REFRESH MATERIALIZED VIEW CONCURRENTLY active_tree_region
        """
        cur.execute(updateMaterializedViewSQL)

        print("time elapsed:", datetime.datetime.now() - start)
        start = datetime.datetime.now()

        # SQL
        zoomLevel14SQL = """
            SELECT 'cluster' AS type,
            St_centroid(clustered_locations) centroid,
            St_numgeometries(clustered_locations) count
            FROM
            (
            SELECT Unnest(St_clusterwithin(estimated_geometric_location, 0.005)) clustered_locations
            FROM   trees
            WHERE  active = true
            ) clusters
        """
        cur.execute(zoomLevel14SQL)

        print("time elapsed:", datetime.datetime.now() - start)
        start = datetime.datetime.now()

        # get all rows
        rows = cur.fetchall()
        print("rows count:", len(rows))
        zoomLevel = 14
        deleteZoomLevelSQL = f"""
            DELETE FROM clusters WHERE zoom_level = {zoomLevel}
        """
        cur.execute(deleteZoomLevelSQL)

        # go through each row
        for row in rows:
            insertSQL = """
                INSERT INTO clusters (count, zoom_level, location) values (%s, %s, %s) RETURNING *
            """
            # execute sql
            cur.execute(insertSQL, (row[2], zoomLevel, row[1]))

        # commit transaction
        if(dry_run == False):
            conn.commit()
            print("transaction committed")
        else:
            print("transaction rollback for dry run")
            conn.rollback()
    except Exception as e:
        print("Error:", e)
        conn.rollback()

    # print time elapsed
    print("total time elapsed:", datetime.datetime.now() - very_start)

    return True