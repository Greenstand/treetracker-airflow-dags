import datetime

import psycopg2

def upload_planter_info(conn, host, user, password, dry_run = True):
    print("dump planter info and upload...")

    # print host, user, password
    print("host:", host)
    print("user:", user)
    # replace the password with stars
    print("password:", "*" * len(password))

    # check host and user
    if host is None or user is None or password is None:
        print("host or user is None")
        return
    

    # assign current time to variable
    very_start = datetime.datetime.now()

    # execute query
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.first_name, p.last_name, count(t.id), p.organization_id, p.image_url
        FROM public.trees t join planter p on t.planter_id = p.id
        WHERE p.organization_id in (select entity_id from getEntityRelationshipChildren(178))
        group by p.id order by count(t.id) desc;
    """)
    print("SQL result:", cur.query)
    # get result
    planter = cur.fetchall();
    print("planter count:", len(planter))

    # compose csv 
    csv = "id,first_name,last_name,tree_count,organization_id,image_url\n"
    for p in planter:
        csv += "%s,%s,%s,%s,%s,%s\n" % (p[0], p[1], p[2], p[3], p[4], p[5])
    print("csv:", csv)

    import io
    bio = io.BytesIO()
    bio.write(csv.encode())
    bio.seek(0)  # move to beginning

    # if not dry_run
    if not dry_run:
        print("uploading...")
        # upload to FTP: 
        import ftplib
        ftp = ftplib.FTP(host)
        ftp.login(user, password)
        ftp.cwd("/")
        ftp.storbinary("STOR planter_info.csv", bio)

    # print time elapsed
    print("total time elapsed:", datetime.datetime.now() - very_start)

    return True