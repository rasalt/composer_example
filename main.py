from flask import Request as request
import logging, json
from flask import Flask, request, jsonify
import os
import sqlalchemy

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:

connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
db_user = os.environ["DB_USER"]
db_pass = os.environ["DB_PASS"]
db_name = os.environ["DB_NAME"]
db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")

driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

def getsqlstring(request):
    request_json = request.get_json()
    stmt = sqlalchemy.text("SELECT * FROM customers LIMIT 2") #("INSERT INTO entries (guestName, content) values ('third guest', 'Also this one');")
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
          drivername=driver_name,
          username=db_user,
          password=db_pass,
          database=db_name,
          query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    print("Cloud SQL was successfully queried "+request_json['tablename'])
    try:
        with db.connect() as conn:
            custy = conn.execute(stmt).fetchall()
            print(type(custy[0]))
            print(custy)
    except Exception as e:
        return 'Error: {}'.format(str(e))
    # This ia an important line. This is returned by the cloud function
    return json.dumps({"sql":"SELECT * FROM customers"})

def main():
    requestmsg = {
      "tablename": "customers"
    }
    getsqlstring(requestmsg) 

if __name__ == "__main__":
    main()

