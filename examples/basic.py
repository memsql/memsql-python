from memsql.common import database

conn = database.connect(host="127.0.0.1", port=3306, user="root", password="")
print(conn.query("show databases"))
