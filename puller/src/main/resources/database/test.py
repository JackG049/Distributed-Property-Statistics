import mysql.connector

cnx = mysql.connector.connect(user='root', password='password',
                              host='localhost',
                              database='daft_ie')

cursor = cnx.cursor()
cursor.execute("SELECT * FROM historic_data")
result = cursor.fetchall()
for entry in result:
    print(entry)

cnx.close()
