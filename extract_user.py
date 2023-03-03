import pymongo
import mysql.connector

# Test de connexion à MongoDB
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["g2-MOOC"]
    collection1 = db["user"]
    collection2 = db["forum"]
    print("Connexion à MongoDB réussie !")
except pymongo.errors.ConnectionFailure as e:
    print("Erreur de connexion à MongoDB :", e)

# Test de connexion à MySQL
try:
    mysql_conn = mysql.connector.connect(
      host="localhost",
      user="root",
      password="greta2023",
      database="g2",
    )
    print("Connexion à MySQL réussie !")
except mysql.connector.Error as e:
    print("Erreur de connexion à MySQL :", e)

# Sélection des champs que vous voulez récupérer
query = {}
projection = {"username": 1, "id": 1, "year_of_birth" : 1 }

#username = collection.distinct("username", "id")

# Récupération des données depuis MongoDB
username = collection1.find(query, projection).limit(1000)
'''for doc in username:
    print (doc)'''

# Insertion des données dans la table MySQL
count=0
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("CREATE TABLE IF NOT EXISTS User (user_id VARCHAR(255), username VARCHAR(255), year_of_birth VARCHAR(255))")

for d in username:
    count=count+1
    sql = "INSERT INTO g2.User (user_id, username, year_of_birth) VALUES (%s, %s, %s)"
    values = (d["id"], d["username"], d["year_of_birth"])
    print(f'{d["id"]} ----- {d["username"]} ----- {d["username"]} ----- {count}')
    mysql_cursor.execute(sql, values)
    mysql_conn.commit()