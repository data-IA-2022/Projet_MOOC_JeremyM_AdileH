import pymongo
import json
from bson import json_util

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["user"]

new_docs = []
seen_usernames = set()

for doc in collection.find():
    for key in doc.keys():
        if key not in ['_id', 'id', 'username']:
            course_name = key
            course_data = doc[key]

            # Récupération des champs
            new_course = {}
            username = doc.get('username', None)
            if username in seen_usernames:
                continue

            new_course['username'] = username
            new_course['year_of_birth'] = course_data.get(
                'year_of_birth', None)
            new_course['country'] = course_data.get('country', None)
            new_course['gender'] = course_data.get('gender', None)
            new_course['city'] = course_data.get('city', None)

            new_docs.append(new_course)

            # Ajout du nom d'utilisateur à la liste des utilisateurs déjà vus
            seen_usernames.add(username)

# Écriture des données dans un fichier JSON
with open("user.json", "w") as f:
    json.dump(new_docs, f, indent=4)
