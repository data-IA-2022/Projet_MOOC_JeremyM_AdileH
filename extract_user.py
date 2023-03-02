import pymongo
import json
from bson import json_util

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["sample_user"]

new_docs = []
for doc in collection.find():
    # Création d'un nouveau dictionnaire pour chaque document
    new_doc = {}

    # Récupération de l'ID et de l'utilisateur
    new_doc['_id'] = json.loads(json_util.dumps(doc['_id']))
    new_doc['id'] = doc.get('id', None)
    new_doc['username'] = doc.get('username', None)

    # Parcours des cours
    for key in doc.keys():
        if key not in ['_id', 'id', 'username']:
            course_name = key
            course_data = doc[key]

            # Récupération du grade et des messages
            if 'grade' in course_data:
                new_course = {
                    'grade': course_data['grade']
                }
                if 'messages' in course_data:
                    new_course['messages'] = course_data['messages']

                new_doc[course_name] = new_course

    new_docs.append(new_doc)

# Écriture des données dans un fichier JSON
with open("extract_user.json", "w") as f:
    json.dump(new_docs, f)
