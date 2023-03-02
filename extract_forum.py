import pymongo
import json


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["sample_forum"]


def extract_forum(document):
    content = document.get('content')
    if not content:
        return None

    username = content.get('username')
    if not username:
        return None

    children = content.get('children')
    if children:
        extracted_children = extract_children(children)
    else:
        extracted_children = []

    extracted_data = {
        '_id': document['_id'],
        'content': {
            'username': username,
            'type' : content.get('type'),
            'updated_at': content.get('updated_at'),
            'children': extracted_children,
            'user_id': content.get('user_id'),
            'comments_count': content.get('comments_count'),
            'body': content.get('body'),
            'id': content.get('id'),
            'course_id': content.get('course_id'),
            
        },
       
    }

    return extracted_data


def extract_children(children):
    result = []
    for i, child in enumerate(children):
        if 'id' not in child:
            # Si l'enfant n'a pas d'identifiant, on lui attribue un identifiant unique
            child['_id'] = f'child_{i}'
        child_result = {
            'username': child['username'],
            'thread_id': child['thread_id'],
            'created_at': child['created_at'],
            'id': str(child['id']),
            'body': child['body'],
            'course_id': child['course_id'],
            'type': child['type'],
            'depth': child['depth'],
            'user_id': child['user_id'],
            'children': extract_children(child['children']) if child['children'] else []
        }
        result.append(child_result)
    return result


# Récupérer tous les documents de la collection
documents = collection.find({})

# Convertir les documents en une liste de dictionnaires
data = [extract_forum(document) for document in documents]

# Afficher le résultat
if data:
    with open('extract_forum.json', 'a') as f:
        json.dump(data, f, indent=4)
        f.write('\n')
