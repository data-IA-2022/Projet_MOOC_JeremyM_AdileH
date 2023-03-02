import pymongo

# Se connecter à la base de données MongoDB
client = pymongo.MongoClient('localhost', 27017)
db = client['mooc']
collection = db['user']

# Compter le nombre de champs uniques contenant un champ "grade" égal à zéro
pipeline = [
    {"$project": {"fields": {"$objectToArray": "$$ROOT"}}},
    {"$unwind": "$fields"},
    {"$match": {"fields.v.grade": 0}},
    {"$group": {"_id": "$fields.k", "count": {"$sum": 1}}}
]

result_zero = list(collection.aggregate(pipeline))
print(
    f"Nombre de champs uniques avec un grade égal à zéro : {len(result_zero)}")

pipeline = [
    {"$project": {"fields": {"$objectToArray": "$$ROOT"}}},
    {"$unwind": "$fields"},
    {"$match": {"fields.k": {"$not": {"$in": ["_id", "username"]}}}},
    {"$project": {"grade": "$fields.v.grade"}},
    {"$group": {"_id": None, "zero_count": {"$sum": {"$cond": {"if": {"$eq": ["$grade", "0.0"]}, "then": 1, "else": 0}}}, "greater_count": {
        "$sum": {"$cond": {"if": {"$gt": ["$grade", "0.0"]}, "then": 1, "else": 0}}}}}
]

result = list(collection.aggregate(pipeline))
for field in result:
    print(
        f"Le nombre de cours avec grade = 0.0 est {field['zero_count']} et le nombre de cours avec grade > 0.0 est {field['greater_count']}.")
