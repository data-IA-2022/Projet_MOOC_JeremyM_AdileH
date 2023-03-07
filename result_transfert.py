import json
import pymongo
from sqlmodel import create_engine, Session, SQLModel, Column, Relationship, ForeignKey, String, Integer, Field, Text
from typing import List, Optional
from datetime import date, datetime

import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

host = config['database']['host']
port = config['database']['port']
user = config['database']['user']
password = config['database']['password']
db_name = config['database']['db_name']

# Créez une connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["user"]


# Récupérez tous les documents de la collection
documents = collection.find({})


class Result(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str | None
    course_id: str
    grade: Optional[float]
    certificat_eligible: str
    certificat_delivered: str


# Liste des résultats
results = []

# Parcours des utilisateurs
for doc in documents:
    # Récupération de l'ID et de l'utilisateur
    username = doc['username']
    if not username:
        continue

    # Parcours des cours
    for key in doc.keys():
        if key not in ['_id', 'id', 'username']:
            course_name = key
            course_data = doc[key]

            # Récupération des champs
            if isinstance(course_data, dict):
                if 'grade' in course_data:
                    grade = str(course_data['grade'])
                if 'Certificate Eligible' in course_data:
                    cert_eligible = course_data['Certificate Eligible']
                if 'Certificate Delivered' in course_data:
                    cert_delivered = course_data['Certificate Delivered']

            # Création d'un résultat
            result = Result(
                username=username,
                course_id=course_name,
                grade=grade,
                certificat_eligible=cert_eligible,
                certificat_delivered=cert_delivered
            )

            # Ajout du résultat à la liste
            results.append(result)


# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata

# Créez une connexion à la base de données MySQL
engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}", echo=True)

metadata.create_all(engine)

# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les résultats dans la base de données
    session.add_all(results)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
