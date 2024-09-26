import datetime
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


class Course(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    course_id: str = Field(unique=True)


# Liste des résultats
courses = []

# Liste des cours existants
existing_courses = []

# Parcours des utilisateurs
for doc in documents:

    # Parcours des cours
    for key in doc.keys():
        if key not in ['_id', 'id', 'username']:
            course_name = key

            # Vérification de l'unicité du cours
            if course_name not in existing_courses:

                # Création d'un résultat
                course = Course(
                    course_id=course_name,
                )

                # Ajout du résultat à la liste
                courses.append(course)

                # Ajout du cours à la liste des cours existants
                existing_courses.append(course_name)
# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata

# Créez une connexion à la base de données MySQL
engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}", echo=True)

metadata.create_all(engine)

# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les résultats dans la base de données
    session.add_all(courses)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
