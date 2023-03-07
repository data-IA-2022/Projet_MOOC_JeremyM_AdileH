import pymongo
import pymysql
from sqlmodel import create_engine, Session, SQLModel, Column, Relationship, ForeignKey,  String, Integer, Field, Text
from typing import List, Optional
from datetime import date

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
collection = db["forum"]


# Récupérez tous les documents de la collection
documents = collection.find({})


class Thread(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    thread_id: str
    username: str
    course_id: str
    body: str = Field(max_length=50000)
    type: str
    comment_count: int
    created_at: str


# Parcourez les documents et créez des instances de modèles SQLModel
threads = []

for document in documents:
    # Vérifier si la clé "username" est présente dans le dictionnaire
    if "username" in document["content"]:
        username = document["content"]["username"]
    else:
        username = None

    # Créer une instance de modèle SQLModel pour chaque document de thread
    thread = Thread(
        thread_id=str(document["_id"]),
        username=username,
        body=document["content"]["body"],
        course_id=document["content"]["course_id"],
        type=document["content"]["type"],
        comment_count=document["content"]["comments_count"],
        created_at=document["content"]["created_at"],
    )

    # Ajouter l'instance de modèle à la liste de threads
    threads.append(thread)


# Créez une
# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata

# Ajoutez les modèles Thread et Message à la métadonnée
# Créez une connexion à la base de données MySQL


# Créez une connexion à la base de données SQL et utilisez les instances de modèle pour insérer des données
# Créez une instance de moteur de base de données en utilisant le pilote MySQL
# engine = create_engine("sqlite:///message_thread.db", echo=True)
engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}", echo=True)


metadata.create_all(engine)
# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les threads dans la base de données
    session.add_all(threads)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
