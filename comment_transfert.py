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
collection = db["forum2"]


# Récupérez tous les documents de la collection
documents = collection.find({})


class Comment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    comment_id: str = Column(String, primary_key=True)
    parent_id: str
    username: str
    thread_id: str
    type: str
    depth: int = Column(Integer)
    body: str = Field(max_length=50000)
    created_at: date


# Parcourez les documents et créez des instances de modèles SQLModel

comments = []
for document in documents:
    # Créez des instances de modèle SQLModel pour chaque document de message imbriqué
    for comment_data in document["content"]["children"]:
        message = Comment(
            comment_id=comment_data["id"],
            depth=comment_data["depth"],
            body=comment_data["body"][:50000],
            type=comment_data["type"],
            username=comment_data["username"],
            parent_id=str(document["_id"]),
            thread_id=str(document["_id"]),
            created_at=comment_data["created_at"],
        )

        # Ajoutez l'instance de modèle à la liste de messages
        comments.append(message)

        # Utilisez la récursion pour traiter les niveaux imbriqués de la hiérarchie des messages
        if comment_data.get("children"):
            for sub_comment_data in comment_data["children"]:
                sub_comment = Comment(
                    comment_id=sub_comment_data["id"],
                    depth=sub_comment_data["depth"],
                    body=sub_comment_data["body"],
                    type=sub_comment_data["type"],
                    username=sub_comment_data["username"],
                    parent_id=str(document["_id"]),
                    thread_id=str(document["_id"]),
                    created_at=sub_comment_data["created_at"],
                )

                # Ajoutez l'instance de modèle à la liste de messages
                comments.append(sub_comment)

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

    # Insérez les messages dans la base de données
    session.add_all(comments)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
