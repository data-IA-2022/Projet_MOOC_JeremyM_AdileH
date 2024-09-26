import pymongo
import pymysql
from sqlmodel import create_engine, Session, SQLModel, Column, Relationship, ForeignKey,  String, Integer, Field, Text
from typing import List, Optional
from datetime import date

import sqlite3


# Créez une connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["forum2"]


# Récupérez tous les documents de la collection
documents = collection.find({})


class Thread(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    thread_id: str
    username: str
    course_id: str
    comment_count: int
    updated_at: str


class Message(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: str = Column(String, primary_key=True)
    depth: int = Column(Integer)
    body: str = Field(max_length=50000)
    type: str
    username: str
    parent_id: str
    thread_id: str
    created_at: str


# Parcourez les documents et créez des instances de modèles SQLModel
threads = []
messages = []
for document in documents:
    # Créez une instance de modèle SQLModel pour chaque document de thread
    thread = Thread(
        thread_id=str(document["_id"]),
        username=document["content"]["username"],
        course_id=document["content"]["course_id"],
        comment_count=document["content"]["comments_count"],
        updated_at=document["content"]["updated_at"],
    )

    # Ajoutez l'instance de modèle à la liste de threads
    threads.append(thread)

    # Créez des instances de modèle SQLModel pour chaque document de message imbriqué
    for message_data in document["content"]["children"]:
        message = Message(
            message_id=message_data["id"],
            depth=message_data["depth"],
            body=message_data["body"][:50000],
            type=message_data["type"],
            username=message_data["username"],
            parent_id=str(document["_id"]),
            thread_id=str(document["_id"]),
            created_at=message_data["created_at"],
        )

        # Ajoutez l'instance de modèle à la liste de messages
        messages.append(message)

        # Utilisez la récursion pour traiter les niveaux imbriqués de la hiérarchie des messages
        if message_data.get("children"):
            for sub_message_data in message_data["children"]:
                sub_message = Message(
                    message_id=sub_message_data["id"],
                    depth=sub_message_data["depth"],
                    body=sub_message_data["body"],
                    type=sub_message_data["type"],
                    username=sub_message_data["username"],
                    parent_id=str(document["_id"]),
                    thread_id=str(document["_id"]),
                    created_at=sub_message_data["created_at"],
                )

                # Ajoutez l'instance de modèle à la liste de messages
                messages.append(sub_message)

# Créez une
# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata

# Ajoutez les modèles Thread et Message à la métadonnée
# Créez une connexion à la base de données MySQL


# Créez une connexion à la base de données SQL et utilisez les instances de modèle pour insérer des données
# Créez une instance de moteur de base de données en utilisant le pilote MySQL
# engine = create_engine("sqlite:///message_thread.db", echo=True)
engine = create_engine(
    "mysql+pymysql://root:greta2023@localhost:3306/g2", echo=True)
metadata.create_all(engine)
# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les threads dans la base de données
    session.add_all(threads)

    # Insérez les messages dans la base de données
    session.add_all(messages)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
