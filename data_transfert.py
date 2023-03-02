from pymongo import MongoClient
from mysql.connector import connect, Error
from sqlmodel import create_engine, Session, SQLModel, Column, ForeignKey, Relationship, PrimaryKey, Field
from typing import List, Optional
from datetime import date


# Créez une connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["forum"]

# Récupérez tous les documents de la collection
documents = collection.find({})




# Modèle de données pour la table thread
class Thread(SQLModel, table=True):
    thread_id: str = Field(primary_key=True, autoincrement=True)
    username: str
    course_id: str
    comment_count: int
    created_at: date

    # Relation avec la table message
    messages: List["Message"] = Field(
        default=None, sa_relationship_kwargs={"lazy": "select"}
    )


# Modèle de données pour la table message
class Message(SQLModel, table=True):
    message_id: int = Field(primary_key=True, autoincrement=True)
    depth: int
    body: str
    type: str
    username: str
    parent_id: str = Field(foreign_key="thread.thread_id")
    thread_id: str = Field(foreign_key="thread.thread_id")
    created_at: date
    
    
    
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
        created_at=document["content"]["created_at"],
    )

    # Ajoutez l'instance de modèle à la liste de threads
    threads.append(thread)

    # Créez des instances de modèle SQLModel pour chaque document de message imbriqué
    for message_data in document["content"]["children"]:
        message = Message(
            message_id=message_data["id"],
            depth=message_data["depth"],
            body=message_data["body"],
            type=message_data["type"],
            username=message_data["username"],
            parent_id=str(document["_id"]),
            thread_id=str(document["_id"]),
            created_at=message_data["created_at"],
        )

        # Ajoutez l'instance de modèle à la liste de messages
        messages.append(message)
        
        
# Créez une connexion à la base de données SQL et utilisez les instances de modèle pour insérer des données
# Créez une instance de moteur de base de données en utilisant le pilote MySQL
engine = create_engine("mysql+pymysql://root:greta2023@/g2")

# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les threads dans la base de données
    session.add_all(threads)

    # Insérez les messages dans la base de données
    session.add_all(messages)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()