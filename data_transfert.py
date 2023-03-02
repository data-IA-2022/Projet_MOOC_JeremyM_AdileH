from typing import List
from sqlmodel import create_engine, SQLModel, Field, ForeignKey
from datetime import date


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