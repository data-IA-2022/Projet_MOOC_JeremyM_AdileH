
import pymongo
from sqlmodel import create_engine, Session, SQLModel, Field
from typing import Optional
from datetime import date
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

host = config['database']['host']
port = config['database']['port']
user_connect = config['database']['user']
password = config['database']['password']
db_name = config['database']['db_name']
# Créez une connexion à la base de données SQL et utilisez les instances de modèle pour insérer des données
# Créez une instance de moteur de base de données en utilisant le pilote MySQL

# Créez une connexion à la base de données MongoDB
client = pymongo.MongoClient(
    "mongodb://localhost:27017/",  connectTimeoutMS=1800000)
db = client["mooc"]
collection = db["user_filtered"]
documents = collection.find({})


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    city: str
    country: str
    gender: str
    year_of_birth: date


# Parcourez les documents et créez des instances de modèles SQLModel
users = []

for document in documents:

    # Créer une instance de modèle SQLModel pour chaque document de thread
    user = User(
        username=str(document["username"]),
        city=document["city"],
        country=document["country"],
        gender=document["gender"],
        year_of_birth=document["year_of_birth"],

    )

    # Ajouter l'instance de modèle à la liste de threads
    users.append(user)


engine = create_engine(
    f"mysql+pymysql://{user_connect}:{password}@{host}:{port}/{db_name}", echo=True)
# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata


metadata.create_all(engine)

# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les threads dans la base de données
    session.add_all(users)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()
