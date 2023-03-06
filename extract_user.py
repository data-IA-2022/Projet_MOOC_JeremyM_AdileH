import yaml
import pymongo
from sqlmodel import create_engine, Session, SQLModel, Column, Relationship, ForeignKey, String, Integer, Field, Text
from typing import List, Optional
from datetime import date, datetime

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

host = config['database']['host']
port = config['database']['port']
user_connect = config['database']['user']
password = config['database']['password']
db_name = config['database']['db_name']
# Créez une connexion à la base de données SQL et utilisez les instances de modèle pour insérer des données
# Créez une instance de moteur de base de données en utilisant le pilote MySQL
engine = create_engine(
    f"mysql+pymysql://{user_connect}:{password}@{host}:{port}/{db_name}", echo=True)

# Créez une connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mooc"]
collection = db["user"]


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    city: Optional[str] = None
    country: Optional[str] = None
    gender: Optional[str] = None
    year_of_birth: Optional[date] = None


# Liste des résultats
users = []

# Champs attendus pour chaque cours
expected_fields = ['city', 'country', 'gender', 'year_of_birth']

# Parcours des utilisateurs
for doc in collection.find({}):
    # Récupération de l'utilisateur
    username = doc.get('username')
    if not username:
        continue

    # Récupération des champs
    fields = {}
    for key, value in doc.items():
        if key in expected_fields:
            fields[key] = value

    # Création d'un résultat
    user = User(
        username=username,
        **fields
    )

    # Vérification de l'existence de l'utilisateur
    with Session(engine) as session:
        existing_user = session.query(User).filter(
            User.username == username).first()

    if existing_user:
        # Mise à jour de l'utilisateur existant avec les nouvelles données
        existing_user.city = user.city
        existing_user.country = user.country
        existing_user.gender = user.gender
        existing_user.year_of_birth = user.year_of_birth
        user = existing_user

    # Ajout du résultat à la liste
    users.append(user)

# Créez une instance de métadonnées SQLModel
metadata = SQLModel.metadata

metadata.create_all(engine)

# Utilisez la session pour interagir avec la base de données
with Session(engine) as session:
    # Insérez les threads dans la base de données
    session.add_all(users)

    # Validez les transactions et confirmez les modifications dans la base de données
    session.commit()