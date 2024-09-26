
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

# Create a connection to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/", connectTimeoutMS=1800000)
db = client["mooc"]
collection = db["user_filtered"]
documents = collection.find({})

class User(SQLModel, table=True):
    __tablename__ = "user"

    username: str = Field(primary_key=True)
    city: Optional[str] = Field(default=None)
    country: Optional[str] = Field(default=None)
    gender: Optional[str] = Field(default=None)
    year_of_birth: Optional[date] = Field(default=None)

# Create a connection to the MySQL database using the SQLModel engine
engine = create_engine(f"mysql+pymysql://{user_connect}:{password}@{host}:{port}/{db_name}", echo=True, pool_pre_ping=True)

# Create an instance of the SQLModel metadata
metadata = SQLModel.metadata
metadata.create_all(engine)

# Create an empty list to hold the users to be added to the database
users = []

# Iterate over the documents in the MongoDB collection
for document in documents:
    # Check if the user already exists in the MySQL database
    with Session(engine) as session:
        existing_user = session.get(User, document['username'])
        if existing_user:
            continue # skip adding the user if it already exists
    # Create a new User instance from the MongoDB document
    user = User(
        username=str(document["username"]),
        city=document.get("city"),
        country=document.get("country"),
        gender=document.get("gender"),
        year_of_birth=document.get("year_of_birth"),
    )
    # Add the User instance to the list of users to be added to the database
    users.append(user)

# Use a session to interact with the database and insert the users
with Session(engine) as session:
    session.add_all(users)
    session.commit()
