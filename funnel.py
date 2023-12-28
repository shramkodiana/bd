# Install the required libraries
# pip install sqlalchemy psycopg2
import logging

from sqlalchemy import create_engine, Column, String, Integer, Text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv  # Assuming your unstructured data is in a CSV file

# Define the PostgreSQL connection string
username = 'postgres'
password = 'newpassword'
database = 'OpenDataZNO2019'
host = 'localhost'

DATABASE_URL = f"postgresql://{username}:{password}@{host}/{database}"

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define the data model using SQLAlchemy ORM
Base = declarative_base()


class YourTable(Base):
    __tablename__ = 'Ukr'

    id = Column(Integer, primary_key=True)
    OUTID = Column(String)
    Birth = Column(String)
    SexTypeName = Column(String)
    RegName = Column(String)
    AREANAME = Column(String)
    TERNAME = Column(String)
    RegTypeName = Column(String)
    TerTypeName = Column(String)
    ClassProfileNAME = Column(String)
    ClassLangName = Column(String)
    EONAME = Column(String)
    EOTypeName = Column(String)
    EORegName = Column(String)
    EOAreaName = Column(String)
    EOTerName = Column(String)
    EOParent = Column(String)
    UkrTest = Column(String)
    UkrTestStatus = Column(String)
    UkrBall100 = Column(String)
    UkrBall12 = Column(String)
    UkrBall = Column(String)
    UkrPTName = Column(String)
    UkrPTRegName = Column(String)
    UkrPTAreaName = Column(String)
    UkrPTTerName = Column(String)


# # Create the table in the database
Base.metadata.create_all(bind=engine)
#
# # Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Read data from an external file (e.g., CSV) and insert it into the database
with open("C:\pythonProject4\lab1db\Odata2019File.csv", 'r', encoding='utf8') as file:
    reader = csv.DictReader(file, delimiter=';')
    row_limit = 10000
    current_row = 0
    for row in reader:
        try:
            data_entry = YourTable(
                OUTID=row['\ufeff"OUTID"'],
                Birth=row['Birth'],
                SexTypeName=row['SexTypeName'],
                RegName=row['RegName'],
                AREANAME=row['AREANAME'],
                TERNAME=row['TERNAME'],
                RegTypeName=row['RegTypeName'],
                TerTypeName=row['TerTypeName'],
                ClassProfileNAME=row['ClassProfileNAME'],
                ClassLangName=row['ClassLangName'],
                EONAME=row['EONAME'],
                EOTypeName=row['EOTypeName'],
                EORegName=row['EORegName'],
                EOAreaName=row['EOAreaName'],
                EOTerName=row['EOTerName'],
                EOParent=row['EOParent'],
                UkrTest=row['UkrTest'],
                UkrTestStatus=row['UkrTestStatus'],
                UkrBall100=row['UkrBall100'],
                UkrBall12=row['UkrBall12'],
                UkrBall=row['UkrBall'],
                UkrPTName=row['UkrPTName'],
                UkrPTRegName=row['UkrPTRegName'],
                UkrPTAreaName=row['UkrPTAreaName'],
                UkrPTTerName=row['UkrPTTerName'],)

            # Add more columns as needed
            session.add(data_entry)
            res = row['\ufeff"OUTID"']
            print(f"---success for {res}---")

            # Increment the row counter
            current_row += 1

            # Check if the row limit is reached
            if current_row >= row_limit:
                break
        except Exception as e:
            logging.error(f"Error adding row: {e}")
            session.rollback()

# Commit the changes to the database
session.commit()
print("---------Ukr Table filled successfully!-------")