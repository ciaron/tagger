from sqlalchemy import Text, ForeignKey, Column, MetaData, String, Integer, Table
from sqlalchemy import create_engine
from sqlalchemy import or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker, exc
from sqlalchemy.exc import *
from sqlalchemy.sql import select
import hashlib
import os

def main():

    print "IN MAIN"

    Base = declarative_base()

    engine = create_engine('sqlite:///:memory:', echo=False)

    metadata = Base.metadata

    # association table: file <-> tag
    file_tags = Table('file_tags', metadata,
        Column('file_id', Integer, ForeignKey('files.id')),
        Column('tag_id', Integer, ForeignKey('tags.id'))
    )

    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

