from sqlalchemy import Text, ForeignKey, Column, MetaData, String, Integer, Table
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.exc import *
from sqlalchemy.sql import select
#import sqlite3
import hashlib
import os

"""
TODO: 
    - command line argument parsing
"""

db = '/tmp/tagger.db'

Base = declarative_base()

# for now, we'll do everything in memory
engine = create_engine('sqlite:///:memory:', echo=True)

metadata = Base.metadata

# association table: file <-> tag
file_tags = Table('file_tags', metadata,
    Column('file_id', Integer, ForeignKey('files.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class File(Base):
    __tablename__ = 'files'
    
    id = Column(Integer, primary_key=True)
    hash = Column(String(255), nullable=False, unique=True)
    name = Column(String(255), nullable=False)

    tags = relationship('Tag', secondary=file_tags, backref='files')

    def __init__(self, hash, name):
        self.hash = hash
        self.name = name

    def __repr__(self):
        return "File(%r, %r, %r)" % (self.id, self.hash, self.name)

class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag = Column(String(50), nullable=False, unique=True)

    def __init__(self, tag):
        self.tag = tag

class TaggedFile(object):

    def __init__(self, filename):
        """
        set the hash attribute on this file
        """
        self.hash = self.__gethash(filename)   

    def __gethash(self, filename):
        """
        Get the hash for the given file. Private to this class
        """
        if os.path.exists(filename) == False:
            return None

        md5 = hashlib.md5()

        with open(filename, 'rb') as f: 
            for chunk in iter(lambda: f.read(8192), ''): 
                md5.update(chunk)

            # include the filename in the digest
            md5.update(filename)

        return md5.hexdigest()

if __name__ == '__main__':
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filenames = {'/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG': ['file1tag1', 'file1tag2'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/B.JPG': ['file2tag1', 'file1tag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/C.JPG': ['file3tag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/D.JPG': ['file4tag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/E.JPG': ['file5tag1', 'file1tag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/DOES_NOT_EXIST.JPG': ['file6tag1']}

    for f in filenames:
        tf = TaggedFile(f)
        hash = tf.hash

        # TODO use properties on TaggedFile and move SA stuff in there
        if hash != None:
            file_ = File(hash, f)

            for v in filenames[f]:

                # check if the tag exists in the database
                try:
                    existing = session.query(Tag).filter_by(tag=v).one()
                    print "TAG EXISTS!!!"
                except:
                    file_.tags.append(Tag(v))

        session.add(file_)

    try:
        session.commit()
    except IntegrityError:
        session.rollback()

    # let's try again, but with a file that already exists in the DB:
    tf = TaggedFile('/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG')
    hash = tf.hash
    file_ = File(hash, '/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG')
    session.add(file_)

    try:
        session.commit()
    except IntegrityError:
        session.rollback()

    # fetch a row from DB based on a hash
    h = 'b9ee11b40b2741d92dd75fd8b7d09be1'
    myfile = session.query(File).filter_by(hash=h).first()
    print myfile

    # fetch a row from DB based on a filename (not a good way: files could get moved)
    myfile = session.query(File).filter_by(name='/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG').first()
    print myfile

    # Select all from the tables:
    s = select([Tag.__table__])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row
    # end select all

