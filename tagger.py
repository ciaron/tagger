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
def gethash(filename):
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

    tags = relationship('Tag', secondary=file_tags, backref='files', cascade="all, delete", lazy='dynamic')

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

def getfiles(tag):
    """ accept a list of tags, return a list of all files with all of those tags
    """
    files = session.query(File).filter(File.tags.any(tag=tag)).all()
    return files

def gettags(file_):
    """ accept a filename, return a list of all tags on that file
    """
    tags = session.query(Tag).filter_by(tag=tag).all()
    return files

if __name__ == '__main__':
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filenames = {'/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG': ['fileAtag1', 'fileAtag2'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/B.JPG': ['fileBtag1', 'fileAtag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/C.JPG': ['fileCtag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/D.JPG': ['fileDtag1', 'fileDtag2'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/E.JPG': ['fileEtag1', 'fileAtag1'], \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/DOES_NOT_EXIST.JPG': ['fileDNEtag1']}

    for f in filenames:
        hash = gethash(f)

        if hash != None: # i.e. file exists
            # Do files ...
            file_ = File(hash, f)
            session.add(file_)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

            #... then do tags

            for v in filenames[f]:
                print v
                file_.tags.append(Tag(v))

                # check if the tag exists in the database
                try:
                    existing = session.query(Tag).filter_by(tag=v).one()
                    print "**********************TAG EXISTS!!!"
                except:
                    file_.tags.append(Tag(v))

            session.commit()

    # let's try again, but with a file that already exists in the DB:
#    fn = '/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG'
#    file_ = File(gethash(fn), fn)
#    session.add(file_)
#
#    try:
#        session.commit()
#    except IntegrityError:
#        session.rollback()

    # fetch a row from DB based on a filename (not a good way: files could get moved)
#    myfile = session.query(File).filter_by(name='/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG').first()
#    print myfile

    # Select all from the tables:
    s = select([File.__table__])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row

    s = select([Tag.__table__])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row

    s = select([file_tags])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row
    # end select all

    # get tags for a file
    files = getfiles('fileCtag1')
    for file_ in files:
        print file_

"""
    # delete a row from DB based on a hash
    h = 'b9ee11b40b2741d92dd75fd8b7d09be1'
    myfile = session.query(File).filter_by(hash=h).first()
    session.delete(myfile)
    session.commit()
    s = select([File.__table__])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row
    s = select([Tag.__table__])
    conn = engine.connect()
    res = conn.execute(s)
    rows = res.fetchall()
    for row in rows:
        print row
"""
