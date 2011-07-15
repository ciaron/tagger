from sqlalchemy import Text, ForeignKey, Column, MetaData, String, Integer, Table
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker, exc
from sqlalchemy.exc import *
from sqlalchemy.sql import select
import hashlib
import os

"""
TODO: 
    - existence of file is checked in gethash(): move this up the hierarchy
    - command line argument parsing
"""

Base = declarative_base()
metadata = Base.metadata
engine = create_engine('sqlite:///:memory:', echo=False)

# many-to-many association table: files <-> tags
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

    def __init__(self, name):
        self.hash = self.gethash(name)
        self.name = name

    def __repr__(self):
        return "File(%r, %r, %r)" % (self.id, self.hash, self.name)


    def gethash(self, filename):
        """ Get the hash for the given file. Private to this class
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

    def tag(self, *vals):
        for t in vals:

            # check if the tag exists in the database, either add this or create new Tag
            try:
                existing = session.query(Tag).filter_by(tag=t).one()
                self.tags.append(existing)
            except:
                self.tags.append(Tag(t))

        session.add(self)

        try:
            session.commit()
        except IntegrityError:
            session.rollback()

    def remove(self, text):
        try:
            tag_to_delete = session.query(Tag).filter(Tag.tag==text).one()
            self.tags.remove(tag_to_delete)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()
        except exc.NoResultFound:
            pass
        
class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag = Column(String(50), nullable=False, unique=True)

    def __init__(self, tag):
        self.tag = tag

class Tagger():

    def __init__(self):

        Base = declarative_base()
        metadata = Base.metadata
        engine = create_engine('sqlite:///:memory:', echo=False)

        # many-to-many association table: files <-> tags
        file_tags = Table('file_tags', metadata,
            Column('file_id', Integer, ForeignKey('files.id')),
            Column('tag_id', Integer, ForeignKey('tags.id'))
        )

        metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

    def getfiles(self, *tags):
        """ accept a (list of) tag(s), return a list of all files with all of those tags
        """

        q = session.query(File)
        for t in tags:
            q = q.filter(File.tags.any(tag=t))
        q = q.all()
        return [files.name for files in q]

    def gettags(self, filename):

        f = File(filename)
        hash = f.gethash(filename)
        ts = session.query(Tag).filter(Tag.files.any(hash=hash)).all()
        return [t.tag for t in ts]

    def addtags(self, filename, *tags):
        try:
            f = session.query(File).filter(File.name==filename).one()
        except exc.NoResultFound: # file not in DB, add it
            f = File(filename)

        for tag in tags:
            f.tag(tag)

    def rmtags(self, filename, *tags):
        try:
            f = session.query(File).filter(File.name==filename).one()
        except exc.NoResultFound:
            return None

        for tag in tags:
            f.remove(tag)

def getfiles(*tags):
    """ accept a (list of) tag(s), return a list of all files with all of those tags
    """

    q = session.query(File)
    for t in tags:
        q = q.filter(File.tags.any(tag=t))
    q = q.all()
    return [files.name for files in q]

def gettags(filename):

    f = File(filename)
    hash = f.gethash(filename)
    ts = session.query(Tag).filter(Tag.files.any(hash=hash)).all()
    return [t.tag for t in ts]

def addtags(filename, *tags):
    try:
        f = session.query(File).filter(File.name==filename).one()
    except exc.NoResultFound: # file not in DB, add it
        f = File(filename)

    for tag in tags:
        f.tag(tag)

def rmtags(filename, *tags):
    try:
        f = session.query(File).filter(File.name==filename).one()
    except exc.NoResultFound:
        return None

    for tag in tags:
        f.remove(tag)

if __name__ == '__main__':

    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filenames = {'./IMAGES/A.JPG': ['fileAtag1', 'fileAtag2'], \
                 './IMAGES/B.JPG': ['fileBtag1', 'fileAtag1'], \
                 './IMAGES/C.JPG': ['fileCtag1'], \
                 './IMAGES/D.JPG': ['fileAtag1', 'fileDtag1', 'fileDtag2'], \
                 './IMAGES/DOES_NOT_EXIST.JPG': ['fileDNEtag1']}

    # want to be able to do something like:
#    t = Tagger()
#    t.addtags(filename, tag1, tag2)
#    print "E.JPG TAGS:", t.gettags('./IMAGES/E.JPG')
    # i.e. without handling database stuff in client code

    addtags('./IMAGES/E.JPG', 'fileEtag1', 'fileEtag2', 'fileEtag3', 'fileEtag4')
    rmtags('./IMAGES/E.JPG', 'fileEtag3', 'fileEtag1')
    print "E.JPG TAGS:", gettags('./IMAGES/E.JPG')

#    for f in filenames:
#        addtags(f, *filenames[f])
#
#    rmtags('./IMAGES/A.JPG', 'fileAtag1')
#    addtags('./IMAGES/A.JPG', 'a new tag for A')
#    addtags('./IMAGES/DOESNOTEXIST.JPG', 'DNE_tag1', 'DNE_tag2')
#    if rmtags('./IMAGES/DOESNOTEXIST.JPG', 'DNE_tag1', 'DNE_tag2') == None:
#        print 'rmtags failed for some reason'
#
#    print "A.JPG TAGS:", gettags('./IMAGES/A.JPG')
#    print "B.JPG TAGS:", gettags('./IMAGES/B.JPG')
#    print "C.JPG TAGS:", gettags('./IMAGES/C.JPG')
#    print "D.JPG TAGS:", gettags('./IMAGES/D.JPG')
#    print "E.JPG TAGS:", gettags('./IMAGES/E.JPG')
#    print "DOESNOTEXIST.JPG TAGS:", gettags('./IMAGES/DOESNOTEXIST.JPG')
#
#    # get files for a (list of) tag(s)
#    ts = 'fileAtag1'
#    fs = getfiles(ts)
#    print "Files with tags %s: %s" % (ts, fs)
#
#    ts = ['fileAtag1', 'fileDtag1']
#    fs = getfiles(*ts)
#    print "Files with tags %s: %s" % (ts, fs)
