from sqlalchemy import Text, ForeignKey, Column, MetaData, String, Integer, Table
from sqlalchemy import create_engine
from sqlalchemy import or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker, exc
from sqlalchemy.exc import *
from sqlalchemy.sql import select
import hashlib
import os

"""
TODO: 
    - command line argument parsing
"""
Base = declarative_base()

# for now, we'll do everything in memory
#engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///:memory:', echo=False)
#engine = create_engine('sqlite:///data.db', echo=True) # create DB file in current dir

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

if __name__ == '__main__':
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filenames = {'./IMAGES/A.JPG': ['fileAtag1', 'fileAtag2'], \
                 './IMAGES/B.JPG': ['fileBtag1', 'fileAtag1'], \
                 './IMAGES/C.JPG': ['fileCtag1'], \
                 './IMAGES/D.JPG': ['fileAtag1', 'fileDtag1', 'fileDtag2'], \
                 './IMAGES/DOES_NOT_EXIST.JPG': ['fileDNEtag1']}

    f1 = File('./IMAGES/E.JPG')
    f1.tag('fileEtag1', 'fileEtag2')
    f1.tag('fileEtag3')
    f1.tag('fileEtag4')
    f1.remove('fileEtag3')
    f1.remove('fileAtag3')
    print "E.JPG: %s" % gettags('./IMAGES/E.JPG')

    for f in filenames:
        file_ = File(f)
        if file_:
            for tag in filenames[f]:
                file_.tag(tag)

    fname = './IMAGES/A.JPG'
    f2 = session.query(File).filter(File.name==fname).one()
    print "A.JPG TAGS BEFORE:", gettags(fname)
    f2.remove('fileAtag1')
    f2.tag('a new tag for A')
    print "A.JPG TAGS AFTER:", gettags(fname)

    fname = './IMAGES/D.JPG'
    f3 = session.query(File).filter(File.name==fname).one()
    print "D.JPG TAGS:", gettags(fname)

    fname = './IMAGES/C.JPG'
    f4 = session.query(File).filter(File.name==fname).one()
    print "C.JPG TAGS:", gettags(fname)

    fname = './IMAGES/DOESNOTEXIST.JPG'
    try: # we should only get one File object!
        f5 = session.query(File).filter(File.name==fname).one()
        print "DOESNOTEXIST.JPG TAGS:", gettags(fname)
    except exc.NoResultFound:
        print "nothing found for", fname

    # get files for a (list of) tag(s)
    fs = getfiles('fileAtag1')
    print fs

    fs = getfiles('fileAtag1', 'fileDtag1')
    print fs
