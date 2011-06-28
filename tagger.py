from sqlalchemy import Text, ForeignKey, Column, MetaData, String, Integer, Table
from sqlalchemy import create_engine
from sqlalchemy import or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
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
engine = create_engine('sqlite:///:memory:', echo=True)
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

    def __init__(self, name):
        self.hash = self.gethash(name)
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
    """ accept a (list of) tag(s), return a list of all files with all of those tags
    """
    files = session.query(File).filter(File.tags.any(tag=tag)).all()
    return files

def gettags(file_):
    """ accept a filename, return a list of all tags on that file
    """
    f = File(file_)
    h = f.hash

    tags = session.query(Tag).filter(Tag.files.any(hash=h)).all()
    return tags

if __name__ == '__main__':
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filenames = {'./IMAGES/A.JPG': ['fileAtag1', 'fileAtag2'], \
                 './IMAGES/B.JPG': ['fileBtag1', 'fileAtag1'], \
                 './IMAGES/C.JPG': ['fileCtag1'], \
                 './IMAGES/D.JPG': ['fileDtag1', 'fileDtag2'], \
                 './IMAGES/E.JPG': ['fileEtag1', 'fileAtag1'], \
                 './IMAGES/DOES_NOT_EXIST.JPG': ['fileDNEtag1']}

    for f in filenames:

        file_ = File(f)

        if file_:
            for v in filenames[f]:

                # check if the tag exists in the database, either add this or create new Tag
                try:
                    existing = session.query(Tag).filter_by(tag=v).one()
                    file_.tags.append(existing)
                except:
                    file_.tags.append(Tag(v))

            session.add(file_)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

    # get tags for a file
    print "**** files with tag fileAtag1 *******"
    files = getfiles('fileAtag1')
    for file_ in files:
        print file_.name

    print "**** tags on file D.JPG *******"
    tags = gettags('./IMAGES/D.JPG')
    for tag in tags:
        print tag.tag

    # lists of tags not supported yet
#    print "**** files with tags fileDtag1 AND fileDtag2 *******"
#    files = getfiles(['fileDtag1', 'fileDtag2'])
#    for file_ in files:
#        print file_.name

"""
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
