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

NOTES:
SELECT files.name FROM files JOIN file_tags ON files.id=file_tags.file_id WHERE file_tags.tag_id IN (5, 6, 7) GROUP BY files.name;

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

    _tags = relationship('Tag', secondary=file_tags, backref='files', cascade="all, delete", lazy='dynamic')

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
        self.ts = [] # the tags on this file instance

    def __repr__(self):
        return "File(%r, %r, %r)" % (self.id, self.hash, self.name)

    def gettags(self):
        self.ts = session.query(Tag).filter(Tag.files.any(hash=self.hash)).all()
        return [t.tag for t in self.ts]

    def settags(self, vals):
        # vals is expected to be a list

        for t in vals:

            # check if the tag exists in the database, either add this or create new Tag
            try:
                existing = session.query(Tag).filter_by(tag=t).one()
                self._tags.append(existing)
                self.ts.append(existing)
            except:
                self._tags.append(Tag(t))
                self.ts.append(Tag(t))

        session.add(self)

        try:
            session.commit()
        except IntegrityError:
            session.rollback()

    def deltag(self):
        print "Delete not implemented"
        #tag = self.ts
        #t = session.query(Tag).filter_by(tag=tag).one()
        #self._tags.delete(Tag(t))

        pass
        
    tags = property(gettags, settags, deltag, "I'm the tags property on File")

class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag = Column(String(50), nullable=False, unique=True)

    def __init__(self, tag):
        self.tag = tag

#def getfiles(*tag):
#    """ accept a (list of) tag(s), return a list of all files with all of those tags
#    """
#
#    q = session.query(File.name)
#    for t in tag:
#        q = q.filter(File._tags.any(tag=t))
#    q = q.all()
#    return q

#def gettags(file_):
#    """ accept a filename, return a list of all tags on that file
#    """
#    f = File(file_)
#    h = f.hash
#
#    tags = session.query(Tag).filter(Tag.files.any(hash=h)).all()
#    return tags

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
    f1.tags = ['fileEtag1', 'fileEtag2']
    f1.tags = ['fileEtag3']
    f1.tags = ['fileEtag4']

    f1.tags = ['fileEtag1']
    del f1.tags

    for f in filenames:

        file_ = File(f)

        if file_:
            file_.tags = filenames[f] # send a list for APPENDING!
            # or
#           file_.tags = 'aNewTag' # send a single tag for APPENDING
            # then
#           del file_.tags # to delete that tag

            # or, in keeping with list syntax
            #file_.remove('tag')

    # get tags for a file
    f = File('./IMAGES/A.JPG')
    print "f TAGS:", f.tags

    print "f1 TAGS:", f1.tags

    f3 = File('./IMAGES/D.JPG')
    print "D.JPG TAGS:", f3.tags

    # get files for a (list of) tag(s)
#    t1 = Tag('fileAtag1')
#    print t1.files

#    t2 = Tag(['fileAtag1', 'fileDtag1'])
#    print t2.files

"""
    # OLD STYLE GETTERS

    # get tags for a file
    print "**** tags on file A.JPG *******"
    t1 = gettags('./IMAGES/A.JPG')
    for tag in t1:
        print tag.tag

    print "**** tags on file D.JPG *******"
    tags = gettags('./IMAGES/D.JPG')
    for tag in tags:
        print tag.tag

    # get files which have a list of tags
    tags = ['fileAtag1', 'fileDtag1', 'fileDtag2']
    print "**** files with all these tags: %s *******" % (" ".join(tags))
    files = getfiles(*tags) # passing a list, treated like getfiles(tag1, tag2, tag3)
    for file_ in files:
        print file_.name

    # get files with a single tag
    tag = 'fileAtag1'
    print "**** files with this tag: %s *******" % (tag)
    files = getfiles(tag)
    for file_ in files:
        print file_.name
"""

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
