import tagger
import unittest

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

class TestTagger(unittest.TestCase):

    filenames = {'./IMAGES/A.JPG': ['fileAtag1', 'fileAtag2'], \
                 './IMAGES/B.JPG': ['fileBtag1', 'fileAtag1'], \
                 './IMAGES/C.JPG': ['fileCtag1'], \
                 './IMAGES/D.JPG': ['fileAtag1', 'fileDtag1', 'fileDtag2'], \
                 './IMAGES/DOES_NOT_EXIST.JPG': ['fileDNEtag1']}

    def setUp(self):
        print "setup"
        Base = declarative_base()
        self.metadata = Base.metadata
        self.engine = create_engine('sqlite:///:memory:', echo=False)
        self.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        print "teardown"
        pass

    def test_tag(self):
        #self.assertNotEqual(self.session, None)

        tagger.addtags('./IMAGES/E.JPG', 'fileEtag1', 'fileEtag2', 'fileEtag3', 'fileEtag4')
        tagger.rmtags('./IMAGES/E.JPG', 'fileEtag3', 'fileEtag1')
        
        tags = tagger.gettags('./IMAGES/E.JPG')
        print tags

if __name__ == '__main__':
    unittest.main()
