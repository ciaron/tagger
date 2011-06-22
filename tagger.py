import sqlite3
import hashlib
import os

"""
TODO: 
    1. use sqlalchemy instead of sqlite3
    2. command line argument parsing
    3. check if tag exists for given filehash: do not repeat the update/insert
"""

db = '/tmp/tagger.db'

class Tagger:

    def __init__(self, config):
        """
        initialise the tagger from the config file.
         - check that DB exists, create if not found where config expects
        """

        conn = sqlite3.connect(db)
        c = conn.cursor()

        try:
            c.execute('''create table if not exists tags (md5hash text, filename text, tag text)''')
        except sqlite3.OperationalError, msg:
            print msg

        conn.commit()
        c.close()

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

    def settags(self, filename, tags):
        """ Assume filename exists!
        """

        filehash = self.__gethash(filename)

        if filehash != None:
            conn = sqlite3.connect(db)
            c = conn.cursor()

            for tag in tags:
                c.execute('select count(*) from tags where md5hash=? and filename=? and tag=?', (filehash, filename, tag))
                count = int(c.fetchone()[0])

                if count == 0:
                    vals = (filehash, filename, tag)
                    try:
                        c.execute('insert into tags values (?, ?, ?)', vals)
                    except sqlite3.OperationalError, msg:
                        print msg
            
            conn.commit()
            c.close()

    def gettags(self, filename):
        """ Assume filename exists!
        """
        tags = []
        filehash = self.__gethash(filename)

        if filehash != None:
            conn = sqlite3.connect(db)
            c = conn.cursor()

            try:
                t = (filehash,)
                c.execute('select tag from tags where md5hash=?', t)
            except sqlite3.OperationalError, msg:
                print msg

            tags = [x[0] for x in c.fetchall()]
           
            conn.commit()
            c.close()

        return tags

    def getfiles(self, tags):
        """
        get all file that have the give tags (a list)
        """
        pass

if __name__ == '__main__':
    tagger = Tagger('TESTCONFIG')

    files = ['/home/linstead/Dropbox/Code/tagger/IMAGES/A.JPG', \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/B.JPG', \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/C.JPG', \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/D.JPG', \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/E.JPG', \
              '/home/linstead/Dropbox/Code/tagger/IMAGES/DOES_NOT_EXIST.JPG']

    # Set some tags
    for f in files:
        tagger.settags(f, ['tag1', 'tag2'])

#    tagger.settags(files[0], ['tag3'])
#    tagger.settags(files[1], ['tag4 and something'])
#    tagger.settags(files[3], 'tag999')

    # Read the tags for the given files
    for f in files:
        tags = tagger.gettags(f)
        print "File: %s; Tags: %s" % (f, ", ".join(tags))
