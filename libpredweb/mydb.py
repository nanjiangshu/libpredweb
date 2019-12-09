import os
import sys
from . import mydb_common
class MyDB: #{{{
# Description:
#   A class to handle a database of dumped data. The content for each query id
#   can be accessed quickly by GetRecord(id)
# variables:
#     indexedIDList  :  list of record IDs
# 
# Functions:
#     GetRecord(id)  : retrieve record for id, 
#                      return None if failed
#     GetAllRecord() : retrieve all records in the form of list

    def __init__(self, dbname, index_format = mydb_common.FORMAT_BINARY,#{{{
                    isPrintWarning = False):
#        print "Init", dbname
        self.failure = False
        self.index_type = mydb_common.TYPE_DICT
        self.dbname = dbname
        self.dbname_basename = os.path.basename(dbname)
        self.dbname_dir = os.path.dirname(self.dbname)
        self.dbname_dir_full = os.path.realpath(self.dbname_dir)
        self.dbname_full = self.dbname_dir_full + os.sep + self.dbname_basename
        self.index_format = index_format
        self.isPrintWarning = isPrintWarning
        self.fpdbList = []
        (self.indexfile, self.index_format) =\
                        mydb_common.GetIndexFile(self.dbname_full,
                                        self.index_format)
        if self.indexfile != "":
            (self.indexList, self.headerinfo, self.dbfileindexList) =\
                            self.ReadIndex(self.indexfile, self.index_format)
            if self.indexList == None:
                msg = "Failed to read index file {}. Init database {} failed."
                print(msg.format(self.indexfile,
                                self.dbname_full), file=sys.stderr)
                self.failure = True
                return None
            if self.OpenDBFile() == 1:
                self.failure = True
                return None
            self.indexedIDList = self.indexList[0]
            self.numRecord = len(self.indexedIDList)
            if self.index_type == mydb_common.TYPE_DICT:
                self.indexDict = {}
                for i in range(self.numRecord):
                        self.indexDict[self.indexedIDList[i]] = i
        else:
            msg = "Failed to find indexfile for db {}"
            print(msg.format(dbname), file=sys.stderr)
            self.failure = True
            return None
          #}}}
    def __del__(self):#{{{
#        print "Leaving %s"%(self.dbname)
        try: 
            for fp in self.fpdbList:
                fp.close()
            return 0
        except IOError:
            print("Failed to close db file", file=sys.stderr)
        #}}}
    def ReadIndex(self, indexfile, index_format):#{{{
# return (headerinfo, dbfileindexList, index, idList)
# return (indexList, headerinfo, dbfileindexList)
        if index_format == mydb_common.FORMAT_TEXT:
            return mydb_common.ReadIndex_text(indexfile, self.isPrintWarning)
        else:
            return mydb_common.ReadIndex_binary(indexfile, self.isPrintWarning)
#}}}
    def OpenDBFile(self):#{{{
        for i in self.dbfileindexList:
            dbfile = self.dbname_full + "%d.db"%(i)
            try:
                self.fpdbList.append(open(dbfile,"r"))
            except IOError:
                print("Failed to read dbfile %s"%(dbfile), file=sys.stderr)
                return 1
        return 0
#}}}
    def GetRecordByIndexList(self, record_id):#{{{
        try:
            idxItem = self.indexedIDList.index(record_id);
            fpdb = self.fpdbList[self.indexList[1][idxItem]];
            fpdb.seek(self.indexList[2][idxItem]);
            data = fpdb.read(self.indexList[3][idxItem]);
            return data
        except (IndexError, IOError):
            print("Failed to retrieve record %s"%(record_id), file=sys.stderr)
            return None
#}}}
    def GetRecordByIndexDict(self, record_id):#{{{
        try:
            idxItem = self.indexDict[record_id]
            fpdb = self.fpdbList[self.indexList[1][idxItem]];
            fpdb.seek(self.indexList[2][idxItem]);
            data = fpdb.read(self.indexList[3][idxItem]);
            return data
        except (KeyError, IndexError, IOError):
            print("Failed to retrieve record %s"%(record_id), file=sys.stderr)
            return None
#}}}
    def GetRecord(self, record_id):#{{{
        if self.index_type == mydb_common.TYPE_LIST:
            return self.GetRecordByIndexList(record_id)
        elif self.index_type == mydb_common.TYPE_DICT:
            return self.GetRecordByIndexDict(record_id)
#}}}
    def GetAllRecord(self): #{{{
        recordList = []
        for idd in self.idList:
            recordList.append(self.GetRecord(idd))
        return recordList
    def close(self):#{{{
        try: 
            for fp in self.fpdbList:
                fp.close()
            return 0
        except IOError:
            print("Failed to close db file", file=sys.stderr)
            return 1
        #}}}
    #}}}
#}}}
