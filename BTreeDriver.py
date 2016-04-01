#-------------------------------------------------------------------------------
# Name:        BTree Driver
# Purpose:     To test the BTree module
#
# Author:      Krishna Durai
#
# Created:     09/07/2013
# Copyright:   (c) kd 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from BTree import BTree
from BTreeBuffer import BTreeBuffer

class OwnBTree(BTree):
    def readNodeIntoBuffer(self, node_ref):
        '''A method to read BTree Node into buffer.
           Logging of metadata can be taken care of here.
           For e.g., maintaining page trailer, heap page map etc.
           Return the buffer number where the node is buffered at the end.'''
        #raise NotImplementedError('Implement readNodeIntoBuffer in class')
        buffer_number = self.btree_buffer.getBuffer()
        self.btree_buffer.readIntoBuffer(buffer_number, node_ref, self.nodeSize)
        return buffer_number

    def writeNodeFromBuffer(self, buffer_number, node_ref):
        '''A method to write BTree Node from buffer to PST file.
           Logging of metadata can be taken care of here.
           For e.g., maintaining page trailer, heap page map etc.
           Return the reference of the node in the PST file.'''
        #raise NotImplementedError('Implement writeNodeFromBuffer in class')
        self.btree_buffer.writeFromBuffer(buffer_number, node_ref, self.nodeSize)
        return node_ref

    def genIntermediateEntry(self, key, node_ref):
        '''Returns a generated Itermediate Entry with given key and reference.
           The Intermediate entry bytearray MUST be of size non Leaf Entry (entrySize).'''
        #raise NotImplementedError, 'Implement genIntermediateEntry in class'
        entry = self.toLitteEndian(key, self.keySize) + self.toLitteEndian(node_ref, 4)
        return entry


    def getChildRef(self, entry):
        '''Returns the the reference to a child node in an entry.'''
        #raise NotImplementedError('Implement getChild in class')
        child_ref = self.toBigEndian(entry[self.keySize:])
        return child_ref

    def allocateNode(self):
        '''Returns an reference where a new node can can be written to.'''
        #raise NotImplementedError('Implement allocateNode in class')
        self.btree_buffer.pstfile.seek(0)
        location = self.toBigEndian(bytearray(self.btree_buffer.pstfile.read(4)))

        new_location = location + self.nodeSize

        self.btree_buffer.pstfile.seek(0)
        self.btree_buffer.pstfile.write(self.toLitteEndian(new_location, 4))
        return location

    def delNodeAllocation(self, node_ref):
        '''Deletes a given node and its allocation.'''
        #raise NotImplementedError('Implement delNodeAllocation in class')
        del_indicator = 0xFFFFFFFF
        self.btree_buffer.pstfile.seek(node_ref)
        self.btree_buffer.pstfile.write(self.toLitteEndian(del_indicator, 4))

def toBigEndian(bytelist):
    result = 0
    shift = 0
    for byte in bytelist:
        result = result + (byte << shift)
        shift = shift + 8
    return result

def main():

    ## Opening the test file for BTree
    pst_file = open('test.pst', 'rb+')

    ## Reading root reference of the BTree from file
    pst_file.seek(4)
    root_loc = toBigEndian(bytearray(pst_file.read(4)))

    ## Preparing the BTree
    new_buffer = BTreeBuffer(pst_file)
    test_btree = OwnBTree(new_buffer,   # BTreeBuffer object to be used by the BTree object.
                          60,           # nodeEntrySize
                          60,           # nodeMetaData
                          64,           # nodeSize
                          8,            # entrySize
                          12,           # leafEntrySize
                          4,            # keySize
                          root_loc)     # root_ref

    ## Test for BTreeSearch
    # key = 0x32
    # test_btree.printByteArray(test_btree.BTreeSearch(key))

    ## Test for BTreeInsertEntry
    # new_entry = bytearray('\x10\x01\x00\x00\x10\x01\x00\x00\x00\x00\x00\x00')
    # test_btree.BTreeInsertEntry(new_entry)

    ## Test for BTreeRemoveEntry
    # key = 0x70
    # test_btree.BTreeRemoveEntry(key)

    ## Writing root reference of the BTree from file
    pst_file.seek(4)
    pst_file.write(test_btree.toLitteEndian(int(test_btree.root_ref), 4))

    ## Closing the test file for BTree
    pst_file.close()

if __name__ == '__main__':
    main()
