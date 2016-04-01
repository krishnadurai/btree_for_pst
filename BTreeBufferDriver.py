#-------------------------------------------------------------------------------
# Name:        BTreeBufferDriver
# Purpose:     Testing BTreeBuffer module
#
# Author:      Krishna Durai
#
# Created:     29/06/2013
# Copyright:   (c) kd 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from BTreeBuffer import BTreeBuffer

def main():
    pst_file = open('test.pst', 'rb+')
    btree_buffer = BTreeBuffer(pst_file)
    blist = btree_buffer.BufferList
    print btree_buffer.freeBufferQ
    buffer_number = btree_buffer.getBuffer()
    print buffer_number
    print btree_buffer.freeBufferQ
    btree_buffer.readIntoBuffer(buffer_number,0,496)
    btree_buffer.writeFromBuffer(buffer_number,4,496)
    btree_buffer.returnBuffer(buffer_number)
    print btree_buffer.freeBufferQ
    btree_buffer.resetBuffer()
    print btree_buffer.freeBufferQ


if __name__ == '__main__':
    main()
