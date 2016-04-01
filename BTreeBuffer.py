#-------------------------------------------------------------------------------
# Name:        BTreeBuffer
# Purpose:     Buffering BTree from PST file
#
# Author:      Krishna Durai
#
# Created:     24/06/2013
# Copyright:   (c) kd 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------

class BTreeBufferException(RuntimeError):
    '''Class to raise BTreeBuffer Errors.'''
    "problem in bufferfile"


class BTreeBuffer(object):
    '''BTreeBuffer implements a buffer for BTree module.
       It implements getBuffer, resetBuffer, returnBuffer, readIntoBuffer and writeFromBuffer functions.'''

    BufferList = []     # List containing bytearrays which act as buffers to BTree Nodes.
    freeBufferQ = []    # Queue of buffers which are free to be alloted to BTree Nodes.
    sections = 0        # Maximum Number of bytearray buffers which can be alloted.
    buffersize = 0      # Size of the bytearray buffers.
    pstfile = None      # The complete file path of the pst file to be buffered.

    def __init__(self, pstfile, sections = 10, buffersize = 3850):
        self.pstfile = pstfile
        self.sections = sections
        self.buffersize = buffersize
        self.BufferList = [None] * sections
        self.freeBufferQ = range(sections)
        for count in range(sections):
            self.BufferList[count] = bytearray(buffersize)

    def getBuffer(self):
        '''Returns an unallocated buffer.'''
        if self.freeBufferQ:
            return self.freeBufferQ.pop(0)
        raise BTreeBufferException, 'Too many buffers used'

    def resetBuffer(self):
        '''Frees all buffers, i.e. sets all buffers as unallocated.'''
        self.freeBufferQ = range(self.sections)

    def returnBuffer(self, buffer_number):
        '''Returns given buffer to unallocated buffer queue.'''
        self.freeBufferQ.append(buffer_number)

    def readIntoBuffer(self, buffer_number, seek_pos, read_size):
        '''Reads bytes into given buffer from the PST file from 'seek_pos' till the given 'read_size'.'''
        if(read_size <= self.buffersize):
            self.pstfile.seek(seek_pos)
            byte_string = self.pstfile.read(read_size)
            count = 0
            while count < read_size:
                self.BufferList[buffer_number][count] = byte_string[count]
                count = count + 1
        else:
            raise BTreeBufferException, 'Too big to read into Buffer'

    def writeFromBuffer(self, buffer_number, seek_pos, write_till):
        '''Writes bytes from given buffer into the PST file from 'seek_pos' to 'write_till'.'''
        self.pstfile.seek(seek_pos)
        self.pstfile.write(self.BufferList[buffer_number][:write_till])
