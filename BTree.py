#-------------------------------------------------------------------------------
# Name:        BTree
# Purpose:     Generalized BTree for PST file format
#
# Author:      Krishna Durai
# Created:     25/06/2013
# Copyright:   (c) kd 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import BTreeBuffer

class BTreeError(RuntimeError):
    '''Class to raise BTree Errors.'''
    'problem in btree'

class BTreeOpCode:
    '''Operation status codes for BTree operations.'''
    SUCCESS = 0
    DUPLICATE = 1
    OVERFLOW = 2
    NOTPRESENT = 3

class NodeSearchResult(object):
    '''Class to save properties of a search result.'''
    def __init__(self, outcome = False, position = 0):
        self.outcome = outcome
        self.position = position

class NodeLocationInfo(object):
    '''Class to save the location and buffer number of a Node in BTree.'''
    def __init__(self, buffer_number, location_infile):
        self.buffer_number = buffer_number
        self.location_infile = location_infile

class EntryInfo(object):
    '''Class to save information of a given node entry.'''
    def __init__(self, isValid = False, entry = bytearray(), key = 0):
        self.isValid = isValid # isValid = True indicates that this EntryInfo Object contains a meaningful entry.
        self.entry = entry
        self.key = key
    def reset(self):
        self.isValid = False
        self.entry = bytearray()
        self.key = 0

class BTree(object):
    '''BTree class implements a genralized BTree for the MS-PST file format.
       It implements BTreeCreate, BTreeSearch, BTreeInsertEntry and BTreeRemoveEntry funtions.
       It can only be used as a parent class to different versions of BTrees used by the MS- PST file format.
       One needs to inherit BTree class in their child class and implement the following functions in their own class:
       readNodeIntoBuffer, writeNodeFromBuffer, genIntermediateEntry, getChildRef, allocateNode and delNodeAllocation.'''

    btree_buffer = []       # A BtreeBuffer object which has to be declared outside the Btree. Passed as a parameter to initialize the BTree.
    nodeSize = 0            # The size of node including space required for metadata of node.
    nodeEntriesSize = 0     # The actual size of the node bucket to store entries.
    nodeBucketSize = 0      # The recommended size of the node bucket to store entries.
    nodeMetaData = 0        # The offset from the start position of node to its metadata location (in terms of bytes).
    cEntIndex = 0           # The offset from the start position of node to its cEnt record (in terms of bytes).
    cEntMaxIndex = 0        # The offset from the start position of node to its cEntMax record (in terms of bytes).
    cbEntMaxIndex = 0       # The offset from the start position of node to its cbEntMax record (in terms of bytes).
    cLevelIndex = 0         # The offset from the start position of node to its cLevelIndex (in terms of bytes).
    entrySize = 0           # The size of an intermediate node entry in bytes.
    leafEntrySize = 0       # The size of an leaf node entry in bytes.
    keySize = 0             # The size of a key value in an entry in bytes.
    root_ref = 0            # The reference for the root node of the BTree.
    maxEntries = 0          # The actual maximum number of intermediate entries that can be contained in a node.
    leafMaxEntries = 0      # The actual maximum number of leaf entries that can be contained in a node.
    recMaxEntries = 0       # The recommended maximum number of intermediate entries that can be contained in a node.
    recLeafMaxEntries = 0   # The recommended maximum number of leaf entries that can be contained in a node.

    def __init__(self, btree_buffer, nodeEntriesSize, nodeMetaData, nodeSize, entrySize, leafEntrySize, keySize, root_ref = None):
        self.btree_buffer = btree_buffer
        self.nodeSize = nodeSize
        self.nodeEntriesSize = nodeEntriesSize
        self.nodeMetaData = nodeMetaData
        self.cEntIndex = nodeMetaData
        self.cEntMaxIndex = nodeMetaData + 1
        self.cbEntMaxIndex = nodeMetaData + 2
        self.cLevelIndex = nodeMetaData + 3
        self.nodeBucketSize = int((nodeEntriesSize) * 0.9)
        self.entrySize = entrySize
        self.leafEntrySize = leafEntrySize
        self.keySize = keySize
        self.root_ref = root_ref
        self.recMaxEntries = int(self.nodeBucketSize / entrySize)
        self.recLeafMaxEntries = int(self.nodeBucketSize / leafEntrySize)
        self.maxEntries = int(nodeEntriesSize / entrySize)
        self.leafMaxEntries = int(nodeEntriesSize / leafEntrySize)

    def readNodeIntoBuffer(self, node_ref):
        '''A method to read BTree Node into buffer.
           Logging of metadata can be taken care of here.
           For e.g., maintaining page trailer, heap page map etc.
           Return the buffer number where the node is buffered at the end.'''
        raise NotImplementedError, 'Implement readNodeIntoBuffer in class'

    def writeNodeFromBuffer(self, buffer_number, node_ref):
        '''A method to write BTree Node from buffer to PST file.
           Logging of metadata can be taken care of here.
           For e.g., maintaining page trailer, heap page map etc.
           Return the reference of the node in the PST file.'''
        raise NotImplementedError, 'Implement writeNodeFromBuffer in class'

    def genIntermediateEntry(self, key, node_ref):
        '''Returns a generated Itermediate Entry with given key and reference.
           The Intermediate entry bytearray MUST be of size non Leaf Entry (entrySize).'''
        raise NotImplementedError, 'Implement genIntermediateEntry in class'

    def getChildRef(self, entry):
        '''Returns the the reference to a child node in an entry.'''
        raise NotImplementedError, 'Implement getChild in class'

    def allocateNode(self):
        '''Returns an reference where a new node can can be written to.'''
        raise NotImplementedError, 'Implement allocateNode in class'

    def delNodeAllocation(self, node_ref):
        '''Deletes a given node and its allocation.'''
        raise NotImplementedError, 'Implement delNodeAllocation in class'

    def createNode(self, level):
        '''Creates an empty new node with required metadata.
           The level given as a parameter is maintained to be the level of the creaed node.
           Returns the information of the created node.'''
        buffer_number = self.btree_buffer.getBuffer()
        currentArray = self.btree_buffer.BufferList[buffer_number]
        self.clearArray(currentArray)
        currentArray[self.cEntIndex] = 0
        if level == 0:
            currentArray[self.cEntMaxIndex] = self.leafMaxEntries
            currentArray[self.cbEntMaxIndex] = self.leafEntrySize
            currentArray[self.cLevelIndex] = 0
        else:
            currentArray[self.cEntMaxIndex] = self.maxEntries
            currentArray[self.cbEntMaxIndex] = self.entrySize
            currentArray[self.cLevelIndex] = level
        node_ref = self.allocateNode()
        return NodeLocationInfo(buffer_number, node_ref)

    def nbind(self, position):
        '''Returns the actual index (Node Bytearray Index) of the non-leaf entry.
           The 'position' parameter indicates the position of the entry in the node.'''
        return position * self.entrySize

    def lnbind(self, position):
        '''Returns the actual index (Leaf Node Bytearray Index) of the leaf entry.
           The 'position' parameter indicates the position of the entry in the node.'''
        return position * self.leafEntrySize

    def getKey(self, currentNode, key_index):
        '''Returns a key present in the given location.'''
        key_bytearray = currentNode[key_index:(key_index + self.keySize)]
        return self.toBigEndian(key_bytearray)

    def printByteArray(self, new_array):
        '''Utility fuction for printing a bytearray.'''
        if new_array:
            for item in new_array:
                print item,
        else:
            print new_array,
        print 'Over'

    def toBigEndian(self, bytelist):
        '''Converts a given bytearray in Little-Endian format to an int in Big-Endian format.'''
        result = 0
        shift = 0
        for byte in bytelist:
            result = result + (byte << shift)
            shift = shift + 8
        return result

    def toLitteEndian(self, number, size):
        '''Converts an int in Big-Endian to a bytearray in Litte-Endian format.'''
        result_array = bytearray(size)
        count = 0
        while count < size:
            result_array[count] = number % (2 << 8) & 0xFF
            number = number >> 8
            count = count + 1
        return result_array

    def clearArray(self, currentArray):
        '''Utility fuction to clear an array, i.e. fill all array entries with 0.'''
        count = 0
        while count < len(currentArray):
            currentArray[count] = 0
            count = count + 1

    def BTreeCreate(self):
        '''Returns the root reference of a new BTree with an empty root node.'''
        node_loc = self.createNode(0)
        self.root_ref = node_loc.location_infile
        self.writeNodeFromBuffer(node_loc.buffer_number, node_loc.location_infile)
        self.btree_buffer.resetBuffer()
        return self.root_ref

    def BTreeSearch(self, key):
        '''Wrapper funtion to search for an entry in the BTree.
           Returns the value associated with the key if search is a success otherwise it returns None.'''
        if self.root_ref != None:
            result = self.BTreeSearch_t(self.root_ref, key)
            self.btree_buffer.resetBuffer()
        else:
            raise BTreeError, 'btree does not exist'
        return result

    def BTreeSearch_t(self, node_ref, key):
        '''The recursive search function called by BTreeSearch.
           Return value is same as BTreeSearch.'''
        btree_searchRes = None
        buffer_number = self.readNodeIntoBuffer(node_ref)
        currentNode = self.btree_buffer.BufferList[buffer_number]

        # bind converts the logical index of the BTree Node Entry to Buffer Bytearray index.
        # It assumes the nbind funtion or lbind funtion depending if it is a non-leaf or not.
        isLeaf = False
        bind = self.nbind
        if currentNode[self.cLevelIndex] == 0:
            isLeaf = True
            bind = self.lnbind

        searchRes = self.findInNode(currentNode, key, bind)

        if searchRes.outcome == False and not isLeaf:
            if searchRes.position == 0:
                entryIndex = bind(searchRes.position)
                # Assuming that the first entry is not consistent.
                # Otherwise btree_searchRes = None
            else:
                entryIndex = bind(searchRes.position - 1)
            child_ref = self.getChildRef(currentNode[entryIndex : entryIndex + self.entrySize])
            btree_searchRes = self.BTreeSearch_t(child_ref, key)

        elif searchRes.outcome == True and not isLeaf:
            entryIndex = bind(searchRes.position)
            child_ref = self.getChildRef(currentNode[entryIndex : entryIndex + self.entrySize])
            btree_searchRes = self.BTreeSearch_t(child_ref, key)

        elif searchRes.outcome == False: # and isLeaf
            btree_searchRes = None

        else: # searchRes.outcome == True and isLeaf
            entryValIdx = bind(searchRes.position) + self.keySize # Extracting the value part of the leaf entry.
            btree_searchRes = currentNode[entryValIdx : entryValIdx + self.leafEntrySize - self.keySize]

        self.btree_buffer.returnBuffer(buffer_number)
        return btree_searchRes


    def findInNode(self,
                   currentNode, # Reference to current bytearray.
                   key,         # The key to be searched for in the Node.
                   bind):       # The function lnbind or nbind is passed as a parameter if the given node's level is 0 or more respecively.
        ''' This function searches for an existing entry in the node with the same key as 'key' parameter.
            This returns a value of type NodeSearchResult.
            If the search result is unsuccessful it will return the tentative position of the given key in node if it were present in this node.'''

        low = 0
        high = currentNode[self.cEntIndex] - 1
        mid = 0

        while(low<=high):
            mid = (low + high)/2
            bufferedKey = self.getKey(currentNode, bind(mid))
            if key > bufferedKey:
                low = mid + 1
            elif key < bufferedKey:
                high = mid - 1
            else:
                break

        if low>high:
            return NodeSearchResult(False, low)
            # Returns the tentative position of the search key if it were present inside this node.
            # The code was written carefully to ensure that the position of low falls at the tentative positon mentioned above.
        else:
            return NodeSearchResult(True, mid)

    def shiftNodeEntsRight(self,
                           currentArray,    # Reference to current bytearray.
                           index,           # Start index (inclusive in shift) from where the shift should take place.
                           shiftBy,         # Number of bytes to be shifted by.
                           toIndex):        # Last index (inclusive in shift) till where the shift should end.
        '''Shifts a given bytearray towards right by 'shiftBy' bytes from the given 'index' parameter.
           This does not change the values in bytearray from 'index' to 'index + shiftBy - 1', both inclusive.'''

        fromIndex = toIndex - shiftBy
        while fromIndex >= index:
            currentArray[toIndex] = currentArray[fromIndex]
            toIndex = toIndex - 1
            fromIndex = fromIndex - 1

    def shiftNodeEntsLeft(self,
                          currentArray,    # Reference to current bytearray.
                          index,           # Start index (inclusive in shift) from where the shift should take place.
                          shiftBy):        # Number of bytes to be shifted by.
        '''Shifts a given bytearray towards left by 'shiftBy' bytes from the given 'index' parameter.
           This consumes bytearray values from 'index' to 'index + shiftBy - 1', both inclusive.'''

        fromIndex = index + shiftBy
        toIndex = index
        while fromIndex < self.nodeBucketSize:
            currentArray[toIndex] = currentArray[fromIndex]
            toIndex = toIndex + 1
            fromIndex = fromIndex + 1
        while toIndex <= self.nodeBucketSize: # Fills the rest of the bytearray values from 'nodeBucketSize - shiftBy' to 'nodeBucketSize - 1' to 0.
            currentArray[toIndex] = 0
            toIndex = toIndex + 1

    def BTreeInsertEntry(self, new_entry): # new_entry is the entry to be inserted into the BTree. It MUST be of type bytearray and of leaf entry size.
        '''This function inserts the given new entry into the BTree.
           This returns a BTreeOpCode value depending on the outcome of the insert operation.'''
        op_result = BTreeOpCode.SUCCESS
        gen_entry = EntryInfo()
        level = 0
        new_first_ent = EntryInfo(True)

        if len(new_entry) == self.leafEntrySize:
            key = self.getKey(new_entry, 0)
            child_ref = self.root_ref
            child_buffer_number = self.readNodeIntoBuffer(self.root_ref)
            childNode = self.btree_buffer.BufferList[child_buffer_number]
            child_level = childNode[self.cLevelIndex]
            op_result = self.pushEntryDown(self.root_ref, childNode, child_buffer_number, key, new_entry, gen_entry, child_level, new_first_ent)

            if op_result == BTreeOpCode.OVERFLOW: # if overflow create new root.
                level = child_level + 1
                new_root = self.createNode(level)
                newRootArray = self.btree_buffer.BufferList[new_root.buffer_number]
                present_first_ent = self.genIntermediateEntry(self.getKey(childNode, 0), child_ref)
                self.pushEntryIn(newRootArray, present_first_ent, nbind(0))
                self.pushEntryIn(newRootArray, gen_entry.entry, self.entrySize)
                self.root_ref = new_root.location_infile
                self.writeNodeFromBuffer(new_root.buffer_number, new_root.location_infile)
                self.btree_buffer.returnBuffer(new_root.buffer_number)

            self.writeNodeFromBuffer(child_buffer_number, child_ref)
            self.btree_buffer.returnBuffer(child_buffer_number)
        else:
            raise BTreeError, 'Size of new entry does not match expected entry size.'
        self.btree_buffer.resetBuffer()
        return self.root_ref

    def pushEntryIn(self,
                    currentNode,    # Reference to current bytearray.
                    new_entry,      # new_entry is the entry to be inserted into the Node. It MUST be of type bytearray and of appropriate entry size for the Node.
                    index):         # Index where the entry is to be inserted into.
        '''This function inserts the new entry in the current node at the given index.
           It assumes that there is enough space in the node to accomodate the entry and 'new_entry' size is correct.'''

        self.shiftNodeEntsRight(currentNode, index, len(new_entry), self.nodeBucketSize - 1)
        count = 0
        while count < len(new_entry):
            currentNode[index + count] = new_entry[count]
            count = count + 1
        currentNode[self.cEntIndex] = currentNode[self.cEntIndex] + 1

    def pushEntryDown(self,
                      node_ref,         # The reference to the current node. readNodeIntoBuffer MUST be able to fetch the node with this reference from the PST file.
                      currentNode,      # Reference to the buffered bytearray containing the node represented in node_ref.
                      buffer_number,    # The buffer number of 'currentNode' bytearray in self.btree_buffer.
                      key,              # The key to be inserted into the BTree.
                      new_entry,        # new_entry is the entry to be inserted into the BTree. It MUST be of type bytearray and of appropriate entry size for the node.
                      gen_entry,        # This parameter is a return value: Generated intermediate entry (EntryInfo type) produced for parent node to allow node splits.
                      level,            # The BTree level of 'currentNode'.
                      new_first_ent):   # This parameter is a return value: Generated intermediate entry (EntryInfo type) produced for parent node when the first entry of it's child node changes.
        '''This fuction is for recursively accessing the nodes in BTree for insert operation and to control the logic of BTree insertion.
           This returns a BTreeOpcode value depending on the result of BTree insert operation on its sub-tree.'''

        op_result = BTreeOpCode.SUCCESS

        # bind converts the logical index of the BTree Node Entry to Buffer Bytearray index.
        # It assumes the nbind funtion or lbind funtion depending if it is a non-leaf or not.
        bind = self.nbind
        isLeaf = False
        if level == 0:
            bind = self.lnbind
            isLeaf = True

        searchRes = self.findInNode(currentNode, key, bind)

        # The following section of code is responsible to track whether or not the first entry of the BTree is to be changed.
        if searchRes.position == 0 and new_first_ent.isValid == True :
            new_first_ent.isValid = True
        else:
            new_first_ent.isValid = False
        # End of section with respect to previous comment.

        if isLeaf:
            if searchRes.outcome == True:
                op_result = BTreeOpCode.DUPLICATE
            else:
                if currentNode[self.cEntIndex] < self.recLeafMaxEntries:
                    insert_pos = bind(searchRes.position)
                    self.pushEntryIn(currentNode, new_entry, insert_pos)
                    # gen_entry = EntryInfo()
                    op_result = BTreeOpCode.SUCCESS
                else:
                    self.splitNode(currentNode, new_entry, gen_entry, searchRes.position, 0)
                    op_result = BTreeOpCode.OVERFLOW

            # This section of the code generates a new intermediate entry containing the key of the changed first entry and the reference of the 'currentNode'.
            if new_first_ent.isValid == True:
                new_first_ent.key = self.getKey(currentNode, 0)
                new_first_ent.entry = self.genIntermediateEntry(new_first_ent.key, node_ref)
            # End of section with respect to previous comment.

        else:
            child_ref = 0
            if searchRes.outcome == True:
                child_ref = self.getChildRef(currentNode[bind(searchRes.position): bind(searchRes.position) + self.entrySize])
                # Just for consistency. Otherwise its an obvious duplicate.
                # Plus we may need duplicates to be replaced with a new value.
            else:
                if searchRes.position == 0:
                    child_ref = self.getChildRef(currentNode[bind(searchRes.position): bind(searchRes.position) + self.entrySize])
                else:
                    child_ref = self.getChildRef(currentNode[bind(searchRes.position - 1): bind(searchRes.position - 1) + self.entrySize])

            child_buffer_number = self.readNodeIntoBuffer(child_ref)
            childNode = self.btree_buffer.BufferList[child_buffer_number]
            child_level = childNode[self.cLevelIndex]
            op_result = self.pushEntryDown(child_ref, childNode, child_buffer_number, key, new_entry, gen_entry, child_level, new_first_ent)
            self.writeNodeFromBuffer(child_buffer_number, child_ref)
            self.btree_buffer.returnBuffer(child_buffer_number)

            # This section of code generates a new intermediate entry containing the key of the changed first entry and the reference of the 'currentNode'.
            if new_first_ent.isValid == True:
                count = 0
                while count < self.entrySize:
                    currentNode[count] = new_first_ent.entry[count]
                    # new_first_ent's position in currentNode will always be 0.
                    count = count + 1

                new_first_ent.key = self.getKey(currentNode, 0)
                new_first_ent.entry = self.genIntermediateEntry(new_first_ent.key, node_ref)
            # End of section with respect to previous comment.

            # The following section of code inserts the entry generated in the child node, in the currentNode.
            # This generated entry is a consequence of splitting in child node.
            if op_result == BTreeOpCode.OVERFLOW:
                genSearchRes = self.findInNode(currentNode, gen_entry.key, bind)
                if currentNode[self.cEntIndex] < self.recMaxEntries:
                    insert_pos = bind(genSearchRes.position)
                    self.pushEntryIn(currentNode, gen_entry.entry, insert_pos)
                    gen_entry.reset()
                    op_result = BTreeOpCode.SUCCESS
                else:
                    self.splitNode(currentNode, gen_entry.entry, gen_entry, genSearchRes.position, level)
                    op_result = BTreeOpCode.OVERFLOW
            # End of section with respect to previous comment.

        return op_result


    def splitNode(self,
                  currentNode,  # Reference to the buffered bytearray of the node to be split.
                  new_entry,    # new_entry is the entry to be inserted into the node. It MUST be of type bytearray and of appropriate entry size for the node.
                  gen_entry,    # This parameter is a return value: Generated intermediate entry (EntryInfo type) produced for parent node to allow node splits.
                  position,     # This is the tentative position where the new entry can be inserted into the node.
                  level):       # The BTree level of 'currentNode'.

        '''This fuction splits 'currentNode' into two.
           Returns a generated new entry for its parent node which contains the reference to the new node created after split.'''

        right_half = self.createNode(level)
        rightHalfNode = self.btree_buffer.BufferList[right_half.buffer_number]
        max_ents = self.recMaxEntries
        ent_size = self.entrySize
        bind = self.nbind
        mid = self.recMaxEntries/2

        if level == 0:
            max_ents = self.recLeafMaxEntries
            ent_size = self.leafEntrySize
            bind = self.lnbind
            mid = self.recLeafMaxEntries/2

        if position <= mid:
            count = bind(mid)
            right_count = 0
            right_half_ents = max_ents - mid
            copy_till = count + right_half_ents * ent_size
            while count < copy_till:
                rightHalfNode[right_count] = currentNode[count]
                currentNode[count] = 0
                count = count + 1
                right_count = right_count + 1

            rightHalfNode[self.cEntIndex] = right_half_ents
            self.pushEntryIn(currentNode, new_entry, bind(position))
            currentNode[self.cEntIndex] = mid + 1
        else:
            mid = mid + 1
            count = bind(mid)
            right_count = 0
            right_half_ents = max_ents - mid
            copy_till = count + right_half_ents * ent_size
            while count < copy_till:
                rightHalfNode[right_count] = currentNode[count]
                currentNode[count] = 0
                count = count + 1
                right_count = right_count + 1
            currentNode[self.cEntIndex] = mid
            self.pushEntryIn(rightHalfNode, new_entry, bind(position - mid))
            rightHalfNode[self.cEntIndex] = right_half_ents + 1

        gen_entry.isValid = True
        gen_entry.key = self.getKey(rightHalfNode, 0)
        gen_entry.entry = self.genIntermediateEntry(gen_entry.key, right_half.location_infile)
        self.writeNodeFromBuffer(right_half.buffer_number, right_half.location_infile)
        self.btree_buffer.returnBuffer(right_half.buffer_number)

    def BTreeRemoveEntry(self, key): # 'key' represents the entry, with the same key value, which is to be deleted.
        '''This function removes an entry with the matching key from the BTree.
           This returns a BTreeOpCode value depending on the outcome of the delete operation.'''

        op_result = BTreeOpCode.SUCCESS
        new_first_ent = EntryInfo()

        root_buffer_number = self.readNodeIntoBuffer(self.root_ref)
        rootNode = self.btree_buffer.BufferList[root_buffer_number]
        root_level = rootNode[self.cLevelIndex]

        if rootNode[self.cEntIndex] >= 1:
            op_result = self.recursiveRemove(self.root_ref, rootNode, root_buffer_number, key, root_level, new_first_ent)
            if rootNode[self.cEntIndex] == 1 and root_level != 0:
                old_root_ref = self.root_ref
                self.root_ref = self.getChildRef(rootNode[self.nbind(0) : self.nbind(0) + self.entrySize])
                self.delNodeAllocation(old_root_ref)
            else:
                self.writeNodeFromBuffer(root_buffer_number, self.root_ref)
        else:
            op_result = BTreeOpCode.NOTPRESENT

        self.btree_buffer.returnBuffer(root_buffer_number)
        self.btree_buffer.resetBuffer()
        return self.root_ref

    def recursiveRemove(self,
                        node_ref,       # The reference to the current node.
                        currentNode,    # Reference to the buffered bytearray containing the node represented in node_ref.
                        buffer_number,  # The buffer number of 'childNode' bytearray in self.btree_buffer.
                        key,            # 'key' represents the entry, with the same key value, which is to be deleted.
                        level,          # The BTree level of 'currentNode'.
                        new_first_ent): # This parameter is a return value: Generated intermediate entry (EntryInfo type) produced for parent node when the first entry of it's child node changes.
        '''This fuction is for recursively accessing the nodes in BTree for remove operation and to control the logic of BTree deletion.
           This returns a BTreeOpcode value depending on the result of BTree remove operation on its sub-tree.'''

        op_result = BTreeOpCode.SUCCESS

        # bind converts the logical index of the BTree Node Entry to Buffer Bytearray index.
        # It assumes the nbind funtion or lbind funtion depending if it is a non-leaf or not.
        bind = self.nbind
        isLeaf = False
        if level == 0:
            bind = self.lnbind
            isLeaf = True

        searchRes = self.findInNode(currentNode, key, bind)

        if isLeaf:
            if searchRes.outcome == True:
                self.removeNodeEntry(currentNode, bind(searchRes.position), self.leafEntrySize)

                # The following section of code tracks the change of first entry in the leaf node and sets 'new_first_ent' accordingly.
                if searchRes.position == 0:
                    new_first_ent.isValid = True
                    new_first_ent.key = self.getKey(currentNode, 0)
                    new_first_ent.entry = self.genIntermediateEntry(new_first_ent.key, node_ref)
                else:
                    new_first_ent.reset()
                # End of section with respect to previous comment.

                op_result = BTreeOpCode.SUCCESS
            else:
                op_result = BTreeOpCode.NOTPRESENT

        else:
            child_ref = 0
            child_pos = 0
            if searchRes.outcome == True:
                child_pos = searchRes.position
            else:
                if searchRes.position == 0:
                    child_pos = 0
                    # Assuming that the first entry is not consistent.
                    # Otherwise op_result == BTreeOpCode.NOTPRESENT
                else:
                    child_pos = searchRes.position - 1

            child_ref = self.getChildRef(currentNode[bind(child_pos): bind(child_pos) + self.entrySize])


            child_buffer_number = self.readNodeIntoBuffer(child_ref)
            childNode = self.btree_buffer.BufferList[child_buffer_number]
            child_level = childNode[self.cLevelIndex]
            op_result = self.recursiveRemove(child_ref, childNode, child_buffer_number, key, child_level, new_first_ent)

            # The following section of code checks for first entry change in child node and re-adjusts the entry containing to the child node accordingly.
            if new_first_ent.isValid == True:
                count = bind(searchRes.position)
                from_count = 0
                end_count = count + self.entrySize
                while count < end_count:
                    currentNode[count] = new_first_ent.entry[from_count]
                    from_count = from_count + 1
                    count = count + 1
            # End of section with respect to previous comment.

                # The following section of code tracks the change of first entry in the intermediate node and sets 'new_first_ent' accordingly.
                if searchRes.position == 0:
                    new_first_ent.key = self.getKey(currentNode, 0)
                    new_first_ent.entry = self.genIntermediateEntry(new_first_ent.key, node_ref)
                else:
                    new_first_ent.reset()
                # End of section with respect to previous comment.

            ent_size = self.entrySize
            max_ents = self.recMaxEntries
            if level == 1:
                ent_size = self.leafEntrySize
                max_ents = self.recLeafMaxEntries

            # This section of code restores the number of minimum entries in the 'childNode' of BTree
            if childNode[self.cEntIndex] <= (max_ents - 1)/2:
                restore_first_ent = EntryInfo()
                self.restoreNode(currentNode, child_ref, childNode, child_buffer_number, child_pos, level, restore_first_ent)
                # End of section with respect to previous comment.

                # Change of first entry in any child node is reflected in 'currentNode', which is the parent.
                if restore_first_ent.isValid == True:
                    resFirstEntRes = self.findInNode(currentNode, restore_first_ent.key, bind)
                    self.pushEntryIn(currentNode, restore_first_ent.entry, bind(resFirstEntRes.position))

            else:
                self.writeNodeFromBuffer(child_buffer_number, child_ref)

            self.btree_buffer.returnBuffer(child_buffer_number)

        return op_result

    def removeNodeEntry(self,
                        currentNode,    # Reference to current bytearray.
                        index,          # Index where the entry is to be inserted into.
                        entry_size):    # The size of the entry to be removed in bytes.
        '''This function removes an entry in the current node at the given index.'''

        self.shiftNodeEntsLeft(currentNode, index, entry_size)
        currentNode[self.cEntIndex] = currentNode[self.cEntIndex] - 1

    def restoreNode(self,
                    currentNode,            # Reference to the buffered bytearray containing the node represented in 'node_ref' in recursiveRemove.
                    child_ref,              # The reference to the current node's child node. readNodeIntoBuffer MUST be able to fetch the node with this reference from the PST file.
                    childNode,              # Reference to the buffered bytearray containing the node represented in child_ref.
                    child_buffer_number,    # The buffer number of 'childNode' bytearray in self.btree_buffer.
                    position,               # Position of the entry contained in 'currentNode' which contains the reference to child node ('child_ref').
                    current_level,          # The BTree level of 'currentNode'.
                    restore_first_ent):     # This parameter is a return value: Generated intermediate entry (EntryInfo type) produced for parent node when the first entry of it's child node changes.
        '''This fuction restores the minimum number of entries in child node.
            Returns a generated new entry for its parent node which contains the reference to the new node created after moveEntryBetweenNodes.'''

        ent_size = self.entrySize
        max_ents = self.recMaxEntries
        bind = self.nbind
        if current_level == 1:
            ent_size = self.leafEntrySize
            max_ents = self.recLeafMaxEntries
            bind = self.lnbind

        if position == currentNode[self.cEntIndex] - 1: # if the the child node is the last node in its parent node

            left_node_ref = self.getChildRef(currentNode[self.nbind(position - 1) : self.nbind(position - 1) + self.entrySize])
            left_buffer_number = self.readNodeIntoBuffer(left_node_ref)
            leftNode = self.btree_buffer.BufferList[left_buffer_number]

            if leftNode[self.cEntIndex] > (max_ents + 1)/2:
                # moveRight()
                self.moveEntryBetweenNodes(leftNode, bind(leftNode[self.cEntIndex] - 1), childNode, bind(0), ent_size)
                self.removeNodeEntry(currentNode, self.nbind(position), self.entrySize)
                restore_first_ent.isValid = True
                restore_first_ent.key = self.getKey(childNode, 0)
                restore_first_ent.entry = self.genIntermediateEntry(restore_first_ent.key, child_ref)

                self.writeNodeFromBuffer(child_buffer_number, child_ref)

            else:
                self.combineSiblings(currentNode, position, leftNode, childNode, bind)
                self.delNodeAllocation(child_ref)
                restore_first_ent.reset()

            self.writeNodeFromBuffer(left_buffer_number, left_node_ref)
            self.btree_buffer.returnBuffer(left_buffer_number)

        elif position == 0: # if the child node is the first node in its parent node

            right_node_ref = self.getChildRef(currentNode[self.nbind(position + 1) : self.nbind(position + 1) + self.entrySize])
            right_buffer_number = self.readNodeIntoBuffer(right_node_ref)
            rightNode = self.btree_buffer.BufferList[right_buffer_number]

            if rightNode[self.cEntIndex] > (max_ents + 1)/2:
                # moveLeft()
                self.moveEntryBetweenNodes(rightNode, bind(0), childNode, bind(childNode[self.cEntIndex]), ent_size)
                self.removeNodeEntry(currentNode, self.nbind(position + 1), self.entrySize)
                restore_first_ent.isValid = True
                restore_first_ent.key = self.getKey(rightNode, 0)
                restore_first_ent.entry = self.genIntermediateEntry(restore_first_ent.key, right_node_ref)

                self.writeNodeFromBuffer(right_buffer_number, right_node_ref)

            else:
                self.combineSiblings(currentNode, position + 1, childNode, rightNode, bind)
                self.delNodeAllocation(right_node_ref)
                restore_first_ent.reset()

            self.writeNodeFromBuffer(child_buffer_number, child_ref)
            self.btree_buffer.returnBuffer(right_buffer_number)

        else:

            left_node_ref = self.getChildRef(currentNode[self.nbind(position - 1) : self.nbind(position - 1) + self.entrySize])
            left_buffer_number = self.readNodeIntoBuffer(left_node_ref)
            leftNode = self.btree_buffer.BufferList[left_buffer_number]

            if leftNode[self.cEntIndex] > (max_ents + 1)/2:
                # moveRight()
                self.moveEntryBetweenNodes(leftNode, bind(leftNode[self.cEntIndex] - 1), childNode, bind(0), ent_size)
                self.removeNodeEntry(currentNode, self.nbind(position), self.entrySize)
                restore_first_ent.isValid = True
                restore_first_ent.key = self.getKey(childNode, 0)
                restore_first_ent.entry = self.genIntermediateEntry(restore_first_ent.key, child_ref)

                self.writeNodeFromBuffer(left_buffer_number, left_node_ref)
                self.btree_buffer.returnBuffer(left_buffer_number)

            else:

                self.btree_buffer.returnBuffer(left_buffer_number)

                right_node_ref = self.getChildRef(currentNode[self.nbind(position + 1) : self.nbind(position + 1) + self.entrySize])
                right_buffer_number = self.readNodeIntoBuffer(right_node_ref)
                rightNode = self.btree_buffer.BufferList[right_buffer_number]

                if rightNode[self.cEntIndex] > (max_ents + 1)/2:
                    # moveLeft()
                    self.moveEntryInSiblings(rightNode, bind(0), childNode, bind(childNode[self.cEntIndex]), ent_size)
                    self.removeNodeEntry(currentNode, self.nbind(position + 1), self.entrySize)
                    restore_first_ent.isValid = True
                    restore_first_ent.key = self.getKey(rightNode, 0)
                    restore_first_ent.entry = self.genIntermediateEntry(restore_first_ent.key, right_node_ref)

                    self.writeNodeFromBuffer(right_buffer_number, right_node_ref)

                else:
                    self.combineSiblings(currentNode, position + 1, childNode, rightNode, bind)
                    self.delNodeAllocation(right_node_ref)
                    restore_first_ent.reset()

                self.btree_buffer.returnBuffer(right_buffer_number)

            self.writeNodeFromBuffer(child_buffer_number, child_ref)


    def moveEntryBetweenNodes(self,
                                 fromNode,      # Reference to the buffered bytearray containing the node from where the entry is to be moved.
                                 from_index,    # Index where the entry is to be moved from.
                                 toNode,        # Reference to the buffered bytearray containing the node to where the entry is to be moved.
                                 to_index,      # Index where the entry is to be moved into.
                                 ent_size):     # Size of entry to be moved in bytes.
        '''This function moves an entry between nodes of similar entry type and size.'''

        entry = fromNode[from_index : from_index + ent_size]
        self.pushEntryIn(toNode, entry, to_index)
        self.removeNodeEntry(fromNode, from_index, ent_size)

    def combineSiblings(self,
                        currentNode,        # Reference to the buffered bytearray containing the node represented in 'node_ref' in recursiveRemove.
                        right_child_pos,    # Position of the entry contained in 'currentNode' which contains the reference to right child node used here.
                        leftChildNode,      # Reference to the buffered bytearray containing the left child node to be combined.
                        rightChildNode,     # Reference to the buffered bytearray containing the right child node to be combined.
                        bind):              # The function lnbind or nbind is passed as a parameter if the given children nodes' level is 0 or more respecively.
        '''This function combines two neighbouring sibling nodes and deletes the reference of the right sibling in the parent node.'''

        count = bind(leftChildNode[self.cEntIndex])
        from_count = 0
        end_pos = count + bind(rightChildNode[self.cEntIndex])
        while from_count < end_pos:
            leftChildNode[count] = rightChildNode[from_count]
            count = count + 1
            from_count = from_count + 1
        leftChildNode[self.cEntIndex] = leftChildNode[self.cEntIndex] + rightChildNode[self.cEntIndex]
        self.removeNodeEntry(currentNode, self.nbind(right_child_pos), self.entrySize)
