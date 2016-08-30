from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node:
    def __init__(self):
        self.characterID = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'
    #Format of line is ID|EDGES|DISTANCE|COLOR 
    def fromLine(self, line):
        fields = line.split('|')
        if (len(fields) == 4):
            self.characterID = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]
    def getLine(self):
        connections = ','.join(self.connections)
        return '|'.join((self.characterID, connections, str(self.distance), self.color))


class MRBFSIteration(MRJob):
    #we can use the output as the input without additional processing
    #Otherwise, the default format for output is json format
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def configure_options(self):
        super(MRBFSIteration, self).configure_options();
        #which character we want to measure degree of seperation to
        #This target will be passed along each node
        self.add_passthrough_option('--target', help = "ID of character we are searching for")
    
    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)
        #if this node needs to be expanded. Set the color as gray
        #is like pushing the node into the queue
        #Note: the starting point is Hulk (ID: 2548). So, the color
        #of node 2548 is gray, and the distance is 0
        if (node.color == 'GRAY'):
            for connection in node.connections:
                vnode = Node()
                vnode.characterID = connection
                vnode.distance = int(node.distance) + 1
                vnode.color = 'GRAY'
                if (self.options.target == connection):
                    counterName = ("Target ID " + connection +
                                  " was hit with distance " + str(vnode.distance))
                    self.increment_counter('Degree of Seperation',
                                          counterName, 1)
                yield connection, vnode.getLine()
            #we have processed this node, so color it black (poped
            #out of the queue)
            node.color = 'BLACK'
        #emit input node
        yield node.characterID, node.getLine()
    
    def reducer(self, key, values):
        edges = []
        distance = 9999
        color = 'WHITE'
        
        for value in values:
            node = Node()
            node.fromLine(value)
            
            if (len(node.connections) > 0):
                edges.extend(node.connections)
                
            #make sure degree of sepeartion is shortest
            if (node.distance < distance):
                distance = node.distance
            
            #if this node has been explored
            if (node.color == "BLACK"):
                color = 'BLACK'
            
            #if this node has not been explored at this moment
            if (node.color == 'GRAY' and color == 'WHITE'):
                color = 'GRAY'
                
        
        node = Node()
        node.characterID = key
        node.distance = distance
        node.color = color
        #There's a bug in mrjob for Windows where sorting fails
        #with too much data. As a workaround, we're limiting the
        #number of edges to 500 here. You'd remove the [:500] if you
        #were running this for real on a Linux cluster.
        
        node.connections = edges
        yield key, node.getLine()
        
if __name__ == '__main__':
    MRBFSIteration.run()