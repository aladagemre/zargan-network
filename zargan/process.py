"""Create a multi-graph G where nodes are stemmed query terms and edges will represent relatedness.
Construct a HashMap<IPAddress, List<(Query,Time)>> h to aggregate search terms according to their IP Address.
for each IPAddress in HashMap h:
    L = Get List<(Query, Time)> for IPAddress
    List<Group> LG = Find longest query groups among which there are t seconds where a < t < b  (a=5s and b=300s intuitively)
    for each query group R in LG:
        # we assume these are performed by the same person.
        add edges between all of the members in the group R. There may be multiple edges.

# We now have a relationship graph.
Apply a outlier filtering on the edge frequencies such that edges between specific two nodes that are repeated so much and so few will be eliminated.

Resulting graph should be representing a semantically related word network.

Sample Input:
Arama|IP|Tarih|UyeID|VisitorID|KurumID|CountTurkish|CountEnglish|Dil|Eklendi|TurkceYazim|SayfaNo
tide |078.171.172.145|2011-05-07 05:32:47.780000000||67983982||0|46|2|0|0|

"""
import sys
import codecs
import collections
import datetime
import time
import itertools
import logging
from memory_profiler import profile

import networkx as nx
import matplotlib.pyplot as plt

logger = logging.getLogger("ZarganApp")
logging.basicConfig(level=logging.DEBUG)


class Record(object):
    def __init__(self, fields):
        self.arama = fields[0].lower()
        self.ip = fields[1]
        self.tarih = fields[2].split(".")[0]
        #self.uye_id = fields[3]
        #self.visitor_id = fields[4]
        #self.kurum_id = fields[5]
        #self.count_turkish = fields[6]
        #self.count_english = fields[7]
        #self.dil = fields[8]
        #self.eklendi = fields[9]
        #self.turkce_yazim = fields[10]
        #self.sayfa_no = fields[11]
        self.secs = None

    def __str__(self):
        return ("Tarih: %(tarih)-29s\t IP: %(ip)s\t Kurum: %(kurum_id)s\t arama: %(arama)s\t" % self.__dict__).encode(
            "utf8")
        #return "|".join(["%s: %s" % (key, value) for key,value in self.__dict__.iteritems()])

    def __repr__(self):
        return self.arama.encode("utf8")
        #return ("Tarih: %(tarih)-29s\t IP: %(ip)s\t Kurum: %(kurum_id)s\t arama: %(arama)s\t" % self.__dict__).encode("utf8")

    def get_date_in_secs(self):
        """
        Returns the date of the record in seconds.
        """
        if self.secs:
            return self.secs
        self.secs = time.mktime(datetime.datetime.strptime(self.tarih, "%Y-%m-%d %H:%M:%S").timetuple())
        return self.secs


class OccurrenceGraph(nx.Graph):
    def __init__(self):
        nx.Graph.__init__(self)

    def draw(self):
        logger.info("Calculating the layout of the graph...")
        # draw graph
        pos1 = nx.spring_layout(self)
        logger.info("Drawing the graph...")
        nx.draw_networkx(self, pos1)

        # show graph
        logger.info("Showing the graph...")
        plt.show()

    def prune(self, threshold=2):
        logger.info("Pruning the graph...")
        logger.info("Nodes: {0}; Edges: {1}".format(self.number_of_nodes(), self.number_of_edges()))
        for edge in self.edges(data=True):
            #print edge
            if edge[2]['weight'] < threshold:
                self.remove_edge(edge[0], edge[1])

        for node in self.nodes():
            if not self.degree(node):
                self.remove_node(node)
        logger.info("Nodes: {0}; Edges: {1}".format(self.number_of_nodes(), self.number_of_edges()))

    def write_graphml(self, filename):
        nx.write_graphml(self, filename)


def cluster(data, window_size=300.0):
    """Clusters the data points according to window size
    @param data: [ (second, Record), (second, Record), ... ]
    @param window_size: maximum seperation between two points in a cluster.
    TODO: Test this
    """
    clusters = []
    # Start with the initial item.
    current_cluster = [data[0]]
    i = 0
    N = len(data)

    # for all items except for the last one,
    while i < N - 1:
        current_item = data[i]
        next_item = data[i + 1]

        # if the time between two consecutive searches are less than the window size,
        if next_item[0] - current_item[0] <= window_size:
            # add the item to the current cluster. They should be related.
            current_cluster.append(data[i + 1])
        else:
            # if not, finalize the cluster and create a new one.
            #clusters.append(current_cluster)
            yield current_cluster

            current_cluster = [data[i + 1]]
        i += 1

    # Add the last cluster to the clusters list.
    #clusters.append(current_cluster)
    yield current_cluster

    #return clusters


class ZarganApp(object):
    def __init__(self, filename="zargan/data/filtered2.txt", item_count=1000, window_size=300.0, prune_threshold=2):
        """Main Application that takes the search data in and finds the co-searched terms.
        Adds edges between those terms and generates a graph. Applies pruning and displays the results.
        Resulting graph is expected to reflect meaningful relationships between nodes (words) that are
        connected to each other.

        @param filename: input filename
        @param item_count: take first X lines as input
        @param window_size: Maximum time seperation in seconds between two searches.
        @param prune_threshold: Remove all edges below weight X.
        """

        self.filename = filename
        self.window_size = window_size
        self.prune_threshold = prune_threshold
        self.item_count = item_count

    def run(self):
        """Main method of this class."""
        self.read_input()
        self.generate_hashmap()
        self.generate_graph()
        self.produce_output()

    def read_input(self):
        """Reads from the input file and creates
        a list of Record objects.
        """
        logger.info("Reading from the input file starts...")

        # Create a list for putting the graphs in.
        self.records = records = []
        # Open the input file with latin5 codec.
        f = codecs.open(self.filename, encoding="latin5")
        # Read the first line since it's header.
        f.readline()

        # For each lines, split the text according to pipes
        # Create new Record object.
        i = 0
        for line in f:
            fields = line.strip().split("|")
            if not len(fields) == 12:
                continue
            records.append(Record(fields))
            i+=1
            if i == self.item_count:
                break

    def generate_hashmap(self):
        """From the Record list, generates a hashmap in form:
        IPAddress - [record1, record2, ...]
        """
        logger.info("Hash Map generation starts...")
        # Create a hash map with default value [].
        self.hash_map = hash_map = collections.defaultdict(list)

        # Sort the records according to their dates.
        self.records.sort(key=lambda x: x.tarih)

        # For each record, assign the record to its' IP Address block.
        for record in self.records:
            hash_map[record.ip].append(record)


    def generate_graph(self):
        """Generates a NetworkX Graph according to co-session.
        For each IP Address:
            Detect searches within the same session (by the same person)
            Add an edge between these two search terms in the graph; increase weight if the edge already exists.

        Prune the graph according to edge weights so that noise can be eliminated.
        """
        logger.info("Graph construction starts...")
        # Create the nodes from the record objects.
        self.graph = graph = OccurrenceGraph()
        #graph.add_nodes_from([record.arama for record in self.records])
        del self.records

        # Now we need to detect the sessions.
        # for each ip, search_list pair,
        for ip, searches in self.hash_map.iteritems():
            # get the first one.
            min_date = searches[0].get_date_in_secs()
            # get all of them and subtract the first one, making the first record 0 always. (for performance)
            dates = [(search.get_date_in_secs() - min_date, search) for search in searches]
            if len(dates) > 1:
                clusters = cluster(data=dates, window_size=self.window_size)
                for cluster_ in clusters:
                    combinations = itertools.combinations(cluster_, 2)
                    for combination in combinations:
                        u = combination[0][1].arama
                        v = combination[1][1].arama
                        if graph.has_edge(u, v):
                            graph[u][v]['weight'] += 1
                        else:
                            graph.add_edge(u, v, weight=1)

        self.graph.prune(threshold=self.prune_threshold)

    def produce_output(self):
        """Displays the resulting graph on a window and writes it to a file in GraphML format.
        Output filename will be {input}.graphml
        """
        self.graph.draw()
        self.graph.write_graphml("{0}.graphml".format(self.filename))


if __name__ == "__main__":
    try:
        # Get the command line parameters.
        filename = sys.argv[1]
        item_count = float(sys.argv[2])
        window_size = float(sys.argv[3])
        prune_threshold = int(sys.argv[4])
        # Run the ZarganApp with the parameters.
        app = ZarganApp(filename, item_count, window_size, prune_threshold)
        app.run()

    except (IndexError, IOError) as e:
        # If no parameters specified, run with the defaults.
        app = ZarganApp()
        app.run()









