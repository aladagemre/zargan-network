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

import networkx as nx
import matplotlib.pyplot as plt
from utils import generate_histogram

logger = logging.getLogger("ZarganApp")
logging.basicConfig(level=logging.DEBUG)


class Index(object):
    def __init__(self):
        self.id_to_value = {}
        self.value_to_id = {}
        self.last_id = 0

    def get_index_of(self, value):
        result = self.value_to_id.get(value)
        if result:
            return result
        else:
            self.last_id += 1
            self.id_to_value[self.last_id] = value
            self.value_to_id[value] = self.last_id
            return self.last_id

    def get_value_of(self, key):
        result = self.id_to_value.get(key)
        if not result:
            raise KeyError, "This key ({0}) does not exist in the index.".format(key)
        return result


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
    # Start with the initial item.
    current_cluster = [data[0]]
    i = 0
    N = len(data)
    #print data
    # for all items except for the last one,
    while i < N - 1:
        current_item = data[i]
        next_item = data[i + 1]

        # if the time between two consecutive searches are less than the window size,
        if next_item[0] - current_item[0] <= window_size:
            # add the item to the current cluster. They should be related.
            current_cluster.append(data[i + 1])
        else:
            # if not, finalize the cluster and yield it.
            a = current_cluster
            current_cluster = [data[i + 1]]
            yield a
            # TODO: check if this works properly.

        i += 1

    # yield the last current cluster
    yield list(set(current_cluster))


class ZarganApp(object):
    def __init__(self, filename="zargan/data/filtered.txt", item_count=2400000, window_size=300.0, prune_threshold=20, generate_graph=None):
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
        self.graph_choice = None
        if generate_graph is True:
            self.graph_choice = "y"
        elif generate_graph is False:
            self.graph_choice = "n"

    def run(self):
        """Main method of this class."""
        self.read_input()
        self.generate_hashmap()
        self.check_fraud()
        self.write_hashmap()
        self.generate_histogram()
        self.prune_histogram()
        self.write_text()
        if self.graph_choice is None:
            self.graph_choice = raw_input("Do you want to generate graph? [y/n]")

        if self.graph_choice == "y":
            logger.info("Generating graph may take a while...")
            self.generate_graph()
            self.write_graph()
        else:
            logger.info("No graphs will be generated.")

        logger.info("Computation has finished... See the outputs.")

    def read_input(self):
        """Reads from the input file and creates
        a list of Record objects.
        """
        logger.info("Reading from the input file starts...")

        # Create a list for putting the graphs in.
        self.records = records = []
        # Open the input file with iso-8859-1 codec.
        f = codecs.open(self.filename, encoding="utf-8")
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
            i += 1
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

    def check_fraud(self, top=3):
        ips = sorted(self.hash_map.keys())
        #tuples = []
        remove_candidate = []
        for ip in ips:
            count = len(self.hash_map[ip])
            if count > 1000:
                remove_candidate.append(ip)
                #tuples.append((count, ip))
                #print "{1} - {0}".format(ip, count)
                #print self.hash_map[ip]

        #tuples.sort(reverse=True)
        #top_x = tuples[:top]
        for ip in remove_candidate:
            del self.hash_map[ip]

    def write_hashmap(self):
        ho = open("hashmap.txt","w")
        ips = sorted(self.hash_map.keys())
        for ip in ips:
            ho.write("{ip}\t{records}\n".format(ip=ip, records=";".join(map(lambda x: x.arama.encode("utf-8"), self.hash_map[ip]))))
        ho.close()

    def generate_histogram(self):
        """Generates a histogram according to co-session.
        For each IP Address:
            Detect searches within the same session (by the same person)
            Record co-occurrence frequences.

        Prune the histogram according to occurence frequencies so that noise can be eliminated.
        """
        logger.info("Histogram construction starts...")
        del self.records
        self.occurrence_histogram = hist = collections.defaultdict(int)

        # Now we need to detect the sessions.
        # for each ip, search_list pair,
        ips = 0
        self.index = index = Index()
        for ip, searches in self.hash_map.iteritems():
            ips += 1
            # get the first one.
            min_date = searches[0].get_date_in_secs()
            # get all of them and subtract the first one, making the first record 0 always. (for performance)
            dates = [(search.get_date_in_secs() - min_date, search) for search in searches]
            if len(dates) > 1:
                clusters = cluster(data=dates, window_size=self.window_size)

                for cluster_ in clusters:
                    cluster_size = len(cluster_)
                    if cluster_size > 100:
                        print("Suspicious IP: {ip}, one cluster: {c}".format(ip=ip, c=cluster_size))
                        continue
                    combinations = itertools.combinations(cluster_, 2)
                    for combination in combinations:
                        u = combination[0][1].arama
                        v = combination[1][1].arama
                        if u == v:
                            continue
                        u_id = index.get_index_of(u)
                        v_id = index.get_index_of(v)
                        if (u_id, v_id) in hist:
                            hist[(u_id, v_id)] += 1
                        else:
                            hist[(v_id, u_id)] += 1
            if ips % 50000 == 0:
                logger.debug("{0} IPs - {1} MB".format(ips, sys.getsizeof(hist)/1024.0/1024.0))

    def prune_histogram(self):
        logger.info("Pruning the edges with weight < {0}".format(self.prune_threshold))
        hist = self.occurrence_histogram
        delete_list = collections.deque()
        for (key, value) in hist.iteritems():
            if value < self.prune_threshold:
                delete_list.append(key)
        for item in delete_list:
            del hist[item]

        logger.info("Pruning finished...")

    def write_text(self):
        logger.info("Writing the edges to a text file...")
        hist = self.occurrence_histogram
        #items = ["{0} - {1} : {2}".format(edge[0].encode("latin5"), edge[1].encode("latin5"), hist[edge]) for edge in
                 #sorted(hist.keys())]
        gt = lambda id: self.index.get_value_of(id).encode("utf-8")


        records = [(hist[edge], gt(edge[0]), gt(edge[1])) for edge in hist.keys()]
        records.sort(key=lambda x: (-x[0], x[1], x[2]))

        items = ["{0}; {1}; {2}".format(*record) for record in records]
        filename = "{0}-output.csv".format(".".join(self.filename.split(".")[:-1]))
        o = open(filename, "w")
        o.write("\n".join(items))
        o.close()

        logger.info("Writing finished: {0}".format(filename))

    def generate_graph(self):
        # Create the nodes from the record objects.
        self.graph = graph = OccurrenceGraph()
        gt = lambda id: self.index.get_value_of(id)
        for key, value in self.occurrence_histogram.iteritems():
            graph.add_edge(gt(key[0]), gt(key[1]), weight=value)
        logger.info("Generation of the graph has finished.")

    def write_graph(self):
        """Displays the resulting graph on a window and writes it to a file in GraphML format.
        Output filename will be {input}.graphml
        """
        """if self.graph.number_of_nodes() < 100:
            self.graph.draw()"""
        graph_filename = "{0}.graphml".format(self.filename)
        self.graph.write_graphml(graph_filename)
        logger.info("Wrote the graph: {0}".format(graph_filename))


if __name__ == "__main__":
    try:
        # Get the command line parameters.
        filename = sys.argv[1]
        item_count = float(sys.argv[2])
        window_size = float(sys.argv[3])
        prune_threshold = int(sys.argv[4])
        # Run the ZarganApp with the parameters.
        params = (filename, item_count, window_size, prune_threshold)
        app = ZarganApp(*params)
        app.run()

    except (IndexError, IOError) as e:
        logger.error(e)
        # If no parameters specified, run with the defaults.
        logger.warning("Running with default settings...")
        app = ZarganApp()
        app.run()
