import osmnx as ox
import networkx as nx

G = ox.graph_from_place('Fort Collins, Colorado', network_type='drive')
G = ox.add_edge_speeds(G)
ox.plot_graph(G)

nodes, edges = ox.graph_to_gdfs(G)
edges['weight'] = edges['length']

UG = ox.graph_from_gdfs(nodes, edges)

origin = (40.528275, -105.056626)
target = (40.632868, -105.052467)
print(str(ox.get_nearest_node(UG, origin)) + " " + str(ox.get_nearest_node(UG, target)))

filepath = "./data/graph.graphgml"
ox.save_graphml(UG, filepath)

bb = nx.edges(UG, 'length')
nx.set_edge_attributes(UG, bb, "betweenness")

lister = list(UG.nodes(data=True))

textfile = open("test.nodelist", "w")
for element in lister:
    textfile.write(str(element[0]) + " " + str(element[1]["y"]) + " " + str(element[1]["x"]) + "\n")
textfile.close()

nx.write_adjlist(UG, "test.adjlist")
nx.write_edgelist(UG, "test.edgelist")
nx.write_weighted_edgelist(UG, "test.weighted.edgelist")

