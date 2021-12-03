import osmnx as ox
import networkx as nx

G = ox.graph_from_place('Fort Collins, Colorado', network_type='drive')

fp = open('2.results', 'r')
listy = []
line = fp.readline()
cnt = 1
while line:
    listy = line.split()
    line = fp.readline()
    cnt += 1
fp.close()

listy = listy[:0] + listy[2:]

list2 = []
for val in listy:
    list2.append(val + "," + (str(G.nodes[int(val)]['y']) + "," + str(G.nodes[int(val)]['x'])))

print(list2)
textfile = open("coords.csv", "w")
textfile.write("name,lat,lon" + "\n")
for element in list2:
    textfile.write(element + "\n")
textfile.close()