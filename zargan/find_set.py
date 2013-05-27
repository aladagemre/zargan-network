complete = open("data/filtered-output-complete.csv").read().split("\n")
simple = open("data/filtered-output.csv").read().split("\n")
complete_tuple = [ sorted([line.split("; ")[1:]]) for line in complete]
c = ["-".join(items[0]) for items in complete_tuple]
simple_tuple = [ sorted([line.split("; ")[1:]]) for line in simple]
s = set(["-".join(sorted(items[0])) for items in simple_tuple])
c = set(c)
print "Intersection:", len(c.intersection(s))
print "C-S", len(c - s)
print "S-C", len(s-c)
o1 = open("data/compare-chain-intersection.txt", "w")
o1.write("\n".join(c.intersection(s)))
o1.close()
o2 = open("data/compare-chain-c-s.txt", "w")
o2.write("\n".join(c-s))
o2.close()
o3 = open("data/compare-chain-s-c.txt", "w")
o3.write("\n".join(s-c))
o3.close()

