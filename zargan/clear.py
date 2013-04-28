import codecs

f = codecs.open("data/stats20110912-01.txt", encoding="latin5")
o = open("data/filtered.txt", "w")

f.readline()
for line in f:
    if line.count("|") != 11:
        continue

    o.write(line.encode("utf8"))
o.close()

