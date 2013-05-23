# encoding: utf-8
import codecs

def filter_campaign(cols):
    """
    Returns True if the search is a campaign search and not valuable.
    """
    words = [u"unfamşiar", u"kanıtlamk", u"toplam değişken", u"çalışmak zorunda kalmak", u"conservative", \
             u"conservation", u"conservativative", u"irregular", u"unfair", u"kani"]

    if cols[0].lower() in words:
        return True


def filter_corporation(cols):
    """
    Returns True if the search has corporate id. We won't use corporate search logs.
    """
    return cols[5]

def filter_ip(cols):
    ips = []
    if cols[1] in ips:
        return True

def main():
    f = codecs.open("zargan/data/stats20110912-01.txt", encoding="latin5")
    o = open("zargan/data/filtered.txt", "w")

    o.write(f.readline())
    for line in f:
        cols = line.split("|")

        if len(cols) != 12:
            continue
        if filter_ip(cols):
            continue
        if filter_corporation(cols):
            continue
        if filter_campaign(cols):
            continue

        o.write(line.encode("utf8"))
    o.close()


if __name__ == "__main__":
    main()