# encoding: utf-8
import codecs
import sys

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

def char_fix(line):
    return line.replace('ý', 'ı').replace('ý', 'ı').replace('þ', 'ş').replace('ð', 'ğ')

def main(in_file="zargan/data/stats20080113-02.txt", out_file="zargan/data/filtered.txt"):
    f = codecs.open(in_file, encoding="iso-8859-1")
    o = open(out_file, "w")

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

        o.write(char_fix(line.encode("utf8")))
    o.close()


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2])
    except IndexError:
        main()