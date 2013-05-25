# encoding: utf-8
import codecs
import sys

def char_fix(line):
    try:
        line = line.encode("utf-8")
    except:
        pass
    return line.replace('ý', 'ı').replace('ý', 'ı').replace('þ', 'ş').replace('ð', 'ğ').replace("\r", "").replace('Ý', 'İ').strip()

en_content = char_fix(codecs.open("zargan/data/en_dict.txt", encoding="ascii").read())
tr_content = char_fix(codecs.open("zargan/data/tr_dict.txt", encoding="utf-8").read())

en_dictionary = set(en_content.split("\n"))
tr_dictionary = set(tr_content.split("\n"))

blocked_ips = open("zargan/data/blocked_ips.txt").read().split("\n")

def filter_campaign(cols):
    """
    Returns True if the search is a campaign search and not valuable.
    """
    words = [u"unfamşiar", u"kanıtlamk", u"toplam değişken", u"çalışmak zorunda kalmak", u"conservative", \
             u"conservation", u"conservativative", u"irregular", u"unfair", u"kani"]

    if cols[0].lower() in words:
        return True

def filter_dictionary(cols):
    tokens = char_fix(cols[0]).lower().split()
    for token in tokens:
        if (token not in en_dictionary) and (token not in tr_dictionary):
            #print token
            return True


def filter_corporation(cols):
    """
    Returns True if the search has corporate id. We won't use corporate search logs.
    """
    return cols[5]

def filter_ip(cols):

    ip = ".".join(map(str, map(int, cols[1].split("."))))
    if ip in blocked_ips:
        return True



def main(in_file="zargan/data/stats20080113-02.txt", out_file="zargan/data/filtered.txt"):
    f = codecs.open(in_file, encoding="iso-8859-1")
    o = open(out_file, "w")
    eliminated = open("zargan/data/eliminated.txt", "w")
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
        if filter_dictionary(cols):
            eliminated.write("%s\n" % char_fix(cols[0]).lower())
            continue

        o.write("%s\n" % char_fix(line.encode("utf8")))
    o.close()
    eliminated.close()


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2])
    except IndexError:
        main()