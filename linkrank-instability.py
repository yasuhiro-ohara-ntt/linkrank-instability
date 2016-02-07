#!/usr/bin/env python

collector_filter = 'route-views.linx'
start = 1454284800
# end = start + 3600 * 2 * 12 * 1
end = start + 3600 * 2 * 1 * 1

range = str(start) + '-' + str(end)
filename = 'link-rank-instability-' + collector_filter + '-' + range + '.txt'

from _pybgpstream import BGPStream, BGPRecord, BGPElem
from collections import defaultdict
from time import gmtime, strftime, localtime
from itertools import groupby

f = open(filename, 'w')

buf = filename + ", ribs, range: " + str(start) + " - " + str(end)
print buf
print >> f, buf

list_linkset = []

dictup = {}
dictdown = {}
nevents = {}
history = {}

current = start
current_time_short = strftime("%Y/%m/%d-%H:%M:%S", gmtime(current))
current_time_long = strftime("%a, %d %b %Y %H:%M:%S", gmtime(current))

while current <= end:
    stream = BGPStream()
    rec = BGPRecord()

    stream.add_filter('collector', collector_filter)
    stream.add_filter('record-type','ribs')
    stream.add_interval_filter(current - 10 * 60, current + 10 * 60)
    stream.add_filter('peer-asn','3356')
    stream.add_filter('peer-asn','174')
    stream.add_filter('peer-asn','3257')
    stream.add_filter('peer-asn','1299')
    stream.add_filter('peer-asn','2914')
    stream.add_filter('peer-asn','6453')
    stream.add_filter('peer-asn','6762')
    stream.add_filter('peer-asn','6939')
    stream.add_filter('peer-asn','2828')
    stream.add_filter('peer-asn','3549')

    stream.start()

    linkset = set()
    linkset_indirect = set()

    # Get next record
    count = 0
    while(stream.get_next_record(rec)):
        elem = rec.get_next_elem()
        while(elem):
            # Get the prefix
            pfx = elem.fields['prefix']
            # Get the list of ASes in the AS path
            # ases = elem.fields['as-path'].split(" ")
            ases = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            # print ases
            i = 0
            while i + 1 < len(ases):
                # print ases[i], " ", ases[i + 1]
                linkset.add((ases[i], ases[i + 1]))
                if (ases[i] < ases[i + 1]):
                    linkset_indirect.add((ases[i], ases[i + 1]))
                else:
                    linkset_indirect.add((ases[i + 1], ases[i]))
                linkset.add((ases[i], ases[i + 1]))
                i += 1
            elem = rec.get_next_elem()
            count += 1
            print '\r',
            print count,

    linkset_time = strftime("%Y-%m-%d-%H%M%S", gmtime(current))
    linkset_file = 'link-rank-linkset-' + collector_filter + '-' + linkset_time + '.txt'
    linkset_fp = open(linkset_file, 'w')
    for link in linkset_indirect:
        print >> linkset_fp, link
    linkset_fp.close()

    list_linkset.append(linkset)

    buf = "linkset: " + str(len(linkset)) + " time: " + str(current) + \
          " " + current_time_long
    print buf
    print >> f, buf

    if (len(list_linkset) > 1):
        downs = list_linkset[-2] - list_linkset[-1]
        ups = list_linkset[-1] - list_linkset[-2]
        print "downs:", len(downs), " ups:", len(ups)
        print >> f, "downs:", len(downs), " ups:", len(ups)
        # print "down list:"
        # for link in downs:
        #     print link

        for link in downs:
            if (link in dictdown):
                dictdown[link] += 1
            else:
                dictdown[link] = 1
            if (link in nevents):
                nevents[link] += 1
            else:
                nevents[link] = 1
            if (link in history):
                history[link] += " down-" + current_time_short
            else:
                history[link] = "down-" + current_time_short
        for link in ups:
            if (link in dictup):
                dictup[link] += 1
            else:
                dictup[link] = 1
            if (link in nevents):
                nevents[link] += 1
            else:
                nevents[link] = 1
            if (link in history):
                history[link] += " up-" + current_time_short
            else:
                history[link] = "up-" + current_time_short

        dictlinkranks = {}
        for k, v in sorted(nevents.items(), key=lambda x:x[1]):
            #print k, " ", v
            if (v in dictlinkranks):
                dictlinkranks[v] += 1
            else:
                dictlinkranks[v] = 1
        for k, v in sorted(dictlinkranks.items()):
            print "[", k, "]:", v, " ",
            print >> f, "[", k, "]:", v, " ",
        print ""
        print >> f, ""

    current += 3600 * 2;
    current_time_short = strftime("%Y/%m/%d-%H:%M:%S", gmtime(current))
    current_time_long = strftime("%a, %d %b %Y %H:%M:%S", gmtime(current))

print "writing to '" + filename + "' ... ",

# print >> f, "down list: ", len(dictdown)
# for k, v in sorted(dictdown.items(), key=lambda x:x[1]):
#     if (v > 1):
#         print >> f, k, v
# print >> f, "up list: ", len(dictup)
# for k, v in sorted(dictup.items(), key=lambda x:x[1]):
#     if (v > 1):
#         print >> f, k, v
print >> f, "#events: ", len(nevents)
for k, v in sorted(nevents.items(), key=lambda x:x[1]):
    print >> f, k, " ", v, " ", history[k]

f.close()
print "done."

csv_start = strftime("%Y-%m-%d-%H%M%S", gmtime(start))
csv_end = strftime("%Y-%m-%d-%H%M%S", gmtime(end))
csv_file = 'link-instability-rank-' + collector_filter + '-' + csv_start + '-' + csv_end + '.csv'
print "writing to '" + csv_file + "' ... ",
csv_fp = open(csv_file, 'w')
for link in linkset_indirect:
    print >> csv_fp, link[0], ',', link[1], ',',
    revlink = (link[1], link[0])
    count = 0
    if (link in nevents):
        count += nevents[link]
    if (revlink in nevents):
        count += nevents[revlink]
    print >> csv_fp, count
csv_fp.close()
print "done."

