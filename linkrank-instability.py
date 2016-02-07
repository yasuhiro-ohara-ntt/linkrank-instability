#!/usr/bin/env python

collector_filter = 'route-views.linx'
start = 1438415400 + 10 * 60
end = start + 3600 * 2 * 12

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

    stream.start()

    linkset = set()

    # Get next record
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
                i += 1
            elem = rec.get_next_elem()

    buf = "linkset: " + str(len(linkset)) + " time: " + str(current) + \
          " " + current_time_long

    print buf
    print >> f, buf
    list_linkset.append(linkset)

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

    current += 3600 * 2;
    current_time_short = strftime("%Y/%m/%d-%H:%M:%S", gmtime(current))
    current_time_long = strftime("%a, %d %b %Y %H:%M:%S", gmtime(current))

print "writing to '" + filename + "' ... "

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


