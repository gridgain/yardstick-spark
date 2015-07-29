#!/usr/bin/env python
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from numpy import *
import matplotlib.dates as mdt
import dateutil
from collections import namedtuple

def pr(msg):
    print "%s\n" %msg

if len(sys.argv) < 1:
  pr("Usage: report <yardstick-spark logs dir>")
pr( "** Generating report **")

def grabData(rootdir):
   import glob2
   files = glob2.glob("%s/**/*.log" %rootdir)
   for f in files:
       pr("%s" %f)
   return files

import os
#import sys
#root = os.environ['YARD_SPARK'] if 'YARD_SPARK' in os.environ else '.'
root = sys.argv[1] 
files = grabData(root)

fig = plt.figure()
font = {'family' : 'normal',
  'weight' : 'bold',
  'size'   : 12}

matplotlib.rc('font', **font)
NA=1
for i in range(NA+1):
    ax = fig.add_subplot(2, 2, i + 1)
ax.grid(True, alpha=0.3)
out = []
colors = [('b','blue'), ('g','green'), ('r','red'), ('y','yellow'), ('m','magenta'), ('c','cyan'),('k','black')]
markers = ['o', 'x', '^', '*', 'D', 's','+']
YDateFormat="%Y-%m-%d %H:%M:%S"
def d2n(tstamp):
  return mdt.strpdate2num(YDateFormat)(str(tstamp)) 

# line looks like:
#<22:02:16><yardstick> Completed 0725-100020/CoreSmoke 10000000recs 20parts 1skew native AggregateByKey/Count - duration=10174 millis count=993750
#  
from collections import namedtuple
LLine = namedtuple('LLine', 'tstamp tname nrecs nparts nskew native xform action duration count')

def repch(chars, targ, src):
  out = ''
  for c in src:
    out += c if not c in chars else targ
  return out

incx = 0
def parseLine(line):
    pr(line)
    trunced = line[line.find('Completed ')+ len('Completed '):]
    pr(trunced)
    pr('aht?')
    trunced = 'Data ' + trunced
    t = repch('/-=\n',' ', trunced).split(' ')
    # t = filter(None,[el for subl in t for el in subl])
    t = filter(None,t)
    pr("%s" %t)
    # ['Data', '0727', '095121', 'CoreSmoke', '10000000recs', '20parts', '1skew', 'native', 'BasicMap', 'Count', 'duration', '4942', 'millis', 'count', '10000000']

    global incx
    incx = 0
    def inc(n = 1):
      global incx
      incx += n
      return incx-1

    def rm(src,pat):
      return src.replace(pat,'')

    t = [
        # t[inc()],  # title
        ''.join([t[inc(2)],t[inc()]]),
        t[inc()],  # name
        rm(t[inc()],'recs'),
        rm(t[inc()],'parts'),
        rm(t[inc()],'skew'),
        t[inc()], #native
        t[inc()],  #basicmap
        t[inc()], #'count'
        t[inc(2)], #'duration' duration
        t[inc(3)] # 'millis' 'count' count
        ]
    pr(str(t))
    ll = LLine(*t)
    # ll.nrecs = int(int(ll.nrecs)/1000)
    # ll.duration = int(int(ll.nrecs)/100)
    # ll.count = int(int(ll.count)/1000)
    return ll

#llines = [parseLine(ll) for ll in [open(f) for f in files]]
llines = []
for f in files:
  for ll in open(f):
    llines.append(parseLine(ll) if 'Completed' in ll else None)

llines = filter(None,llines)
print '\n'.join([str(lline) for lline in llines])

yarr = [ll.duration for ll in llines]
xarr = [ll.nrecs for ll in llines]
plt.xscale('log')

def makeLabel(lline):
    return lline.tname 
labels = [makeLabel(ll) for ll in llines]
#xfmt = mdt.DateFormatter('%H:%M:%S')
#ax.xaxis.set_major_formatter(xfmt)
x = 1
title='some title'
color=colors[x%len(colors)]
points = ax.plot(xarr,yarr, linestyle='-', marker=markers[x%len(markers)], label="abc",color=color[0])
ax.set_title(title,color= 'r')
ax.set_xlabel(' ')
ax.set_ylabel('% Resource Utilization')
ax.patch.set_facecolor('white')
legend = ax.legend(loc='upper left', fontsize=8)
for label in legend.get_lines():
  label.set_linewidth(1.0)  # the legend line width
fig.suptitle("OpenChai Heterogenous Spark Servers - By Host", fontsize='x-large')
import  datetime
print 'saving figure at %s\n' %(datetime.now().isoformat())
plt.savefig("/shared/matplotByServer.jpg")
plt.show()

