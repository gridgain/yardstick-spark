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
   # for f in files:
       # pr("%s" %f)
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
    trunced = line[line.find('Completed ')+ len('Completed '):]
    trunced = 'Data ' + trunced
    t = repch('/-=\n',' ', trunced).split(' ')
    # t = filter(None,[el for subl in t for el in subl])
    t = filter(None,t)
    # pr("%s" %t)
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
# pr("# lines is %d" %len(llines))
print '\n'.join([str(lline) for lline in llines])

with open('report.csv','w') as f:
    x = 0
    for lline in llines:
        if not x:
          msg = ','.join([l for l in lline._fields]) + '\n'
          f.write(msg)
          pr(msg.strip())
          x += 1
        msg = ','.join(lline)+'\n'
        f.write(msg)
        pr(msg.strip())

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
# LLine = namedtuple('LLine', 'tstamp tname nrecs nparts nskew native xform action duration count')

def seriesKey(*native):
    # return ' '.join([native[0].native,native[0].nrecs])
    return native[0].native
def lkey(ll):
    return ' '.join([ll.tname,ll.tstamp,ll.nparts,ll.nskew,ll.xform,ll.action, ll.native])
def metrics(ll):
    return ' '.join([ll.duration])

# keys, series, metrics   = zip(*[(lkey(ll),seriesKey(ll),metrics(ll)) for ll in sortedll])

# pr(str(x))
#keys, series, metrics = zip(*x

from collections import *
from itertools import groupby
gls={}
vl=[]
series={}
def mlput(m,k, v):
  l = m[k] if k in m else m.update(k,list())
  l.append(v)

def grouped(llist, keyf):
  pr("In grouped: llist is %s" %str(llist))
  g = {}
  for ll in llist:
    mlput(g,keyf(ll),ll)
  return g

for g,v in grouped(llines,lkey):
  mlput(gls,g,v)

for gl,v in gls.items():
    for gs,vs in grouped(v,seriesKey):
      mlput(series,"%s-%s" %(gl,gs),vs)

from operator import attrgetter
for sk, sv in series.items():
    #series[sk] = sorted(sv,sv.nrecs)
    series[sk] = sorted(sv,attrgetter(sv.nrecs))

for x,ser in enumerate(series):
  yarr = [l.duration for l in ser]
  xarr = [l.nrecs for l in ser]
  points = ax.plot(xarr,yarr, linestyle='-', marker=markers[x%len(markers)], label=ser.native,color=color[x])
  # points2 = ax.plot(xarr,yarr, linestyle='-', marker=markers[x%len(markers)], label="abc",color=color[0])
  ax.set_title(title,color= 'b')
  ax.set_xlabel(' ')
  ax.set_ylabel('% Resource Utilization')
  ax.patch.set_facecolor('white')
  legend = ax.legend(loc='upper left', fontsize=8)
  for label in legend.get_lines():
    label.set_linewidth(1.0)  # the legend line width
  fig.suptitle("IgniteRDD vs Native Core Operations Performance", fontsize='x-large')
  import  datetime
  print 'saving figure at %s\n' %(datetime.datetime.now().isoformat())
  plt.savefig("/tmp/report.jpg")
  plt.show()
