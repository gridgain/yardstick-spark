#!/usr/bin/env python
import os
slaves=os.environ['SL']
newhosts = '\n'.join(map(lambda h: '                                <value>%s</value>' %h,  slaves.split(' ')))
print "newhosts is %s\n" %newhosts
#sparkhome=os.environ['Y']
#fname = "%s/config/slaves" %sparkhome
fname = "config/hosts.template.xml"
with open(fname,'r') as f:
  txt = f.read()
txt = txt.replace("HOSTS_LIST",newhosts)
ofile= 'config/hosts.xml'
with open(ofile,'w') as f:
   f.write(txt)
print "Updated %s with [%s]\n" %(ofile,txt)