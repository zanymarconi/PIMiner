import sys
import math
import random
import numpy as np
from random import shuffle
import matplotlib.pyplot as plt

''' Main entry '''

if len(sys.argv) == 4:
	S = int(sys.argv[1])
	C = int(sys.argv[2])
	E = int(sys.argv[3])
else :
	S = int(input("Enter sequences: "))
	C = int(input("Enter average size of sequence: "))
	E = int(input("Enter total events: "))

#5% frequency"
#10% generate"
Nf = 7
Ne = [12, 8, 4]


print "Total Sequence: " + str(S)
print "Avg Seque size: " + str(C)
print "Avg Frq seq sz: " + str(Nf)
print "TotalNo events: " + str(E)
print "Size of events: " + str(Ne)

#nfrq% sequences with support poisson(support%)"
nfrq = int(math.ceil(.01 * S))
support = int(math.ceil(.5 * S))
print "Generating: " + str(nfrq) + " frequent sequences with support: " + str(support) + " of avg size: " + str(Nf)

rel = ['b', 'm', 'o', 'f', 'c', 's', 'e']


frq_events = []
for i in range(0, nfrq):
	s = np.random.poisson(7)
	if s <= 0:
		s = 1
	s = min(s, E, C) 
	fe = random.sample(range(E), s)
	fr = []
	for j in range(1, s):
		fr.append(random.choice(rel))
	#print "Frq sequence: "
	#sys.stdout.write(str(fe[0])+' ')	
	#for j in range(0, s-1):
	#	sys.stdout.write(str(fr[j]) + ' ' + str(fe[j+1]) + ' ')
	#print ""
	
	#random space
	spc = np.random.poisson(10)
	if spc <=0:
		spc = 10
	prv = [spc, spc+random.choice(Ne)]
	events = []
	events.append([fe[0], prv[0], prv[1]])
	#print fe[0], ":", prv
	for j in range(0, s-1):
		op = fr[j]
		start = prv[0]
		end = prv[1]
		if op == 'b':
			prv = [end+spc, end+spc+random.choice(Ne)]
		elif op == 'f':
			prv = [end-random.choice(Ne)+1, end]
		elif op == 'm':
			prv = [end, end+random.choice(Ne)]
		elif op == 's':
			prv = [start, start+random.choice(Ne)+1]
		elif op == 'e':
			prv = prv
		elif op == 'c':
			prv = [start+1, end-1]
		elif op == 'o':
			mid = int((start+end)/2)			
			prv = [mid, mid+end-start]
		#print fr[j], fe[j+1], ":", prv
		events.append([fe[j+1], prv[0], prv[1]])
	frq_events.append(events)



sys.stdout = open("S"+str(S)+"C"+str(C)+"E"+str(E)+".DB", 'w')
inputdb = [[] for i in range(S)]

#print "Frequent Events:"
for fq in frq_events:
	ct = np.random.poisson(support)
	at = random.sample(range(S), ct)
	#print fq
	#print "in sq: ", at
	for sq in at:
		ptr = -1
		if not inputdb[sq]:
			ptr = 0
		else:
			ptr = inputdb[sq][-1][2]
		ptr += 10
		for e in fq:
			inputdb[sq].append([e[0], e[1]+ptr, e[2]+ptr])

for line in inputdb:
	rem = np.random.poisson(C) - len(line)
	if rem <= 0:
		continue
	if not line:
		ptr = 0
	else:
		ptr = line[-1][2]	
	ptr += 10
	for i in range(rem):
		x = ptr + random.choice(range(rem*8))
		line.append([random.choice(range(E)), x, x+random.choice(Ne)])

'''print "After"
for x in inputdb:
	print x'''

for ix in range(S):
	line = inputdb[ix]
	stri = str(ix+1) + ","
	for e in line:
		stri += ' '+str(e[0])+' '+str(e[1])+' '+str(e[2])
	print stri


	
	


