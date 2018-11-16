import os
import sys
import time
import math



_proc_status = '/proc/%d/status' % os.getpid()

_scale = {'kB': 1024.0, 'mB': 1024.0*1024.0, 'KB': 1024.0, 'MB': 1024.0*1024.0}

def _VmB(VmKey):
    '''Private.
    '''
    global _proc_status, _scale
     # get pseudo file  /proc/<pid>/status
    try:
        t = open(_proc_status)
        v = t.read()
        t.close()
    except:
        return 0.0  # non-Linux?
     # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
    i = v.index(VmKey)
    v = v[i:].split(None, 3)  # whitespace
    if len(v) < 3:
        return 0.0  # invalid format?
     # convert Vm value to bytes
    return float(v[1]) * _scale[v[2]]


def memory(since=0.0):
    '''Return memory usage in bytes.
    '''
    return _VmB('VmSize:') - since


def resident(since=0.0):
    '''Return resident memory usage in bytes.
    '''
    return _VmB('VmRSS:') - since


def stacksize(since=0.0):
    '''Return stack size in bytes.
    '''
    return _VmB('VmStk:') - since


'''
Transform to endpoint
'''
def transform_ep(inp_lst):
	event_lst = []	
	for line in inp_lst:	
		lst = []
		tok = line.split()
		for ix in range(0, len(tok)/3):
			temp = []		
			temp.append(tok[3*ix])
			temp.append(int(tok[3*ix+1]))
			temp.append(int(tok[3*ix+2]))
			lst.append(temp)
		
		lst.sort(key=lambda x: (x[0]))
		
		prev = "$"
		occ = 1
		for ix in lst:
			if (prev == ix[0]):
				occ = occ +1
			else:
				occ = 1	
				prev = ix[0]
			ix[0] = str(ix[0]) + "_" + str(occ)
		
		event_lst.append(lst)

	#for line in event_lst:
	#	print line

	eps_db = []
	for tsq in event_lst:
		eps = []
		for e in tsq:
			t = []
			t.append(e[0]+str('+'))
			t.append(e[1])
			eps.append(t)
			t = []			
			t.append(e[0]+str('-'))
			t.append(e[2])
			eps.append(t)

		eps.sort(key=lambda x: (x[1], x[0]))		
		eps_db.append(eps)	
	
	#print "Endpoint Representation:"	
	#for eps in eps_db:
	#	print eps 
	return eps_db
	
		


def collect_ev(eps_db):
	events = set()
	for eps in eps_db:
		for e in eps:
			if e[0].endswith('+'):
				events.add(e[0][:e[0].rfind('_')])
	#for e in events:
	#	print e
	return events


def count_occ(eps_db, count_dict):
	for eps in eps_db:
		flag = 0
		ev = set()
		for e in eps:
			if e[0].endswith('+'):
				ev.add(e[0][:e[0].rfind('_')])
		for e in ev:
			count_dict[e] += 1
	return


def remove_ev(eps_db, events_to_remove):
	#print events_to_remove
	for eps in eps_db:
		for e in eps:
			pre = e[0][:e[0].rfind('_')]
			if pre in events_to_remove:
				eps.remove(e)
	return



def collect_sp(eps_db):
	frq_sp = set()
	for eps in eps_db:
		for e in eps:
			if e[0].endswith('+'):
				frq_sp.add(e[0])
	#for e in events:
	#	print e
	return frq_sp



def projectdb(eps_db, sp):
	proj_db = []
	for eps in eps_db:
		t = []
		flag = 0		
		for e in eps:
			if e[0] == sp:
				flag = 1
			if flag == 1:
				t.append(e)
		if t:		
			proj_db.append(t)
	return proj_db	



def count_frq(eps_db, sp_dict):
	for eps in eps_db:
		for e in eps:
			if e[0].endswith('+'):
				sp_dict[e[0]] += 1
	return


def count_frq_new(ep, eps_db, order):
	ct = 0
	for eps in eps_db:
		for e in eps:
			if e[0] == ep:
				if order == '<' and eps[0][1] < e[1]:
					ct = ct + 1
				elif order == '=' and eps[0][1] == e[1]:
					ct = ct + 1
				break
	return ct

def scan_prune(sq, eps_db, min_sup):
	frq_ep = set()
	tmp_ep = set()
	#print "Scan pruning for " + str(sq)	
	for eps in eps_db:
		stop = -1
		for i in range(1, len(eps)):
			e = eps[i]
			flag = 0
			if e[0].endswith('-'):
				for t in sq:
					#print str(e[0][:-1]) + " same " + str(t[:-1])
					if t[0].endswith('+') and t[0][:-1] == e[0][:-1]:
						stop = i + 1
						flag = 1
						break
			if flag == 1:
				break
		if stop == -1:
			stop = len(eps)
		#print "stop pos: " + str(stop)
		for ix in range(1, stop):
			tmp_ep.add(eps[ix][0])
	for t in tmp_ep:
		ct = count_frq_new(t, eps_db, '<')
		if ct >= min_sup:
			frq_ep.add((t, '<'))
		ct = count_frq_new(t, eps_db, '=')
		if ct >= min_sup:
			frq_ep.add((t, '=')) 	
	return frq_ep
	


def point_prune(sq, sc_ep):
	tp = set()
	for ep in sc_ep:
		if ep[0].endswith('-'):
			flag = 0
			for t in sq:
				#print t[0] + " same " + ep[0]
				if t[0].endswith('+') and t[0][:-1] == ep[0][:-1]:
					flag = 1
					break
			if flag == 1:
				tp.add(ep)
		else:
			tp.add(ep)
	return tp	



def is_pattern(sq_tuple):
	sq = [x[0] for x in sq_tuple]
	for i in range(0, len(sq)):
		if sq[i].endswith('-'):
			flag = 0			
			for j in range(0, i):
				if sq[j].endswith('+') and sq[i][:-1] == sq[j][:-1]:
					flag = 1
			if flag == 0:
				return False
		else:
			flag = 0
			for j in range(i+1, len(sq)):
				if sq[j].endswith('-') and sq[i][:-1] == sq[j][:-1]:
					flag = 1
			if flag == 0:
				return False
	return True



def post_prune(sq, eps_db):
	proj_db = []	
	
	t_db = projectdb(eps_db, sq[-1][0])		
	#print "\nPost prune: " + str(sq)
	for eps in t_db:
		t_eps = []
		#print "eps " + str(eps)		
		for i in range(0, len(eps)):
			ev = eps[i]
			if ev[0].endswith('-'):
				flag = 0
				for t in sq:
					#print str(ev[0]) + " same " + str(t[0])
					if t[0].endswith('+') and t[0][:-1] == ev[0][:-1]:
						flag = 1
						break
				if flag == 0:
					for j in range(0, i):
						if eps[j][0].endswith('+') and eps[j][0][:-1] == ev[0][:-1]:
							flag = 1
							break
				if flag == 1:
					t_eps.append(ev)
						
			else:
				t_eps.append(ev)	
		#print "t_eps " + str(t_eps)	
		if t_eps:
			proj_db.append(t_eps)
	#print "Proj db: "
	#for eps in proj_db:
	#	print eps
		
	return proj_db



def append_pat(nsq, TPS, FDY):
	sf = ""
	FDY[len(nsq)/2] += 1
	for s in nsq:
		sf = sf + " " + str(s[1]) + " (" + str(s[0]) + ")"
	TPS.append(sf)


'''''''''''''''''''''''''''''''''''''''''''''
Main algorithm
'''''''''''''''''''''''''''''''''''''''''''''

def TPSpan(sq, eps_db, min_sup, max_len, TPS, FDY):

	#print "\n\nTPSpan: " + str(sq) + "\nProj db: "
	#for eps in eps_db:	
	#	print eps

	frq_ep = scan_prune(sq, eps_db, min_sup)
	#print "Scan pruned endpoints:"
	#print frq_ep

	frq_ep = point_prune(sq, frq_ep)
	#print "Point pruned endpoints:"
	#print frq_ep
		
	
	for fep in frq_ep:
		nsq = sq * 1
		nsq.append(fep)
		if is_pattern(nsq):
			append_pat(nsq, TPS, FDY)
		#proj_db = post_prune(nsq, eps_db)
		proj_db = projectdb(eps_db, nsq[-1][0])
		#print "Post pruned db for " + str(nsq) + ":"
		#for eps in proj_db:
		#	print eps
		if proj_db and len(nsq) < max_len:
			TPSpan(nsq, proj_db, min_sup, max_len, TPS, FDY)
	TPS.sort()
	return





'''''''''''''''''''''''''''''''''''''''''''''''
Main entry
'''''''''''''''''''''''''''''''''''''''''''''''
stime = time.time()

if len(sys.argv) < 4:
	print 'Usage: file_name support maxpatlen'
       	exit(0)
	
file_name = str(sys.argv[1])
support = float(sys.argv[2])
max_len = int(sys.argv[3])
sys.stdout = open("TP_"+file_name+"_s"+str(support)+"_l"+str(max_len)+".LOG", 'w')
max_len *= 2

#print "DATASET: " + str(file_name)
file_p = open(file_name)

inp_lst = []
for line in file_p.readlines():
	if ',' in line:
		line = line[1+line.find(',') : ]
	line = " ".join(line.split())
	inp_lst.append(line)	

total_duration = 0
for line in inp_lst:
	line = line.split(" ")
	for i in range(0, len(line)/3):
		total_duration += int(line[3*i+2])-int(line[3*i+1])



print "Checkpoint: Starting Endpoint Representation: ", round(time.time()-stime, 4), "s"
sys.stdout.flush()

eps_db = transform_ep(inp_lst)
#print "Endpoint Representation:"	
#for eps in eps_db:
#	print eps 

print "Checkpoint: Endpoint Representation complete: ", round(time.time()-stime, 4), "s"
sys.stdout.flush()



temporal_pattern = []
events = collect_ev(eps_db)


total_event = len(events)
total_sequence = len(eps_db)
min_support = int(math.ceil(support*total_sequence))
total_interval = 0

for eps in eps_db:
	total_interval += len(eps)/2

#print "Events frequency:"
events_to_remove = []
count_dict = {}
for e in events:
	count_dict[e] = 0
count_occ(eps_db, count_dict)

for ev in events:
	#print ev + " " + str(ct)
	if count_dict[ev] < min_support:
		events_to_remove.append(ev)

remove_ev(eps_db, events_to_remove)

print "Checkpoint: Removed Infrequent Endpoints: ", round(time.time()-stime, 4), "s"
#print "Removed Infrequent Endpoints:"	
#for eps in eps_db:
#	print eps 


frq_sp_old = collect_sp(eps_db)
sp_dict = {}
for f in frq_sp_old:
	sp_dict[f] = 0

count_frq(eps_db, sp_dict)
frq_sp = set()
for f in frq_sp_old:
	if  sp_dict[f] >= min_support:
		frq_sp.add(f)

#print "Frq SP:"
#print frq_sp



print "Checkpoint: Frequent 1 patterns found: ", round(time.time()-stime, 4), "s"
print "Total starting points: " + str(len(frq_sp))
sys.stdout.flush()

count_completion = 1
count_flags = [0]

TPS = []
FDY = {}
for x in range(1, max_len/2 + 1):
	FDY[x] = 0

sys.stdout.write('Completion 0% ')
sys.stdout.flush()
for sp in frq_sp:
	proj_db = projectdb(eps_db, sp)
	#print "\n\nStarting Point: " + str(sp) + "\nProj db: "
	#for eps in proj_db:	
	#	print eps
	sq = []
	sq.append((sp, '<'))
	TPSpan(sq, proj_db, min_support, max_len, TPS, FDY)
	

	percent = int(count_completion*100/len(frq_sp))
	if percent not in count_flags:
		sys.stdout.write(str(percent)+"% ")
		count_flags.append(percent)
	count_completion += 1
	sys.stdout.flush()


etime = time.time()

print ""
print "-----------------------------------------------------"
print "Dataset: " + str(file_name)
print "# Sequence: " + str(total_sequence)
print "# Interval: " + str(total_interval)
print "# Event: " + str(total_event)
print "avg. Sequence Length: " + str(total_interval/total_sequence)
print "avg. Duration: " + str(total_duration/total_interval)

print "Support: " + str(support) + "(" + str(min_support) + ")"
print "Max Len: " + str(max_len/2)
print "Frequent Temporal Patterns: " + str(len(TPS))
for pat in TPS:
	print pat
print "-----------------------------------------------------"
for x in range(1, max_len/2 + 1):
	print "Frequent", x, "len patterns:", FDY[x]
print "-----------------------------------------------------"

# Output final runtime
print "-----------------------------------------------------"
print "Execution Complete"
print 'Total time taken: ', round(etime - stime, 4), "s"
print 'Total memory usage: ' + str(round(float(memory())/(1024.0*1024.0), 4)), "MB"
print 'Resident memory usage: ' + str(round(float(resident())/(1024.0*1024.0), 4)), "MB"
print 'Stack size usage: ' + str(round(float(stacksize())/(1024.0*1024.0), 4)), "MB"
print "-----------------------------------------------------"


