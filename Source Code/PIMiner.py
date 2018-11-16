import os
import sys
import math
import time
import Queue
import pyspark
import threading
from threading import Lock
from pyspark import SparkConf, SparkContext, StorageLevel



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


def printRDD(prdd):
	for x in prdd.collect():
		print str(x)
	return

def transform_ep(line):
	if ',' in line:
		line = line.split(',')[1]
	tknList = line.split()
	
	evList = []
	for ix in range(0, len(tknList)/3):
		evList.append([ tknList[3*ix], int(tknList[3*ix+1]), int(tknList[3*ix+2]) ])

	
	evList.sort(key=lambda x: (x[0]))
	prev = "$"
	occ = 1
	ename = ""
	eps = []
	for ev in evList:
		if prev != ev[0]:
			occ = 1
			prev = ev[0]
		else:
			occ += 1
		ename = ev[0] + "_" + str(occ)
		eps.append([ename + "+", ev[1]])
		eps.append([ename + "-", ev[2]]) 
	
	eps.sort(key=lambda x: (x[1], x[0]))
	return eps


def get_events(eps):
	evs = set()
	for e in eps:
		temp = e[0][:e[0].rfind('_')]
		evs.add(temp)
	return map(lambda x: (x, 1), evs)
	

def filter_db(eps, bcast_ev):
	fLst = []
	for e in eps:
		temp = e[0][:e[0].rfind('_')]
		if temp in bcast_ev.value:
			fLst.append(e)
	
	return fLst


def collect_sp(eps):
	sp = []
	for e in eps:
		if e[0].endswith('+'):
			sp.append(e[0])
	return map(lambda x: (x, 1), sp)
	
	

def projectdb(eps, pat):
	'''
	def presentX(ev, pat):
		sq = [x[0] for x in pat]
		for p in sq:
			if p[0].endswith('+') and p[0][:-1] == ev[:-1]:
				return True
		return False

	def presentY(ev, post):
		for p in post:
			if p[0].endswith('+') and p[0][:-1] == ev[:-1]:
				return True
		return False
	'''
	post = []
	flag = 0
	sp = pat[-1][0]
	for e in eps:
		if e[0] == sp:
			flag = 1
		if flag == 1:
			post.append(e)			
			#if e[0].endswith('+'):			
			#	post.append(e)
			#elif e[0].endswith('-') and ( presentX(e[0], pat) or presentY(e[0], post) ):
			#	post.append(e)
	return post


def prune_util(eps, pat):
	tmp_ep = []
	flag = 0
	for ix in range(1, len(eps)):
		ev = eps[ix]
		if ev[0].endswith('-'):
			for t in pat:
				if t[0].endswith('+') and t[0][:-1] == ev[0][:-1]:
					flag = 1
					break
		if ev[0].endswith('+') or flag == 1:
			if ev[1] == eps[0][1]:
				tmp_ep.append((ev[0], '='))
			else:
				tmp_ep.append((ev[0], '<'))			
		if flag == 1:
			break
	return map(lambda x: (x, 1), tmp_ep)
			

def scan_point_prune(pat, eps_rdd):
	#print "Scan pruning for " + str(sq)	
	frq_ep_rdd = eps_rdd.flatMap(lambda x: prune_util(x, pat))
	
	
	frq_ep_rdd = frq_ep_rdd.reduceByKey(lambda x, y: x+y)
	frq_ep_rdd = frq_ep_rdd.filter(lambda x: x[1] >= min_support)

	#printRDD(frq_ep_rdd)	
	
	return frq_ep_rdd.keys()

	
def is_pattern(pat):
	sq = [x[0] for x in pat]
	ctp = ctm = 0
	for s in sq:
		if s.endswith('+'):
			ctp += 1
		else:
			ctm += 1
	return ctp == ctm


class spark_component_worker(threading.Thread):
    
	def __init__(self, queue, TPS):
        	threading.Thread.__init__(self)
        	self.queue = queue
		self.TPS = TPS

    	def run(self):
        	# Get the threadId
        	threadId = self.getName()
		queue = self.queue
		TPS = self.TPS	
		global lock 

       		while True:
            		# Get the component off the execution queue
           		# Pattern defined as a python class with
            		# 1. name of pattern (representation)
            		# 2. it's indexing (projection)
            		# 3. a method that use its indexing and find the pattern
			#lock.acquire()            		
			
			pat, eps_rdd = queue.get()

			# Run the component on spark
			#print "\n\n" + str(threadId) + ": " + str(pat)
			#print "EPS "			
			#printRDD(eps_rdd)
			
			frq_rdd = scan_point_prune(pat, eps_rdd)
					
			#print "Pruned items: "
			#printRDD(frq_rdd)
			
			frq_ep = frq_rdd.collect()
			for fep in frq_ep:
				npat = pat * 1
				npat.append(fep)
				prj_rdd = eps_rdd.map(lambda x: projectdb(x, npat)).filter(lambda x: x)

				intervals = prj_rdd.map(lambda x: len(x)/2).reduce(lambda x, y: x+y)
				num_part = int(math.ceil((2*intervals)/(mb_inv*blocksz)))
				prj_rdd = prj_rdd.repartition(num_part).persist(StorageLevel.MEMORY_AND_DISK)

				if is_pattern(npat):
					TPS.put(npat)
				if len(npat) < max_len and (not prj_rdd.isEmpty()):
					queue.put((npat, prj_rdd))
			
			eps_rdd.unpersist()			
			#sys.stdout.flush()
			#lock.release()
           		# Signal to queue that the job is done
          		queue.task_done()



def LISP(num_threads, frq_sp_rdd, eps_rdd, TPS):	
	frq_sp = frq_sp_rdd.keys().collect()
	#print "Frq SP"	
	#printRDD(frq_sp_rdd)
	
	run_queue = Queue.Queue()
	
	for sp in frq_sp:
		pat = []
		pat.append((sp, '<'))
		prj_rdd = eps_rdd.map(lambda x: projectdb(x, pat)).filter(lambda x: x)
		
		intervals = prj_rdd.map(lambda x: len(x)/2).reduce(lambda x, y: x+y)
		num_part = int(math.ceil((2*intervals)/(mb_inv*blocksz)))
		prj_rdd = prj_rdd.repartition(num_part).persist(StorageLevel.MEMORY_AND_DISK)

		run_queue.put((pat, prj_rdd))
	
	#global lock
	#lock = Lock()
	for i in range(num_threads):
	        t = spark_component_worker(run_queue, TPS)
        	t.setDaemon(True)
        	t.start()

	run_queue.join()


'''''''''''''''''''''''''''''''''''''''''''''''
Main entry
'''''''''''''''''''''''''''''''''''''''''''''''
stime = time.time()

if len(sys.argv) < 4:
	print 'Usage: file_name support maxpatlen [threads] [block]'
       	exit(0)

#64mb
global blocksz
blocksz = 64
num_threads = 8	
file_name = str(sys.argv[1])
support = float(sys.argv[2])

global max_len
max_len = int(sys.argv[3])

if (len(sys.argv) >= 5):
	num_threads = int(sys.argv[4])
if (len(sys.argv) >= 6):
	blocksz = float(sys.argv[5])
sys.stdout = open("PI_"+file_name+"_s"+str(support)+"_l"+str(max_len)+"_t"+str(num_threads)+"_b"+str(blocksz)+"_spark.LOG", 'w')
max_len *=2

conf = SparkConf().setAppName("PI_"+file_name+"_s"+str(support)+"_l"+str(max_len/2)+"_t"+str(num_threads)+"_b"+str(blocksz))
sc = SparkContext(conf = conf)
input_rdd = sc.textFile("hdfs://blade1.cloud.org:8020/user/hdfs/"+file_name, None, False).map(transform_ep)
#printRDD(input_rdd)


total_sequence = input_rdd.count()

global mb_inv
mb_inv = 131072
intervals = input_rdd.map(lambda x: len(x)/3).reduce(lambda x, y: x+y)
num_part = int(math.ceil((intervals*3)/(mb_inv*blocksz)))
input_rdd = input_rdd.repartition(num_part)
print "Intervals: ", intervals
print "Block size: ", blocksz
print "Initial partitions: ", num_part

global min_support
min_support = int(math.ceil(support*total_sequence))


events_rdd = input_rdd.flatMap(get_events, True)
events_rdd = events_rdd.reduceByKey(lambda x, y: x+y, num_part)
events_rdd = events_rdd.filter(lambda x: x[1] >= min_support)
#printRDD(events_rdd)


print "Checkpoint: Starting Endpoint Representation: ", round(time.time()-stime, 4), "s"
sys.stdout.flush()

bcast_ev = sc.broadcast(events_rdd.keys().collect())
eps_rdd = input_rdd.map(lambda x: filter_db(x, bcast_ev), True).filter(lambda x: x)
input_rdd.unpersist()
#print "EPS RDD"
#printRDD(eps_rdd)


print "Checkpoint: Endpoint Representation complete: ", round(time.time()-stime, 4), "s"
sys.stdout.flush()


frq_sp_rdd = eps_rdd.flatMap(collect_sp, True).reduceByKey(lambda x, y: x+y, num_part).filter(lambda x: x[1] >= min_support)

print "Checkpoint: Frequent 1 patterns found: ", round(time.time()-stime, 4), "s"
print "Total starting points: " + str(frq_sp_rdd.count())
sys.stdout.flush()

print "-----------------------------------------------------"
print "Dataset: " + str(file_name)
print "# Sequence: " + str(total_sequence)
print "# Interval: " + str(intervals)
print "# nThreads: " + str(num_threads)
print "Support: " + str(support) + "(" + str(min_support) + ")"
TPS = Queue.Queue()
print "Max Len: " + str(max_len/2)
LISP(num_threads, frq_sp_rdd, eps_rdd, TPS)
print "-----------------------------------------------------"

etime = time.time()
TPSL = list(TPS.queue)
ANS = []
for t in TPSL:
	sf = ""
	for s in t:
		sf = sf + " " + str(s[1]) + " (" + str(s[0]) + ")"
	ANS.append(sf)

ANS.sort()

print "Frequent Temporal Patterns: " + str(len(ANS))
for pat in ANS:
	print pat


# Output final runtime
print "-----------------------------------------------------"
print "Execution Complete"
print 'Total time taken: ', round(etime - stime, 4), "s"
print 'Total memory usage: ' + str(round(float(memory())/(1024.0*1024.0), 4)), "MB"
print 'Resident memory usage: ' + str(round(float(resident())/(1024.0*1024.0), 4)), "MB"
print 'Stack size usage: ' + str(round(float(stacksize())/(1024.0*1024.0), 4)), "MB"
print "-----------------------------------------------------"



