#!/usr/bin/python
__author__ = 'jon'

import sys
import getopt
import os
import itertools
import redis
import random
import time

from multiprocessing import Pool

# Just color formatting for output
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Overload KeyboardInterruptError to more gracefully (less verbose) stop threads
class KeyboardInterruptError(Exception): pass

# Iterative redis command tester, can be scaled by increasing number of cores (processes)
def test_redis(iterative, total, host, port, db):
    response = []
    r = redis.StrictRedis(host=host, port=port, db=db)
    key = str(iterative)
    response.append("Iterative " + key)

    # Sets
    response.append("Testing Set" )

    pipeline = r.pipeline()

    testRange = range(total)
    random.shuffle(testRange)
    testRange.pop()

    r.delete('compare:' + str(iterative))

    for i in testRange:
        pipeline.sadd('compare:' + str(iterative), i)
    pipeline.execute()

    start = time.clock()
    response.append(bcolors.OKBLUE + "Response::" + bcolors.ENDC)
    response.append(bcolors.OKGREEN + str(r.sdiff('primer', 'compare:' + str(iterative))) + bcolors.ENDC)
    response.append(bcolors.WARNING + "Runtime = " + str('{0:.20f}'.format(time.clock() - start)) + bcolors.ENDC)

    return response

# Set up the base/primer list in redis
def prime_redis(host, port, db, iterative, total):
    r = redis.StrictRedis(host=host, port=port, db=db)

    r.delete('primer')

    pipeline = r.pipeline()
    for i in range(total):
        pipeline.sadd('primer', i)
    response = pipeline.execute()

    return 'Done priming redis'

# Pass parameters to redis tester in a distributed manner (star function)
def star_test(a_b):
    return test_redis(*a_b)

def main(argv):
    outDirectory = './out'
    cores = 4
    iterative = 100
    total = 100
    host = 'localhost'
    port = 6379
    db = 0

    try:
        opts, args = getopt.getopt(argv, "ho:c:i:t:u:p:d:w:y", ["out=", "cores=", "iterative=", "total=", "host=", "port=", "db="])
    except getopt.GetoptError:
        print 'main.py -o <out> -c <number of cores> -i <number of times to run> -u <host name> -p <port> -d <db>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'main.py -o <out> -c <number of cores> -i <number of times to run> -u <host name> -p <port> -d <db>'
            sys.exit()
        elif opt in ("-o", "--out"):
            outDirectory = arg
        elif opt in ("-c", "--cores"):
            cores = int(arg)
        elif opt in ("-i", "--iterative"):
            iterative = int(arg)
        elif opt in ("-t", "--total"):
            total = int(arg)
        elif opt in ("-u", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = arg
        elif opt in ("-d", "--db"):
            db = arg

    if 'outDirectory' in locals():
        if not os.path.isdir(outDirectory):
            os.makedirs(outDirectory)

    pool = Pool(processes=cores)

    print 'Starting pool'

    if host is not None:
        try:
            prime = prime_redis(host, port, db, iterative, total)
            print prime

            a_args = range(iterative)

            print "Starting " + str(iterative) + " workers"
            results = pool.map(star_test, itertools.izip(a_args, itertools.repeat(total),itertools.repeat(host), itertools.repeat(port), itertools.repeat(db)))
            print "Workers complete"
            pool.close()

            for array in results:
                for line in array:
                    print(line)

            print 'pool map complete'
        except KeyboardInterrupt:
            print 'got ^C while pooling, terminating the pool'
            pool.terminate()
            print 'pool is terminated'
        except Exception, e:
            print 'got exception: %r, terminating the pool' % (e,)
            pool.terminate()
            print 'pool is terminated'
        finally:
            print 'joining pool processes'
            pool.join()
            print 'join complete'
        print 'the end'
    else:
        print "Please define a host and port"


if __name__ == "__main__":
    main(sys.argv[1:])