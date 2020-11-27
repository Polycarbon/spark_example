import importlib
import argparse
import time
import datetime

from jobs import demo_job

if __name__ == '__main__':
    start = time.time()
    try:
        demo_job.main()
        end = time.time()
        print ("\nExecution of job took %s seconds" % (end-start))
    except Exception as e:
         print (str(datetime.datetime.now()) + "____________ Abruptly Exited________________")
         raise Exception("Exception::Job failed with msg %s" %(str(e)))



