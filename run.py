import os , sys , time , datetime 
import atexit , signal
from optparse import OptionParser

def daemonize(pidfile,*,stdin='/dev/null',stdout='/dev/null',stderr='/dev/null') :
    if os.path.exists(pidfile) :
        raise RuntimeError('Already running')
    try :
        if os.fork() > 0 :
            raise SystemExit(0)
    except OSError as e :
        raise RuntimeError('fork #1 failed.')

    os.umask(0)
    os.setsid()

    try :
        if os.fork() > 0 :
            raise SystemExit(0)
    except OSError as e :
        raise RuntimeError('fork #2 failed.')

    sys.stdout.flush()
    sys.stderr.flush()

    with open(stdin,'rb',0) as f :
        os.dup2(f.fileno(),sys.stdin.fileno())
    with open(stdout,'ab',0) as f :
        os.dup2(f.fileno(),sys.stdout.fileno())
    with open(stderr,'ab',0) as f :
        os.dup2(f.fileno(),sys.stderr.fileno())

    with open(pidfile,'w') as f :
        print(os.getpid(),file=f)

    atexit.register(lambda: os.remove(pidfile))

    def sigterm_handler(signo,frame):
        raise SystemExit(1)

    signal.signal(signal.SIGTERM,sigterm_handler)

def main(start,period,topics,overlapping):
    import time 
    sys.stdout.write("Daemon started with pid {}\n".format(os.getpid()))
    period *= 86400
    overlapping *= 86400
    START = time.mktime(datetime.datetime.strptime(start, "%Y-%m-%d").timetuple())
    END   = START + period
    while True :
        current = time.time()
        if END > current :
            sys.stdout.write("Wait for enough period ! {}\n".format(time.ctime()))
            time.sleep(END-current)
        else :
            start_token = datetime.datetime.fromtimestamp(START).strftime('%Y-%m-%d')
            end_token = datetime.datetime.fromtimestamp(END).strftime('%Y-%m-%d')
            sys.stdout.write("Topics Formation @{} - @{} ! {}\n".format(start_token,end_token,time.ctime()))
            os.system("python main.py {} {} {} {}".format(START,END,topics,os.getpid()))
            START += ( period - overlapping ) 
            END   += ( period - overlapping )
            
            

if __name__ == "__main__" :
    PIDFILE = '/tmp/daemon.pid'
    parser = OptionParser()
    parser.add_option("-s",'--start',action="store_true",help="Start system",default=False)
    parser.add_option("-t","--terminate",action="store_true",help="Terminate system",default=False)
    parser.add_option("-d","--date",action="store",type="string",help="Setting date , eg.-d 2014-10-16",default="2014-10-16")
    parser.add_option("-o","--overlapping",action="store",type="float",help="Setting overlapping , eg.-o 1",default="1")
    parser.add_option("-p","--period",action="store",type="float",help="Setting period , eg. -p 1 => one day",default=2)
    parser.add_option("-z","--topic",action="store",type="int",help="Topic number, eg. -z 12",default=12)
    parser.add_option("-l","--log",action="store",type="string",help="Logging file eg. -l /tmp/daemon.log",default='/tmp/daemon.log')

    (options , args) = parser.parse_args()

    if options.terminate :
        if os.path.exists(PIDFILE):
            with open(PIDFILE) as f :
                os.kill(int(f.read()),signal.SIGTERM)
            print("System Stop at {}".format(time.ctime()))
        else :
            print("Not running",file=sys.stderr)
            raise SystemExit(1)
    elif options.start :
        try :
            daemonize(PIDFILE,stdout=options.log,stderr=options.log)
        except RuntimeError as e :
            print(e,file=sys.stderr)
            raise SystemExit(1)
        if options.overlapping > options.period :
            print("Period setting error : #1 , overlapping should smaller than period")
            raise SystemExit(1)
        print("System Start at {}".format(time.ctime()))
        main(options.date,options.period,options.topic,options.overlapping)
    else :
        print("Unknown command")
        raise SystemExit(1)
