import json
from optparse import OptionParser

parser = OptionParser()

parser.add_option("-a",'--alpha',action="store",type="float",help="set alpha",default=0.3)
parser.add_option("-d",'--default',action="store",type="float",help="set user default value",default=0.15)

(options , args) = parser.parse_args()

f = open('config.json','w')
d = {'alpha':options.alpha,'default':options.default}
parameters = json.dumps(d)
f.write(parameters)
f.close()
