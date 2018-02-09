import argparse
import parsl
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


## Define Apps ##
@App('bash', dfk)
def WireDelay(inputs=[], outputs=[], geoDir='', daqId='', fw=''):
		return 'perl perl/WireDelay.pl %s %s %s %s %s' %(threshIn,wireDelayOut,geoDir,daqId,fw)


@App('python', dfk)
def WireDelayMultiple(threshFiles=[], outputs=[], geoDir='', daqIds=[], firmwares=''):
		for i in range(len(threshFiles)):
				WireDelay(inputs=[threshFiles[i]],outputs=[outputs[i]],geoDir=geoDir,daqId=daqIds[i],fw=firmwares[i])

@App('bash', dfk)
def Combine(inputs=[],outputs=[]):
		return 'perl perl/Combine.pl ' + ' '.join(inputs) + ' ' + outputs[0]

@App('bash', dfk)
def Sort(inputs=[], outputs=[], key1='1', key2='1'):
		return 'perl perl/Sort.pl %s %s %s %s' %(inputs[0], outputs[0], key1, key2)

@App('bash', dfk)
def EventSearch(inputs=[], outputs=[], gate='', detCoinc='2', chanCoinc='2', eventCoinc='2'):
		return 'perl perl/EventSearch.pl %s %s %s %s %s %s' %(inputs[0],outputs[0],gate,detCoinc,chanCoinc,eventCoinc)


## Parse the command-line arguments ##
parser = argparse.ArgumentParser()

parser.add_argument("--thresholdAll", nargs='+', help="All threshold files")
parser.add_argument("--wireDelayData", nargs='+', help="Filenames for intermediate Wire Delay data")
parser.add_argument("--geoDir", help="Directory containing DAQ ID directories that contain .geo files")
#parser.add_argument("--geoFiles", nargs='+', help=".geo filenames for each CRD")
parser.add_argument("--detectors", nargs='+', help="IDs of all CRDs in the analysis")
parser.add_argument("--firmwares", nargs='+', help="DAQ firmware versions")
parser.add_argument("--combineOut", help="Combined data from all intermediate Wire Delay files")
parser.add_argument("--sort_sortKey1")
parser.add_argument("--sort_sortKey2")
parser.add_argument("--sortOut")
parser.add_argument("--gate")
parser.add_argument("--detectorCoincidence")
parser.add_argument("--channelCoincidence")
parser.add_argument("--eventCoincidence")
parser.add_argument("--eventCandidates", help="eventCandidates file")

args = parser.parse_args()


## The Workflow ##

# 1) WireDelayMultiple() takes input Threshold (.thresh) files and converts
#    each to a Wire Delay (.wd) file:
WdmFuture = WireDelayMultiple(threshFiles=args.thresholdAll, outputs=args.wireDelayData, geoDir=args.geoDir, daqIds=args.detectors, firmwares=args.firmwares)

# 2) Combine() takes the WD files output by WireDelayMultiple() and combines
#    them into a single file with name given by --combineOut 
CombineFuture = Combine(inputs=WdmFuture.outputs, outputs=[args.combineOut])

# 3) Sort() sorts the --combineOut file, producing a new file with name given
#    by --sortOut
SortFuture = Sort(inputs=CombineFuture.outputs, outputs=[args.sortOut], key1=args.sort_sortKey1, key2=args.sort_sortKey2)


# 4) EventSearch() processes the --sortOut file and identifies event
#    candidates in a output file with name given by --eventCandidates
# NB: This output file is interpreted by the e-Lab webapp, which expects it
#    to be named "eventCandidates"
EventSearch(inputs=SortFuture.outputs, outputs=[args.eventCandidates], gate=args.gate, detCoinc=args.detectorCoincidence, chanCoinc=args.channelCoincidence, eventCoinc=args.eventCoincidence)
