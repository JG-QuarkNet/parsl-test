import argparse
import parsl
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])

# Define Apps
@App('bash', dfk)
def WireDelay(inputs=[], outputs=[], geoDir='', daqId='', fw=''):
		pass
		#print(inputs)
		#print(inputs,outputs,geoDir,daqId,fw)
#		return 'perl perl/WireDelay.pl %s %s %s %s %s' %(inputs[0],outputs[0],geoDir,daqId,fw)

@App('python', dfk)
def WireDelayMultiple(inputs=[], outFileNames=[], geoDir='', daqIds=[], firmwares=''):
		for i in range(len(inputs)):
				WireDelay(inputs=inputs[i],outputs=outFileNames[i],geoDir=geoDir,daqId=daqIds[i],fw=firmwares[i])

@App('bash', dfk)
def Combine(inputs=[],outputs=[]):
		return 'perl perl/Combine.pl ' + ' '.join(inputs) + ' ' + outputs[0]

@App('bash', dfk)
def Sort(inputs=[], outputs=[], key1='1', key2='1'):
		return 'perl perl/Sort.pl %s %s %s %s' %(inputs[0], outputs[0], key1, key2)

@App('bash', dfk)
def EventSearch(inputs=[], outputs=[], gate='', detCoinc='2', chanCoinc='2', eventCoinc='2'):
		return 'perl perl/EventSearch.pl %s %s %s %s %s %s' %(inputs[0],outputs[0],gate,detCoinc,chanCoinc,eventCoinc)


# Parse the command-line arguments
parser = argparse.ArgumentParser()

parser.add_argument("--thresholdAll", nargs='+', help="All threshold files")
parser.add_argument("--wireDelayData", nargs='+', help="Wire delay data filenames")
parser.add_argument("--geoDir", help="Directory containing DAQ ID directories that contain .geo files")
#parser.add_argument("--geoFiles", nargs='+', help=".geo filenames for each CRD")
parser.add_argument("--detectors", nargs='+', help="IDs of all CRDs in the analysis")
parser.add_argument("--firmwares", nargs='+', help="DAQ firmware versions")
parser.add_argument("--combineOut")
parser.add_argument("--sort_sortKey1")
parser.add_argument("--sort_sortKey2")
parser.add_argument("--sortOut")
parser.add_argument("--gate")
parser.add_argument("--detectorCoincidence")
parser.add_argument("--channelCoincidence")
parser.add_argument("--eventCoincidence")
parser.add_argument("--eventCandidates", help="eventCandidates file")

args = parser.parse_args()


# The Workflow
print(args.thresholdAll)
WireDelayMultiple(inputs=args.thresholdAll, outFileNames=args.wireDelayData, geoDir=args.geoDir, daqIds=args.detectors, firmwares=args.firmwares)
Combine(inputs=args.wireDelayData, outputs=args.combineOut)
Sort(inputs=args.combineOut, outputs=args.sortOut, key1=args.sort_sortKey1, key2=args.sort_sortKey2)
EventSearch(inputs=args.sortOut, outputs=args.eventCandidates, gate=args.gate, detCoinc=args.detectorCoincidence, chanCoinc=args.channelCoincidence, eventCoinc=args.eventCoincidence)
