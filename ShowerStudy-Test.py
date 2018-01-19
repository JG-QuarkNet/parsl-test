import argparse
import parsl
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])

@App('bash', dfk)
def WireDelay(threshold, wireDelayOutput, geoDir, detector, firmware):
		return 'perl perl/WireDelay.pl %s %s %s %s %s' %(threshold,wireDelayOutput,geoDir,detector,firmware)
#		return 'echo "perl perl/WireDelay.pl %s %s %s %s %s"' %(threshold,wireDelayOutput,geoDir,detector,firmware)

@App('python', dfk)
def WireDelayMultiple():
		#print(args.thresholdAll,args.wireDelayData,args.geoDir,args.detectors,args.firmwares)
		for i in range(len(args.thresholdAll)):
				#print(args.thresholdAll[i],args.wireDelayData[i],args.geoDir,args.detectors[i],args.firmwares[i])
				WireDelay(args.thresholdAll[i],args.wireDelayData[i],args.geoDir,args.detectors[i],args.firmwares[i])

@App('bash', dfk)
##def Combine():
def Combine(CombineOutput):
##		return 'perl perl/Combine.pl ' + ' '.join(args.wireDelayData) + ' ' + args.combineOut
		return 'perl perl/Combine.pl ' + ' '.join(args.wireDelayData) + ' ' + CombineOutput

@App('bash', dfk)
def Sort(dataIn, fileOut, key1, key2):
		return 'perl perl/Sort.pl %s %s %s %s' %(dataIn,fileOut,key1,key2)

@App('bash', dfk)
def EventSearch(dataIn, fileOut, gate, detectorCoincidence, channelCoincidence, eventCoincidence):
		return 'perl perl/EventSearch.pl %s %s %s %s %s %s' %(dataIn,fileOut,gate,detectorCoincidence,channelCoincidence,eventCoincidence)
		
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

WireDelayMultiple()
Combine(args.combineOut)
Sort(args.combineOut, args.sortOut, args.sort_sortKey1, args.sort_sortKey2)
EventSearch(args.sortOut, args.eventCandidates, args.gate, args.detectorCoincidence, args.channelCoincidence, args.eventCoincidence)
