# parsl-test
Test script for debugging Swift > Parsl conversion of e-Lab analysis scripts

When untarred, Threshold files should go in ./files/

When untarred, WD files should go in the main work directory; these are also produced by the script.

The command for running the script is

python3 ShowerStudy-Test.py --thresholdAll files/6119.2010109.0.thresh files/6148.2016.0109.0.thresh --wireDelayData 6119.2016.0109.0.wd 6148.2016.0109.0.wd --geoDir ./geo --detectors 6119 6148 --firmwares 1.12 1.12 --combineOut combineOut --sort_sortKey1 2 --sort_sortKey2 3 --sortOut sortOut --gate 1000 --detectorCoincidence 2 --channelCoincidence 2 --eventCoincidence 2 --eventCandidates eventCandidates
