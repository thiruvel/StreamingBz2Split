#!/bin/bash
# Copyright (c) 2012 Yahoo! Inc. All rights reserved.
# Copyrights licensed under the New BSD License. See the accompanying LICENSE file for terms.
#

while read items
do
	INPUTFILE=`echo $items | cut -d: -f1 | awk '{print $2}'`
	OUTPUTFILE=`echo $items | cut -d: -f2`

	echo "Processing files - $INPUTFILE - $OUTPUTFILE" >&2
	$HADOOP_HOME/bin/hadoop fs -get $INPUTFILE .
	FILE=`basename $INPUTFILE`
	bunzip2 $FILE
	NFILE=${FILE%%".bz2"}
	gzip $NFILE
	$HADOOP_HOME/bin/hadoop fs -put $NFILE.gz $OUTPUTFILE
	rm -f $NFILE.gz
done
