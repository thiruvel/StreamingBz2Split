#!/bin/bash
# Copyright (c) 2012 Yahoo! Inc. All rights reserved.
# Copyrights licensed under the New BSD License. See the accompanying LICENSE file for terms.
#

# Driver to split a HDFS directory full of BZ2 files into smaller files of gzip format.
#
# Hadoop 0.20.x does not have the ability to split bz2 files. Hence, this tool will split
# them into smaller bz2 files and convert them into gzip files. The conversion to gzip is
# only an internal limitation and could be fixed in future, though not foreseen yet. The
# additional job to convert bzip to gzip is small overhead and seems OK for now.
#
# Author: Thiruvel Thirumoolan @thiruvel
#

export TESTBZIP=0			# Bzip input files not verified by default
export CHUNKSIZE=0			# 16MB bzip2 splits by default
export NO_CHUNKS=0			# Default splits based on SIZE, no based on number of files - disabled.
export VERIFYSPLITS=0		# No verification done by default, dataquality feature, used on demand.
DEFAULT_CHUNKSIZE=4
INPUT=""
OUTPUT=""
DEBUG=${DEBUG:-1}
FILESPERMAP=1
NOMAPS=0

log_error () {
	echo "$0: ERROR: $*" >&2
}

usage () {
	cat <<EOF

$0: [-t] [-c <chunk size> | -n <number of chunks>] [-v] [-m no_maps] -i input_dir -o output_dir
	-t - Verify integrity of the input bzip2 files. OFF by default.
	-c - Chunk size of each bzip2 split in MB, final size of gzip files may vary. $DEFAULT_CHUNKSIZE by default.
	-n - Number of chunks to be generated, mutually exclusive to -c. Disabled by default.
	-v - Verify rowcounts between input and output - OFF by default.
	-m - Number of Maps to be launched, default number of maps = number of files.
	-i - Input dir. The directory should exist and contain bz2 files. Other files will be ignored.
	-o - Output dir. The directory will be cleaned if it exists and the output split files in .gz
	     format will be placed here. It will also be used as a scratch directory.
	-h - Print usage

EOF
}

while getopts "hi:o:tc:n:vm:" option
do
	case "$option" in
		t) TESTBZIP=1;;
		c) CHUNKSIZE=$OPTARG;;
		n) NO_CHUNKS=$OPTARG;;
		v) VERIFYSPLITS=1;;
		i) INPUT=$OPTARG;;
		o) OUTPUT=$OPTARG;;
		m) NOMAPS=$OPTARG;;
		h) usage
		   exit 0;;
		*) usage
		   exit 1;;
	esac
done

# Verify arguments
# Input and output paths should be provided and not empty
if [ -z "$INPUT" -o -z "$OUTPUT" ]
then
	log_error "Input/Output paths not provided"
	usage
	exit 1
fi

if [ $CHUNKSIZE -ne 0 -a $NO_CHUNKS -ne 0 ]
then
	log_error "Only one option - -c/-n can be specified"
	usage && exit 1
fi

if [ $CHUNKSIZE -eq 0 -a $NO_CHUNKS -eq 0 ]
then
	CHUNKSIZE=$DEFAULT_CHUNKSIZE
fi

# Scratch space setup for the whole processing
BZ2OUTPUT=$OUTPUT/bz2out
SCRATCH=$OUTPUT/scratchstreaming
STREAMINGOUTPUT=$OUTPUT/hadoop_streaming_todelete

# Cleanup stage
hadoop fs -rmr -skipTrash $OUTPUT

# Create input list
# Format: <input URI>:<Output Directory>
# TODO: Can we set output directory through configuration?
INPUTLIST="$SCRATCH/$USER_$$_$HOSTNAME"
hadoop fs -ls $INPUT/*.bz2 |\
	awk -v output=$BZ2OUTPUT '{
		if (NF > 3)
			printf("%s:%s\n", $NF, output);
		}' 2> /dev/null | hadoop fs -put - $INPUTLIST

if [ $DEBUG -eq 1 ]
then
	hadoop fs -cat $INPUTLIST
fi

hadoop fs -mkdir $BZ2OUTPUT

NO_LINES=`hadoop fs -cat $INPUTLIST 2>&- | wc -l`
if [ $NOMAPS -ne 0 ]
then
	FILESPERMAP=`expr $NO_LINES / $NOMAPS`
fi

if [ -x bzip2recover ]
then
	BZIP2RECOVER=bzip2recover
else
	BZIP2RECOVER=/usr/bin/bzip2recover
fi

$HADOOP_HOME/bin/hadoop  jar $HADOOP_HOME/hadoop-streaming.jar \
		-D mapreduce.fileoutputcommitter.marksuccessfuljobs=false	\
		-D hadoop.job.history.user.location=none	\
		-D mapred.line.input.format.linespermap=$FILESPERMAP \
		-cmdenv TESTBZIP=$TESTBZIP	\
		-cmdenv CHUNKSIZE=$CHUNKSIZE	\
		-cmdenv VERIFYSPLITS=$VERIFYSPLITS	\
		-cmdenv NO_CHUNKS=$NO_CHUNKS	\
		-input $INPUTLIST	\
		-output $STREAMINGOUTPUT	\
		-mapper splitFile.sh	\
		-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
		-outputformat org.apache.hadoop.mapred.lib.NullOutputFormat	\
		-file splitFile.sh	\
		-file splitBzip2.sh	\
		-file verifyRecordCount.sh \
		-file $BZIP2RECOVER \
		-numReduceTasks 0

if [ $? -ne 0 ]
then
	log_error "Split job failed, please inspect logs"
	log_error "Directories not cleaned on HDFS - $BZ2OUTPUT, $STREAMINGOUTPUT, $SCRATCH"
	exit 1
fi

# Cleanup
hadoop fs -rmr -skipTrash $STREAMINGOUTPUT $SCRATCH

# Display
if [ $DEBUG -eq 1 ]
then
	hadoop fs -lsr $BZ2OUTPUT
fi

#
# Do the GZIP merge
#

# Create input list
INPUTLIST="$SCRATCH/$USER_$$_$HOSTNAME"
hadoop fs -ls $BZ2OUTPUT/*.bz2 |\
	awk -v output=$OUTPUT '{
		if (NF > 3)
			printf("%s:%s\n", $NF, output);
		}' 2> /dev/null | hadoop fs -put - $INPUTLIST

hadoop fs -cat $INPUTLIST

$HADOOP_HOME/bin/hadoop  jar $HADOOP_HOME/hadoop-streaming.jar \
		-D mapreduce.fileoutputcommitter.marksuccessfuljobs=false	\
		-D hadoop.job.history.user.location=none	\
		-D mapred.line.input.format.linespermap=$FILESPERMAP \
		-input $INPUTLIST	\
		-output $STREAMINGOUTPUT	\
		-mapper createGzipFromBzip.sh	\
		-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
		-outputformat org.apache.hadoop.mapred.lib.NullOutputFormat	\
		-file createGzipFromBzip.sh	\
		-numReduceTasks 0

if [ $? -ne 0 ]
then
	log_error "Cannot convert bz2 into gzip files"
	log_error "Directories not cleaned on HDFS - $BZ2OUTPUT, $STREAMINGOUTPUT, $SCRATCH"
	exit 1
fi

# Cleanup
hadoop fs -rmr -skipTrash $BZ2OUTPUT $STREAMINGOUTPUT $SCRATCH

if [ $DEBUG -eq 1 ]
then
	hadoop fs -lsr $OUTPUT
fi
