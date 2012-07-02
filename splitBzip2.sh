#!/bin/bash
# Copyright (c) 2012 Yahoo! Inc. All rights reserved.
# Copyrights licensed under the New BSD License. See the accompanying LICENSE file for terms.
#

# Author - @thiruvel
#
# Takes a Bzip2 file and splits it into 'N' sized multiple files.
#
# Assumes records have to be maintained and record delimiter is '\n'.
# Assumes no bzip2recovered files are present in current directory.


SIZE=${CHUNKSIZE:-16}			# in MB
TESTBZIP=${TESTBZIP:-0}
VERIFYSPLITS=${VERIFYSPLITS:-0}
NO_CHUNKS=${NO_CHUNKS:-0}
FILE=$1

# Validations
[ ! -f $FILE ] && echo "ERROR: File $FILE does not exist" >&2 && exit 1

log_status () {
	echo "$*"
	echo "reporter:status:$*" >&2
}

update_counter () {
	echo "reporter:counter:Bzip2Split,$1,$2" >&2
}

## Is the file proper bzip2?
if [ $TESTBZIP -eq 1 ]
then
	log_status "Verifying bzip integrity of $FILE"
	bzip2 -t $FILE >/dev/null 2>&1
	if [ $? -ne 0 ]
	then
		log_status "$FILE is not proper bzip2 or corrupted"
		update_counter "Files_Integrity_Failed" "1"
		echo "ERROR: problem with $FILE's integrity, bzip2 --test fails, can't proceed" >&2 && exit 1
	fi
	echo "Verification of $FILE - SUCCESS"
	update_counter "Files_Integrity_Verified" "1"
fi

## Split into multiple parts
log_status "Splitting source - $FILE"
if [ -f bzip2recover ]
then
	BZIPRECOVER=./bzip2recover
else
	BZIPRECOVER=bzip2recover
fi
$BZIPRECOVER $FILE >/dev/null 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED"
	echo "ERROR: problem with bzip2recover of $FILE, can't proceed" >&2 && exit 1
fi
echo "Splitting source - $FILE - DONE"
update_counter "SplitFiles" "1"

## Calculate number of final chunks that will be created
START_FILE=`ls | grep -E "rec[0]+1$FILE"`
PART_SIZE=`du -ks $START_FILE | awk '{print $1}'`

# Calculate how many small files combined to form a large one - size/number of chunks basis
if [ $NO_CHUNKS -eq 0 ]
then
	FILES_PER_PART=`expr \( $SIZE \* 1024 \) / $PART_SIZE`		# Approx no, actual value (+1).
else
	# Get file size, divide by no_chunks, then divide that by PART_SIZE
	FILE_SIZE=`du -ks $FILE | awk '{print $1}'`
	SIZE_OF_CHUNK=`expr $FILE_SIZE / $NO_CHUNKS`
	FILES_PER_PART=`expr $SIZE_OF_CHUNK / $PART_SIZE`
fi

## Create final file, handle records at merge points.
TMPLIST=.list
ls | grep -E "^rec.*$FILE" | sort >$TMPLIST
NO_RECORDS=`cat $TMPLIST | wc -l`

## Variables for the chunk construction - cudn't make it less ;)
CHUNK_PAT="chunk"
COUNT=1
CHUNK_COUNT=1
TMP_CHUNK=1
LAST_RECORD=.last_record
TMPFILE=.tmp
touch $LAST_RECORD

while read partfile
do
	echo -ne "Creating chunks of size $SIZE - Progress - $COUNT/$NO_RECORDS\r"
	log_status "Chunks Progress - $COUNT/$NO_RECORDS"
	CHUNK_FILE="$CHUNK_PAT-$CHUNK_COUNT-$FILE"
	[ $TMP_CHUNK -eq 1 ] && rm -f $CHUNK_FILE

	# If its the first part of a chunk, merge with remaining rec from last partfile
	if [ $TMP_CHUNK -eq 1 ]
	then
		cat $LAST_RECORD > $TMPFILE
		bunzip2 -c $partfile >> $TMPFILE
		bzip2 -c $TMPFILE > $partfile
		rm -f $TMPFILE
	fi

	# If its last partfile of the chunk, spit out the last record to be included in next chunk.
	if [ $TMP_CHUNK -eq $FILES_PER_PART -a $COUNT -ne $NO_RECORDS ]
	then
		bunzip2 -c $partfile > $TMPFILE
		tail -1 $TMPFILE > $LAST_RECORD
		sed '$d' $TMPFILE | bzip2 -c > $partfile
		rm -f $TMPFILE
	fi

	cat $partfile >> $CHUNK_FILE
	rm -f $partfile

	((COUNT+=1))
	((TMP_CHUNK+=1))

	if [ "$TMP_CHUNK" -gt $FILES_PER_PART ]
	then
		((CHUNK_COUNT+=1))
		TMP_CHUNK=1
	fi
done < $TMPLIST
echo
update_counter "MergedBzip2" "1"

if [ $VERIFYSPLITS -eq 1 ]
then
	./verifyRecordCount.sh $FILE
	if [ $? -eq 0 ]
	then
		update_counter "VerificationSuccess" "1"
	else
		update_counter "VerificationFailure" "1"
	fi
fi

rm -f $TMPLIST $LAST_RECORD $TMPFILE
