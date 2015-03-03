#!/bin/sh
HOME_DIR=/home/ubuntu
DATA_DIR=$HOME_DIR/data
LOG_DIR=$HOME_DIR/logs
MANIFEST_DIR=$HOME_DIR/manifest
BUCKET_PATH=s3://monitorscripts/manifest

DB_HOST="samplecluster.cwtfton66evp.us-west-2.redshift.amazonaws.com"
DB_PORT=5439
DB_NAME=testdb
DB_USER=testusr
DB_PASS=Testusr123

cat > $HOME_DIR/.pgpass << EOF
$DB_HOST:$DB_PORT:$DB_NAME:$DB_USER:$DB_PASS
EOF
chmod 0600 $HOME_DIR/.pgpass
#export PGPASSWORD="$DB_HOST:$DB_PORT:$DB_NAME:$DB_USER:$DB_PASS"

make_manifest_entry()
{

	#HOST_IP="54.68.82.209"
	MANIFEST_FILE=DB_$4_$(date +%y%m%d).MF
	HOST_IP="10.0.0.106"
	HOST_PUBLIC_KEY=`cat /etc/ssh/ssh_host_rsa_key.pub | cut -d " " -f2`
	HOST_USER="ubuntu"

	if [ $1 = 1 ]; then
		echo "{\n\t\"entries\"\t:\t[" > $MANIFEST_DIR/$MANIFEST_FILE
	fi
	echo "\n\t\t{\"endpoint\"\t:\t\"$HOST_IP\",\n\t\t\"command\"\t:\t\"$3\","\
		"\n\t\t\"mandatory\"\t:\ttrue,\n\t\t\"publickey\"\t:\t\"$HOST_PUBLIC_KEY\","\
		"\n\t\t\"username\"\t:\t\"$HOST_USER\"" >> $MANIFEST_DIR/$MANIFEST_FILE
	if [ $1 = $2 ] ; then
		echo "\n\t\t}\n\t]\n}" >> $MANIFEST_DIR/$MANIFEST_FILE
	else
		echo "\n\t\t}," >> $MANIFEST_DIR/$MANIFEST_FILE
	fi

	echo $MANIFEST_FILE	
}

#uploading the manifest file to s3 bucket
upload_manifest_to_s3()
{
	UPLOAD_RESULT=`aws s3 cp $MANIFEST_DIR/$1 $BUCKET_PATH/`
	echo $UPLOAD_RESULT
	error=`echo $UPLOAD_RESULT | grep "Errorno" 2>&1`
	if  [ -n "$error" ] || [ "$error" != "" ] ; then
		echo $UPLOAD_RESULT >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		exit 1
	fi
}

upload_data_to_cluster()
{
	AWS_ACCESS_KEY_ID="AKIAIY67DF5O64F4QMFA"
	AWS_SECRET_ACCESS_KEY="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"
#	SQL_FILE_NAME=$HOME_DIR/scripts/"REDSHIFT_LOAD_FILE.sql"
        
	COPY_CMD="copy $2 from '$BUCKET_PATH/$1' CREDENTIALS 'aws_access_key_id=$AWS_ACCESS_KEY_ID;aws_secret_access_key=$AWS_SECRET_ACCESS_KEY'ssh maxerror 5 COMPUPDATE;"

        LOAD_RESULT=`psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$COPY_CMD" 2>&1`
        error=`echo $LOAD_RESULT | grep -i "stl_load_errors" 2>&1`
	if [ -n "$error" ] || [ "$error" != "" ] ; then
		err_cnt=`echo $error | grep "record(s)* could not be loaded" | cut -d "," -f3 |cut -d " " -f2`
		echo $err_cnt
                echo $LOAD_RESULT >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		SQL_CMD="select querytxt,a.starttime,a.endtime,line_number,raw_field_value,err_reason from stl_query a, stl_load_errors b where a.query=b.query limit $err_cnt;"
        	LOAD_RESULT=`psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -A -t -c "$SQL_CMD" 2>&1`
		echo $LOAD_RESULT
		echo "COPY CMD : "`echo $LOAD_RESULT|cut -d "|" -f1` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		echo "START TIME : "`echo $LOAD_RESULT|cut -d "|" -f2` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		echo "END TIME : "`echo $LOAD_RESULT|cut -d "|" -f3` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		echo "LINE NUMBER : "`echo $LOAD_RESULT|cut -d "|" -f4` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		echo "VALUE : "`echo $LOAD_RESULT|cut -d "|" -f5` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log
		echo "REASON : "`echo $LOAD_RESULT|cut -d "|" -f6` >> $LOG_DIR/ERROR_LOAD_$(date +%y%m%d).log

		
	else
        	echo $LOAD_RESULT >> $LOG_DIR/SUCCESS_LOAD_$(date +%y%m%d).log
        fi
}


#Checks whether the zipped files are present for the given table
if [ ! -d "$DATA_DIR" ] ; then
        echo "Data Directory  is not present"
        exit 1
fi

Tables=`ls $DATA_DIR`
echo $Tables

for TABLE_NAME in $Tables
do
	zip_file_count=`ls $DATA_DIR/$TABLE_NAME|wc -w`
	count=1

	for files in `ls $DATA_DIR/$TABLE_NAME`
	do
		MANIFEST_FILE=$( make_manifest_entry $count $zip_file_count "cat $DATA_DIR/$TABLE_NAME/$files" $TABLE_NAME )
 		echo $MANIFEST_FILE
		count=$((count+1))
	done
	upload_manifest_to_s3 $MANIFEST_FILE
	upload_data_to_cluster $MANIFEST_FILE $TABLE_NAME
done
