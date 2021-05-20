#!/bin/sh
#-----------------------------------------------------
# Descrption : 
#-----------------------------------------------------
#
# Script Name:
# Usage		 :
# Dependecy  :
#
#-----------------------------------------------------
# Date 		  Developer				 Change Descrption
#-----------------------------------------------------
#13-May-21		Venkat 				 Initial Draft
#-----------------------------------------------------

#Folders creations
mkdir -p {unzipped_csv/files, files, logs}
hdfs dfs -mkdir -p /user/cloudera/hdfs/inputs_py
hdfs dfs -mkdir -p /user/harsha/hdfs/hdfs_path2
dt=`date +%Y_%m_%d_%H_%M_%S`

#dt=2021_05_18  ==== > mysql.sh 
#DT='date +%Y.%m.%d %H:%M:%S'

# Importing main param file

source script.param

echo  "\n ######################## Logfile:$dt ########################" | tee -a ${d_log}
# Executing the Python file 
python $python_file/python_pro.py | tee -a ${d_log}

#python3 /Users/arjunkummetha/Desktop/Venkat_Project_work/python_pro.py | tee -a ${d_log}
rc=$?
	if [ $rc -eq 0 ]
	then 
		echo  " \n \n Python execution successfully completed " | tee -a ${d_log}
	else
		echo  " \n \n Python execution failed" | tee -a ${d_log}
	    exit 1
	fi

#Unziping the CSV files which are generated from Python execution
unzip venkat_files.zip -d unzipped_csv/files/$dt/ | tee -a ${d_log}   


# Checking the Log path avialble or not
# if [ -d $d_log ]
#then
#	chmod -R 777 $d_log
#	echo "Logdirectory $local_path exists" | tee -a ${d_log}
#else
#	echo "Log directory not created, Check it once $d_log" | tee -a ${d_log}
#fi


# Checking the Input locaton path avialble or not
 if [ -d $local_path ]
then
	chmod -R 777 $local_path
	echo "Input directory $local_path exists" | tee -a ${d_log}
else
	echo "Input directory not created, Check it once $local_path" | tee -a ${d_log}
	exit 1
fi


# Checking the HDFS locaton path avialble or not
 if(hdfs dfs -test -d $hdfs_path)
then
	hdfs dfs -chmod -R 777 $hdfs_path
	echo "Input directory $hdfs_path exists" | tee -a ${d_log}
else
	echo "Input directory not created, Check it once $hdfs_path" | tee -a ${d_log}
fi


# Checking the HDFS locaton path avialble or not
# if(hdfs dfs -test -d $hdfs_path)
#then
#	hdfs dfs -chmod -R 777 $hdfs_path
#	echo "Input directory $hdfs_path exists" | tee -a ${d_log}
#else
#	echo "Input directory not created, Check it once $hdfs_path" | tee -a ${d_log}
#fi

# Copying the data from Local system to Hdfs layer 
hdfs dfs -put ${local_path} ${hdfs_path}
rc=$?
	if [ $rc -eq 0 ]
		then 
		echo  " \n \n File successfully copied to HDFS location 1 from local path" | tee -a ${d_log}
	else
		echo  " \n \n File not copied to HDFS location check it once" | tee -a ${d_log}
	fi


# Copying Hdfs layer to another HDFS layer 2 
# If incase if destination dir diferent server then we have to use Discp
#hdfs dfs -cp ${hdfs_path}/$dt ${hdfs_path2}
#rc=$?
#	if [ $rc -eq 0 ]
#		then 
#		echo  " \n \n File successfully copied to HDFS location 2 from HDFS Location 1" | tee -a ${d_log}
#	else
#		echo  " \n \n File not copied to HDFS2 location check it once" | tee -a ${d_log}
#	fi


scp <i.paddresss> soucre target 



# calling mysql scirt 


sh mysql.sh | tee -a ${d_log}

# Sending mail to DL
#
#echo -e "\n \n Sending mail to DL"
#DL='kmrsoft.harsha@gmail.com, hasrsha.bigdata.python@gmail.com'
#
#echo -e "Please open the attachment to get the complete detais" | mailx -s  "File_arrival_Report" -a ${d_log} {DL}
#rc=$?
#	if [ $rc -eq 0 ]
#		then 
#		echo -e " \n \n Mail sent successfully \n  " | tee -a ${d_log}
#	else
#		echo -e " \n \n Unable to send Mail,check !" | tee -a ${d_log}
#	fi



# Deleting the local files which are older than 3 days
echo  " \n\n Deleting the local files which are older than 3 days "


#find $local_path/* -mtime +3 -exec rm {} \; | tee -a ${d_log}
#find $logs/* -mtime +3 -exec rm {} \; | tee -a ${d_log}
#
#rc=$?
#	if [ $rc -eq 0 ]
#		then 
#		echo  " \n \n  successfully deleted the files old files" | tee -a ${d_log}
#	else
#		echo  " \n \n  Unable to delete the old files" | tee -a ${d_log}
#	fi
#





# Calling my sql script to sending the  hdfs location2 to mysql table
#echo  "\n\n Calling my sql script to sending the  hdfs location2 to mysql table" | tee -a ${d_log}

#mysql -h "localhost" -u root "-pHarsha.123$" "venkat_mysql" < "filename.sql" | tee -a ${d_log}
#mysql -h "localhost" -u $userid -p$pwd "venkat_mysql" < "filename.sql" | tee -a ${d_log}
#
#rc=$?
#	if [ $rc -eq 0 ]
#		then 
#		echo  " \n \n  successfully files copied to Mysql tables " | tee -a ${d_log}
#	else
#		echo  " \n \n  Unable to copy data to Mysql tables,check !!" | tee -a ${d_log}
#	fi

#mysql -u root "-pHarsha.123$" -e $query






echo "\n \n"
echo "###############################################################################" | tee -a ${d_log}
echo "################################    END   #####################################" | tee -a ${d_log}
echo "###############################################################################" | tee -a ${d_log}