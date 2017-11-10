EXECUTOR_MEMORY=10240M
NUM_EXECUTORS=32
#EXECUTOR_MEMORY=5G
#NUM_EXECUTORS=2
#NUM_EXECUTORS=2
ETL_PATH="/home/tkonline/taikangtrack/ETL"
LOG_PREF=`date +%Y%m%d-%H%M`
LOG_FILE=$ETL_PATH/logs/etl-${LOG_PREF}.log
TOTAL_LOG_FILE=$ETL_PATH/logs/etl-${LOG_PREF}_total.log
DRIVER_MEM=4G
STEPS='2 3 4 5 6 7 8 10 11 13 14 15 16 17 18 19 20 21 22 25 27 28 29 30 32 33 35 37 40 44 42 43 45 999 46 47 0'
APP="com.tk.track.fact.sparksql.main.App"
#DRIVER_PARAM=--driver-memory $DRIVER_MEM
DEP_JARS='--driver-class-path /opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/jars/htrace-core-3.1.0-incubating.jar --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/jars/htrace-core-3.1.0-incubating.jar'
TIME_OUT='--conf spark.network.timeout=600'
BMHB="--conf spark.storage.blockManagerHeartBeatsMs=300000"
SLAVE_TIMEOUT="--conf spark.storage.blockManagerSlaveTimeoutMs=240000"
#flow monitor datetime
NOW_DATE_TIME=`date +%Y-%m-%d_%H:%M:%S`
/home/tkonline/taikangscore/Automation/continue-flag.sh -c get
if [ $? = 0 ]; then
  /home/tkonline/taikangscore/Automation/continue-flag.sh -c delete
sh /home/tkonline/taikangtrack/flowmonitor/flowmonitor.sh insert_jobData 用户行为-ETL处理 $NOW_DATE_TIME
  for STEP in $STEPS; do
    spark-submit $TIME_OUT $SLAVE_TIMEOUT $DEP_JARS --num-executors $NUM_EXECUTORS --conf spark.shuffle.consolidateFiles=true --conf spark.reducer.maxSizeInFlight=100 --executor-memory $EXECUTOR_MEMORY --driver-memory ${DRIVER_MEM}  --class ${APP}${STEP}  $ETL_PATH/a.jar > $ETL_PATH/logs/etl-${LOG_PREF}-$STEP.log 2>&1
  done
else
  echo NOTHING TO DO!
  exit 1;
fi
sh /home/tkonline/taikangtrack/flowmonitor/flowmonitor.sh update_jobData 用户行为-ETL处理 $NOW_DATE_TIME
/home/tkonline/taikangscore/Automation/continue-flag.sh -c create
