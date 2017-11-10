EXECUTOR_MEMORY=10240M
NUM_EXECUTORS=64
#exit 1;
#EXECUTOR_MEMORY=5G
#NUM_EXECUTORS=2
#NUM_EXECUTORS=2
ETL_PATH="/home/tkonline/taikangtrack/ETL"
LOG_PREF=`date +%Y%m%d-%H%M`
LOG_FILE=$ETL_PATH/logs/etl-${LOG_PREF}.log
TOTAL_LOG_FILE=$ETL_PATH/logs/etl-${LOG_PREF}_total.log
DRIVER_MEM=4G
STEPS='1 9'
APP="com.tk.track.fact.sparksql.main.App"
#DRIVER_PARAM=--driver-memory $DRIVER_MEM
DEP_JARS='--driver-class-path /opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/jars/htrace-core-3.1.0-incubating.jar --conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/jars/htrace-core-3.1.0-incubating.jar'

if [ $? = 0 ]; then
  for STEP in $STEPS; do
    spark-submit $DEP_JARS --num-executors $NUM_EXECUTORS --executor-memory $EXECUTOR_MEMORY --driver-memory ${DRIVER_MEM}  --class ${APP}${STEP}  $ETL_PATH/a.jar >> ${LOG_FILE} 2>&1
  done
else
  echo NOTHING TO DO!
  exit 1;
fi