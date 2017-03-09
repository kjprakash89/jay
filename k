import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark._
import org.apache.spark.SparkContext._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage._
import scala.util.parsing.json._
import java.io.{ FileInputStream, IOException, InputStream }
import com.typesafe.config.ConfigFactory
import java.io.File
import java.util.Date
import java.util.SimpleTimeZone
import java.util.Calendar
import java.util.Locale
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.capitalone.enterprisedatahub.echo_jobruns_streaming.AWS

/* 
 * File Name : jobsruns_streaming.scala
* Author : oao717 v1.0
* Last Modified Date : 02/07/2017
* Purpose of file : Process the streaming data for the job runs and creates the Json file.
* */

// The main class to run the streaming application.

case class logEntry(job_name: String, order_date: String, order_id: String, rerun_count: String, date_time: String, end_date_time: String, node_id: String, description: String, documentation: String, application: String, next_run: String, application_group: String, executable_path: String, executable: String, duration: String, cpu_time: String, validityFlag: String)
object JobRunsStreaming {

  val aws = new AWS()
  //val success,failure = 0

  /**
   * Converts the String to an array of maps
   * Parameters: str - The string we would like to convert to a map.
   *
   * Return: Array[Map[String, Any]] - An array of maps.  Each map is an event, so the array will contain multiple events stored as maps
   *
   */

  val logger = Logger.getRootLogger()
  logger.setLevel(Level.ERROR)
  case class logEntry(job_name: String, order_date: String, order_id: String, rerun_count: String, date_time: String, end_date_time: String, node_id: String, description: String, documentation: String, application: String, next_run: String, application_group: String, executable_path: String, executable: String, duration: String, cpu_time: String, status: String, val_flg: String)

  def createMap(str: String): logEntry = {
    var eventString = str
     val index = eventString.indexOf("JOBNO=")
            // Ensure the event has a JOBNO and is an actual job event
         
              
    var validityflag = "V"
    // The string is split into an array of strings, then quotes and whitespace are cleaned up.
    var eventArr = eventString.split("\",")
    if(eventArr(0).contains("JOBNO"))
      {  
    eventArr = eventArr.map { _.trim }
    eventArr = eventArr.map { x =>
      if (x.last.toString == "\"") x.init
      else x
    }
   }
   else{
     println("Invalid entry")
   }
    // The array of Strings is split and the split strings are converted to a map.
    val keyValueArr = eventArr.map(x => x.split("=\""))
    val resultMap = keyValueArr.map { x =>
      if (x.size > 1)
        (x(0), x(1))
      else
        ("", "")
    }.toMap

    if (!(resultMap.contains("STARTRUN")) && !(resultMap.contains("ENDRUN")))
      validityflag = "NV"
    else if (resultMap.getOrElse("STARTRUN", "").isEmpty && resultMap.getOrElse("ENDRUN", "").isEmpty)
      validityflag = "R"
               
    return logEntry(resultMap.getOrElse("JOBNAME", ""), resultMap.getOrElse("ODATE", ""), resultMap.getOrElse("ORDERNO", ""),
      resultMap.getOrElse("RERUN_NO", ""), resultMap.getOrElse("STARTRUN", ""), resultMap.getOrElse("ENDRUN", ""),
      resultMap.getOrElse("NODEID", ""), resultMap.getOrElse("DESCRIPT", ""), resultMap.getOrElse("DOCLIB", ""),
      resultMap.getOrElse("APPLIC", ""), resultMap.getOrElse("NEXTDATE", ""), resultMap.getOrElse("APPLGROUP", ""),
      resultMap.getOrElse("MEMLIB", ""), resultMap.getOrElse("MEMNAME", ""), resultMap.getOrElse("duration", ""),
      resultMap.getOrElse("CPUTIME", ""), resultMap.getOrElse("STATUS", ""), validityflag)
        

            }
  def main(args: Array[String]) {

    //  To check the required arguments are passed to run the Job. 
    if (args.length < 3) {
      logger.error("Required Arguements Not Passed !!!")
      logger.error("Arguements: <project_directory> <env> (dev/qa/prod) <job_type>")
      System.exit(1)
    }
    //To read Command Line arguments to distinguish the environment, Like: (dev/qa/prod) and type (jobruns/incidents).
    val projdir = args(0).toString
    val env = args(1).toString
    val jobType = args(2).toString
    //Tracking the arguments in Log file
    logger.error("**********Arguements Details***********")
    logger.error("Project Directory Is:" + projdir)
    logger.error("Environment Is:" + env)
    logger.error("Job Type:" + jobType)
    logger.error("***********************************")

    try {

      //Reading the Parameters from Configuration file  
      val jobrunsconfig = ConfigFactory.parseFile(new File(projdir + "/config/" + env + "_" + jobType + "_config.cfg"))
      val zooKeeper = jobrunsconfig.getString("ZOOKEEPER")
      val brokerList = jobrunsconfig.getString("BROKERLIST")
      val topic = jobrunsconfig.getString("TOPIC")
      val groupId = jobrunsconfig.getString("GROUPID")
      val reSet = jobrunsconfig.getString("RESET")
      val bucketName = jobrunsconfig.getString("BUCKET_NAME")
      val masterFilePath = jobrunsconfig.getString("MASTERFILEPATH")
      val elasticInputPath = jobrunsconfig.getString("ELASTICINPUTPATH")
      val rejectFilePath = jobrunsconfig.getString("REJECTFILEPATH")

      logger.error("**********KAFKA Details***********")
      logger.error("ZOOKEEPER: " + zooKeeper)
      logger.error("BROKERLIST: " + brokerList)
      logger.error("Topic: " + topic)
      logger.error("Group ID: " + groupId)
      logger.error("Reset: " + reSet)
      logger.error("***********************************")

      logger.error("**********S3 Information***********")
      logger.error("Bucket Name: " + bucketName)
      logger.error("Master File Path: " + masterFilePath)
      logger.error("Elastic Input File Path: " + elasticInputPath)
      logger.error("Reject  File Path: " + rejectFilePath)
      logger.error("***********************************")

      // Creates all the default spark configurations and the aws s3 class
      val conf = new SparkConf()
      //conf.setMaster("local[*]").setAppName(jobType) or .setMaster("local[*]").setAppName("localtest") //un-comment to run the code in eclipse
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val ssc = new StreamingContext(sc, Seconds(15))
      import sqlContext.implicits._

      //ssc.checkpoint("/opt/app/":String)  //This needs to be implemented so streaming remembers where it left off.

      // The kafka parameters are set up and a stream is run based on these parameters
      val kafkaParams = Map("zookeeper.connect" -> zooKeeper, "group.id" -> groupId, "metadata.broker.list" -> brokerList, "auto.offset.reset" -> reSet)
      val jobRunStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map(topic -> 4), StorageLevel.MEMORY_ONLY).map(_._2)

      //val rdd1 = sc.parallelize(Seq("""2017-01-09 15:58:10 JOBNO="985113", ORDERNO="587735", PRIORITY="0", CRITICAL="N ", TASKTYPE="U ", CYCLIC="N ", CONFIRM_R="N ", CONFIRMED="N ", RETRO="N ", AUTOARCH="  ", TASKCLASS="   ", HOLDFLAG="N ", STATUS="N ", STATE="C ", CYCLICINT="00000M", APPLGROUP="BDW_CUST", NODEGRP="ddeprd20", NODEID="ddeprd20", MEMLIB=" ", MEMNAME="D_BDW_NGTB_OLTP_LD_DUM2", OVERLIB=" ", CMDLINE=" ", ODATE="20170109", PROCID=" ", RERUN_NO="0", OSCOMPSTAT="0", OSCOMPMSG=" ", NEXTTIME=" ", PREVDATE="20170108", NEXTDATE="20170110", STARTRUN="20170109155003", ENDRUN="20170109013042", MAXRERUN="0", FROMTIME="    ", UNTIL="    ", JOBNAME="D_BDW_NGTB_OLTP_LD_DUM2", SCHEDTAB="BDW_CUST-ddeapp20", OWNER="ddebatch", MAXWAIT="0", APPLIC="BANK-BDW_CUST_TELLR_DETL", RUNCOUNT="0", DAILYNAME="UNIX_SYS1", AJFSONSTR="YYNNNNYNNNNNN ", DESCRIPT=" ", DOCMEM=" ", DOCLIB=" ", MAXDAYS="0", MAXRUNS="0", UNKNOWNTIM="0", STARTENDCYCIND="S ", TRIGGER_TAG=" ", GROUP_ORD="0", AUTHOR="kjq887", ADJUST_COND="  ", MULTIAGENT="N ", APPLTYPE="OS        ", TIMEZONE=" ", STATEMSK="N        ", APPLVER=" ", TIMEREF="  ", CMVER="          ", APPLFORM=" ", ORDER_TIME="20170109000850", RJX_AGENT_NAME=" ", INSTREAM_IND="N ", TOLERANCE="0", CURRENT_RUN="0", CYCLIC_TYPE="C ", CPUTIME="0", ELAPTIME="0", INSTREAM_SCRIPT=" ", RUN_TIMES=" ", INTERVAL_SEQUENCE=" ", DEF_NODEGRP="ddeprd20", NODEGRP_REASON="D", BYPASS_OPTIONS="NNNNNNNNNN", CM_STATUS=" """","""2017-01-09 15:58:10 JOBNO="985113", ORDERNO="587736", PRIORITY="0", CRITICAL="N ", TASKTYPE="U ", CYCLIC="N ", CONFIRM_R="N ", CONFIRMED="N ", RETRO="N ", AUTOARCH="  ", TASKCLASS="   ", HOLDFLAG="N ", STATUS="N ", STATE="C ", CYCLICINT="00000M", APPLGROUP="BDW_CUST", NODEGRP="ddeprd20", NODEID="ddeprd20", MEMLIB=" ", MEMNAME="D_BDW_NGTB_OLTP_LD_DUM2", OVERLIB=" ", CMDLINE=" ", ODATE="20170109", PROCID=" ", RERUN_NO="0", OSCOMPSTAT="0", OSCOMPMSG=" ", NEXTTIME=" ", PREVDATE="20170108", NEXTDATE="20170110", STARTRUN="20170109155003", ENDRUN="20170109013042", MAXRERUN="0", FROMTIME="    ", UNTIL="    ", JOBNAME="D_BDW_NGTB_OLTP_LD_DUM2", SCHEDTAB="BDW_CUST-ddeapp20", OWNER="ddebatch", MAXWAIT="0", APPLIC="BANK-BDW_CUST_TELLR_DETL", RUNCOUNT="0", DAILYNAME="UNIX_SYS1", AJFSONSTR="YYNNNNYNNNNNN ", DESCRIPT=" ", DOCMEM=" ", DOCLIB=" ", MAXDAYS="0", MAXRUNS="0", UNKNOWNTIM="0", STARTENDCYCIND="S ", TRIGGER_TAG=" ", GROUP_ORD="0", AUTHOR="kjq887", ADJUST_COND="  ", MULTIAGENT="N ", APPLTYPE="OS        ", TIMEZONE=" ", STATEMSK="N        ", APPLVER=" ", TIMEREF="  ", CMVER="          ", APPLFORM=" ", ORDER_TIME="20170109000850", RJX_AGENT_NAME=" ", INSTREAM_IND="N ", TOLERANCE="0", CURRENT_RUN="0", CYCLIC_TYPE="C ", CPUTIME="0", ELAPTIME="0", INSTREAM_SCRIPT=" ", RUN_TIMES=" ", INTERVAL_SEQUENCE=" ", DEF_NODEGRP="ddeprd20", NODEGRP_REASON="D", BYPASS_OPTIONS="NNNNNNNNNN", CM_STATUS=" """","""2017-01-09 15:58:10 JOBNO="985113", ORDERNO="587737", PRIORITY="0", CRITICAL="N ", TASKTYPE="U ", CYCLIC="N ", CONFIRM_R="N ", CONFIRMED="N ", RETRO="N ", AUTOARCH="  ", TASKCLASS="   ", HOLDFLAG="N ", STATUS="N ", STATE="C ", CYCLICINT="00000M", APPLGROUP="BDW_CUST", NODEGRP="ddeprd20", NODEID="ddeprd20", MEMLIB=" ", MEMNAME="D_BDW_NGTB_OLTP_LD_DUM2", OVERLIB=" ", CMDLINE=" ", ODATE="20170109", PROCID=" ", RERUN_NO="0", OSCOMPSTAT="0", OSCOMPMSG=" ", NEXTTIME=" ", PREVDATE="20170108", NEXTDATE="20170110", STARTRUN="20170109155003", ENDRUN="20170109013042", MAXRERUN="0", FROMTIME="    ", UNTIL="    ", JOBNAME="D_BDW_NGTB_OLTP_LD_DUM2", SCHEDTAB="BDW_CUST-ddeapp20", OWNER="ddebatch", MAXWAIT="0", APPLIC="BANK-BDW_CUST_TELLR_DETL", RUNCOUNT="0", DAILYNAME="UNIX_SYS1", AJFSONSTR="YYNNNNYNNNNNN ", DESCRIPT=" ", DOCMEM=" ", DOCLIB=" ", MAXDAYS="0", MAXRUNS="0", UNKNOWNTIM="0", STARTENDCYCIND="S ", TRIGGER_TAG=" ", GROUP_ORD="0", AUTHOR="kjq887", ADJUST_COND="  ", MULTIAGENT="N ", APPLTYPE="OS        ", TIMEZONE=" ", STATEMSK="N        ", APPLVER=" ", TIMEREF="  ", CMVER="          ", APPLFORM=" ", ORDER_TIME="20170109000850", RJX_AGENT_NAME=" ", INSTREAM_IND="N ", TOLERANCE="0", CURRENT_RUN="0", CYCLIC_TYPE="C ", CPUTIME="0", ELAPTIME="0", INSTREAM_SCRIPT=" ", RUN_TIMES=" ", INTERVAL_SEQUENCE=" ", DEF_NODEGRP="ddeprd20", NODEGRP_REASON="D", BYPASS_OPTIONS="NNNNNNNNNN", CM_STATUS=" """","""2017-01-09 15:58:10 JOBNO="985113", ORDERNO="587735", PRIORITY="0", CRITICAL="N ", TASKTYPE="U ", CYCLIC="N ", CONFIRM_R="N ", CONFIRMED="N ", RETRO="N ", AUTOARCH="  ", TASKCLASS="   ", HOLDFLAG="N ", STATUS="N ", STATE="C ", CYCLICINT="00000M", APPLGROUP="BDW_CUST", NODEGRP="ddeprd20", NODEID="ddeprd20", MEMLIB=" ", MEMNAME="D_BDW_NGTB_OLTP_LD_DUM2", OVERLIB=" ", CMDLINE=" ", ODATE="20170109", PROCID=" ", RERUN_NO="0", OSCOMPSTAT="0", OSCOMPMSG=" ", NEXTTIME=" ", PREVDATE="20170108", NEXTDATE="20170110", STARTRUN="20170109155003", ENDRUN="20170109013042", MAXRERUN="0", FROMTIME="    ", UNTIL="    ", JOBNAME="D_BDW_NGTB_OLTP_LD_DUM2", SCHEDTAB="BDW_CUST-ddeapp20", OWNER="ddebatch", MAXWAIT="0", APPLIC="BANK-BDW_CUST_TELLR_DETL", RUNCOUNT="0", DAILYNAME="UNIX_SYS1", AJFSONSTR="YYNNNNYNNNNNN ", DESCRIPT=" ", DOCMEM=" ", DOCLIB=" ", MAXDAYS="0", MAXRUNS="0", UNKNOWNTIM="0", STARTENDCYCIND="S ", TRIGGER_TAG=" ", GROUP_ORD="0", AUTHOR="kjq887", ADJUST_COND="  ", MULTIAGENT="N ", APPLTYPE="OS        ", TIMEZONE=" ", STATEMSK="N        ", APPLVER=" ", TIMEREF="  ", CMVER="          ", APPLFORM=" ", ORDER_TIME="20170109000850", RJX_AGENT_NAME=" ", INSTREAM_IND="N ", TOLERANCE="0", CURRENT_RUN="0", CYCLIC_TYPE="C ", CPUTIME="0", ELAPTIME="0", INSTREAM_SCRIPT=" ", RUN_TIMES=" ", INTERVAL_SEQUENCE=" ", DEF_NODEGRP="ddeprd20", NODEGRP_REASON="D", BYPASS_OPTIONS="NNNNNNNNNN", CM_STATUS=" """"))
      //Seq(rdd1).foreach { rdd =>
      jobRunStream.foreachRDD { rdd =>
      
            //if (index > -1)
          
        //val refined = rdd.map { x => createMap(x) }
        val refined = rdd.filter{ x => x.contains("ORDERNO") }.map{ x => createMap(x) }     
        val df = sqlContext.createDataFrame(refined)
        val totalRecordCount = df.count()
            
        df.cache

        val rejectDF = df.filter($"val_flg" === "R").drop("val_flg")
        val reject_count = rejectDF.count
        
        val InvalidDF = df.filter($"val_flg" === "NV").drop("val_flg")
        val failure = InvalidDF.count
        logger.error("reject_count:"+reject_count) //uncomment while going for production
        val reject = rejectDF.select("order_id")
        //.rdd.collect
        reject.foreach { x => logger.error(x) }

        val validDF = df.filter($"val_flg" === "V").drop("val_flg")
        val success = validDF.count()
        val toUTC = udf[String, String, Int]((x, y) => {
          val sourceFormat = new SimpleDateFormat(if (y == 1 || y == 2) "yyyyMMdd" else "yyyyMMddHHmmss")
          val targetFormat = new SimpleDateFormat(if (y == 1) "yyyy-MM-dd" else (if (y == 3) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" else "yyyy-MM-dd HH:mm:ss"))
          targetFormat.format(sourceFormat.parse(x))

        })

        val startRunDF = validDF
          //.filter($"date_time".isNotNull && !trim($"date_time") == "")
          .withColumn("date_time", toUTC($"date_time", lit(3)))
          .withColumn("run_type", lit("Start"))
          .withColumn("order_date", toUTC($"order_date", lit(1)))
          .dropDuplicates()
          .drop($"end_date_time")
          .drop($"duration")
          .drop($"cpu_time")
          .select("application", Seq("order_id", "description", "node_id", "job_name", "run_type", "application_group", "rerun_count", "order_date", "date_time", "next_run", "executable", "executable_path"): _*)

        val startJson = startRunDF.toJSON

        //val startRun = startRunDF.rdd.map(x => x.mkString(","))

        val toLong = udf[Long, String](x => if (x != null && x.trim != "") x.toLong else 0L)

        //"job_name", "order_date", "order_id", "rerun_count", "end_date_time", "duration", "cpu_time", "status", "run_type
        val endRunDF = validDF
          //.filter(validDF("end_date_time").isNotNull && !trim($"end_date_time") == "")
          .withColumn("date_time", toUTC($"end_date_time", lit(3)))
          .withColumn("cpu_time", when(toLong(trim($"cpu_time")) > 9L, trim($"cpu_time").substr(0, validDF("cpu_time").toString.length - 2)).otherwise("0"))
          .withColumn("duration", when(toLong(trim($"duration")) > 9L, trim($"duration").substr(0, validDF("duration").toString.length - 2)).otherwise(""))
          .withColumn("status", when(trim($"status") === "Y", "Ended OK").otherwise("Ended Not OK"))
          .withColumn("order_date", toUTC($"order_date", lit(1)))
          .withColumn("run_type", lit("End"))
          .dropDuplicates()
          .drop($"node_id")
          .drop($"description")
          .drop($"documentation")
          .drop($"application")
          .drop($"next_run")
          .drop($"application_group")
          .drop($"executable_path")
          .drop($"executable")
          .drop($"application")
          .select("duration", Seq("order_id", "job_name", "run_type", "rerun_count", "order_date", "status", "date_time", "cpu_time"): _*)

        val endJson = endRunDF.toJSON
        val temp = validDF.col("job_name")
        val finalDF = startJson.union(endJson)
        
        val combinedJSON = "[" + finalDF.collect.toList.mkString(",") + "]"
        df.unpersist

        //Calendar.getInstance
        val currentTime = Calendar.getInstance
        // && !("".equals(temp))
        if (!(combinedJSON.isEmpty)) {
          println(currentTime.get(Calendar.YEAR).toString + "/" + currentTime.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ENGLISH) + "/" + currentTime.get(Calendar.DAY_OF_MONTH).toString + "/" + currentTime.get(Calendar.HOUR_OF_DAY).toString + currentTime.get(Calendar.MINUTE).toString + currentTime.get(Calendar.SECOND).toString)
          println("\n\n\n")
          println(combinedJSON)
         // aws.saveToAWS(final_result, bucketName, masterFilePath + currentTime.get(Calendar.YEAR).toString + "/" + currentTime.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ENGLISH) + "/" + currentTime.get(Calendar.DAY_OF_MONTH).toString + "/" + currentTime.get(Calendar.HOUR_OF_DAY).toString + currentTime.get(Calendar.MINUTE).toString + currentTime.get(Calendar.SECOND).toString)
          //currentTime.getOrElse(Calendar.YEAR).toString + "/" + currentTime.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ENGLISH) + "/" + currentTime.getOrElse(Calendar.DAY_OF_MONTH).toString + "/" + currentTime.getOrElse(Calendar.HOUR_OF_DAY).toString + currentTime.getOrElse(Calendar.MINUTE).toString + currentTime.getOrElse(Calendar.SECOND).toString)
         // aws.saveToAWS(final_result, bucketName, elasticInputPath + currentTime.get(Calendar.YEAR).toString + "/" + currentTime.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ENGLISH) + "/" + currentTime.get(Calendar.DAY_OF_MONTH).toString + "/" + currentTime.get(Calendar.HOUR_OF_DAY).toString + currentTime.get(Calendar.MINUTE).toString + currentTime.get(Calendar.SECOND).toString)
                                       aws.saveToAWSTimestamp(combinedJSON, bucketName, masterFilePath,totalRecordCount) 
           aws.saveToAWSTimestamp(combinedJSON, bucketName, elasticInputPath, totalRecordCount) 
                                
        }
        logger.error("Total Records recieved for this batch from Kafka : " + totalRecordCount )
        logger.error("------------------------------------------------------")
        logger.error("Record count which are having START & END field names : " + success)
        logger.error("Record count which dont have both START & END fields : " + failure)
        logger.error("reject_count:" + reject_count)
        //logger.error("Success records - accum_counter :" + succ_accum) -- Uncomment If we want the success count. 

      }

      ssc.start
      ssc.awaitTermination
    } catch {
      //Capturing all the possible exceptions raised
      case ce: com.typesafe.config.ConfigException => logger.error("echo_jobruns_streaming: Object JobRunsStreaming: ConfigException:\n" + ce + "Exiting with error code 1");
      case re: java.lang.RuntimeException          => logger.error("echo_jobruns_streaming: Object JobRunsStreaming: RuntimeException:\n" + re + "Exiting with error code 1");
      case e: Exception                            => logger.error("echo_jobruns_streaming: Object JobRunsStreaming: Generic Exception:\n" + e + "Exiting with error code 1");
    }
  }
}
