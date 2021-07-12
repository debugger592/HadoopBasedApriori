package apriori;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Class Configuration : https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/conf/Configuration.html

public class AprioriDriver extends Configured implements Tool {
    static String jobPrefix = "Phase ";

    enum File{
        LINES_WRITTEN
    }

    public static final Log log = LogFactory.getLog(AprioriDriver.class);

    // The run() method constructs a Job object based on the tool's configuration, which it uses to launch a job.
    // Among the possible job configuration parameters, we set the input and output file paths, the mapper,
    // reducer etc..
    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 6) {
            System.err.println("USAGE : hadoop jar HadoopBasedApriori.jar AprioriDriver [dataSet] [outputFileName] [passNum] [minSup] [minConf] [numTransactions]");
            return -1;
        }

        String hdfsInputDir = args[0];                        // dataSet
        String hdfsOutputDirPrefix = args[1];                 // outputFileName
        int maxPasses = Integer.parseInt(args[2]);            // passNum
        Double minSup = Double.parseDouble(args[3]);          // minSup
        Double minConf = Double.parseDouble(args[4]);          // minConf
        Integer numTransactions = Integer.parseInt(args[5]);  // numberOfTransaction
        int i;
        for (i = 1; i <= maxPasses; i++) {
            boolean isPassKJobDone = runPassKJob(hdfsInputDir, hdfsOutputDirPrefix, i, minSup, numTransactions);

            if (!isPassKJobDone) {
//                System.err.println("Phase1 MapReduce job failed. Exiting !!");
                break;
            }
        }
        //Run last map-only job
        runMapOnlyJob(hdfsOutputDirPrefix,i-1,minSup,minConf,numTransactions);
        return 1;
    }



    static boolean runPassKJob(String inputDir, String outputDirPrefix, int passNum, Double minSup, Integer numTransactions)
            throws IOException, InterruptedException, ClassNotFoundException {

        boolean isJobSuccess;

        // Create configuration
        Configuration conf = new Configuration();
        conf.setInt("passNum", passNum);
        conf.setDouble("minSup", minSup);
        conf.setInt("numTxns", numTransactions);
        conf.set("outPrefix",outputDirPrefix);
        System.out.println("Starting Phase" + passNum + "Job");

        // Create job
        Job job = Job.getInstance(conf, jobPrefix + passNum);

        job.setJarByClass(AprioriDriver.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDirPrefix, "pass" + passNum));

        // Specify key / value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Setup map reduce job
        if (passNum == 1) {
            job.setMapperClass(AprioriPass1Mapper.class);
        }
        else {
            job.setMapperClass(AprioriPassKMapper.class);
        }
        job.setReducerClass(AprioriReducer.class);

        // Execute job
        isJobSuccess = (job.waitForCompletion(true));
        long linesWritten=job.getCounters().findCounter(File.LINES_WRITTEN).getValue();
        System.out.println("Lines written: "+linesWritten);
        if(linesWritten==0) {
            System.out.println("Phase" + passNum + " returned empty result. Finished iterations.");
            isJobSuccess=false;
        }
        System.out.println("Finished Phase" + passNum + "Job");

        return isJobSuccess;
    }

    static void runMapOnlyJob(String outputDirPrefix, int lastPass,Double minSupp, Double minConf, Integer numTransactions) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("passNum", lastPass);
        conf.setDouble("minConf", minConf);
        conf.setDouble("minSup", minSupp);
        conf.setInt("numTxns", numTransactions);
        conf.set("outPrefix",outputDirPrefix);
        System.out.println("Starting Association Rule Job");

        Job job = Job.getInstance(conf,"Rule generation phase");

        job.setJarByClass(AprioriDriver.class);

        FileInputFormat.addInputPath(job, new Path(outputDirPrefix+"/pass"+lastPass,"part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(outputDirPrefix, "result"));

        // Specify key / value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AssociationMapper.class);
        job.setNumReduceTasks(0);

        boolean isJobSuccess = (job.waitForCompletion(true));
        long linesWritten=job.getCounters().findCounter(File.LINES_WRITTEN).getValue();
        System.out.println("Mined "+linesWritten+" rule/s");
        System.out.println("Finished All Jobs");

    }



    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AprioriDriver(), args);
        System.exit(exitCode);
    }
}
