
/*
 * HadoopJob.java
 *
 * Created on Apr 15, 2012, 5:31:25 PM
 */

package HitterLog;


import java.util.concurrent.Callable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 *
 * @author mike
 */
public class HadoopJob implements Callable<String> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("HitterLog.HadoopJob");

    /*
     * Arguments which are only known at runtime, and might change
     * from one run to another can be added to this file as Properties.
     * In Netbeans, right-click, select "Insert Code..." and "Property".
     *
     * Two example properties are given here, for simple jobs.
     *
     * Hint: If you use obscure types, register a PropertyEditor for them.
     */

    private String inputPaths;

    public void setInputPaths(String inputPaths) {
        this.inputPaths = inputPaths;
    }

    public String getInputPaths() {
        return inputPaths;
    }

    private String outputPath;

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    private RunningJob runningJob;

    public RunningJob getRunningJob() {
        return runningJob;
    }

    public String call() throws Exception {
        JobConf job = new JobConf();
        job.setJarByClass(getClass());

        /* Autogenerated initialization. */
        initJobConf(job);

        /* Custom initialization, if any. */
        initCustom(job);

        /* This is an example of how to set input and output. */
        FileInputFormat.setInputPaths(job, getInputPaths());
        if (getOutputPath() != null)
            FileOutputFormat.setOutputPath(job, new Path(getOutputPath()));

        /* You can now do any other job customization. */
        // job.setXxx(...);

        /* And finally, we submit the job.
         * If you run the job from within Karmasphere Studio, this returned
         * RunningJob or JobID is given to the monitoring UI. */
        JobClient client = new JobClient(job);
        this.runningJob = client.submitJob(job);
        return runningJob.getID().toString();
    }

    /**
     * This method is executed by the workflow
     */
    public static void initCustom(JobConf conf) {
        // Add custom initialisation here, you may have to rebuild your project before
        // changes are reflected in the workflow.
    }

    /** This method is called from within the constructor to
     * initialize the job.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Job Editor.
     */
    
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initJobConf
    public static void initJobConf(JobConf conf) {
// Generating code using Karmasphere Protocol for Hadoop 0.18
// CG_GLOBAL

// CG_INPUT_HIDDEN
conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);

// CG_MAPPER_HIDDEN
conf.setMapperClass(HitterLog.HadoopMapper.class);

// CG_MAPPER
conf.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
conf.setMapOutputValueClass(org.apache.hadoop.io.DoubleWritable.class);

// CG_PARTITIONER_HIDDEN
conf.setPartitionerClass(org.apache.hadoop.mapred.lib.HashPartitioner.class);

// CG_PARTITIONER

// CG_COMPARATOR_HIDDEN
conf.setOutputKeyComparatorClass(org.apache.hadoop.io.Text.Comparator.class);

// CG_COMPARATOR

// CG_COMBINER_HIDDEN

// CG_REDUCER_HIDDEN
conf.setReducerClass(HitterLog.HadoopReducer.class);

// CG_REDUCER
conf.setNumReduceTasks(1);
conf.setOutputKeyClass(org.apache.hadoop.io.Text.class);
conf.setOutputValueClass(org.apache.hadoop.io.Text.class);

// CG_OUTPUT_HIDDEN
conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

// CG_OUTPUT

// Others
}









    // </editor-fold>//GEN-END:initJobConf

    /**
     * The main method.
     *
     * You can add custom argument parsing here.
     */
    public static void main(String[] args) throws Exception {
        HadoopJob job = new HadoopJob();
        if (args.length >= 1)
            job.setInputPaths(args[0]);
        if (args.length >= 2)
            job.setOutputPath(args[1]);
        job.call();
        /* Wish we didn't have to reproduce code from runJob() here. */
        job.getRunningJob().waitForCompletion();
    }

}
