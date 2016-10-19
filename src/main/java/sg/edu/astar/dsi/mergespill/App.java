/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sg.edu.astar.dsi.mergespill;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapreduce.TaskType;

/**
 *
 * @author hduser
 */
public class App {

    
    
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        System.out.println("args 0:" + args[0]);
        //SETUP
        JobConf  job = new JobConf();
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        Class<Text> keyClass = (Class<Text>)job.getMapOutputKeyClass();
        Class<IntWritable> valClass = (Class<IntWritable>)job.getMapOutputValueClass();
        FileSystem rfs;
        CompressionCodec codec = null;
        Counters.Counter spilledRecordsCounter = null;
        rfs =((LocalFileSystem)FileSystem.getLocal(job)).getRaw();
        
        
        if ( new File(args[0]).isDirectory()){
            ArrayList<Path> spillFile = new ArrayList();
            ArrayList<Path> spillFileIndex = new ArrayList();
        
        
        
            App myApp;
            myApp = new App();
    
            myApp.getSpillFilesAndIndices(new File(args[0]), spillFile, spillFileIndex);
            
            ArrayList<SpillRecord> indexCacheList = new ArrayList<>();
            int numSpills= 0;
            
            Iterator itrSpillFileIndex = spillFileIndex.iterator();
            while (itrSpillFileIndex.hasNext()){
                numSpills++;
                Path temp = (Path)itrSpillFileIndex.next();
                System.out.println(temp);
                SpillRecord sr = new SpillRecord(temp,job);
                indexCacheList.add(sr);
                
                System.out.println("indexFile partition size: " + sr.size());
                long startOffset = 0;
                for (int i = 0;i<sr.size();i++){ //sr.size is the number of partitions
                    IndexRecord ir = sr.getIndex(i);
                    System.out.println("index[" + i + "] rawLength = " + ir.rawLength);
                    System.out.println("index[" + i + "] partLength = " + ir.partLength);
                    System.out.println("index[" + i + "] startOffset= " + ir.startOffset);
                    startOffset = ir.startOffset;
                }
                System.out.println("========================================");
            }
            System.out.println("Number of spills: " + numSpills);
            //FinalOutputFile
            Path finalOutputFile = new Path("/home/hduser/FINALOUTPUTFILE");
            FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);
            
            //ONE PARTITION ONLY
            List<Segment<Text, IntWritable>> segmentList = new ArrayList<>(numSpills);
            for (int i = 0 ; i < numSpills; i ++){
                IndexRecord theIndexRecord = indexCacheList.get(i).getIndex(0);
                Path temp = spillFileIndex.get(i);
                String temp1 = temp.toString();
                String temp2 = temp1.substring(0,temp1.length()-6);
                //System.out.println(temp2);
                //System.out.println(new Path(temp2).getParent());
                //File myFile = new File(temp2);
                //System.out.println(myFile.getPath());
                Segment<Text, IntWritable> s = new Segment<> (job, 
                                                             rfs, 
                                                             new Path(temp2),
                                                             theIndexRecord.startOffset,
                                                             theIndexRecord.partLength,
                                                             codec,
                                                             true);
                segmentList.add(i,s);
            }
                
            RawKeyValueIterator kvIter = Merger.merge(job, rfs, keyClass, 
                                                     valClass, 
                                                     null,
                                                     segmentList, 
                                                     4, 
                                                     new Path("/home/hduser/spillSample2/My"), 
                                                     job.getOutputKeyComparator(),
                                                     null,
                                                     false, 
                                                     null,
                                                     spilledRecordsCounter, 
                                                     null, 
                                                     TaskType.MAP);
                                                     
        
        
        
        
        
        }else{
            System.out.println("argument is not a directory!");
        }
        
    
    
    
    
    
    }
    
    public  void getSpillFilesAndIndices(final File folder, ArrayList spillFile, ArrayList spillFileIndex) throws IOException{
        for (final File fileEntry: folder.listFiles()){
            
                //System.out.println(fileEntry.getName());
                //System.out.println(fileEntry.getAbsolutePath());
                
                if (fileEntry.getName().endsWith(".out")){
                    spillFile.add(new Path(fileEntry.getAbsolutePath()));
                }else{
                    spillFileIndex.add(new Path(fileEntry.getAbsolutePath()));
                }
            }
    }
}
    
    

