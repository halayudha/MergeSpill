/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sg.edu.astar.dsi.mergespill;

import java.io.File;
import java.io.IOException;
import static java.lang.Thread.sleep;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.TaskType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

/**
 *
 * @author hduser
 */
public class App {

    public static void main(String[] args) {
        // TODO code application logic here
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket clients = context.socket(ZMQ.ROUTER);
        clients.bind("tcp://*:5556");
        
        ZMQ.Socket workers = context.socket(ZMQ.DEALER);
        workers.bind("inproc://workers");
        
        for (int thread_nbr = 0; thread_nbr < 50; thread_nbr++){
            Thread worker = new Worker (context);
            worker.start();
        }
        
        //Connect work threads to client threads via a queue
        ZMQ.proxy(clients, workers, null);
        
        clients.close();
        workers.close();
        context.term();
    }
    
    private static class Worker extends Thread{
        private Context context;
        private Worker (Context context){
            this.context = context;
        }
        @Override
        public void run(){
            ZMQ.Socket socket = context.socket(ZMQ.REP);
            socket.connect("inproc://workers");
            while (true){
                byte[] request = socket.recv(0);
                socket.send("",0);
                SpecificDatumReader<mergeinfo> reader = new SpecificDatumReader<mergeinfo>(mergeinfo.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(request, null);
                try{
                    mergeinfo mInfo = reader.read(null,decoder);
                    System.out.println("MINFO DATA");
                    System.out.println(mInfo.getMaptaskid());
                    System.out.println(mInfo.getNumberofreducer());
                    System.out.println(mInfo.getNumberofspill());
                    System.out.println(mInfo.getReduceno());
                    System.out.println(mInfo.getJobid());
                    String directory = "/home/hduser/"+mInfo.getJobid()+File.separator+mInfo.getMaptaskid().substring(0,35)+File.separator+"reduce_"+mInfo.getReduceno();
                    doProcess(directory, mInfo.getNumberofspill());
                } catch (IOException ex) {
                    Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    
    public synchronized static void  doProcess(String directory, int spillNumber) throws IOException, InterruptedException{
        // TODO code application logic here
        System.out.println("directory: " + directory);
        System.out.println("numberOfSpill: " + spillNumber);
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
        
        while( !new File(directory).isDirectory()){
            sleep(5000);
        }
        
        if ( new File(directory).isDirectory()){
            ArrayList<Path> spillFile = new ArrayList();
            ArrayList<Path> spillFileIndex = new ArrayList();
        
        
        
            App myApp;
            myApp = new App();
    
            myApp.getSpillFilesAndIndices(new File(directory), spillFile, spillFileIndex, spillNumber);
            
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
            Path finalOutputFile = new Path(directory + File.separator + "FINALOUTPUTFILE");
            FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);
            System.out.println("GOT HERE 1");
            Path finalIndexFile = new Path(directory + File.separator + "FINALOUTPUTFILE.index");
            
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
            System.out.println("GOT HERE 2");    
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
            System.out.println("GOT HERE 3");
            //write merged output to disk
            long segmentStart = finalOut.getPos();
            FSDataOutputStream finalPartitionOut = CryptoUtils.wrapIfNecessary(job, finalOut);
            Writer<Text, IntWritable> writer = new Writer<Text, IntWritable>(job,
                            finalPartitionOut,
                            Text.class,
                            IntWritable.class,
                            codec,
                            spilledRecordsCounter);
            System.out.println("GOT HERE 4");
            Merger.writeFile(kvIter, writer, null, job);
            writer.close();
            finalOut.close();
            System.out.println("GOT HERE 5");
        
            IndexRecord rec = new IndexRecord();
            final SpillRecord spillRec = new SpillRecord(1);
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
            rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
            System.out.println("rec.startOffset: " + rec.startOffset);
            System.out.println("rec.rawLength  : " + rec.rawLength);
            System.out.println("rec.partLength : " + rec.partLength);
            spillRec.putIndex(rec, 0);
            spillRec.writeToFile(finalIndexFile, job);
            System.out.println("GOT HERE 6");
        
        
        
        }else{
            System.out.println("argument is not a directory! : " + directory);
        }
        
    
    
    
    
    
    }
    
    public  void getSpillFilesAndIndices(final File folder, ArrayList spillFile, ArrayList spillFileIndex, int spillNumber) throws IOException, InterruptedException{
        while(folder.listFiles().length < spillNumber*2){
            
        }
        
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
    
    

