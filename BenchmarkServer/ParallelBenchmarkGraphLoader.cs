using System;
using System.Collections.Generic;
using System.IO;
using Trinity;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;


namespace BenchmarkServer
{
    public class ParallelBenchmarkGraphLoader
    {
        // NODES WITH NO OUTGOING EDGES ARE NOT CONSIDERED!
        // cat example-directed.e | awk '{print $1}' | uniq | grep -w -v -f - example-directed.v

        public long max_node;
        public long[] mapping1;
        public Queue<long> vertex_queue = new Queue<long>();
        public String path = "/home/jkliss/";
        public String vpath = "";
        public Dictionary<long, long> mapping2 = new Dictionary<long, long>();
        public long elapsedTime_lastLoadEdge = 0;
        public long elapsedTime_lastLoadVertex = 0;
        public bool hasWeight = false;
        public bool directed = false;
        public static int num_threads = Environment.ProcessorCount;
        public static int num_servers = 2;
        public Thread[] threads = new Thread[num_threads];
        public ConcurrentQueue<SimpleGraphNode>[] thread_cache = new ConcurrentQueue<SimpleGraphNode>[num_threads];
        public bool finished = false;
        public DistributedLoad[] distributedLoads = new DistributedLoad[num_servers];
        public int[] distributed_load_current_index = new int[num_servers];
        public bool[] serverFinished = new bool[num_servers];
        public int this_server_id;
        public int finish_counter = 0;

        // PARALLEL READER
        public int num_parts = num_servers * num_threads;
        public long[] all_starts = new long[num_servers];
        public long[] thread_starts = new long[num_threads];
        int finished_readers = 0;

        public ConcurrentQueue<Load>[] load_sender_queue = new ConcurrentQueue<Load>[num_servers];
        public Thread[] load_sender_threads = new Thread[num_servers];

        public void setPath(String new_path){
            path = new_path;
        }

        public void setVertexPath(String new_path){
            vpath = new_path;
        }

        public long getMaxNode(){
            return max_node;
        }

        public void loadVertices(){
          this_server_id = Global.MyServerID;
          if(this_server_id == 0){
            prepareParallelRead();
          }
          vertex_queue = new Queue<long>();
          var watch = System.Diagnostics.Stopwatch.StartNew();
          Console.WriteLine("Read Vertex File at: {0}", vpath);
          using (StreamReader reader = new StreamReader(vpath))
          {
              string line;
              long read_lines = 0;
              while (null != (line = reader.ReadLine()))
              {
                  read_lines++;
                  if (read_lines % 1000000 == 0)
                  {
                      Console.Write(" lines read: " + read_lines / 1000000 + "M\r");
                  }
                  try
                  {
                      long read_node = long.Parse(line);
                      vertex_queue.Enqueue(read_node);
                      mapping2[read_node] = read_lines;
                  }
                  catch (Exception ex)
                  {
                      Console.Error.WriteLine("Failed to import the line: (only numeric edges)");
                      Console.Error.WriteLine(ex.Message);
                      Console.Error.WriteLine(ex.StackTrace.ToString());
                      Console.Error.WriteLine(line);
                  }
              }
              max_node = read_lines;
          }
          mapping1 = vertex_queue.ToArray();
          var elapsedMs = watch.ElapsedMilliseconds;
          elapsedTime_lastLoadVertex = elapsedMs;
          Console.WriteLine("----------------------------------");
          Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadVertex);
          Console.WriteLine("##################################");
          Console.WriteLine("####### All vertices loaded ######");
          Console.WriteLine("##################################");
        }

        public void prepareParallelRead(){
          // Prepare Finish Communicators for all Server and Threads
          for(int i = 0; i < num_servers; i++){
            serverFinished[i] = false;
            all_starts[i] = -1;
            if(i != this_server_id){
              load_sender_queue[i] = new ConcurrentQueue<Load>();
              load_sender_threads[i] = new Thread(new ParameterizedThreadStart(SenderThread));
              load_sender_threads[i].Start(i);
            }
            for(int j = 0; j < num_threads+1; j++){
              FinishCommunicator fc = new FinishCommunicator();
              fc.Finished = false;
              fc.LastLoad = false;
              fc.startReading = -1;
              long fcid = (j+(i*num_threads));
              //Console.WriteLine("CREATE COMMUNICATION CELL:" + fcid);
              Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-fcid, fc);
            }
          }
        }

        public void LoadGraph()
        {
            num_servers = Global.ServerCount;
            finished = false;
            Thread[] read_threads = new Thread[num_threads];
            var watch = System.Diagnostics.Stopwatch.StartNew();
            Console.WriteLine("Read File at: {0}", path);
            // Create Worker Threads

            threads = new Thread[num_threads];
            for(int i = 0; i < num_threads; i++){
              threads[i] = new Thread(new ParameterizedThreadStart(ConsumerThread));
              thread_cache[i] = new ConcurrentQueue<SimpleGraphNode>();
              threads[i].Start(i);
            }
            for(int i = 0; i < num_servers; i++){
              //distributed_load_current_index[i] = 0;
            }
            for(int i = num_threads+(num_threads*this_server_id); i >= (num_threads*this_server_id)+1; i--){
              Console.WriteLine("Start " + i);
              read_threads[i-(num_threads*this_server_id)-1] = new Thread(new ParameterizedThreadStart(ParallelReading));
              read_threads[i-(num_threads*this_server_id)-1].Start(i);
            }
            for(int i = 0; i < num_threads; i++){
              read_threads[i].Join();
            }
        }

        public void ParallelReading(object par_part)
        {
            int part = (int) par_part;
            int read_thread = part-1-(this_server_id*num_threads);
            Console.WriteLine("Start Read Thread " + read_thread + " for Part: " + (part-1));
            long length = new FileInfo(path).Length;
            string[] fields;
            long read_node = -1;
            long current_node = -1;
            long first_read_node;
            int vertices_position = 0;
            using (FileStream fs = new FileStream(@path, FileMode.Open, FileAccess.Read)) {
              Console.WriteLine("["+part+"] JUMP TO: " + ((length/num_parts)*(part-1)));
              fs.Seek((length/num_parts)*(part-1), SeekOrigin.Begin);
              string line = "";
              using(StreamReader sr = new StreamReader(fs)){
                  // SKIP ALL LINES UNTIL NEW NODE REACHED
                  if(part != 1){
                    // SKIP FIRST LINE AS JUMP CAN BE BETWEEN LINES
                    sr.ReadLine();
                    // READ FIRST LINE
                    line = sr.ReadLine();
                    fields = line.Split(' ');
                    read_node = long.Parse(fields[0]);
                    current_node = read_node;
                    // JUMP OVER ALL LINES OUTGOING FROM THIS NODE
                    while(current_node == read_node){
                      line = sr.ReadLine();
                      if(line == null) break;
                      fields = line.Split(' ');
                        read_node = long.Parse(fields[0]);
                    }
                    // FIRST NODE IS READ AT THIS POINT
                    FinishCommunicator fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part-1));
                    fc.startReading = read_node;
                    Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-(part-1), fc);
                    // SKIP ALL PREVIOUS POSITIONS
                    while(mapping1[vertices_position] < read_node){
                      vertices_position++;
                    }
                  } else {
                    // IF FIRST PART JUST START READING
                    line = sr.ReadLine();
                    fields = line.Split(' ');
                    read_node = long.Parse(fields[0]);
                    current_node = read_node;
                    FinishCommunicator fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part-1));
                    fc.startReading = read_node;
                    Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-(part-1), fc);
                  }
                  while(part < num_parts && Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part)).startReading == -1) {
                    Thread.Sleep(50);
                  }
                  thread_starts[read_thread] = read_node;
                  first_read_node = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part)).startReading;

                  //OUTPUT MIGHT BE OBSOLETE
                  using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"out"+part,true))
                  {
                    do{
                        if(line == null) break;
                        fields = line.Split(' ');
                        read_node = long.Parse(fields[0]);
                        if(part != num_parts && read_node >= first_read_node) break;
                        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //file.WriteLine(line);
                        if(current_node != read_node && read_node != 1){
                          vertices_position++;
                        }
                        // add vertices without edges
                        while(read_node > mapping1[vertices_position]){
                          long insertable_vertex = mapping1[vertices_position++];
                          //Console.WriteLine("Special Insert of " + insertable_vertex + " at " + mapping2[insertable_vertex]);
                          AddEdge(mapping2[insertable_vertex], -1, -1, true, read_thread);
                        }
                        long read_edge = long.Parse(fields[1]);
                        // directed edges
                        if(hasWeight){
                          AddEdge(mapping2[read_node], mapping2[read_edge], float.Parse(fields[2]), false, read_thread);
                        } else {
                          AddEdge(mapping2[read_node], mapping2[read_edge], -1, false, read_thread);
                        }
                        if(!directed){
                          // inversion for undirected
                          if(hasWeight){
                            AddEdge(mapping2[read_edge], mapping2[read_node], float.Parse(fields[2]), true, read_thread);
                          } else {
                            AddEdge(mapping2[read_edge], mapping2[read_node], -1, true, read_thread);
                          }
                        }
                        current_node = read_node;
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        line = sr.ReadLine();
                    } while (part == num_parts || read_node < first_read_node);
                    current_node = read_node;
                  }
              }
              Console.WriteLine("Finish Read Thread " + (part-1));
              Interlocked.Increment(ref finished_readers);
           }
        }

        public SimpleGraphNode[] simpleBufferNode = new SimpleGraphNode[num_threads];

        public void AddEdge(long cellid1, long cellid2, float weight, bool single_element, int threadid){
          // Single Element is either undirected inversion or special insert because no outgoing edges of vertex exist
          if(single_element){
            if(cellid2 == -1){
              // Insert Empty Node
              SimpleGraphNode emptyGraphNode = new SimpleGraphNode();
              emptyGraphNode.ID = cellid1;
              emptyGraphNode.Outlinks = new List<long>();
              emptyGraphNode.Weights = new List<float>();
              thread_cache[threadid].Enqueue(emptyGraphNode);
            } else {
              // Insert Inversion
              int destination_server = findServer(cellid1);
              if(destination_server == this_server_id){
                SimpleGraphNode invGraphNode = new SimpleGraphNode();
                invGraphNode.ID = cellid1;
                invGraphNode.Outlinks = new List<long>();
                invGraphNode.Weights = new List<float>();
                invGraphNode.Outlinks.Add(cellid2);
                if(hasWeight){
                  invGraphNode.Weights.Add(weight);
                }
                thread_cache[threadid].Enqueue(invGraphNode);
              } else {
                  // Add to DistributedLoad
                  Load new_load = new Load();
                  new_load.cellid1 = cellid1;
                  new_load.cellid2 = cellid2;
                  new_load.weight = weight;
                  new_load.single_element = single_element;
                  load_sender_queue[destination_server].Enqueue(new_load);
              }
            }
          } else {
            if(simpleBufferNode[threadid].ID != cellid1){
                //Console.WriteLine("Submit " + last_added + " Cache");
                thread_cache[threadid].Enqueue(simpleBufferNode[threadid]);
                simpleBufferNode[threadid] = new SimpleGraphNode();
                simpleBufferNode[threadid].ID = cellid1;
                simpleBufferNode[threadid].Outlinks = new List<long>();
                simpleBufferNode[threadid].Weights = new List<float>();
                //copy of else below
                simpleBufferNode[threadid].Outlinks.Add(cellid2);
                if(hasWeight){
                  simpleBufferNode[threadid].Weights.Add(weight);
                }
            } else {
                simpleBufferNode[threadid].Outlinks.Add(cellid2);
                if(hasWeight){
                  simpleBufferNode[threadid].Weights.Add(weight);
                }
            }
          }
        }

        public int findServer(long cell){
          for(int i = 1; i < num_servers; i++){
            if(i < all_starts[i]) return i-1;
          }
          return num_servers-1;
        }

        public void AddSimpleGraphNode(SimpleGraphNode new_node){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode;
          if(!Global.LocalStorage.Contains(new_node.ID)){
            simpleGraphNode = new SimpleGraphNode();
            simpleGraphNode.CellId = new_node.ID;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(new_node.ID);
          }
          if(hasWeight) {
            if(new_node.Outlinks.Count > 0){
              foreach(long i in simpleGraphNode.Outlinks){
                  simpleGraphNode.Outlinks.Add(i);
              }
              foreach(float i in simpleGraphNode.Outlinks){
                  simpleGraphNode.Weights.Add(i);
              }
            }
          } else {
            foreach(long i in simpleGraphNode.Outlinks){
                simpleGraphNode.Outlinks.Add(i);
            }
          }
          // printGraphNode(simpleGraphNode);
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void printGraphNode(SimpleGraphNode node){
          Console.WriteLine("Node " + node.CellId + " OUT: " + String.Join(",", node.Outlinks));
        }

        public void ConsumerThread(object nthread){
          HashSet<long> set = new HashSet<long>();
          int exponential_delay = 1;
          bool no_action = true;
          int ThreadNumber = (int) nthread;
          SimpleGraphNode dequeued_node;
          while(!finished || thread_cache[ThreadNumber].Count > 0){
            no_action = true;
            while(thread_cache[ThreadNumber].TryDequeue(out dequeued_node)){
                no_action = false;
                //Console.WriteLine("["+ ThreadNumber +"] Clear Cache of " + dequeued_cellid1);
                AddSimpleGraphNode(dequeued_node);
                set.Add(dequeued_node.ID);
            }
            if(no_action && exponential_delay <= 8192){
              exponential_delay = exponential_delay * 2;
            } else if (!no_action){
              exponential_delay = 1;
            }
            Thread.Sleep(exponential_delay);
          }
          // transfer all cells to global space
          Console.WriteLine("["+ ThreadNumber +"] Start Saving to Cloud");
          foreach (long i in set){
            Global.CloudStorage.SaveSimpleGraphNode(i, Global.LocalStorage.LoadSimpleGraphNode(i));
          }
          long cellid_comm = (ThreadNumber+(this_server_id*num_threads));
          Console.WriteLine("["+ ThreadNumber +"] setting finished to " + cellid_comm);
          //FinishCommunicator fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
          FinishCommunicator fc = new FinishCommunicator();
          fc.Finished = true;
          fc.LastLoad = true;
          Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-cellid_comm, fc);
          Interlocked.Increment(ref finish_counter);
          if(this_server_id != 0 && finish_counter == num_threads){
            Console.WriteLine("Last Thread on Server Finished!");
            // reset for next load run
            for(int i = 0; i < num_threads; i++){
              threads[i] = null;
            }
            finished = false;
            finish_counter = 0;
          }
        }

        public void SenderThread(object nthread){
          int senderThreadId = (int) nthread;
          Load new_load;
          DistributedLoad distributedLoad = new DistributedLoad();
          distributedLoad.Loads = new Load[8192];
          int index = 0;
          while(finished_readers < num_threads || load_sender_queue[senderThreadId].Count > 0){
            if(load_sender_queue[senderThreadId].TryDequeue(out new_load)){
              distributedLoad.Loads[index] = new_load;
              index++;
              if(index >= 8192){
                  using (var request = new DistributedLoadWriter(this_server_id, index, distributedLoad.Loads))
                  {
                    Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(this_server_id, request);
                  }
                  distributedLoad = new DistributedLoad();
                  distributedLoad.Loads = new Load[8192];
                  index = 0;
              }
            } else {
              Thread.Sleep(50);
            }
          }
          // Last Send
          using (var request = new DistributedLoadWriter(this_server_id, index, distributedLoad.Loads))
          {
            Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(this_server_id, request);
          }
        }

        public void startServerConsumerThreads(int serverid){
          this_server_id = serverid;
          threads = new Thread[num_threads];
          for(int i = 0; i < num_threads; i++){
            Console.WriteLine("[" + i + "] Start Remote Consumer Thread");
            thread_cache[i] = new ConcurrentQueue<SimpleGraphNode>();
            threads[i] = new Thread(new ParameterizedThreadStart(ConsumerThread));
            threads[i].Start(i);
          }
        }

        public void startServerConsumerThread(int serverid, int threadid){
          this_server_id = serverid;
          if(threads == null){
            threads = new Thread[num_threads];
          }
          Console.WriteLine("[" + threadid + "] Start Remote Consumer Thread");
          thread_cache[threadid] = new ConcurrentQueue<SimpleGraphNode>();
          threads[threadid] = new Thread(new ParameterizedThreadStart(ConsumerThread));
          threads[threadid].Start(threadid);
        }

        public int findThread(long cell){
            for(int i = 1; i < num_threads; i++){
              if(thread_starts[i] > cell) return i-1;
            }
            return num_threads-1;
        }

        public void addDistributedLoadToServer(DistributedLoad load){
           this_server_id = load.serverID;
           Load this_load;
           for(int i = 0; i < load.num_elements; i++){
             this_load = load.Loads[i];
             SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
             simpleGraphNode.ID = this_load.cellid1;
             simpleGraphNode.Outlinks = new List<long>();
             simpleGraphNode.Outlinks.Add(this_load.cellid2);
             if(this_load.weight != -1){
                simpleGraphNode.Weights = new List<float>();
                simpleGraphNode.Weights.Add(this_load.weight);
             }
             thread_cache[findThread(this_load.cellid1)].Enqueue(simpleGraphNode);
           }
           if(load.lastLoad){
              Console.WriteLine("Last Load Arrived!");
              finished = true;
              Console.WriteLine("All Threads will be Finished!");
              serverFinished[this_server_id] = true;
           }
        }

        public void dumpLoadCells(){
              try{
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"load_dump.dmp",true))
                {
                  foreach(KeyValuePair<long, long> entry in mapping2)
                  {
                      try{
                          SimpleGraphNode simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(entry.Key);
                          foreach(long link in simpleGraphNode.Outlinks){
                          file.WriteLine(entry.Value + " " + mapping1[link]);
                        }
                      } catch (Exception ex){
                        TextWriter errorWriter = Console.Error;
                        errorWriter.WriteLine(ex.Message);
                      }
                  }
                }
              } catch (Exception ex){
                TextWriter errorWriter = Console.Error;
                errorWriter.WriteLine(ex.Message);
              }
        }
    }
}
