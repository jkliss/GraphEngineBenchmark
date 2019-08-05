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
        //public static int num_threads = Environment.ProcessorCount;
        public static int num_threads = 1;
        public static int num_servers = 2;
        public Thread[] threads = new Thread[num_threads];
        public ConcurrentQueue<SimpleGraphNode>[] thread_cache = new ConcurrentQueue<SimpleGraphNode>[num_threads];
        public bool finished = false;
        public DistributedLoad[] distributedLoads = new DistributedLoad[num_servers];
        public int[] distributed_load_current_index = new int[num_servers];
        public int this_server_id;
        public int finish_counter = 0;

        // PARALLEL READER
        public int num_parts = num_servers * num_threads;
        public long[] all_starts = new long[num_servers];
        public long[] thread_starts = new long[num_threads];
        int finished_readers = 0;
        public bool all_sent = false;

        int all_threads_read_lines = 0;
        int all_threads_inserted_edges = 0;
        int all_threads_equeued_edges = 0;
        int all_threads_recieved_load_edges = 0;
        int all_threads_sent_edges = 0;
        int all_threads_equeued_load_edges = 0;

        public int all_sends = 0;
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
          prepareServer();
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
            for(int j = 0; j < num_threads+1; j++){
              FinishCommunicator fc = new FinishCommunicator();
              fc.Finished = false;
              fc.LastLoad = false;
              fc.startReading = -1;
              fc.FinishedReading = false;
              fc.FinishedSending = false;
              fc.FinishedConsuming = false;
              long fcid = (j+(i*num_threads));
              //Console.WriteLine("CREATE COMMUNICATION CELL:" + fcid);
              Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-fcid, fc);
            }
          }
          Console.WriteLine("Preparations for parallel done!");
        }

        public void prepareServer(){
          for(int i = 0; i < num_servers; i++){
            all_starts[i] = -1;
            if(this_server_id != i){
              load_sender_queue[i] = new ConcurrentQueue<Load>();
              Console.WriteLine("START LOAD SENDER " + i);
              load_sender_threads[i] = new Thread(new ParameterizedThreadStart(SenderThread));
              load_sender_threads[i].Start(i);
            }
          }
          Console.WriteLine("Preparations for Server done!");
        }

        public void LoadGraph()
        {
          try{
            Thread reporter_thread = new Thread(new ThreadStart(Reporter));
            reporter_thread.Start();
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
            for(int i = num_threads+(num_threads*this_server_id); i >= (num_threads*this_server_id)+1; i--){
              Console.WriteLine("Start " + i);
              read_threads[i-(num_threads*this_server_id)-1] = new Thread(new ParameterizedThreadStart(ParallelReading));
              read_threads[i-(num_threads*this_server_id)-1].Start(i);
            }
            ////////////////////////// WAITING FOR THREADS ////////////////////////////////////////////
            // READING THREADS
            for(int i = 0; i < num_threads; i++){
              read_threads[i].Join();
              long fcid = (i+(this_server_id*num_threads));
              //FinishCommunicator fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-fcid);
              //fcr.FinishedReading = true;
              //Global.CloudStorage.SaveFinishCommunicator(fcr);
            }
            Console.WriteLine("All Reader on this Server Finished");
            // SENDING THREADS
            for(int i = 0; i < num_servers; i++){
              if(i != this_server_id){
                  load_sender_threads[i].Join();
              }
            }
            for(int i = 0; i < num_threads; i++){
              long fcid = (i+(this_server_id*num_threads));
              Console.WriteLine("TRY TO ACCESS " + fcid);
              FinishCommunicator fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-fcid);
              fcr.FinishedSending = true;
              fcr.FinishedReading = true;
              //fcr.Finished = true;
              Global.CloudStorage.SaveFinishCommunicator(fcr);
            }
            Console.WriteLine("All Sender on this Server Finished");
            // CHECK GLOBAL SENDING THREADS
            while(!checkAllSenderGlobal()){
              Thread.Sleep(1000);
            }
            Console.WriteLine("----> All Sender Global Finished <-------");
            while(all_sends < num_servers-1){
              Thread.Sleep(1000);
            }
            all_sent = true;
            // CONSUMING THREADS
            for(int i = 0; i < num_threads; i++){
              threads[i].Join();
            }
            Console.WriteLine("All Consumer on this Server Finished");
            for(int i = 0; i < num_servers; i++){
              for(int j = 0; j < num_threads; j++){
                  long cellid_comm = (j+(i*num_threads));
                  Console.WriteLine("Test ThreadCell: " + cellid_comm);
                  FinishCommunicator fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
                  bool message_sent = false;
                  while(!fcr.Finished){
                      if(!message_sent){
                        Console.WriteLine("Wait for Cell: " + cellid_comm);
                        message_sent = true;
                      }
                      Thread.Sleep(60);
                      fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
                  }
              }
              Console.WriteLine("Remote Server " + i + " finished");
            }
            finished = true;
            watch.Stop();
            vertex_queue = null;
            var elapsedMs = watch.ElapsedMilliseconds;
            elapsedTime_lastLoadEdge = elapsedMs;
            Console.WriteLine("----------------------------------");
            Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadEdge);
            Console.WriteLine("##################################");
            Console.WriteLine("#######  All edges loaded  #######");
            Console.WriteLine("##################################");
            if(this_server_id == 0)  Global.LocalStorage.SaveStorage();
          } catch (Exception ex){
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine(ex.StackTrace.ToString());
          }
        }

        public void Reporter(){
          Thread.Sleep(5000);
          while(!finished){
            if(directed){
              Console.WriteLine("LINES: " + all_threads_read_lines + " ENQUEUED EDGES: " + all_threads_equeued_edges + " INSERTED NODES: " + all_threads_inserted_edges);
            } else {
              Console.WriteLine("LINES: " + all_threads_read_lines + " ENQUEUED EDGES: " + all_threads_equeued_edges + " INSERTED NODES: " + all_threads_inserted_edges + " QUEUELOAD EDGES: " + all_threads_equeued_load_edges + " LOAD EDGES: " + all_threads_sent_edges + " RECIEVED LOAD EDGES: " + all_threads_recieved_load_edges);
            }
            Thread.Sleep(5000);
          }
        }

        public bool checkAllSenderGlobal(){
          for(int i = 0; i < num_servers; i++){
            for(int j = 0; j < num_threads; j++){
              long fcid = (j+(i*num_threads));
              FinishCommunicator fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-fcid);
              if(fcr.FinishedSending == false){
                 Console.WriteLine("[SENDER] Wait for " + fcid);
                 return false;
              }
            }
          }
          return true;
        }

        public void ParallelReading(object par_part)
        {
            int part = (int) par_part;
            int read_thread = part-1-(this_server_id*num_threads);
            Console.WriteLine("Start Read Thread " + read_thread + " for Part: " + part);
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
                    if(line == null){
                      FinishCommunicator fc_spec = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part-1));
                      fc_spec.startReading = Int64.MaxValue;
                      Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-(part-1), fc_spec);
                      return;
                    }
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
                  bool msg_sent = false;
                  while(part < num_parts && Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part)).startReading == -1) {
                    Thread.Sleep(50);
                    if(msg_sent == false){
                      Console.WriteLine("["+part+"] WAIT FOR OTHER THREAD");
                      msg_sent = true;
                    }
                  }
                  thread_starts[read_thread] = read_node;
                  first_read_node = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(part)).startReading;
                  if(part < num_parts){
                    Console.WriteLine("["+part+"] FROM: " + read_node + " UNTIL: " + first_read_node);
                  } else {
                    Console.WriteLine("["+part+"] FROM: " + read_node + " UNTIL: END");
                  }
                  //OUTPUT MIGHT BE OBSOLETE
                  try
                  {
                    do{
                        if(line == null) break;
                        fields = line.Split(' ');
                        read_node = long.Parse(fields[0]);
                        if(part != num_parts && read_node >= first_read_node) break;
                        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //file.WriteLine(line);
                        Interlocked.Increment(ref all_threads_read_lines);
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
                    // DUMMY NODE TO CLEAR BUFFER NODE
                    AddEdge(1, 1, -1, false, read_thread);
                    current_node = read_node;
                  } catch (Exception ex){
                      Console.Error.WriteLine(ex.Message);
                      Console.Error.WriteLine(ex.StackTrace.ToString());
                  }
              }
              while(first_read_node > mapping1[vertices_position]){
                long insertable_vertex = mapping1[vertices_position++];
                //Console.WriteLine("Special Insert of " + insertable_vertex + " at " + mapping2[insertable_vertex]);
                AddEdge(mapping2[insertable_vertex], -1, -1, true, read_thread);
              }
              Console.WriteLine("Finish Read Thread " + (part-1));
              Interlocked.Increment(ref finished_readers);
           }
        }

        public SimpleGraphNode[] simpleBufferNode = new SimpleGraphNode[num_threads];

        public void AddEdge(long cellid1, long cellid2, float weight, bool single_element, int threadid){
          //Console.WriteLine("[READ"+threadid+"] AddEdge " + cellid1 + " -> " + cellid2);
          Interlocked.Increment(ref all_threads_equeued_edges);
          // Single Element is either undirected inversion or special insert because no outgoing edges of vertex exist
          if(single_element){
            if(cellid2 == -1){
              // Insert Empty Node
              //Console.WriteLine("[READ"+threadid+"] AddEdge (EMPTY) " + cellid1 + " -> " + cellid2);
              SimpleGraphNode emptyGraphNode = new SimpleGraphNode();
              emptyGraphNode.ID = cellid1;
              emptyGraphNode.Outlinks = new List<long>();
              emptyGraphNode.Weights = new List<float>();
              thread_cache[threadid].Enqueue(emptyGraphNode);
            } else {
              // Insert Inversion
              int destination_server = findServer(cellid1);
              if(threadid != this_server_id){
                Console.WriteLine("[READ"+threadid+"] AddEdge{"+this_server_id+"} (INVERSION S["+destination_server+"]) " + cellid1 + " -> " + cellid2);
              }
              if(destination_server == this_server_id){
                //Console.WriteLine("[READ"+threadid+"] AddEdge (INVERSION LOCAL) " + cellid1 + " -> " + cellid2);
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
                  //Console.WriteLine("[READ"+threadid+"] AddEdge (to Load) " + cellid2 + " -> " + cellid1);
                  // Add to DistributedLoad
                  Load new_load = new Load();
                  new_load.cellid1 = cellid1;
                  new_load.cellid2 = cellid2;
                  new_load.weight = weight;
                  new_load.single_element = single_element;
                  Interlocked.Increment(ref all_threads_equeued_load_edges);
                  load_sender_queue[destination_server].Enqueue(new_load);
              }
            }
          } else {
            if(simpleBufferNode[threadid].ID != cellid1){
                if(simpleBufferNode[threadid].ID > 0){
                  //Console.WriteLine("[READ"+threadid+"] AddEdge (Cache to Queue) for " + simpleBufferNode[threadid].ID + " OUT: " + String.Join(",", simpleBufferNode[threadid].Outlinks));
                  thread_cache[threadid].Enqueue(simpleBufferNode[threadid]);
                }
                //Console.WriteLine("[READ"+threadid+"] AddEdge (to BufferNode) " + cellid1 + " -> " + cellid2);
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
                //Console.WriteLine("[READ"+threadid+"] AddEdge (to BufferNode) " + cellid1 + " -> " + cellid2);
                simpleBufferNode[threadid].Outlinks.Add(cellid2);
                if(hasWeight){
                  simpleBufferNode[threadid].Weights.Add(weight);
                }
            }
          }
        }

        public int findServer(long cell){
          try{
            if(all_starts[0] == -1){
              for(int i = num_servers-1; i >= 0; i--){
                while(all_starts[i] == -1){
                  all_starts[i] = mapping2[Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(num_threads*i)).startReading];
                  Thread.Sleep(50);
                }
              }
              for(int i = 0; i < num_servers; i++){
                Console.WriteLine("LOWER BOUND SERVER" + i + " " + all_starts[i]);
              }
            }
            for(int i = 0; i < num_servers; i++){
              if(cell < all_starts[i]) return i-1;
            }
            return num_servers-1;
          }  catch {
            Console.WriteLine("ERROR UNABLE TO GET SERVERID");
          }
          return -1;
        }

        public void AddSimpleGraphNode(SimpleGraphNode new_node){
          Interlocked.Increment(ref all_threads_inserted_edges);
          SimpleGraphNode simpleGraphNode;
          if(!Global.LocalStorage.Contains(new_node.ID)){
            simpleGraphNode = new SimpleGraphNode();
            simpleGraphNode.CellId = new_node.ID;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(new_node.ID);
          }
          simpleGraphNode.ID = new_node.ID;
          if(hasWeight) {
            for(int i = 0; i < new_node.Outlinks.Count; i++){
              simpleGraphNode.Outlinks.Add(new_node.Outlinks[i]);
              simpleGraphNode.Weights.Add(new_node.Weights[i]);
              //Console.WriteLine("+->" + new_node.Outlinks[i]);
            }
          } else {
            for(int i = 0; i < new_node.Outlinks.Count; i++){
              simpleGraphNode.Outlinks.Add(new_node.Outlinks[i]);
              //Console.WriteLine("+->" + new_node.Outlinks[i]);
            }
          }
          //printGraphNode(simpleGraphNode);
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void printGraphNode(SimpleGraphNode node){
          Console.WriteLine("Save Node " + node.ID + " OUT: " + String.Join(",", node.Outlinks));
        }

        public void ConsumerThread(object nthread){
          HashSet<long> set = new HashSet<long>();
          int exponential_delay = 1;
          bool no_action = true;
          int ThreadNumber = (int) nthread;
          Console.WriteLine("Start Consumer Thread " + ThreadNumber);
          SimpleGraphNode dequeued_node;
          while(!all_sent || thread_cache[ThreadNumber].Count > 0){
            try{
              no_action = true;
              while(thread_cache[ThreadNumber].TryDequeue(out dequeued_node)){
                  no_action = false;
                  //Console.WriteLine("[CONSUMER"+ThreadNumber+"] Add Node: " + dequeued_node.ID + " OUT: " + String.Join(",", dequeued_node.Outlinks));
                  AddSimpleGraphNode(dequeued_node);
                  set.Add(dequeued_node.ID);
              }
              if(no_action && exponential_delay <= 65536){
                exponential_delay = exponential_delay * 2;
              } else if (!no_action){
                exponential_delay = 1;
              }
              Thread.Sleep(exponential_delay);
            } catch (Exception ex){
              Console.Error.WriteLine(ex.Message);
              Console.Error.WriteLine(ex.StackTrace.ToString());
            }
          }
          // transfer all cells to global space
          Console.WriteLine("["+ ThreadNumber +"] Start Saving to Cloud");
          foreach (long i in set){
            Global.CloudStorage.SaveSimpleGraphNode(i, Global.LocalStorage.LoadSimpleGraphNode(i));
          }
          long cellid_comm = (ThreadNumber+(this_server_id*num_threads));
          Console.WriteLine("["+ ThreadNumber +"] setting finished to " + cellid_comm);
          FinishCommunicator fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
          while(fc.FinishedSending == false){
            Thread.Sleep(100);
            fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
          }
          fc.Finished = true;
          fc.LastLoad = true;
          Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-cellid_comm, fc);
          Interlocked.Increment(ref finish_counter);
          if(finish_counter == num_threads){
            Console.WriteLine("Last Thread on Server Finished!");
            // reset for next load run
          }
        }

        public void SenderThread(object nthread){
          int senderThreadId = (int) nthread;
          Load new_load;
          DistributedLoad distributedLoad = new DistributedLoad();
          distributedLoad.cellid1 = new long[65536];
          distributedLoad.cellid2 = new long[65536];
          distributedLoad.weight = new float[65536];
          distributedLoad.single_element = new bool[65536];
          int index = 0;
          while(finished_readers < num_threads || load_sender_queue[senderThreadId].Count > 0){
            try{
              if(load_sender_queue[senderThreadId].TryDequeue(out new_load)){
                distributedLoad.cellid1[index] = new_load.cellid1;
                distributedLoad.cellid2[index] = new_load.cellid2;
                distributedLoad.weight[index] = new_load.weight;
                distributedLoad.single_element[index] = new_load.single_element;
                Interlocked.Increment(ref all_threads_sent_edges);
                index++;
                if(index >= 65500){
                    Console.WriteLine("Send Load to Server " + senderThreadId);
                    using (var request = new DistributedLoadWriter(senderThreadId, this_server_id ,index, distributedLoad.cellid1, distributedLoad.cellid2, distributedLoad.weight, distributedLoad.single_element, false))
                    {
                      Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(senderThreadId, request);
                    }
                    distributedLoad = new DistributedLoad();
                    distributedLoad.cellid1 = new long[65536];
                    distributedLoad.cellid2 = new long[65536];
                    distributedLoad.weight = new float[65536];
                    distributedLoad.single_element = new bool[65536];
                    index = 0;
                }
              } else {
                Thread.Sleep(50);
              }
            } catch (Exception ex){
              Console.Error.WriteLine(ex.Message + " INDEX:" + index);
              Console.Error.WriteLine(ex.StackTrace.ToString());
            }
          }
          try{
              // Last Send
              Console.WriteLine("Send LAST Load to Server " + senderThreadId + " " + index);
              using (var request = new DistributedLoadWriter(senderThreadId, this_server_id, index, distributedLoad.cellid1, distributedLoad.cellid2, distributedLoad.weight, distributedLoad.single_element, true))
              {
                Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(senderThreadId, request);
              }
          } catch (Exception ex){
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine(ex.StackTrace.ToString());
          }
        }

        public int findThread(long cell){
            for(int i = 1; i < num_threads; i++){
              if(thread_starts[i] > cell) return i-1;
            }
            return num_threads-1;
        }

        public void addDistributedLoadToServer(DistributedLoad load){
          try{
            for(int i = 0; i < load.num_elements; i++){
              Interlocked.Increment(ref all_threads_recieved_load_edges);
              SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
              simpleGraphNode.ID = load.cellid1[i];
              simpleGraphNode.Outlinks = new List<long>();
              simpleGraphNode.Outlinks.Add(load.cellid2[i]);
              if(load.weight[i] != -1){
                 simpleGraphNode.Weights = new List<float>();
                 simpleGraphNode.Weights.Add(load.weight[i]);
              }
              thread_cache[findThread(load.cellid1[i])].Enqueue(simpleGraphNode);
            }
            if(load.lastLoad){
               Console.WriteLine("Last Load Arrived of Server:" + load.fromServerID);
               Interlocked.Increment(ref all_sends);
               //finished = true;
               Console.WriteLine("All Threads will be Finished!");
            }
          } catch (Exception ex){
              Console.Error.WriteLine(ex.Message);
              Console.Error.WriteLine(ex.StackTrace.ToString());
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
