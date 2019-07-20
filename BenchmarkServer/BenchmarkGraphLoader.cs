using System;
using System.Collections.Generic;
using System.IO;
using Trinity;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;


namespace BenchmarkServer
{
    public class BenchmarkGraphLoader
    {
        // NODES WITH NO OUTGOING EDGES ARE NOT CONSIDERED!
        // cat example-directed.e | awk '{print $1}' | uniq | grep -w -v -f - example-directed.v

        public long max_node;
        public List<long> mapping = new List<long>();
        public Queue<long> vertex_queue = new Queue<long>();
        public String path = "/home/jkliss/";
        public String vpath = "";
        public Dictionary<long, long> mapping1 = new Dictionary<long, long>();
        public Dictionary<long, long> mapping2 = new Dictionary<long, long>();
        //public long[] mapping1_array;
        public long elapsedTime_lastLoadEdge = 0;
        public long elapsedTime_lastLoadVertex = 0;
        public bool hasWeight = false;
        public bool directed = false;
        public static int num_threads = Environment.ProcessorCount;
        public static int num_servers = 2;
        public Thread[] threads = new Thread[num_threads];
        public ConcurrentQueue<long>[] thread_single_cellid1 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<long>[] thread_single_cellid2 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<float>[] thread_single_weight = new ConcurrentQueue<float>[num_threads];
        public ConcurrentQueue<long>[] thread_cache_cellid1 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<Queue<long>>[] thread_cache_cellid2s = new ConcurrentQueue<Queue<long>>[num_threads];
        public ConcurrentQueue<Queue<float>>[] thread_cache_weights = new ConcurrentQueue<Queue<float>>[num_threads];
        public bool finished = false;
        public DistributedLoad[] distributedLoads = new DistributedLoad[num_servers];
        public int[] distributed_load_current_index = new int[num_servers];
        public bool[] serverFinished = new bool[num_servers];
        public int this_server_id;

        public void setPath(String new_path){
            path = new_path;
        }

        public void setVertexPath(String new_path){
            vpath = new_path;
        }

        public long getMaxNode(){
            return max_node;
        }

        public List<long> getMapping(){
          return mapping;
        }

        public void loadVertices(){
          vertex_queue = new Queue<long>();
          // If graph is undirected process file with
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
                      //Console.WriteLine(line);
                      //Console.WriteLine("Mapped " + read_node + " to " + read_lines);
                      vertex_queue.Enqueue(read_node);
                      mapping2[read_node] = read_lines;
                      mapping1[read_lines] = read_node;
                  }
                  catch (Exception ex)
                  {
                      Console.Error.WriteLine("Failed to import the line: (only numeric edges)");
                      Console.Error.WriteLine(ex.Message);
                      Console.Error.WriteLine(ex.StackTrace.ToString());
                      Console.Error.WriteLine(line);
                  }
              }
              //mapping1_array = vertex_queue.ToArray();
              max_node = read_lines;
          }
          var elapsedMs = watch.ElapsedMilliseconds;
          elapsedTime_lastLoadVertex = elapsedMs;
          Console.WriteLine("----------------------------------");
          Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadVertex);
          Console.WriteLine("##################################");
          Console.WriteLine("####### All vertices loaded ######");
          Console.WriteLine("##################################");
        }

        public void LoadGraph()
        {
            this_server_id = 0;
            num_servers = Global.ServerCount;
            finished = false;
            var watch = System.Diagnostics.Stopwatch.StartNew();
            // If graph is undirected process file with;
            Console.WriteLine("Read File at: {0}", path);
            // Create Worker Threads
            threads = new Thread[num_threads];
            for(int i = 0; i < num_servers; i++){
              serverFinished[i] = false;
              createDistributedLoad(i);
              for(int j = 0; j < num_threads+1; j++){
                FinishCommunicator fc = new FinishCommunicator();
                fc.Finished = false;
                fc.LastLoad = false;
                long fcid = (j+(i*num_threads));
                Console.WriteLine("CREATE COMMUNICATION CELL:" + fcid);
                Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-(j+(i*num_threads)), fc);
              }
            }
            Global.LocalStorage.SaveStorage();
            for(int i = 0; i < num_threads; i++){
              threads[i] = new Thread(new ParameterizedThreadStart(ConsumerThread));
              thread_single_cellid1[i] = new ConcurrentQueue<long>();
              thread_single_cellid2[i] = new ConcurrentQueue<long>();
              thread_single_weight[i] = new ConcurrentQueue<float>();
              thread_cache_cellid1[i] = new ConcurrentQueue<long>();
              thread_cache_cellid2s[i] = new ConcurrentQueue<Queue<long>>();
              thread_cache_weights[i] = new ConcurrentQueue<Queue<float>>();
              threads[i].Start(i);
            }
            for(int i = 0; i < num_servers; i++){
              createDistributedLoad(i);
              distributed_load_current_index[i] = 0;
            }
            using (StreamReader reader = new StreamReader(path))
            {
                string line;
                string[] fields;
                // reserve -1 for first
                long read_lines = 0;
                long current_node = -1;
                while (null != (line = reader.ReadLine()))
                {
                    read_lines++;
                    if (read_lines % 1000000 == 0)
                    {
                        Console.Write(" lines read: " + read_lines / 1000000 + "M\r");
                    }
                    try
                    {
                        //Console.WriteLine("LINE:" + line);
                        fields = line.Split(' ');
                        long read_node = long.Parse(fields[0]);
                        if(current_node != read_node && read_node != 1){
                          vertex_queue.Dequeue();
                        }
                        // add vertices without edges
                        while(read_node > vertex_queue.Peek()){
                          long insertable_vertex = vertex_queue.Dequeue();
                          //Console.WriteLine("Special Insert of " + insertable_vertex + " at " + mapping2[insertable_vertex]);
                          //AddEdge(mapping2[insertable_vertex], -1, -1, true);
                          AddEdgeThreaded(mapping2[insertable_vertex], -1, -1, true);
                        }
                        long read_edge = long.Parse(fields[1]);
                        //Console.WriteLine("Translated-LINE:" + mapping2[read_node] + "->" + mapping2[read_edge]);
                        // directed edges
                        if(hasWeight){
                          //AddEdge(mapping2[read_node], mapping2[read_edge], float.Parse(fields[2]), false);
                          AddEdgeThreadedToServer(mapping2[read_node], mapping2[read_edge], float.Parse(fields[2]), false);
                        } else {
                          //AddEdge(mapping2[read_node], mapping2[read_edge], -1, false);
                          AddEdgeThreadedToServer(mapping2[read_node], mapping2[read_edge], -1, false);
                        }
                        if(!directed){
                          // inversion for undirected
                          if(hasWeight){
                            //AddEdge(mapping2[read_edge], mapping2[read_node], float.Parse(fields[2]), true);
                            AddEdgeThreadedToServer(mapping2[read_edge], mapping2[read_node], float.Parse(fields[2]), true);
                          } else {
                            //AddEdge(mapping2[read_edge], mapping2[read_node], -1, true);
                            AddEdgeThreadedToServer(mapping2[read_edge], mapping2[read_node], -1, true);
                          }
                        }

                        current_node = read_node;
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("Failed to import the line:" + read_lines);
                        Console.Error.WriteLine(line);
                        Console.Error.WriteLine(ex.Message);
                        Console.Error.WriteLine(ex.StackTrace.ToString());
                    }
                }
                vertex_queue.Dequeue();
                AddEdgeQueueCache();
                while(vertex_queue.Count > 0){
                    long insertable_vertex = vertex_queue.Dequeue();
                    //Console.WriteLine("E_Special Insert of " + insertable_vertex + " at " + mapping2[insertable_vertex]);
                    //AddEdge(mapping2[insertable_vertex], -1, -1, true);
                    AddEdgeThreaded(mapping2[insertable_vertex], -1, -1, true);
                }
                finished = true;
                for(int i = 0; i < num_threads; i++){
                  threads[i].Join();
                  thread_single_cellid1[i] = null;
                  thread_single_cellid2[i] = null;
                  thread_single_weight[i] = null;
                  thread_cache_cellid1[i] = null;
                  thread_cache_cellid2s[i] = null;
                  thread_cache_weights[i] = null;
                }
                for(int i = 1; i < num_servers; i++){
                  Console.WriteLine("Send to Server:" + i);
                  using (var request = new DistributedLoadWriter(i, distributed_load_current_index[i] ,distributedLoads[i].cellid1s, distributedLoads[i].cellid2s, distributedLoads[i].weights, distributedLoads[i].single_element, true))
                  {
                    Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(i, request);
                  }
                }
                for(int i = 1; i < num_servers; i++){
                  for(int j = 0; j <= num_threads; j++){
                      FinishCommunicator fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(1+j+(i*num_threads)));
                      while(!fcr.Finished){
                          Thread.Sleep(2000);
                          long cellid_comm = (1+j+(i*num_threads));
                          Console.WriteLine("Finished CELL: " + cellid_comm);
                          fcr = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-(1+j+(i*num_threads)));
                      }
                  }

                  Console.WriteLine("Remote Server " + i + " finished");
                }
                //AddNodesWithoutEdges(current_node);
                //Console.WriteLine("Counted Node:" + current_node);
                //Console.WriteLine("Mapped  Node:" +mapping2[current_node]);
            }
            watch.Stop();
            vertex_queue = null;
            var elapsedMs = watch.ElapsedMilliseconds;
            elapsedTime_lastLoadEdge = elapsedMs;
            Console.WriteLine("----------------------------------");
            Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadEdge);
            Console.WriteLine("##################################");
            Console.WriteLine("#######  All edges loaded  #######");
            Console.WriteLine("##################################");
            Global.LocalStorage.SaveStorage();
        }

        public long last_added     = -1;
        public Queue<long> outlinks_cache = new Queue<long>();
        public Queue<float> weights_cache  = new Queue<float>();

        // Single Elemeent is either undirected inversion or special insert because no outgoing edges of vertex exist
        public void AddEdge(long cellid1, long cellid2, float weight, bool single_element){
          if(single_element){
            //Console.WriteLine("Add " + cellid1 + " to " + cellid2 + " as SingleElement:" + single_element);
            AddEdgeBasic(cellid1, cellid2, weight);
          } else {
            if(outlinks_cache.Count > 0 && last_added != cellid1){
                //Console.WriteLine("Submit " + last_added + " Cache");
                AddEdgeQueueCache();
                //copy of else below
                last_added = cellid1;
                //Console.WriteLine("Add " + cellid2 + " to Cache " + last_added);
                if(hasWeight){
                  outlinks_cache.Enqueue(cellid2);
                  weights_cache.Enqueue(weight);
                } else {
                  outlinks_cache.Enqueue(cellid2);
                }
            } else {
                last_added = cellid1;
                //Console.WriteLine("Add " + cellid2 + " to Cache " + last_added);
                if(hasWeight){
                  outlinks_cache.Enqueue(cellid2);
                  weights_cache.Enqueue(weight);
                } else {
                  outlinks_cache.Enqueue(cellid2);
                }
            }
            last_added = cellid1;
          }
        }

        public void AddEdgeBasic(long cellid1, long cellid2, float weight){
          SimpleGraphNode simpleGraphNode;
          if(!Global.LocalStorage.Contains(cellid1)){
            Global.LocalStorage.SaveSimpleGraphNode(
              cellid1,
              new List<long>(),
              new List<float>());
          }
          if(cellid2 == -1 && weight == -1){
            return;
          } else if(weight == -1) {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(cellid1);
            simpleGraphNode.Outlinks.Add(cellid2);
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(cellid1);
            simpleGraphNode.Outlinks.Add(cellid2);
            simpleGraphNode.Weights.Add(weight);
          }
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void AddEdgeQueueCache(){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.LocalStorage.Contains(last_added)){
            simpleGraphNode.CellId = last_added;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(last_added);
          }
          if(hasWeight) {
            while(outlinks_cache.Count > 0){
              simpleGraphNode.Outlinks.Add(outlinks_cache.Dequeue());
              simpleGraphNode.Weights.Add(weights_cache.Dequeue());
            }
          } else {
            while(outlinks_cache.Count > 0){
              simpleGraphNode.Outlinks.Add(outlinks_cache.Dequeue());
            }
          }
          // printGraphNode(simpleGraphNode);
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void printGraphNode(SimpleGraphNode node){
          Console.WriteLine("Node " + node.CellId + " OUT: " + String.Join(",", node.Outlinks));
        }

        // Single Elemeent is either undirected inversion or special insert because no outgoing edges of vertex exist
        public void AddEdgeThreaded(long cellid1, long cellid2, float weight, bool single_element){
          if(single_element){
            //Console.WriteLine("Add " + cellid1 + " to " + cellid2 + " as SingleElement:" + single_element);
            AddEdgeBasicThreaded(cellid1, cellid2, weight);
          } else {
            if(outlinks_cache.Count > 0 && last_added != cellid1){
                //Console.WriteLine("Submit " + last_added + " Cache");
                AddEdgeQueueCacheThreaded();
                outlinks_cache = new Queue<long>();
                weights_cache = new Queue<float>();
                //copy of else below
                last_added = cellid1;
                //Console.WriteLine("Add " + cellid2 + " to Cache " + last_added);
                if(hasWeight){
                  outlinks_cache.Enqueue(cellid2);
                  weights_cache.Enqueue(weight);
                } else {
                  outlinks_cache.Enqueue(cellid2);
                }
            } else {
                last_added = cellid1;
                //Console.WriteLine("Add " + cellid2 + " to Cache " + last_added);
                if(hasWeight){
                  outlinks_cache.Enqueue(cellid2);
                  weights_cache.Enqueue(weight);
                } else {
                  outlinks_cache.Enqueue(cellid2);
                }
            }
            last_added = cellid1;
          }
        }

        public void AddEdgeBasicThreaded(long cellid1, long cellid2, float weight){
          int index = (int) (cellid1%(num_threads*num_servers))%num_threads;
          Console.WriteLine("[>] Add BASIC " + cellid1 + " at Thread: " + index + " on Server " + this_server_id);
          try{
            if(threads[index] == null){
              startServerConsumerThread(this_server_id,index);
            }
            if(thread_single_cellid2[index] == null){
              Console.WriteLine("["+index+"] Init new Concurrent Queue");
              thread_single_cellid2[index] = new ConcurrentQueue<long>();
              thread_single_weight[index] = new ConcurrentQueue<float>();
              thread_single_cellid1[index] = new ConcurrentQueue<long>();
            }
            thread_single_cellid2[index].Enqueue(cellid2);
            if(hasWeight) thread_single_weight[index].Enqueue(weight);
            thread_single_cellid1[index].Enqueue(cellid1);
          } catch (Exception ex) {
              Console.Error.WriteLine(ex.Message);
              Console.Error.WriteLine(ex.StackTrace.ToString());
          }
        }

        public void AddEdgeQueueCacheThreaded(){
          int index = (int) (last_added%(num_threads*num_servers))%num_threads;
          Console.WriteLine("[>] Add CACHE " + last_added + " at Thread: " + index + " on Server " + this_server_id);
          try{
            if(threads[index] == null){
              startServerConsumerThread(this_server_id,index);
            }
            if(thread_cache_cellid2s[index] == null){
              Console.WriteLine("["+index+"] Init new Concurrent Queue");
              thread_cache_cellid1[index] = new ConcurrentQueue<long>();
              thread_cache_cellid2s[index] = new ConcurrentQueue<Queue<long>>();
              thread_cache_weights[index] = new ConcurrentQueue<Queue<float>>();
            }
            thread_cache_cellid2s[index].Enqueue(new Queue<long>(outlinks_cache.ToArray()));
            if(hasWeight) thread_cache_weights[index].Enqueue(new Queue<float>(weights_cache.ToArray()));
            thread_cache_cellid1[index].Enqueue(last_added);
          } catch (Exception ex) {
              Console.Error.WriteLine(ex.Message);
              Console.Error.WriteLine(ex.StackTrace.ToString());
          }
        }

        public void ConsumerThread(object nthread){
          HashSet<long> set = new HashSet<long>();
          int exponential_delay = 1;
          bool no_action = true;
          int ThreadNumber = (int) nthread;
          long dequeued_cellid1;
          while(!finished || thread_single_cellid1[ThreadNumber].Count > 0 || thread_cache_cellid1[ThreadNumber].Count > 0){
            no_action = true;
            while(thread_cache_cellid1[ThreadNumber].TryDequeue(out dequeued_cellid1)){
                no_action = false;
                Console.WriteLine("["+ ThreadNumber +"] Clear Cache of " + dequeued_cellid1);
                Queue<long> cellid2s;
                while(!thread_cache_cellid2s[ThreadNumber].TryDequeue(out cellid2s)){
                  Thread.Sleep(1);
                }
                if(hasWeight){
                  Queue<float> weights;
                  while(!thread_cache_weights[ThreadNumber].TryDequeue(out weights)){
                    Thread.Sleep(1);
                  }
                  AddEdgeQueue(dequeued_cellid1, cellid2s, weights);
                } else {
                  AddEdgeQueue(dequeued_cellid1, cellid2s);
                }
                set.Add(dequeued_cellid1);
            }
            while(thread_single_cellid1[ThreadNumber].TryDequeue(out dequeued_cellid1)){
                no_action = false;
                long cellid2;
                while(!thread_single_cellid2[ThreadNumber].TryDequeue(out cellid2)){
                  Thread.Sleep(1);
                }
                Console.WriteLine("["+ ThreadNumber +"] Insert of " + dequeued_cellid1 + "->" + cellid2);
                if(hasWeight){
                  float weight;
                  while(!thread_single_weight[ThreadNumber].TryDequeue(out weight)){
                    Thread.Sleep(1);
                  }
                  AddEdgeBasic(dequeued_cellid1, cellid2, weight);
                } else {
                  AddEdgeBasic(dequeued_cellid1, cellid2, -1);
                }
                set.Add(dequeued_cellid1);
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
          long cellid_comm = (1+ThreadNumber+(this_server_id*num_threads));
          Console.WriteLine("["+ ThreadNumber +"] setting finished to " + cellid_comm);
          FinishCommunicator fc = Global.CloudStorage.LoadFinishCommunicator(Int64.MaxValue-cellid_comm);
          fc.Finished = true;
          Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-cellid_comm, fc);
        }

        public void AddEdgeQueue(long cellid1, Queue<long> cellid2s, Queue<float> weights){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.LocalStorage.Contains(cellid1)){
            simpleGraphNode.CellId = cellid1;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(cellid1);
          }
          while(cellid2s.Count > 0){
            simpleGraphNode.Outlinks.Add(cellid2s.Dequeue());
            simpleGraphNode.Weights.Add(weights.Dequeue());
          }
          // printGraphNode(simpleGraphNode);
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void AddEdgeQueue(long cellid1, Queue<long> cellid2s){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.LocalStorage.Contains(cellid1)){
            simpleGraphNode.CellId = cellid1;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.LocalStorage.LoadSimpleGraphNode(cellid1);
          }
          while(cellid2s.Count > 0){
            simpleGraphNode.Outlinks.Add(cellid2s.Dequeue());
          }
          // printGraphNode(simpleGraphNode);
          Global.LocalStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void createDistributedLoad(int ServerID){
            distributedLoads[ServerID] = new DistributedLoad();
            distributedLoads[ServerID].cellid1s = new long[32768];
            distributedLoads[ServerID].cellid2s = new long[32768];
            distributedLoads[ServerID].weights = new float[32768];
            distributedLoads[ServerID].single_element = new bool[32768];
        }

        public void AddToDistributedLoad(long cellid1, long cellid2, float weight, bool single){
            int ServerID = (int) (cellid1%(num_threads*num_servers))/num_threads;
            Console.WriteLine("Add " + cellid1 + " to " + cellid2 + " at " + ServerID + " Position in Load: " + distributed_load_current_index[ServerID]);
            DistributedLoad distributed_load = distributedLoads[ServerID];
            distributedLoads[ServerID].serverID = ServerID;
            distributedLoads[ServerID].cellid1s[distributed_load_current_index[ServerID]] = cellid1;
            distributedLoads[ServerID].cellid2s[distributed_load_current_index[ServerID]] = cellid2;
            distributedLoads[ServerID].weights[distributed_load_current_index[ServerID]] = weight;
            distributedLoads[ServerID].single_element[distributed_load_current_index[ServerID]] = single;
            distributed_load_current_index[ServerID]++;
            // 32768 is buffersize
            if(distributed_load_current_index[ServerID] >= 32768){
                using (var request = new DistributedLoadWriter(ServerID, distributed_load_current_index[ServerID], distributedLoads[ServerID].cellid1s, distributedLoads[ServerID].cellid2s, distributedLoads[ServerID].weights, distributedLoads[ServerID].single_element))
                {
                  Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(ServerID, request);
                }
                createDistributedLoad(ServerID);
                distributed_load_current_index[ServerID] = 0;
            }
        }

        public void startServerConsumerThreads(int serverid){
          this_server_id = serverid;
          threads = new Thread[num_threads];
          for(int i = 0; i < num_threads; i++){
            Console.WriteLine("[" + i + "]Start Remote Consumer Thread");
            thread_single_cellid1[i] = new ConcurrentQueue<long>();
            thread_single_cellid2[i] = new ConcurrentQueue<long>();
            thread_single_weight[i] = new ConcurrentQueue<float>();
            thread_cache_cellid1[i] = new ConcurrentQueue<long>();
            thread_cache_cellid2s[i] = new ConcurrentQueue<Queue<long>>();
            thread_cache_weights[i] = new ConcurrentQueue<Queue<float>>();
            threads[i] = new Thread(new ParameterizedThreadStart(ConsumerThread));
            threads[i].Start(i);
          }
        }

        public void startServerConsumerThread(int serverid, int threadid){
          this_server_id = serverid;
          if(threads == null){
            threads = new Thread[num_threads];
          }
          Console.WriteLine("[" + threadid + "]Start Remote Consumer Thread");
          thread_single_cellid1[threadid] = new ConcurrentQueue<long>();
          thread_single_cellid2[threadid] = new ConcurrentQueue<long>();
          thread_single_weight[threadid] = new ConcurrentQueue<float>();
          thread_cache_cellid1[threadid] = new ConcurrentQueue<long>();
          thread_cache_cellid2s[threadid] = new ConcurrentQueue<Queue<long>>();
          thread_cache_weights[threadid] = new ConcurrentQueue<Queue<float>>();
          threads[threadid] = new Thread(new ParameterizedThreadStart(ConsumerThread));
          threads[threadid].Start(threadid);
        }

        public void addDistributedLoadToServer(DistributedLoad load){
           this_server_id = load.serverID;
           for(int i = 0; i < load.num_elements; i++){
             AddEdgeThreaded(load.cellid1s[i], load.cellid2s[i], load.weights[i], load.single_element[i]);
           }
           if(load.lastLoad){
              Console.WriteLine("Last Load Arrived!");
              finished = true;
              for(int i = 0; i < num_threads; i++){
                threads[i].Join();
                thread_single_cellid1[i] = null;
                thread_single_cellid2[i] = null;
                thread_single_weight[i] = null;
                thread_cache_cellid1[i] = null;
                thread_cache_cellid2s[i] = null;
                thread_cache_weights[i] = null;
              }
              Console.WriteLine("All Threads Finished!");
              serverFinished[(int) load.cellid1s[1]%(num_threads*num_servers)/num_threads] = true;

           }
        }

        public void AddEdgeThreadedToServer(long cellid1, long cellid2, float weight, bool single){
           int ServerID = (int) (cellid1%(num_threads*num_servers))/num_threads;
           if(ServerID == 0){
              AddEdgeThreaded(cellid1, cellid2, weight, single);
           } else {
              AddToDistributedLoad(cellid1, cellid2, weight, single);
           }
        }
    }
}
