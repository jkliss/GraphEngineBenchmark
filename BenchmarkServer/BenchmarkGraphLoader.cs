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
        public Thread[] threads = new Thread[num_threads];
        public ConcurrentQueue<long>[] thread_single_cellid1 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<long>[] thread_single_cellid2 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<float>[] thread_single_weight = new ConcurrentQueue<float>[num_threads];
        public ConcurrentQueue<long>[] thread_cache_cellid1 = new ConcurrentQueue<long>[num_threads];
        public ConcurrentQueue<Queue<long>>[] thread_cache_cellid2s = new ConcurrentQueue<Queue<long>>[num_threads];
        public ConcurrentQueue<Queue<float>>[] thread_cache_weights = new ConcurrentQueue<Queue<float>>[num_threads];
        public bool finished = false;

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
            finished = false;
            var watch = System.Diagnostics.Stopwatch.StartNew();
            // If graph is undirected process file with;
            Console.WriteLine("Read File at: {0}", path);
            // Create Worker Threads
            threads = new Thread[num_threads];
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
                          AddEdgeThreaded(mapping2[read_node], mapping2[read_edge], float.Parse(fields[2]), false);
                        } else {
                          //AddEdge(mapping2[read_node], mapping2[read_edge], -1, false);
                          AddEdgeThreaded(mapping2[read_node], mapping2[read_edge], -1, false);
                        }
                        if(!directed){
                          // inversion for undirected
                          if(hasWeight){
                            //AddEdge(mapping2[read_edge], mapping2[read_node], float.Parse(fields[2]), true);
                            AddEdgeThreaded(mapping2[read_edge], mapping2[read_node], float.Parse(fields[2]), true);
                          } else {
                            //AddEdge(mapping2[read_edge], mapping2[read_node], -1, true);
                            AddEdgeThreaded(mapping2[read_edge], mapping2[read_node], -1, true);
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
                //AddNodesWithoutEdges(current_node);
                //Console.WriteLine("Counted Node:" + current_node);
                //Console.WriteLine("Mapped  Node:" +mapping2[current_node]);
                Global.CloudStorage.SaveStorage();
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
          if(!Global.CloudStorage.Contains(cellid1)){
            Global.CloudStorage.SaveSimpleGraphNode(
              cellid1,
              new List<long>(),
              new List<float>());
          }
          if(cellid2 == -1 && weight == -1){
            return;
          } else if(weight == -1) {
            simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(cellid1);
            simpleGraphNode.Outlinks.Add(cellid2);
          } else {
            simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(cellid1);
            simpleGraphNode.Outlinks.Add(cellid2);
            simpleGraphNode.Weights.Add(weight);
          }
          Global.CloudStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void AddEdgeQueueCache(){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.CloudStorage.Contains(last_added)){
            simpleGraphNode.CellId = last_added;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(last_added);
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
          Global.CloudStorage.SaveSimpleGraphNode(simpleGraphNode);
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
          thread_single_cellid2[cellid1%num_threads].Enqueue(cellid2);
          if(hasWeight) thread_single_weight[cellid1%num_threads].Enqueue(weight);
          thread_single_cellid1[cellid1%num_threads].Enqueue(cellid1);
        }

        public void AddEdgeQueueCacheThreaded(){
          thread_cache_cellid2s[last_added%num_threads].Enqueue(new Queue<long>(outlinks_cache.ToArray()));
          if(hasWeight) thread_cache_weights[last_added%num_threads].Enqueue(new Queue<float>(weights_cache.ToArray()));
          thread_cache_cellid1[last_added%num_threads].Enqueue(last_added);
        }

        public void ConsumerThread(object nthread){
          int exponential_delay = 1;
          bool no_action = true;
          int ThreadNumber = (int) nthread;
          long dequeued_cellid1;
          while(!finished || thread_single_cellid1[ThreadNumber].Count > 0 || thread_cache_cellid1[ThreadNumber].Count > 0){
            no_action = true;
            while(thread_cache_cellid1[ThreadNumber].TryDequeue(out dequeued_cellid1)){
                no_action = false;
                //Console.WriteLine("["+ ThreadNumber +"] Clear Cache of " + thread_cache_cellid1[ThreadNumber].Peek());

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
            }
            while(thread_single_cellid1[ThreadNumber].TryDequeue(out dequeued_cellid1)){
                no_action = false;
                //Console.WriteLine("["+ ThreadNumber +"] Insert of " + thread_single_cellid1[ThreadNumber].Peek() + "->" + thread_single_cellid2[ThreadNumber].Peek());
                long cellid2;
                while(!thread_single_cellid2[ThreadNumber].TryDequeue(out cellid2)){
                  Thread.Sleep(1);
                }
                if(hasWeight){
                  float weight;
                  while(!thread_single_weight[ThreadNumber].TryDequeue(out weight)){
                    Thread.Sleep(1);
                  }
                  AddEdgeBasic(dequeued_cellid1, cellid2, weight);
                } else {
                  AddEdgeBasic(dequeued_cellid1, cellid2, -1);
                }
            }
            if(no_action && exponential_delay <= 8192){
              exponential_delay = exponential_delay * 2;
            } else if (!no_action){
              exponential_delay = 1;
            }
            Thread.Sleep(exponential_delay);
          }
        }

        public void AddEdgeQueue(long cellid1, Queue<long> cellid2s, Queue<float> weights){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.CloudStorage.Contains(cellid1)){
            simpleGraphNode.CellId = cellid1;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(cellid1);
          }
          while(cellid2s.Count > 0){
            simpleGraphNode.Outlinks.Add(cellid2s.Dequeue());
            simpleGraphNode.Weights.Add(weights.Dequeue());
          }
          // printGraphNode(simpleGraphNode);
          Global.CloudStorage.SaveSimpleGraphNode(simpleGraphNode);
        }

        public void AddEdgeQueue(long cellid1, Queue<long> cellid2s){
          //Console.WriteLine("Add " + cellid1 + " to " + cellid2);
          SimpleGraphNode simpleGraphNode = new SimpleGraphNode();
          if(!Global.CloudStorage.Contains(cellid1)){
            simpleGraphNode.CellId = cellid1;
            simpleGraphNode.Weights = new List<float>();
            simpleGraphNode.Outlinks = new List<long>();
          } else {
            simpleGraphNode = Global.CloudStorage.LoadSimpleGraphNode(cellid1);
          }
          while(cellid2s.Count > 0){
            simpleGraphNode.Outlinks.Add(cellid2s.Dequeue());
          }
          // printGraphNode(simpleGraphNode);
          Global.CloudStorage.SaveSimpleGraphNode(simpleGraphNode);
        }
    }
}
