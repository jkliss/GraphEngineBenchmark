using System.Collections;
using System;
using System.IO;
using Trinity;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

namespace BenchmarkServer
{
  public class BenchmarkAlgorithm
  {
    public long max_node = 1;
    public bool silent = true;
    public String graph_name = "XXX";
    public long[] mapping1;
    public Dictionary<long, long> mapping2 = new Dictionary<long, long>();
    public String output_path = "output.txt";
    public long elapsedTime_lastRun;
    public String e_log_path = "log.txt";
    public long Start_time_Stamp;
    public long End_time_Stamp;
    public bool directed;
    public static int num_threads = Environment.ProcessorCount;
    public Thread[] threads = new Thread[num_threads];

    public long[] all_starts;

    public void setMaxNode(long new_max_node){
      max_node = new_max_node;
    }

    public void setMapping(Dictionary<long, long> new_map){
      mapping2 = new_map;
    }

    public void setOutputPath(String new_path){
      output_path = new_path;
    }

    public void BFS(SimpleGraphNode root)
    {
      Start_time_Stamp = (long)(DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds;
      var watch = System.Diagnostics.Stopwatch.StartNew();
      //init array with max distances --> requires amount of elements
      int graph_size = (int) max_node;
      Console.WriteLine("Size of Array: {0}", graph_size);
      Console.WriteLine("Try to access: {0}", root.CellId);

      Int64[] depth = new Int64[graph_size + 1];
      for (int i = 1; i <= graph_size; i++)
      {
        depth[i] = Int64.MaxValue;
      }

      Queue queue = new Queue();

      queue.Enqueue(root);

      depth[root.CellId] = 0;
      long nodes_visited = 0;
      while (queue.Count > 0)
      {
        SimpleGraphNode current_node = (SimpleGraphNode) queue.Dequeue();
        //Console.WriteLine(current_node.CellId);
        foreach (var out_edge_id in current_node.Outlinks)
        {
          //Console.WriteLine("Outgoing: " + out_edge_id);
          nodes_visited++;
          if (nodes_visited % 1000000 == 0)
          {
            //Console.Write(" Nodes Visited: " + nodes_visited / 1000000 + "M\r");
          }
          if (depth[out_edge_id] == Int64.MaxValue)
          {
            depth[out_edge_id] = depth[current_node.CellId] + 1;
            try
            {
              queue.Enqueue(Global.CloudStorage.LoadSimpleGraphNode(out_edge_id));
            }
            catch (Exception ex)
            {
              TextWriter errorWriter = Console.Error;
              errorWriter.WriteLine(ex.Message);
              errorWriter.WriteLine("Cell not found Error: " + out_edge_id);
            }
          }
        }
      }
      End_time_Stamp = (long)(DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds;
      watch.Stop();
      var elapsedMs = watch.ElapsedMilliseconds;
      elapsedTime_lastRun = elapsedMs;
      Console.WriteLine("----------------------------------");
      Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastRun);
      using (System.IO.StreamWriter file = new System.IO.StreamWriter("benchmark.info"))
      {
        file.WriteLine("Algorithm: " + graph_name + " Runtime:" + elapsedTime_lastRun);
      }

      /**using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path))
      {
        file.WriteLine("Algorithm: " + graph_name);
      }**/
      if(output_path == null || output_path == ""){
        output_path = "output.txt";
      }
      Console.WriteLine("Write File to " + output_path);

        try{
          using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path,true))
          {
            /**if(!silent)
            {
              if (depth[i] != Int64.MaxValue){
                Console.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
              }
              //file.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
            }**/
            for (int i = 1; i <= graph_size; i++)
            {
              //file.WriteLine(mapping1_array[i] + " " + depth[i]);
              file.WriteLine(mapping1[i-1] + " " + depth[i]); // hash alternative
            }
          }
        } catch (Exception ex){
          TextWriter errorWriter = Console.Error;
          errorWriter.WriteLine(ex.Message);
        }


      if(e_log_path == null || e_log_path == ""){
        e_log_path = "metrics.txt";
      } else {
        e_log_path = e_log_path + "/metrics.txt";
      }
      Console.WriteLine("Write Log File to " + e_log_path);

      try{
        using (System.IO.StreamWriter file = new System.IO.StreamWriter(@e_log_path,true))
        {
          file.WriteLine("Processing starts at " + Start_time_Stamp);
          file.WriteLine("Processing ends at " + End_time_Stamp);
        }
      } catch (Exception ex){
        TextWriter errorWriter = Console.Error;
        errorWriter.WriteLine(ex.Message);
      }

      Console.WriteLine("##################################");
      Console.WriteLine("#######    Finished Run    #######");
      Console.WriteLine("##################################");
    }

    int num_servers;
    public int findServer(long cell){
      try{
        for(int i = 1; i < num_servers; i++){
          if(cell < all_starts[i]) return i-1;
        }
        return num_servers-1;
      }  catch (Exception ex) {
        Console.WriteLine("ERROR UNABLE TO GET SERVERID");
        Console.Error.WriteLine(ex.Message);
        Console.Error.WriteLine(ex.StackTrace.ToString());
      }
      return -1;
    }

    public void BFSLocal(SimpleGraphNode root)
    {
      num_servers = Global.ServerCount;
      for(int i = 0; i < num_servers; i++){
        Console.WriteLine("SERVER" + i + " at " + all_starts[i]);
      }
      Start_time_Stamp = (long)(DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds;
      var watch = System.Diagnostics.Stopwatch.StartNew();
      //init array with max distances --> requires amount of elements
      int graph_size = (int) max_node;
      Console.WriteLine("Size of Array: {0}", graph_size);
      Console.WriteLine("Try to access: {0}", root.CellId);

      Int64[] depth = new Int64[graph_size + 1];
      for (int i = 1; i <= graph_size; i++)
      {
        depth[i] = Int64.MaxValue;
      }
      int this_server_id = Global.MyServerID;

      Queue<long> queue = new Queue<long>();
      queue.Enqueue(root.CellId);

      depth[root.CellId] = 0;
      long nodes_visited = 0;

      while (queue.Count > 0)
      {
        long current_node = queue.Dequeue();
        Console.WriteLine("Dequeued " + current_node);
        int onServer = findServer(current_node);
        Console.WriteLine("Outgoing From: " + current_node + " on Server" + onServer);
        if(onServer == this_server_id)
         {
          Console.WriteLine("[!] LOCAL " + current_node);
          using (var tempCell = Global.LocalStorage.UseSimpleGraphNode(current_node)) {
            for(int i = 0; i < tempCell.Outlinks.Count; i++){
              if (depth[tempCell.Outlinks[i]] > depth[current_node] + 1){
                depth[tempCell.Outlinks[i]] = depth[current_node] + 1;
                Console.WriteLine(tempCell.Outlinks[i] + " depth " + depth[tempCell.Outlinks[i]]);
                queue.Enqueue(tempCell.Outlinks[i]);
              }
            }
          }
        } else {
          Console.WriteLine("[?] ASK FOR " + current_node);
          using (var request = new NodeRequestWriter(current_node))
          {
            using (var response = Global.CloudStorage.NodeCollectionToBenchmarkServer(onServer, request))
            {
              Console.WriteLine("Response contains " + response.num_elements + "elements");
              for(int i = 1; i < response.num_elements; i++){
                int outlink = response.Outlinks[i];
                Console.WriteLine("Cell " + outlink);
                Console.WriteLine("CNODE " + current_node + " has depth " + depth[current_node]);
                if (depth[outlink] > depth[current_node] + 1){
                  depth[outlink] = depth[current_node] + 1;
                  Console.WriteLine(response.Outlinks[i] + " depth " + depth[response.Outlinks[i]]);
                  queue.Enqueue(response.Outlinks[i]);
                }
              }
            }
          }
        }
      }
      ////////////////////////// END ////////////////////////////////777
      End_time_Stamp = (long)(DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds;
      watch.Stop();
      var elapsedMs = watch.ElapsedMilliseconds;
      elapsedTime_lastRun = elapsedMs;
      Console.WriteLine("----------------------------------");
      Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastRun);
      using (System.IO.StreamWriter file = new System.IO.StreamWriter("benchmark.info"))
      {
        file.WriteLine("Algorithm: " + graph_name + " Runtime:" + elapsedTime_lastRun);
      }

      /**using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path))
      {
        file.WriteLine("Algorithm: " + graph_name);
      }**/
      if(output_path == null || output_path == ""){
        output_path = "output.txt";
      }
      Console.WriteLine("Write File to " + output_path);

        try{
          using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path,true))
          {
            /**if(!silent)
            {
              if (depth[i] != Int64.MaxValue){
                Console.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
              }
              //file.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
            }**/
            for (int i = 1; i <= graph_size; i++)
            {
              //file.WriteLine(mapping1_array[i] + " " + depth[i]);
              file.WriteLine(mapping1[i-1] + " " + depth[i]); // hash alternative
            }
          }
        } catch (Exception ex){
          TextWriter errorWriter = Console.Error;
          errorWriter.WriteLine(ex.Message);
        }


      if(e_log_path == null || e_log_path == ""){
        e_log_path = "metrics.txt";
      } else {
        e_log_path = e_log_path + "/metrics.txt";
      }
      Console.WriteLine("Write Log File to " + e_log_path);

      try{
        using (System.IO.StreamWriter file = new System.IO.StreamWriter(@e_log_path,true))
        {
          file.WriteLine("Processing starts at " + Start_time_Stamp);
          file.WriteLine("Processing ends at " + End_time_Stamp);
        }
      } catch (Exception ex){
        TextWriter errorWriter = Console.Error;
        errorWriter.WriteLine(ex.Message);
      }

      Console.WriteLine("##################################");
      Console.WriteLine("#######    Finished Run    #######");
      Console.WriteLine("##################################");
    }




    public void output_server(){

    }
  }
}
