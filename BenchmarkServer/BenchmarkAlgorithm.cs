using System.Collections;
using System;
using System.IO;
using Trinity;
using System.Collections.Generic;
using System.Globalization;

namespace BenchmarkServer
{
  public class BenchmarkAlgorithm
  {
    public long max_node = 1;
    public bool silent = true;
    public String graph_name = "XXX";
    public Dictionary<long, long> mapping1 = new Dictionary<long, long>();
    public Dictionary<long, long> mapping2 = new Dictionary<long, long>();
    public String output_path = "output.txt";
    public long elapsedTime_lastRun;
    public String e_log_path = "log.txt";
    public long Start_time_Stamp;
    public long End_time_Stamp;
    public bool directed;

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
        SimpleGraphNode current_node = (SimpleGraphNode)queue.Dequeue();
        //Console.WriteLine(current_node.CellId);
        foreach (var out_edge_id in current_node.Outlinks)
        {
          //Console.WriteLine("Outgoing: " + out_edge_id);
          nodes_visited++;
          if (nodes_visited % 1000000 == 0)
          {
            Console.Write(" Nodes Visited: " + nodes_visited / 1000000 + "M\r");
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
      for (int i = 1; i <= graph_size; i++)
      {
        try{
          using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path,true))
          {
            if (depth[i] != Int64.MaxValue)
            {
              if(!silent){
                Console.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
              }
              //file.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
            }
            file.WriteLine(mapping1[i] + " " + depth[i]);
          }
        } catch (Exception ex){
          TextWriter errorWriter = Console.Error;
          errorWriter.WriteLine(ex.Message);
        }
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
  }
}
