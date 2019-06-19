using System.Collections;
using System;
using System.IO;
using Trinity;
using System.Collections.Generic;

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
      //init array with max distances --> requires amount of elements
      int graph_size = (int) max_node;
      Console.WriteLine("Size of Array: {0}", graph_size);
      Console.WriteLine("Try to access: {0}", root.CellId);

      int[] depth = new int[graph_size + 1];
      for (int i = 1; i <= graph_size; i++)
      {
        depth[i] = int.MaxValue;
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
          if (nodes_visited % 1000 == 0)
          {
            Console.Write(" Nodes Visited: " + nodes_visited / 1000 + "K\r");
          }
          if (depth[out_edge_id] == int.MaxValue)
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
      Console.WriteLine("-------------------------------------------------------");
      using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path))
      {
        file.WriteLine("Algorithm: " + graph_name);
      }

      for (int i = 1; i <= graph_size; i++)
      {
        try{
          using (System.IO.StreamWriter file = new System.IO.StreamWriter(@output_path,true))
          {
            if (depth[i] != int.MaxValue)
            {
              if(!silent){
                Console.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
              }
              file.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i] + " Mapped to: " + mapping1[i]);
            }
          }
        } catch (Exception ex){
          TextWriter errorWriter = Console.Error;
          errorWriter.WriteLine(ex.Message);
        }
      }
      Console.WriteLine("##################################");
      Console.WriteLine("#######    Finished Run    #######");
      Console.WriteLine("##################################");
    }
  }
}
