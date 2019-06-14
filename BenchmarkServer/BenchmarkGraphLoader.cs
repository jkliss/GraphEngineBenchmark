using System;
using System.Collections.Generic;
using System.IO;
using Trinity;

namespace BenchmarkServer
{
    public class BenchmarkGraphLoader
    {

        public long max_edge;
        public Dictionary<long, long> mapping = new Dictionary<long, long>();
        public String path = "/home/jkliss/";
        public long already_read_nodes = 0;


        public void setPath(String new_path){
            path = new_path;
        }

        public long getMaxEdge(){
            return max_edge;
        }

        public Dictionary<long, long> getMapping(){
          return mapping;
        }

        public long getAlreadyReadNodes(){
          return already_read_nodes;
        }

        public void LoadGraph()
        {
            // If graph is undirected process file with
            Console.WriteLine("Read File at: {0}", path);
            //using (StreamReader reader = new StreamReader("/home/jkliss/dota-league.e_undirected"))
            using (StreamReader reader = new StreamReader(path))
            {
                string line;
                string[] fields;
                // reserve -1 for first
                bool first = true;
                long current_node = -1;
                List<long> edges = new List<long>();
                List<float> weights = new List<float>();
                already_read_nodes = 0;
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
                        fields = line.Split(' ');
                        already_read_nodes++;
                        long read_node = long.Parse(fields[0]);
                        //mapping[already_read_nodes] = current_node;
                        if (current_node != read_node && !first)
                        {
                            Global.CloudStorage.SaveSimpleGraphNode(
                              already_read_nodes,
                              edges,
                              weights);
                            edges = new List<long>();
                            weights = new List<float>();
                            current_node = read_node;
                        }
                        else if (first)
                        {
                            current_node = read_node;
                            first = false;
                        }
                        long read_edge = long.Parse(fields[1]);
                        edges.Add(read_edge);
                        if (read_edge > max_edge)
                        {
                            max_edge = read_edge;
                        }
                        weights.Add(float.Parse(fields[2]));
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("Failed to import the line:");
                        Console.Error.WriteLine(ex.Message);
                        Console.Error.WriteLine(ex.StackTrace.ToString());
                        Console.Error.WriteLine(line);
                    }
                }
                Global.CloudStorage.SaveSimpleGraphNode(
                  current_node,
                  edges,
                  weights);
                Global.CloudStorage.SaveStorage();
            }
        }
    }
}
