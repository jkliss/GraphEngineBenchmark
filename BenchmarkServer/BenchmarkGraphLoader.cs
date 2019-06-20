using System;
using System.Collections.Generic;
using System.IO;
using Trinity;

namespace BenchmarkServer
{
    public class BenchmarkGraphLoader
    {

        public long max_node;
        public List<long> mapping = new List<long>();
        public String path = "/home/jkliss/";
        public String vpath = "";
        public Dictionary<long, long> mapping1 = new Dictionary<long, long>();
        public Dictionary<long, long> mapping2 = new Dictionary<long, long>();

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

        public void LoadGraph()
        {
            // If graph is undirected process file with
            Console.WriteLine("Read File at: {0}", path);
            using (StreamReader reader = new StreamReader(path))
            {
                string line;
                string[] fields;
                // reserve -1 for first
                bool first = true;
                long current_node = -1;
                List<long> edges = new List<long>();
                List<float> weights = new List<float>();
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
                        long read_node = long.Parse(fields[0]);
                        if (current_node != read_node && !first)
                        {
                            Global.CloudStorage.SaveSimpleGraphNode(
                              mapping2[current_node],
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
                        edges.Add(mapping2[read_edge]);
                        if (read_edge > max_node)
                        {
                            max_node = read_edge;
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
                  mapping2[current_node],
                  edges,
                  weights);
                Global.CloudStorage.SaveStorage();
            }
            Console.WriteLine("##################################");
            Console.WriteLine("#######  All edges loaded  #######");
            Console.WriteLine("##################################");
        }

        public void loadVertices(){
          // If graph is undirected process file with
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
              max_node = read_lines;
          }
          Console.WriteLine("##################################");
          Console.WriteLine("####### All vertices loaded ######");
          Console.WriteLine("##################################");
        }
    }
}
