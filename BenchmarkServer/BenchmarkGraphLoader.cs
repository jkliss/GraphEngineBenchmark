using System;
using System.Collections.Generic;
using System.IO;
using Trinity;
using System.Diagnostics;

namespace BenchmarkServer
{
    public class BenchmarkGraphLoader
    {
        // NODES WITH NO OUTGOING EDGES ARE NOT CONSIDERED!
        // cat example-directed.e | awk '{print $1}' | uniq | grep -w -v -f - example-directed.v

        public long max_node;
        public List<long> mapping = new List<long>();
        public String path = "/home/jkliss/";
        public String vpath = "";
        public Dictionary<long, long> mapping1 = new Dictionary<long, long>();
        public Dictionary<long, long> mapping2 = new Dictionary<long, long>();
        public long elapsedTime_lastLoadEdge = 0;
        public long elapsedTime_lastLoadVertex = 0;
        public bool hasWeight = false;
        public bool directed = false;

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
            var watch = System.Diagnostics.Stopwatch.StartNew();
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
                if(!directed){
                    makeUndirectedToDirected();
                }
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
                            //Console.WriteLine("Counted Node:" + current_node);
                            //Console.WriteLine("Mapped  Node:" +mapping2[current_node]);
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
                        if(hasWeight){
                          weights.Add(float.Parse(fields[2]));
                        }
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
                AddNodesWithoutEdges(current_node);
                //Console.WriteLine("Counted Node:" + current_node);
                //Console.WriteLine("Mapped  Node:" +mapping2[current_node]);
                Global.CloudStorage.SaveStorage();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            elapsedTime_lastLoadEdge = elapsedMs;
            Console.WriteLine("----------------------------------");
            Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadEdge);
            Console.WriteLine("##################################");
            Console.WriteLine("#######  All edges loaded  #######");
            Console.WriteLine("##################################");
        }

        public void loadVertices(){
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
          var elapsedMs = watch.ElapsedMilliseconds;
          elapsedTime_lastLoadVertex = elapsedMs;
          Console.WriteLine("----------------------------------");
          Console.WriteLine("Runtime: {0} (ms)", elapsedTime_lastLoadVertex);
          Console.WriteLine("##################################");
          Console.WriteLine("####### All vertices loaded ######");
          Console.WriteLine("##################################");
        }

        public void AddNodesWithoutEdges(long current_node){
            // TEMPORARY VERY BAD SOLUTION
            bash("cat " + path +" | awk '{print $1}' | uniq | grep -w -v -f - " + vpath + " > "+ vpath +"_missing");
            string line;
            using (StreamReader reader = new StreamReader(vpath + "_missing"))
            {
              while (null != (line = reader.ReadLine()))
              {
                long read_node = long.Parse(line);
                    Global.CloudStorage.SaveSimpleGraphNode(
                      mapping2[read_node],
                      new List<long>(),
                      new List<float>());
              }
            }
        }

        public String bash(String cmd)
        {
            var escapedArgs = cmd.Replace("\"", "\\\"");

            var process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                  FileName = "/bin/bash",
                  Arguments = $"-c \"{escapedArgs}\"",
                  RedirectStandardOutput = true,
                  UseShellExecute = false,
                  CreateNoWindow = true,
                }
              };
              process.Start();
              string result = process.StandardOutput.ReadToEnd();
              process.WaitForExit();
              return result;
        }

        public String makeUndirectedToDirected(){
            Console.WriteLine("Making Directed Edge File out of Undirected");
            bash("cat "+ path +" | awk \'{print; print $2\" \"$1\" \"$3}\' | sort -n -S 60% --parallel `nproc` > "+ path +"_directed");
            path = path + "_directed";
            Console.WriteLine("New Edge Path is: " + path);
            return "";
        }
    }
}
