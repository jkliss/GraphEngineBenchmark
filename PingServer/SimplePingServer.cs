using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System;
using System.IO;
using Trinity;

namespace PingServer
{
    class SimplePingServer : PingServerBase
    {

        private long max_edge = 0;
        public override void SynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received SynPing: {0}", request.Message);
            LoadGraph();
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(287770);
            bool dummy = true;
            BFS(dummy, rootNode);
        }

        public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response)
        {
            Console.WriteLine("Received SynEchoPing: {0}", request.Message);
            response.Message = request.Message;
        }

        public override void AsynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received AsynPing: {0}", request.Message);
        }


        void BFS(bool v, SimpleGraphNode root)
        {
            //init array with max distances --> requires amount of elements
            int graph_size = (int)max_edge;
            int[] depth = new int[graph_size + 1];
            for (int i = 1; i <= graph_size; i++)
            {
                depth[i] = int.MaxValue;
            }

            Queue queue = new Queue();

            queue.Enqueue(root);

            depth[root.CellId] = 0;

            while (queue.Count > 0)
            {
                SimpleGraphNode current_node = (SimpleGraphNode)queue.Dequeue();
                //Console.WriteLine(current_node.CellId);
                foreach (var out_edge_id in current_node.Outlinks)
                {
                    //Console.WriteLine("Outgoing: " + out_edge_id);
                    if (depth[out_edge_id] == int.MaxValue)
                    {
                        depth[out_edge_id] = depth[current_node.CellId] + 1;
                        try
                        {
                            queue.Enqueue(Global.CloudStorage.LoadSimpleGraphNode(out_edge_id));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Cell not found Error: " + out_edge_id);
                        }
                    }
                }
            }
            Console.WriteLine("-------------------------------------------------------");
            for (int i = 1; i <= graph_size; i++)
            {
                if (depth[i] != int.MaxValue)
                {
                    Console.WriteLine("Depth of " + i + " (from " + root.CellId + ") is " + depth[i]);
                }
            }
        }


        void LoadGraph()
        {
            // If graph is undirected process file with 
            //using (StreamReader reader = new StreamReader("/home/jkliss/dota-league.e_undirected"))
            using (StreamReader reader = new StreamReader("/home/jkliss/datagen-7_9-fb/datagen-7_9-fb.e_undirected"))
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
                              current_node,
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
