using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System;
using System.IO;
using Trinity;

namespace BenchmarkServer
{
    class SimpleBenchmarkServer : BenchmarkServerBase
    {

        private long max_edge = 0;
        public override void SynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received SynPing: {0}", request.Message);
        }

        public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response)
        {
            Console.WriteLine("Received SynEchoPing: {0}", request.Message);
            response.Message = request.Message;
        }

        public override void AsynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received AsynPing: {0}", request.Message);
            BenchmarkGraphLoader loader = new BenchmarkGraphLoader();
            loader.LoadGraph()
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(287770);
            bool dummy = true;
            BFS(dummy, rootNode);
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
    }
}
