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
        public String vertexPath;
        public String edgePath;
        public String outputPath;
        public Boolean directed;
        public Boolean weighted;
        public long startNode;
        public String algorithm;

        public override void SynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received SynPing: {0}", request.Message);
        }

        public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response)
        {
            Console.WriteLine("Set Algorithm to {0}", request.Message);
            response.Message = request.Message;
        }

        public override void AsynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received AsynPing: {0}", request.Message);
            BenchmarkGraphLoader loader = new BenchmarkGraphLoader();
            loader.setPath(edgePath);
            loader.LoadGraph();
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(startNode);
            bool dummy = true;
            BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
            benchmarkAlgorithm.setMaxEdge(loader.getMaxEdge());
            benchmarkAlgorithm.BFS(dummy, rootNode);
        }

        public override void ConfigurationHandler(ConfigurationMessageReader request)
        {
            Console.WriteLine("CONFIGURATION PACKET:");
            Console.WriteLine("Set Algorithm to {0}", request.algorithm);
            Console.WriteLine("Set edgePath to {0}", request.input_edge_path);
            Console.WriteLine("Set StartNode to {0}", request.source_vertex);
            edgePath = request.input_edge_path;
            startNode = request.source_vertex;
            algorithm = request.algorithm;
        }
    }
}
