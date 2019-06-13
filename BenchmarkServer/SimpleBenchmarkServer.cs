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
            loader.LoadGraph();
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(287770);
            bool dummy = true;
            BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
            benchmarkAlgorithm.setMaxEdge(loader.getMaxEdge());
            benchmarkAlgorithm.BFS(dummy, rootNode);
        }
    }
}
