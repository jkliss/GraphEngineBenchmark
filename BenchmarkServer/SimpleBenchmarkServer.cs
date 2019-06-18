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
      // graph loading
      public String graph_name;
      public String input_vertex_path;
      public String input_edge_path;
      public String l_output_path;
      public bool directed;
      public bool weighted;

      // execuing
      public long e_job_id;
      public String e_log_path;
      public String algorithm;
      public long source_vertex;
      public long maxIteration;
      public double damping_factor;
      public String input_path;
      public String e_output_path = "output.txt";
      public String home_dir;
      public int num_machines;
      public int num_threads;

      // termination
      public long t_job_id;
      public String t_log_path;

      //references_to_values
      public BenchmarkGraphLoader loader = new BenchmarkGraphLoader();
      public bool ranLoader = false;
      public BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
      public bool ranRun = false;

        public override void SynPingHandler(ConfigurationMessageReader request)
        {
          Console.WriteLine("Received Graph: {0}", request.graph_name);
          LoadGraphHandler(request);
          Console.WriteLine("Ran Loader!");
          RunHandler(request);
        }

        public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response){}

        public override void AsynPingHandler(PingMessageReader request){}

        public override void VerifySetupHandler(PingMessageReader request, PingMessageWriter response){
            response.Message = "Everything up and running!";
        }

        public override void LoadGraphHandler(ConfigurationMessageReader request){
          Console.WriteLine("Started LOAD");
          loader.setPath(this.input_edge_path);
          loader.vpath = this.input_vertex_path;
          loader.loadVertices();
          loader.LoadGraph();
          ranLoader = true;
        }

        public override void PrepareHandler(ConfigurationMessageReader request){

        }

        public override void SetupHandler(ConfigurationMessageReader request){

        }

        public override void RunHandler(ConfigurationMessageReader request){
          Console.WriteLine("Started RUN");
          if(ranLoader == false){
            Console.WriteLine("Loader not run before");
          } else {
            benchmarkAlgorithm.setMaxNode(loader.getMaxNode());
            benchmarkAlgorithm.mapping1 = loader.mapping1;
            benchmarkAlgorithm.mapping2 = loader.mapping2;
            benchmarkAlgorithm.graph_name = graph_name;
            benchmarkAlgorithm.setOutputPath(e_output_path);
            int mapped_node = (int) loader.mapping2[this.source_vertex];
            Console.WriteLine("Start at {0}", mapped_node);
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(mapped_node);
            benchmarkAlgorithm.BFS(rootNode);
            ranLoader = true;
          }
        }

        public override void FinalizeHandler(ConfigurationMessageReader request){

        }

        public override void TerminateHandler(ConfigurationMessageReader request){

        }

        public override void DeleteGraphHandler(ConfigurationMessageReader request){

        }


/**
        public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response)
        {
          Console.WriteLine("Received AsynPing: {0}", request.Message);
          BenchmarkGraphLoader loader = new BenchmarkGraphLoader();
          loader.setPath(this.input_vertex_path);
          loader.LoadGraph();
          bool dummy = true;
          BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
          benchmarkAlgorithm.setMaxNode(loader.getMaxNode());
          List<long> mapping = loader.getMapping();
          int mapped_node = (int) mapping.Find(x => x == this.source_vertex);
          SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(mapped_node);
          benchmarkAlgorithm.BFS(dummy, rootNode);
        }

        public override void AsynPingHandler(PingMessageReader request)
        {
            Console.WriteLine("Received AsynPing: {0}", request.Message);
            BenchmarkGraphLoader loader = new BenchmarkGraphLoader();
            loader.setPath(this.input_vertex_path);
            loader.LoadGraph();
            bool dummy = true;
            BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
            benchmarkAlgorithm.setMaxNode(loader.getMaxNode());
            List<long> mapping = loader.getMapping();
            int mapped_node = (int) mapping.Find(x => x == this.source_vertex);
            SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(mapped_node);
            benchmarkAlgorithm.BFS(dummy, rootNode);
        }
**/
        public override void ConfigurationHandler(ConfigurationMessageReader request)
        {
          // graph loading
          graph_name = request.graph_name;
          input_vertex_path = request.input_vertex_path;
          input_edge_path = request.input_edge_path;
          l_output_path = request.l_output_path;
          directed = request.directed;
          weighted = request.weighted;

          // execuing
          e_job_id = request.e_job_id;
          e_log_path = request.e_log_path;
          algorithm = request.algorithm;
          source_vertex = request.source_vertex;
          maxIteration = request.maxIteration;
          damping_factor = request.damping_factor;
          input_path = request.input_path;
          e_output_path = request.e_output_path;
          home_dir = request.home_dir;
          num_machines = request.num_machines;
          num_threads = request.num_threads;

          // termination
          t_job_id = request.t_job_id;
          t_log_path = request.t_log_path;

          Console.WriteLine("############################################");
          Console.WriteLine("SENT CONFIGURATION PACKET:");
          showConfiguration();
          Console.WriteLine("############################################");
        }

        public void showConfiguration(){
          Console.WriteLine("Graph Name:           {0}", graph_name);
          Console.WriteLine("Input Vertex Path:    {0}", input_vertex_path);
          Console.WriteLine("Input Edge Path:      {0}", input_edge_path);
          Console.WriteLine("Loader Output Path:   {0}", l_output_path);
          Console.WriteLine("Directed:             {0}", directed);
          Console.WriteLine("Weighted:             {0}", weighted);
          Console.WriteLine("Execute Job ID:       {0}", e_job_id);
          Console.WriteLine("Execute Log Path:     {0}", e_log_path);
          Console.WriteLine("Algorithm:            {0}", algorithm);
          Console.WriteLine("Source Vertex:        {0}", source_vertex);
          Console.WriteLine("Max Iteration:        {0}", maxIteration);
          Console.WriteLine("Damping Factor:       {0}", damping_factor);
          Console.WriteLine("Input Path:           {0}", input_path);
          Console.WriteLine("Execute Output Path:  {0}", e_output_path);
          Console.WriteLine("Home Dir:             {0}", home_dir);
          Console.WriteLine("Num Machines:         {0}", num_machines);
          Console.WriteLine("Num Threads:          {0}", num_threads);
          Console.WriteLine("Termination Job ID:   {0}", t_job_id);
          Console.WriteLine("Termination Log Path: {0}", t_log_path);
        }
    }
}
