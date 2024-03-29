using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System;
using System.IO;
using Trinity;
using Trinity.Network;
using System.Threading;
using System.Collections.Concurrent;

using Trinity.Core.Lib;
using Trinity.TSL.Lib;
using Trinity.Network.Messaging;


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
    //public int num_servers = Global.ServerCount;

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
    public ParallelBenchmarkGraphLoader[] loader = new ParallelBenchmarkGraphLoader[2];
    public bool ranLoader = false;
    public BenchmarkAlgorithm benchmarkAlgorithm = new BenchmarkAlgorithm();
    public bool ranRun = false;

    public bool isDedicatedLoader = false;
    public Thread consumerThread;
    public Thread communicationThread;
    public ConcurrentQueue<DistributedLoad> consumingQueue = new ConcurrentQueue<DistributedLoad>();
    public int finishCounter = 0;
    public ConcurrentQueue<bool> msgQueue = new ConcurrentQueue<bool>();

    public int running = 0;

    public override void SynPingHandler(ConfigurationMessageReader request)
    {
      Console.WriteLine("Received Graph: {0}", request.graph_name);
      LoadGraphHandler(request);
      RunHandler(request);
    }

    public override void SynEchoPingHandler(PingMessageReader request, PingMessageWriter response){}

    public override void AsynPingHandler(PingMessageReader request){}

    public override void VerifySetupHandler(PingMessageReader request, PingMessageWriter response){
      response.Message = "Everything up and running!";
    }

    public override void LoadGraphHandler(ConfigurationMessageReader request){
      consumerThread = new Thread(new ThreadStart(LoadConsumerThread));
      consumerThread.Start();
      if(Global.MyServerID == 0){
        for(int i = 1; i < Global.ServerCount; i++){
          communicationThread = new Thread(new ParameterizedThreadStart(CommunicationThread));
          communicationThread.Start(i);
        }
      }
      Console.WriteLine("NAME:" + this.input_vertex_path);
      int myID = Global.MyServerID;
      loader[myID] = new ParallelBenchmarkGraphLoader();
      Console.WriteLine("Started Load");
      loader[myID].setPath(this.input_edge_path);
      loader[myID].vpath = this.input_vertex_path;
      loader[myID].hasWeight = this.weighted;
      loader[myID].directed = directed;
      loader[myID].loadVertices();
      loader[myID].LoadGraph();
      ranLoader = true;
      //loader.dumpLoadCells();
    }

    public override void PrepareHandler(ConfigurationMessageReader request){
      // The platform requests computation resources from the cluster environment and makes the background applications ready.
    }

    public override void SetupHandler(ConfigurationMessageReader request){
    }

    public override void RunHandler(ConfigurationMessageReader request){
      Console.WriteLine("Started Run");
      int mapped_node;
      if(ranLoader == false){
        Console.WriteLine("Loader not run before");
      } else {
        benchmarkAlgorithm.setMaxNode(loader[Global.MyServerID].getMaxNode());
        benchmarkAlgorithm.mapping1 = loader[Global.MyServerID].mapping1;
        benchmarkAlgorithm.mapping2 = loader[Global.MyServerID].mapping2;
        //benchmarkAlgorithm.mapping1_array = loader.mapping1_array;
        benchmarkAlgorithm.graph_name = graph_name;
        benchmarkAlgorithm.e_log_path = e_log_path;
        benchmarkAlgorithm.setOutputPath(e_output_path);
        mapped_node = (int) loader[Global.MyServerID].mapping2[this.source_vertex];
        Console.WriteLine("Start at {0}", mapped_node);
        SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(mapped_node);
        benchmarkAlgorithm.all_starts = loader[Global.MyServerID].all_starts;
        benchmarkAlgorithm.BFSLocal(rootNode);
        ranLoader = true;

        /**
        //Distributed Try with Message Sorter
        for(int i = 0; i < Global.ServerCount; i++){
          FinishCommunicator fc = new FinishCommunicator();
          fc.Finished = false;
          fc.LastLoad = false;
          long fcid = i;
          //Console.WriteLine("CREATE COMMUNICATION CELL:" + fcid);
          Global.CloudStorage.SaveFinishCommunicator(Int64.MaxValue-fcid, fc);
        }
        **/
        //StartBFS(mapped_node);
      }
      /**
      Thread.Sleep(1000);
      while(msgQueue.Count > 0){
          Thread.Sleep(10);
      }
      Console.WriteLine("FinishedServer0");**/
    }

    static void DistributedLoad(int server, DistributedLoad dload){
      using (var request = new DistributedLoadWriter(dload.serverID, dload.fromServerID, dload.num_elements, dload.cellid1, dload.cellid2, dload.weight, dload.single_element, false))
      {
        Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(server, request);
      }
    }

    public override void DistributedLoadMessageHandler(DistributedLoadReader request){
      try{
        //Console.WriteLine("Request at:" + request.serverID);
        //Console.WriteLine("[SERVER] Try Request Enqueued");
        consumingQueue.Enqueue(request);
        //Console.WriteLine("[SERVER] Request Enqueued");
        //multi_loaders[request.serverID].addDistributedLoadToServer(request);
        /**
        As soon as finished flag is set --> Set finish too
        **/
      } catch (Exception ex){
          Console.Error.WriteLine(ex.Message);
          Console.Error.WriteLine(ex.StackTrace.ToString());
      }

    }

    public void LoadConsumerThread(){
        DistributedLoad dload;
        while(true){
          try{
            while(consumingQueue.TryDequeue(out dload)){
                //Console.WriteLine("[SERVER] ADD LOAD");
                loader[dload.serverID].addDistributedLoadToServer(dload);
            }
            if(dload.lastLoad){
              break;
            }
          } catch (Exception ex){
              Console.Error.WriteLine(ex.Message);
              Console.Error.WriteLine(ex.StackTrace.ToString());
          }
        }
    }

    public void CommunicationThread(object serverid){
        int i = (int) serverid;
        try{
          using (var request2 = new ConfigurationMessageWriter(this.graph_name,
                                                               this.input_vertex_path,
                                                               this.input_edge_path,
                                                               this.l_output_path,
                                                               this.directed,
                                                               this.weighted,
                                                               this.e_job_id,
                                                               this.e_log_path,
                                                               this.algorithm,
                                                               this.source_vertex,
                                                               this.maxIteration,
                                                               this.damping_factor,
                                                               this.input_path,
                                                               this.e_output_path,
                                                               this.home_dir,
                                                               this.num_machines,
                                                               this.num_threads,
                                                               this.t_job_id,
                                                               this.t_log_path))
          {
            Global.CloudStorage.LoadGraphToBenchmarkServer(i, request2);
          }
        } catch (Exception ex) {
          Console.WriteLine("Server" + i + " died!");
        }

    }

    public override void FinalizeHandler(ConfigurationMessageReader request){
      // The platform reports the benchmark information and makes the environment ready for the next benchmark run.
    }

    public override void TerminateHandler(ConfigurationMessageReader request){
      // The platform forcibly stops the benchmark job and clean up the environment, given that the time-out has been reached.
    }

    public override void DeleteGraphHandler(ConfigurationMessageReader request){
      //Console.WriteLine(Global.GetTotalMemoryUsage());
      Global.CloudStorage.ResetStorage();
    }


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

      Console.WriteLine("##################################");
      Console.WriteLine("SENT CONFIGURATION PACKET:");
      showConfiguration();
      Console.WriteLine("##################################");
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

    public long[] depths;

    public override void NodeCollectionHandler(NodeListReader request, NodeListWriter response){
      if(Global.LocalStorage.Contains(request.cellnum)){
        using (var requestedCell = Global.LocalStorage.UseSimpleGraphNode(request.cellnum)) {
          int index = 0;
          for(int i = 0; i < requestedCell.Outlinks.Count; i++){
            response.Outlinks.Add(requestedCell.Outlinks[i]);
            index++;
          }
          response.num_elements = index;
        }
      } else {
        Console.WriteLine("CELL " + request.cellnum + " not found");
      }
    }

    public override void BatchNodeCollectionHandler(NodeListReader request, NodeListWriter response){
      int index = 0;
      HashSet<long> send_set = new HashSet<long>();
      foreach(long cell_request in request.Outlinks){
        if(Global.LocalStorage.Contains(cell_request)){
          using (var requestedCell = Global.LocalStorage.UseSimpleGraphNode(cell_request)) {
            for(int i = 0; i < requestedCell.Outlinks.Count; i++){
              send_set.Add(requestedCell.Outlinks[i]);
            }
          }
        } else {
          Console.WriteLine("CELL " + request.cellnum + " not found");
        }
      }
      List<long> outlinks = new List<long>();
      foreach(long cell in send_set){
        if(outlinks.Count >= 10000){
          //Console.WriteLine("Send Package with " + outlinks.Count);
          using (var request2 = new NodeListWriter(0, outlinks.Count, outlinks))
          {
            Global.CloudStorage.NodeSenderToBenchmarkServer(0, request2);
          }
          outlinks = new List<long>();
          index = 0;
        }
        //Console.WriteLine("Add " + cell);
        outlinks.Add(cell);
        //response.Outlinks.Add(cell);
        index++;
      }
      //Console.WriteLine("Send Package with " + outlinks.Count);
      using (var request2 = new NodeListWriter(0, outlinks.Count, outlinks))
      {
        Global.CloudStorage.NodeSenderToBenchmarkServer(0, request2);
      }
      //Console.WriteLine("SENDING " + index + " Elements");
      //response.num_elements = index;
    }

    public override void NodeSenderHandler (NodeListReader request){
      //Console.WriteLine("RECIEVED PART CONTAINING " + request.num_elements);
      List<long> array = request.Outlinks;
      for(int i = 0; i < request.num_elements; i++){
        //Console.WriteLine("Cell " + outlink);
        benchmarkAlgorithm.remote_outlinks.Enqueue(array[i]);
      }
    }
  }
}
