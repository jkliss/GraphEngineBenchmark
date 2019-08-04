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
      /**
      Console.WriteLine("Started Run");
      int mapped_node;
      if(ranLoader == false){
        Console.WriteLine("Loader not run before");
      } else {
        benchmarkAlgorithm.setMaxNode(loader.getMaxNode());
        benchmarkAlgorithm.mapping1 = loader.mapping1;
        benchmarkAlgorithm.mapping2 = loader.mapping2;
        //benchmarkAlgorithm.mapping1_array = loader.mapping1_array;
        benchmarkAlgorithm.graph_name = graph_name;
        benchmarkAlgorithm.e_log_path = e_log_path;
        benchmarkAlgorithm.setOutputPath(e_output_path);
        mapped_node = (int) loader.mapping2[this.source_vertex];
        Console.WriteLine("Start at {0}", mapped_node);
        SimpleGraphNode rootNode = Global.CloudStorage.LoadSimpleGraphNode(mapped_node);
        benchmarkAlgorithm.BFS(rootNode);
        ranLoader = true;
        **/
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
      //}
      /**
      Thread.Sleep(1000);
      while(msgQueue.Count > 0){
          Thread.Sleep(10);
      }
      Console.WriteLine("FinishedServer0");**/
    }

    static void DistributedLoad(int server, DistributedLoad dload){
      using (var request = new DistributedLoadWriter(dload.serverID, dload.num_elements, dload.Loads))
      {
        Global.CloudStorage.DistributedLoadMessageToBenchmarkServer(server, request);
      }
    }

    public override void DistributedLoadMessageHandler(DistributedLoadReader request){
      try{
        //Console.WriteLine("Request at:" + request.serverID);
        Console.WriteLine("SEPARATE ALL");
        DistributedLoad dload = new DistributedLoad();
        dload.serverID = request.serverID;
        Console.WriteLine("ID SENT" + dload.serverID);
        dload.num_elements = request.num_elements;
        Console.WriteLine("NUM ELEMENTS SENT" + dload.num_elements);
        dload.Loads = new Load[dload.num_elements];
        for(int i = 0; i < dload.num_elements; i++){
          dload.Loads[i] = request.Loads[i];
        }
        Console.WriteLine("LOADS SENT");
        dload.lastLoad = request.lastLoad;
        Console.WriteLine("[SERVER] Try Request Enqueued");
        consumingQueue.Enqueue(dload);
        Console.WriteLine("[SERVER] Request Enqueued");
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
                Console.WriteLine("[SERVER] ADD LOAD");
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

    private static void StartBFS(long root) {
      for (int i = 0; i < Global.ServerCount; i++) {
        using (var msg = new StartBFSMessageWriter(root)) {
          Global.CloudStorage.StartBFSToBenchmarkServer(i, msg);
        }
      }
    }


    public override void StartBFSHandler(StartBFSMessageReader request) {
      if (Global.CloudStorage.IsLocalCell(request.root)) {
        Console.WriteLine("BFS Started (on this Machine) found Cell " + request.root);
        using (var rootCell = Global.LocalStorage.UseSimpleGraphNode(request.root)) {
          rootCell.Depth = 0;
          rootCell.parent = request.root;
          List<long> aliveNeighbors = new List<long>();
          for (int i = 0; i < rootCell.Outlinks.Count; i++) {
            if (Global.CloudStorage.Contains(rootCell.Outlinks[i])) {
              msgQueue.Enqueue(true);
              Console.WriteLine(">" + rootCell.Outlinks[i]);
              aliveNeighbors.Add(rootCell.Outlinks[i]);
            }
          }
          MessageSorter sorter = new MessageSorter(aliveNeighbors);
          for (int i = 0; i < Global.ServerCount; i++) {
            BFSUpdateMessageWriter msg = new BFSUpdateMessageWriter(rootCell.CellId, 0, sorter.GetCellRecipientList(i));
            Global.CloudStorage.BFSUpdateToBenchmarkServer(i, msg);
          }
        }
      }
    }

    public override void BFSUpdateHandler(BFSUpdateMessageReader request) {
      Console.WriteLine("Outgoing from " + request.senderId);
      Console.WriteLine("RC:" + request.level);
      request.recipients.ForEach((cellId) => {
        Console.WriteLine("<< CALL " + cellId);
        using (var cell = Global.LocalStorage.UseSimpleGraphNode(cellId)) {
          if (cell.Depth > request.level + 1) {
            cell.Depth = request.level + 1;
            cell.parent = request.senderId;
            List<long> aliveNeighbors = new List<long>();
            for (int i = 0; i < cell.Outlinks.Count; i++) {
                msgQueue.Enqueue(true);
                Console.WriteLine(">" + cell.Outlinks[i]);
                aliveNeighbors.Add(cell.Outlinks[i]);
            }
            //MessageSorter sorter = new MessageSorter(cell.Outlinks);
            MessageSorter sorter = new MessageSorter(aliveNeighbors);

            for (int i = 0; i < Global.ServerCount; i++) {
              BFSUpdateMessageWriter msg = new BFSUpdateMessageWriter(cell.CellId, cell.Depth, sorter.GetCellRecipientList(i));
              Global.CloudStorage.BFSUpdateToBenchmarkServer(i, msg);
            }
          }
        }
      });
      bool test;
      while(!msgQueue.TryDequeue(out test)){}
    }
  }
}
