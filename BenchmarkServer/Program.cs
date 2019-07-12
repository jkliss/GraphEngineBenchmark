using System;
using Trinity;
using System.Threading;

namespace BenchmarkServer
{
  class Program
  {
    static void Main(string[] args)
    {
      // Trinity doesn't load the config file correctly if we don't tell it to
      TrinityConfig.LoadConfig();
      TrinityConfig.CurrentRunningMode = RunningMode.Server;
      SimpleBenchmarkServer server = new SimpleBenchmarkServer();
      server.Start();
      while(true){
         Thread.Sleep(2000);
      }
    }
  }
}
