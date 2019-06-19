using System;
using Trinity;

namespace BenchmarkServer
{
  class Program
  {
    static void Main(string[] args)
    {
      // Trinity doesn't load the config file correctly if we don't tell it to
      TrinityConfig.LoadConfig();
      TrinityConfig.CurrentRunningMode = RunningMode.Server;
      // Create our BenchmarkServer implementation an start it.
      SimpleBenchmarkServer server = new SimpleBenchmarkServer();
      server.Start();

      Console.ReadKey();
    }
  }
}
