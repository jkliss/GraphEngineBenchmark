using System;
using Trinity;

namespace BenchmarkClient
{
    class Program
    {
        // graph loading
        static String graph_name;
        static String input_vertex_path;
        static String input_edge_path;
        static String l_output_path;
        static bool directed;
        static bool weighted;

        // execuing
        static long e_job_id;
        static String e_log_path;
        static String algorithm;
        static long source_vertex;
        static long maxIteration;
        static double damping_factor;
        static String input_path;
        static String e_output_path;
        static String home_dir;
        static int num_machines;
        static int num_threads;

        // termination
        static long t_job_id;
        static String t_log_path;


        static void Main(string[] args)
        {
            // Trinity doesn't load the config file correctly if we don't tell it to.
            TrinityConfig.LoadConfig();
            TrinityConfig.CurrentRunningMode = RunningMode.Client;

            setConfiguration(1000,"/asdt/","BFS");
        }

        static void setConfiguration(long startNode, String edgePath, String algorithm){
            using (var request = new ConfigurationMessageWriter(startNode,edgePath,algorithm))
            {
                Global.CloudStorage.ConfigurationToBenchmarkServer(0, request);
            }            
        }


















        static void examples(){
            using (var request = new PingMessageWriter("Ping!1"))
            {
                Global.CloudStorage.SynPingToBenchmarkServer(0, request);
            }

            using (var request = new PingMessageWriter("Ping!2"))
            {
                Global.CloudStorage.AsynPingToBenchmarkServer(0, request);
            }

            using (var request = new PingMessageWriter("Ping!3"))
            {
                using (var response = Global.CloudStorage.SynEchoPingToBenchmarkServer(0, request))
                {
                    Console.WriteLine("Server Response: {0}", response.Message);
                }
            }
        }
    }
}
