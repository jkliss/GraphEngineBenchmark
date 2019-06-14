using System;
using Trinity;

namespace BenchmarkClient
{
    class Program
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
        public String e_output_path;
        public String home_dir;
        public int num_machines;
        public int num_threads;

        // termination
        public long t_job_id;
        public String t_log_path;

        static void Main(string[] args)
        {
            // Trinity doesn't load the config file correctly if we don't tell it to.
            TrinityConfig.LoadConfig();
            TrinityConfig.CurrentRunningMode = RunningMode.Client;

            Program program = new Program();
            program.input_edge_path = "/asdf/";
            program.algorithm = "BFS";
            program.source_vertex = 1000;

            // local object instance
            program.setConfiguration();
        }


        void setConfiguration(){
            using (var request = new ConfigurationMessageWriter(graph_name,input_vertex_path,input_edge_path,l_output_path,directed,weighted,e_job_id,e_log_path,algorithm,source_vertex,maxIteration,damping_factor,input_path,e_output_path,home_dir,num_machines,num_threads,t_job_id,t_log_path))
            {
                Global.CloudStorage.ConfigurationToBenchmarkServer(0, request);
            }
        }





        void examples(){
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
