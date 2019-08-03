using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace SeekRead
{
    class Program
    {
        public static int num_parts = 12;
        public static int buffersize = 4096;
        public static long[] last_node = new long[num_parts];
        public static long[] first_read_node = new long[num_parts];
        public static Thread[] threads = new Thread[num_parts];
        public static String path = "/home/julius/Downloads/example-undirected/example-undirected.e";

        static void Main(string[] args)
        {
          if(new FileInfo(path).Length <= buffersize*num_parts){
            Console.WriteLine("FILE TOO SMALL - SWITCH TO SINGLE THREAD");
            num_parts = 1;
          }
          for(int i = num_parts; i >= 1; i--){
            first_read_node[i-1] = -1;
          }
          for(int i = num_parts; i >= 1; i--){
            threads[i-1] = new Thread(new ParameterizedThreadStart(ParallelReading));
            threads[i-1].Start(i);
          }
          for(int i = 1; i <= num_parts; i++){
            threads[i-1].Join();
          }
          /**for(int i = 0; i < num_parts; i++){
            Console.WriteLine("---" + i + "---");
            Console.WriteLine("FIRST READ " + first_read_node[i]);
            Console.WriteLine("LAST READ " + last_node[i]);
          }**/
        }

        public static void ParallelReading(object par_part)
        {
          int part = (int) par_part;
          long length = new FileInfo(path).Length;
          string[] fields;
          long read_node = -1;
          long current_node = -1;
          using (FileStream fs = new FileStream(@path, FileMode.Open, FileAccess.Read)) {
            Console.WriteLine("["+part+"] JUMP TO: " + ((length/num_parts)*(part-1)));
            fs.Seek((length/num_parts)*(part-1), SeekOrigin.Begin);
            string line = "";
            using(StreamReader sr = new StreamReader(fs)){
              // SKIP ALL LINES UNTIL NEW NODE REACHED
              if(part != 1){
                // SKIP FIRST LINE AS JUMP CAN BE BETWEEN LINES
                sr.ReadLine();
                // READ FIRST LINE
                line = sr.ReadLine();
                fields = line.Split(' ');
                read_node = long.Parse(fields[0]);
                current_node = read_node;
                // JUMP OVER ALL LINES OUTGOING FROM THIS NODE
                while(current_node == read_node){
                  line = sr.ReadLine();
                  if(line == null) break;
                  fields = line.Split(' ');
                  read_node = long.Parse(fields[0]);
                }
                // FIRST NODE READ
                first_read_node[part-1] = read_node;
              } else {
                // IF FIRST PART JUST START READING
                line = sr.ReadLine();
                fields = line.Split(' ');
                read_node = long.Parse(fields[0]);
                current_node = read_node;
                first_read_node[part-1] = read_node;
              }
              while(part < num_parts && first_read_node[part] == -1){
                  Thread.Sleep(50);
                  //Console.WriteLine("["+part+"] WAIT FOR THREAD");
              }
              //if(part < num_parts) Console.WriteLine("["+part+"] UNTIL: " + first_read_node[part]);
              using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"out"+part,true))
              {
                do{
                  if(line == null) break;
                  fields = line.Split(' ');
                  read_node = long.Parse(fields[0]);
                  if(part != num_parts && read_node >= first_read_node[part]) break;
                  file.WriteLine(line);
                  line = sr.ReadLine();
                } while (part == num_parts || read_node < first_read_node[part]);
                current_node = read_node;
                last_node[part-1] = current_node;
              }
            }
          }
        }
    }
}
