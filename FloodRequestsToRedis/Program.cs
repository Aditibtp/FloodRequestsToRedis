using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Threading;
using StackExchange.Redis;
using System.Net.Sockets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions;

namespace FloodRequestsToRedis
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            InitializeConfiguration();

            ThreadPool.SetMinThreads(200, 200);
            //CreateMultipleConnections();
            Thread[] array = new Thread[20];
            for (int i = 0; i < array.Length; i++)
            {
                // Start the thread with a ThreadStart.
                ThreadStart start = new ThreadStart(CallToChildThread);
                array[i] = new Thread(start);
                array[i].Start();
            }
            // Join all the threads.
            for (int i = 0; i < array.Length; i++)
            {
                array[i].Join();
            }
            Console.WriteLine("DONE");

            Console.ReadKey();

            //InitRedis();

        }

        public static void CallToChildThread()
        {
            Console.WriteLine("Child thread starts");

            // the thread is paused for 5000 milliseconds
            int sleepfor = 300000;
            ConnectionMultiplexer conn;
            string cacheConnection = Configuration[SecretName];
            conn = ConnectionMultiplexer.Connect(cacheConnection);
            Console.WriteLine("Connected : " + conn.IsConnected);
            IDatabase cache = conn.GetDatabase(0);
            Console.WriteLine("trying to set key : ");
            Console.WriteLine("Total keys in cache : " + cache.Execute("DBSIZE").ToString());
            string cacheCommand = "PING";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            for(int i = 0; i < 1000; i++)
            {
                Console.WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());
                Thread.Sleep(3000);
            }
            
            Thread.Sleep(sleepfor);
            Console.WriteLine("Child thread resumes");
        }

        private static IConfigurationRoot Configuration { get; set; }
        const string SecretName = "CacheConnection";

        private static void InitializeConfiguration()
        {
            var builder = new ConfigurationBuilder().AddUserSecrets<Program>();

            Configuration = builder.Build();
        }

        private static Lazy<ConnectionMultiplexer> lazyConnection = CreateConnection();
        private static Lazy<ConnectionMultiplexer> CreateConnection()
        {
            return new Lazy<ConnectionMultiplexer>(() =>
            {
                string cacheConnection = Configuration[SecretName];
                return ConnectionMultiplexer.Connect(cacheConnection);
            });
        }

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        public static List<ConnectionMultiplexer> multipleConnections;

        public static void CreateMultipleConnections()
        {
            
            ConnectionMultiplexer conn;
            for (int i=0; i<300; i++)
            {
                string cacheConnection = Configuration[SecretName];
                conn = ConnectionMultiplexer.Connect(cacheConnection);
                Console.WriteLine("Connected : " + conn.IsConnected);
                IDatabase cache = conn.GetDatabase(0);
                Console.WriteLine("trying to set key : ");
                Console.WriteLine("Total keys in cache : " + cache.Execute("DBSIZE").ToString());
                string cacheCommand = "PING";
                Console.WriteLine("\nCache command  : " + cacheCommand);
                Console.WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());

               
            }
        }
        public static void InitRedis()
        {
            InitializeConfiguration();
            Console.WriteLine("Hello World!");

            ThreadPool.SetMinThreads(200, 200);
            Console.WriteLine("Connected : " + Connection.IsConnected);
            IDatabase cache = Connection.GetDatabase(0);
            Console.WriteLine("trying to set key : ");
            GetDBStats();
            // Simple PING command
            string cacheCommand = "PING";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            Console.WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());

            // Simple get and put of integral data types into the cache
            cacheCommand = "GET Message";
            /*Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
            Console.WriteLine("Cache response : " + cache.StringGet("Message").ToString());
            cacheCommand = "SET Message \"Hello! The cache is working from a .NET Core console app!\"";
            Console.WriteLine("\nCache command  : " + cacheCommand + " or StringSet()");
            Console.WriteLine("Cache response : " + cache.StringSet("Message", "Hello! The cache is working from a .NET Core console app!").ToString());

            // Demonstrate "SET Message" executed as expected...
            cacheCommand = "GET Message";
            Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
            Console.WriteLine("Cache response : " + cache.StringGet("Message").ToString());*/

            // Get the client list, useful to see if connection list is growing...
            // Note that this requires allowAdmin=true in the connection string
            cacheCommand = "CLIENT LIST";
            /*Console.WriteLine("\nCache command  : " + cacheCommand);
            var endpoint = (System.Net.DnsEndPoint)GetEndPoints()[0];
            IServer server = GetServer(endpoint.Host, endpoint.Port);
            ClientInfo[] clients = server.ClientList();
            Console.WriteLine("Cache response :");
            foreach (ClientInfo client in clients)
            {
                Console.WriteLine(client.Raw);
            }*/

            //Console.WriteLine("Total keys in cache : " + db.Execute("DBSIZE").ToString());
            //string usedmem = db.Execute("INFO MEMORY").ToString();

            //Console.WriteLine("Total keys in cache : " + db.Execute("INFO").ToString());

            long keys = 0;
            for (int i = 0; i < 20; i++)
            {
                keys += AddDataBulk(Connection, numKeysPerShard: 10000, valueSize: 200);
                Console.WriteLine("Added {0}", keys);
            }
            /*var endpoint = cache.GetEndPoints()[0];
            IServer server = GetServer(endpoint.Host, endpoint.Port);
            ClientInfo[] clients = server.ClientList();
            Console.WriteLine("Cache response :");
            foreach (ClientInfo client in clients)
            {
                Console.WriteLine(client.Raw);
            }*/
            Console.WriteLine("All Done! Keys added: " + keys);
            Task.Delay(600000);
            Console.ReadLine();
        }

        /// <summary>
        /// Adds data to a redis cache using Lua scripting. Ensure that timeout settings are set appropriately on the
        /// ConnectionMultiplexer as each script evaluation may take a few seconds.
        /// </summary>
        /// <returns>The number of keys added to the cache</returns>
        public static long AddDataBulk(ConnectionMultiplexer redis, long numKeysPerShard, int valueSize)
        {
            IDatabase db = redis.GetDatabase(0);

            // total number of keys set, which is returned at the end of this method
            long totalKeys = 0;

            // generate a random value to use for all keys
            var rand = new Random();
            byte[] value = new byte[valueSize];
            rand.NextBytes(value);

            // in order to quickly populate a cache with data, we execute a small number of Lua scripts on each shard
            var clusterConfig = redis.GetServer(redis.GetEndPoints().First()).ClusterConfiguration;

            //Console.WriteLine($"cluster config: {clusterConfig.Nodes.Where(n => !n.IsReplica).Count()}");

            // non clustered case
            if (clusterConfig == null)
            {
                AddDataWithLuaScript(redis, numKeysPerShard, value, rand.NextDouble().ToString());
                return numKeysPerShard;
            }



            // clustered case
            foreach (var shard in clusterConfig.Nodes.Where(n => !n.IsReplica))
            {
                long totalKeysThisShard = 0;
                // numSlots is the number of slots over which to distribute a shard's keys
                int numSlots = Math.Min(100, shard.Slots.Sum((range) => range.To - range.From));
                var slots = new List<int>();
                for (int i = 0; i < numSlots; i++)
                {
                    // compute a value that hashes to this node and use it as a hash tag
                    var hashTagAndSlot = GetHashTagForNode(redis, shard.Slots, slots, rand);
                    // if numKeysPerShard < numSlots, put 1 key into the first numKeysPerShard slots
                    long numKeys = Math.Max(numKeysPerShard / numSlots, totalKeysThisShard < numKeysPerShard ? 1 : 0);

                    //AddDataWithLuaScript(redis, numKeys, value, hashTagAndSlot.Item1);
                    totalKeysThisShard += numKeys;
                    slots.Add(hashTagAndSlot.Item2);
                }
                totalKeys += totalKeysThisShard;
            }

            return totalKeys;
        }

        private static Tuple<string, int> GetHashTagForNode(ConnectionMultiplexer mx, IList<SlotRange> slotRanges, IList<int> excludedSlots, Random rand)
        {
            string hashTag = null;
            while (true)
            {
                hashTag = "-{" + rand.Next() + "}-";
                int slot = mx.HashSlot(hashTag);
                if ((excludedSlots == null || !excludedSlots.Contains(slot)) &&
                    (slotRanges.Any((range) => range.From <= slot && slot <= range.To)))
                {
                    return new Tuple<string, int>(hashTag, slot);
                }
            }
        }

        private static void AddDataWithLuaScript(ConnectionMultiplexer mx, long numKeys, byte[] value, string hashTag)
        {
            Console.WriteLine($"key hash {hashTag}");
            string script = @"
                        local i = 0
                        while i < tonumber(@numKeys) do
                            redis.call('set', 'key'..@hashTag..i, @value)
                            i = i + 1
                        end
                    ";

            var prepared = LuaScript.Prepare(script);
            mx.GetDatabase(0).ScriptEvaluate(prepared, new { numKeys = numKeys, value = value, hashTag = (RedisKey)hashTag });
        }

        public static void GetDBStats()
        {
            IDatabase db = Connection.GetDatabase(0);
            Console.WriteLine("Total keys in cache : " + db.Execute("DBSIZE").ToString());
            //Console.WriteLine("Total keys in cache : " + db.Execute("INFO").ToString());
        }
    }
}
