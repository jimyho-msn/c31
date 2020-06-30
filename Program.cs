using AdoNetCore.AseClient;
using System;
using System.Data;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.IO.Compression;

namespace c31
{
    class Program
    {
        static void Main(string[] args)
        {
            int iParamCount = args.Length;
            Console.WriteLine("Parameters count: " + args.Length.ToString());            
            if (args != null && args.Length > 0) //打印出参数
            {
                foreach (var item in args)
                {
                    Console.WriteLine(item);
                }
            }
            
            if (iParamCount < 2)
            {
                Console.WriteLine("c31 Command descrition, for windows/linux.");
                Console.WriteLine("Ex.");
                Console.WriteLine(@"   c31 db_policy..tpol_mist d:\bcpout\db_policy.dbo.tpol_mist.dat   (export sybase table to file)");
                Console.WriteLine("Ex.");
                Console.WriteLine("   c31 \"select * from db_policy..table_1 wher a = 3\"  c:\\temp\\db_policy..table_1.dat   (export sybase output data to file)");
                Console.WriteLine("Ex.");
                Console.WriteLine(@"   c31 d:\bcpout\db_policy..tpol_mist.dat azure_table_name bulkcopy   (import data file to Azure SQL)");
            }
            else if (iParamCount == 2)
            {
                /*
                if (!System.IO.Directory.Exists(args[1]))
                {
                    System.IO.Directory.CreateDirectory(args[1]);
                }
                */
                Console.WriteLine("Exporting Data, Please Wait for a while.");
                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                Encoding.RegisterProvider(new MyEncodingProvider());
                string sConn = GetConnectionString("Sybase");
                //int iCount = GenerateTableXML(args[0], args[1], ""); //("db_iws_ref..tsys_cntl", "");
                int iCount = GenerateTableBinary(args[0], args[1], sConn);
                string sFile = args[1]; //System.IO.Path.Combine(args[1], args[0] + "");
                Console.WriteLine("Save to: " + sFile + ".table,RowCount = " + iCount.ToString());
                Console.WriteLine("Total： " + (sw.ElapsedMilliseconds).ToString() + " ms");
            }
            else if (iParamCount == 3)
            {
                //Console.WriteLine(args[0] + "," + args[1] + "," + args[2]);
                if (System.IO.File.Exists(args[0]))
                {
                    if (args[2].ToLower() == "bulkcopy")
                    {
                        Console.WriteLine("Importing Data, Please Wait for a while.");
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        //DataTable dt = new DataTable();
                        //int iCount = LoadXml2Table(args[0], ref dt);
                        //int iCount = LoadBinary2Table(args[0], ref dt);
                        DataTable dt = DataSetDeserializeDecompress(args[0]);
                        int iCount = dt.Rows.Count;

                        Console.WriteLine(args[1] + " Loaded,RowCount = " + iCount.ToString());
                        string sConn = GetConnectionString("Azure");
                        Console.WriteLine("Start bulk insert into " + args[1]);
                        BulkInsertTable(ref dt, args[1], sConn);
                        Console.WriteLine("End bulk insert into " + args[1]);
                        Console.WriteLine("Total： " + (sw.ElapsedMilliseconds).ToString() + " ms");
                    }
                }
                else
                {
                    Console.WriteLine("File not found: " + args[0]);
                }
            }
            else
            {

            }
            //Console.ReadKey();            
        }

        private static string GetConnectionString(string db)
        {
            string sRet = "";
            string sConfigFile = Path.Combine(Directory.GetCurrentDirectory(), "conn.config");
            if (!File.Exists(sConfigFile))
            {
                Console.WriteLine("config file( conn.config ) not found!");
                return sRet;
            }
            using (StreamReader sr = new StreamReader(sConfigFile))
            {
                try
                {
                    string sLine1 = sr.ReadLine();
                    string sLine2 = sr.ReadLine();
                    if (db.ToLower() == "azure")
                    {
                        sRet = sLine2;
                    }
                    else if (db.ToLower()  == "sybase")
                    {
                        sRet = sLine1;
                    }
                }
                catch
                {

                }
            }
            return sRet;
        }

        private static void GetConnectionString(ref string sybase, ref string azure)
        {
            
            string sConfigFile = Path.Combine(Directory.GetCurrentDirectory(), "conn.config");
            if (!File.Exists(sConfigFile))
            {
                Console.WriteLine("config file( conn.config ) not found!");                
                return;
            }
            using (StreamReader sr = new StreamReader(sConfigFile))
            {
                try
                {
                    sybase = sr.ReadLine();
                    azure = sr.ReadLine();
                }
                catch
                {

                }
            }
        }

        private static void BulkInsertTable(ref DataTable dt, string fullTableName, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                try
                {
                    Console.WriteLine("Importing Data, Please Wait for a while.");
                    int iBatchSize = 1000;
                    //System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    //sw.Start();
                    conn.Open();
                    string sql = "select count(1) from " + fullTableName;
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    int iRowCount = (Int32)cmd.ExecuteScalar();
                    iBatchSize = getBatchSize(iRowCount);

                    Console.WriteLine("Total " + iRowCount.ToString() + " rows of Table: " + fullTableName);
                    //--get columns : sql = "select top 0 * from " + src;
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.BatchSize = iBatchSize;
                        bulkCopy.NotifyAfter = 1000;
                        bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(bulkCopy_SqlRowsCopied);
                        bulkCopy.DestinationTableName = fullTableName;
                        bulkCopy.WriteToServer(dt);
                        Console.WriteLine(dt.Rows.Count.ToString() + " rows inserted.");
                    }
                    //sw.Stop();
                    //Console.WriteLine("Total：" + (sw.ElapsedMilliseconds/1000).ToString()  + " s");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static int LoadXml2Table(string fileName, ref DataTable dt)
        {
            int iRet = 0;
            dt.Clear();
            if (System.IO.File.Exists(fileName))
            {
                if (dt != null)
                {
                    dt.ReadXmlSchema(fileName);
                    dt.ReadXml(fileName);
                    iRet = dt.Rows.Count;
                };
            }
            return iRet;
        }

        private static int GenerateTableXML(string tableName,string dir,string connectionString)
        {
            int iRet = 0;
            string sConn = "";
            if (connectionString.Length < 3)
            {
                sConn = "Data Source = hkgdcussyb010; Port = 4105; Database = db_policy; Uid = huat059; Pwd = K5hlYMI%; charset = cp850";
            }
            else
            {
                sConn = connectionString;
            }

            try
            {
                AseConnection cn = new AseConnection(sConn);
                AseCommand cmd = new AseCommand();
                cmd.Connection = cn;
                AseDataAdapter da = new AseDataAdapter(cmd);
                string sPureTableName = "";
                if (tableName.ToLower().StartsWith("select"))
                {
                    cmd.CommandText = "select count(1) from (" + tableName + ") a";
                    sPureTableName = "return_data";
                }
                else
                {
                    cmd.CommandText = "select * from " + tableName;
                    sPureTableName = getTableName(tableName);
                }

                DataTable dt = new DataTable(sPureTableName);
                da.Fill(dt);
                iRet = dt.Rows.Count;
                dt.WriteXml(System.IO.Path.Combine(dir, sPureTableName + ".table"), XmlWriteMode.WriteSchema);
                //Console.WriteLine("Save to: "  + tableName + ".table,RowCount = " + iRet.ToString());
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
            }                       

            return iRet;
        }

        private static int LoadBinary2Table(string fileName, ref DataTable dt)
        {
            int iRet = 0;
            dt.Clear();
            if (System.IO.File.Exists(fileName))
            {
                if (dt != null)
                {
                    DeserializeDataTable(fileName, ref dt);
                    //dt.ReadXmlSchema(fileName);
                    //dt.ReadXml(fileName);
                    iRet = dt.Rows.Count;
                };
            }
            return iRet;
        }

        private static int GenerateTableBinary(string tableName, string fileName, string connectionString)
        {
            int iRet = 0;
            string sConn = "";
            if (connectionString.Length < 3)
            {
                sConn = "Data Source = hkgdcussyb010; Port = 4105; Database = db_policy; Uid = huat059; Pwd = K5hlYMI%; charset = cp850";
            }
            else
            {
                sConn = connectionString;
            }

            try
            {
                AseConnection cn = new AseConnection(sConn);
                AseCommand cmd = new AseCommand();
                cmd.Connection = cn;
                AseDataAdapter da = new AseDataAdapter(cmd);
                string sPureTableName = "";
                if (tableName.ToLower().StartsWith("select"))
                {
                    cmd.CommandText = "select count(1) from (" + tableName + ") a";
                    sPureTableName = "return_data";
                }
                else
                {
                    cmd.CommandText = "select * from " + tableName;
                    sPureTableName = getTableName(tableName);
                }

                DataTable dt = new DataTable(sPureTableName);
                da.Fill(dt);
                iRet = dt.Rows.Count;
                //string sFullTableName = getFullTableName(tableName);
                //Boolean isOK = SerializeDataTable(dt, fileName);
                DataSetSerializerCompression(dt, fileName);
                //dt.WriteXml(System.IO.Path.Combine(dir, sPureTableName + ".table"), XmlWriteMode.WriteSchema);
                //Console.WriteLine("Save to: "  + tableName + ".table,RowCount = " + iRet.ToString());
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return iRet;
        }

        private static string getTableName(string tableName)
        {
            if (tableName.IndexOf("..") > 0)
            {
                return tableName.Substring(tableName.IndexOf("..") + 2);
            }
            else if (tableName.IndexOf("dbo.") > 0)
            {
                return tableName.Substring(tableName.IndexOf("dbo.") + 4);
            }
            else if (tableName.IndexOf(".") > 0)
            {
                return tableName.Substring(tableName.IndexOf(".") + 1);
            }
            else
            {
                return tableName;
            }
        }

        private static string getFullTableName(string tableName)
        {
            if (tableName.IndexOf("..") > 0)
            {                
                return tableName.Replace("..", ".dbo.");
            }
            else
            {
                return tableName;
            }
        }

        public static int getBatchSize(int recordCount)
        {
            int iBatchCount = 1000;
            if (recordCount < 10000)
            {
                iBatchCount = recordCount;
            }
            else if (recordCount < 100000)
            {
                iBatchCount = 1000;
            }
            else if (recordCount < 200000)
            {
                iBatchCount = 2000;
            }
            else if (recordCount < 500000)
            {
                iBatchCount = 5000;
            }
            else
            {
                iBatchCount = 10000;
            }
            return iBatchCount;
        }
        public static void bulkCopy_SqlRowsCopied(object obj, SqlRowsCopiedEventArgs e)
        {
            //执行事件处理方法
            Console.WriteLine(e.RowsCopied.ToString() + " rows inserted.");
            //lstLog.Items.Add(e.RowsCopied.ToString());
        }

        //  序列化文件和反序列化
        public static Boolean SerializeDataTable(DataTable dt, string path)
        {
            Boolean isOK = false;
            try
            {
                FileStream fs = new FileStream(path, FileMode.Create);
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(fs, dt);
                fs.Close();
                isOK = true;
                //MessageBox.Show("文件保存成功");
            }
            catch (Exception ex)
            {
                Console.WriteLine("SerializeDataTable Error: " + ex.Message);
            }
            return isOK;
        }

        public static void DeserializeDataTable(string path,ref DataTable dt)
        {            
            if (dt == null)
            {
                dt = new DataTable();
            }
            try
            {
                FileStream fs = new FileStream(path, FileMode.Open);
                BinaryFormatter bf = new BinaryFormatter();
                dt = (DataTable)bf.Deserialize(fs);
                fs.Close();
            }
            catch (Exception)
            {
                //MessageBox.Show("读取文件失败！");
            }            
        }
        
        public class MyEncodingProvider : EncodingProvider
        {
            public override Encoding GetEncoding(int codepage)
            {
                return null; // we're only matching on name, not codepage
            }

            public override Encoding GetEncoding(string name)
            {
                if (string.Equals("cp850", name, StringComparison.OrdinalIgnoreCase))
                {
                    return Encoding.GetEncoding(850); // this will load an encoding from the CodePagesEncodingProvider
                }
                return null;
            }
        }
        //-- 序列化压缩的DataSet  
        static void DataSetSerializerCompression(DataTable dt, string fileName)
        {          
            IFormatter formatter = new BinaryFormatter();//定义BinaryFormatter以序列化DataSet对象</p>
            MemoryStream ms = new MemoryStream();//创建内存流对象</p>
            formatter.Serialize(ms, dt);//把DataSet对象序列化到内存流</p>
            byte[] buffer = ms.ToArray();//把内存流对象写入字节数组</p>
            ms.Close();//关闭内存流对象</p>
            ms.Dispose();//释放资源</p>
            FileStream fs = File.Create(fileName);//创建文件</p>
            GZipStream gzipStream = new GZipStream(fs, CompressionMode.Compress, true);//创建压缩对象</p>
            gzipStream.Write(buffer, 0, buffer.Length);//把压缩后的数据写入文件<br />
            gzipStream.Close();//关闭压缩流,这里要注意：一定要关闭，要不然解压缩的时候会出现小于4K的文件读取不到数据，大于4K的文件读取不完整            </p>
            gzipStream.Dispose();//释放对象</p>
            fs.Close();//关闭流</p>
            fs.Dispose();//释放对象<br />
        }


        //-- 反序列化压缩的DataSet  
        static DataTable DataSetDeserializeDecompress(string fileName)
        {
            FileStream fs = File.OpenRead(fileName);//打开文件  
            fs.Position = 0;//设置文件流的位置  
            GZipStream gzipStream = new GZipStream(fs, CompressionMode.Decompress);//创建解压对象  
            byte[] buffer = new byte[4096];//定义数据缓冲  
            int offset = 0;//定义读取位置  
            MemoryStream ms = new MemoryStream();//定义内存流  
            while ((offset = gzipStream.Read(buffer, 0, buffer.Length)) != 0)
            {
                ms.Write(buffer, 0, offset);//解压后的数据写入内存流  
            }

            BinaryFormatter sfFormatter = new BinaryFormatter();//定义BinaryFormatter以反序列化DataSet对象  
            ms.Position = 0;//设置内存流的位置  

            DataTable dt;

            try
            {
                dt = (DataTable)sfFormatter.Deserialize(ms);//反序列化  
            }
            catch
            {
                throw;
            }
            finally
            {
                ms.Close();//关闭内存流  
                ms.Dispose();//释放资源  
            }
            fs.Close();//关闭文件流  
            fs.Dispose();//释放资源  
            gzipStream.Close();//关闭解压缩流  
            gzipStream.Dispose();//释放资源  
            return dt;
        }

    }
}
