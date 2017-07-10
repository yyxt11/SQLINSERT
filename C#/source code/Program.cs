using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Configuration;
using System.Diagnostics;

namespace SQLIN
{
    class Program
    {
        public static string ConnString
        {
            get
            {
                try
                {
                    return ConfigurationManager.ConnectionStrings["ConnStrs"].ConnectionString;
                }
                catch (Exception)
                {

                    throw new Exception("请配置数据库连接！");
                }
            }
        }

        public static string ConnTableName
        {
            get
            {
                try
                {
                    return ConfigurationManager.AppSettings["TableName"];
                }
                catch (Exception)
                {

                    throw new Exception("请配置数据库表单信息！");
                }
            }
        }

        public static string ConnFilePath
        {
            get
            {
                try
                {
                    return ConfigurationManager.AppSettings["FilePath"];
                }
                catch (Exception)
                {
                    throw new Exception("请配置导入文件的所在路径！");
                }
            }
          
        }


        static private void CopyDataToDestination(DataTable table)
        {
            using (SqlConnection conn = new SqlConnection(ConnString))
            {
                conn.Open();
                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    using (SqlBulkCopy sqlbulkcopy = new SqlBulkCopy(ConnString, SqlBulkCopyOptions.UseInternalTransaction))
                    {
                        SqlBulkCopyColumnMapping mapping1 = new SqlBulkCopyColumnMapping("ip", "ip");
                        SqlBulkCopyColumnMapping mapping2 = new SqlBulkCopyColumnMapping("devicetype", "devicetype");
                        SqlBulkCopyColumnMapping mapping3 = new SqlBulkCopyColumnMapping("os", "os");
                        SqlBulkCopyColumnMapping mapping4 = new SqlBulkCopyColumnMapping("osv", "osv");
                        SqlBulkCopyColumnMapping mapping5 = new SqlBulkCopyColumnMapping("did", "did");
                        SqlBulkCopyColumnMapping mapping6 = new SqlBulkCopyColumnMapping("dpid", "dpid");
                        SqlBulkCopyColumnMapping mapping7 = new SqlBulkCopyColumnMapping("mac", "mac");
                        SqlBulkCopyColumnMapping mapping8 = new SqlBulkCopyColumnMapping("ua", "ua");
                        SqlBulkCopyColumnMapping mapping9 = new SqlBulkCopyColumnMapping("make", "make");
                        SqlBulkCopyColumnMapping mapping10 = new SqlBulkCopyColumnMapping("model", "model");
                        SqlBulkCopyColumnMapping mapping11 = new SqlBulkCopyColumnMapping("h", "h");
                        SqlBulkCopyColumnMapping mapping12 = new SqlBulkCopyColumnMapping("w", "w");
                        SqlBulkCopyColumnMapping mapping13 = new SqlBulkCopyColumnMapping("ppi", "ppi");
                        SqlBulkCopyColumnMapping mapping14 = new SqlBulkCopyColumnMapping("carrier", "carrier");
                        SqlBulkCopyColumnMapping mapping15 = new SqlBulkCopyColumnMapping("connectiontype", "connectiontype");
                        SqlBulkCopyColumnMapping mapping16 = new SqlBulkCopyColumnMapping("screen_orientation", "screen_orientation");
                        SqlBulkCopyColumnMapping mapping17 = new SqlBulkCopyColumnMapping("didmd5", "didmd5");
                        SqlBulkCopyColumnMapping mapping18 = new SqlBulkCopyColumnMapping("dpidmd5", "dpidmd5");
                        SqlBulkCopyColumnMapping mapping19 = new SqlBulkCopyColumnMapping("macmd5", "macmd5");
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(ConnString);
                        bulkCopy.BatchSize = 5000;
                        bulkCopy.BulkCopyTimeout = 1200;
                        bulkCopy.ColumnMappings.Add(mapping1);
                        bulkCopy.ColumnMappings.Add(mapping2);
                        bulkCopy.ColumnMappings.Add(mapping3);
                        bulkCopy.ColumnMappings.Add(mapping4);
                        bulkCopy.ColumnMappings.Add(mapping5);
                        bulkCopy.ColumnMappings.Add(mapping6);
                        bulkCopy.ColumnMappings.Add(mapping7);
                        bulkCopy.ColumnMappings.Add(mapping8);
                        bulkCopy.ColumnMappings.Add(mapping9);
                        bulkCopy.ColumnMappings.Add(mapping10);
                        bulkCopy.ColumnMappings.Add(mapping11);
                        bulkCopy.ColumnMappings.Add(mapping12);
                        bulkCopy.ColumnMappings.Add(mapping13);
                        bulkCopy.ColumnMappings.Add(mapping14);
                        bulkCopy.ColumnMappings.Add(mapping15);
                        bulkCopy.ColumnMappings.Add(mapping16);
                        bulkCopy.ColumnMappings.Add(mapping17);
                        bulkCopy.ColumnMappings.Add(mapping18);
                        bulkCopy.ColumnMappings.Add(mapping19);

                        bulkCopy.DestinationTableName = ConnTableName;
                        bulkCopy.NotifyAfter = 200;

                        try
                        {
                            bulkCopy.WriteToServer(table);
                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            transaction.Rollback();
                        }
                        finally
                        {
                            conn.Close();
                        }
                    }
                }
            }
        }

        static DataTable BulidEmptyTable()
        {
            DataTable dt = new DataTable("In_table");
            DataColumn dc = null;
            dc = dt.Columns.Add("ip", Type.GetType("System.String"));
            dc = dt.Columns.Add("devicetype", Type.GetType("System.String"));
            dc = dt.Columns.Add("os", Type.GetType("System.String"));
            dc = dt.Columns.Add("osv", Type.GetType("System.String"));
            dc = dt.Columns.Add("did", Type.GetType("System.String"));
            dc = dt.Columns.Add("dpid", Type.GetType("System.String"));
            dc = dt.Columns.Add("mac", Type.GetType("System.String"));
            dc = dt.Columns.Add("ua", Type.GetType("System.String"));
            dc = dt.Columns.Add("make", Type.GetType("System.String"));
            dc = dt.Columns.Add("model", Type.GetType("System.String"));
            dc = dt.Columns.Add("h", Type.GetType("System.String"));
            dc = dt.Columns.Add("w", Type.GetType("System.String"));
            dc = dt.Columns.Add("ppi", Type.GetType("System.String"));
            dc = dt.Columns.Add("carrier", Type.GetType("System.String"));
            dc = dt.Columns.Add("connectiontype", Type.GetType("System.String"));
            dc = dt.Columns.Add("screen_orientation", Type.GetType("System.String"));
            dc = dt.Columns.Add("didmd5", Type.GetType("System.String"));
            dc = dt.Columns.Add("dpidmd5", Type.GetType("System.String"));
            dc = dt.Columns.Add("macmd5", Type.GetType("System.String"));
            return dt;
        }



        static DataTable SelectDataFromSource(String path, DataTable table)
        {
            StreamReader sr = new StreamReader(path, Encoding.Default);
            String line,title,value;
            String[] segmentarray,titlearray;
            MatchCollection mc;
            while ((line = sr.ReadLine()) != null)
            {
                if (line.Contains("ip"))
                {
                  //  Console.WriteLine(line.ToString());
                    Regex re = new Regex("(?<=\").*?(?=\")", RegexOptions.None);
                    DataRow dr = table.NewRow();
                    segmentarray = line.Split('}')[1].Split(',');
                    foreach (string i in segmentarray)
                    {
                        if (i.Contains(':'))
                        {
                            titlearray = i.Split(':');
                            mc = re.Matches(titlearray[0]);
                            title = mc.Count > 0 ? mc[0].Value : titlearray[0];
                            if (String.Equals(title, "mac"))
                            {
                                string valuebuff = titlearray[1];
                                for (int j = 2; j < titlearray.Count(); j++)
                                {
                                    valuebuff = valuebuff + ":" + titlearray[j]; 
                                }
                                mc = re.Matches(valuebuff);
                                value = mc.Count > 0 ? mc[0].Value : valuebuff;
                            }
                            else
                            {
                                mc = re.Matches(titlearray[1]);
                                value = mc.Count > 0 ? mc[0].Value : titlearray[1];
                            }
                            dr[title] = value;                           
                        }          
                    }
                    table.Rows.Add(dr);
                }
                else
                {
                    continue;
                }
            }

            return table;
        }


        static void Main(string[] args)
        {
            //String sourceConnectionString = "Data Source=127.0.0.1;Initial Catalog=Northwind;Integrated Security=True";
            //String destinationConnectionString = "Data Source=127.0.0.1;;Initial Catalog=SqlBulkCopySample;Integrated Security=True";
            String exepath = ConnFilePath;
            if (exepath.Length == 0)
            {
                exepath = System.Environment.CurrentDirectory;
            }

            Stopwatch watch = new Stopwatch();
            watch.Start();
            int num = 1;
            foreach (string filename in Directory.GetFiles(exepath, "*.*", SearchOption.AllDirectories))
            {
                        
                  DataTable init_data = BulidEmptyTable();
                  DataTable data = SelectDataFromSource(filename, init_data);//获取数据
                 CopyDataToDestination(data);//复制数据
                 Console.WriteLine("第" + num + "个文件导入完毕");
                 num++;
            }
           watch.Stop();
           num = num - 1;
           Console.WriteLine("导入完成,共计"+num+"个文件,耗时" + watch.Elapsed.TotalMinutes + "分");  
           Console.ReadLine();    
        }
    }
   
}
