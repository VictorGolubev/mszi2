using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlConn
{
    class CreateTable
    {
        static void Main(string[] args)
        {
            Dictionary<string, List<string>> tableAndPacketsIpSIpD = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> tableAndPacketsIpSIpDportS = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> tableAndPacketsIpSIpDportD = new Dictionary<string, List<string>>();

            Console.WriteLine("Getting Connection ...");
            MySqlConnection conn = DBUtils.GetDBConnection();

            try
            {
                Console.WriteLine("Openning Connection ...");
                conn.Open();
                Console.WriteLine("Connection successful!");
                Console.WriteLine();
                Console.WriteLine("Menu");
                Console.WriteLine("1 - create table ipSource - ipDestination");
                Console.WriteLine("2 - create table ipSource - ipDestination - portSource");
                Console.WriteLine("3 - create table ipSource - ipDestination - portDestination");
                Console.WriteLine("Insert number");
                //int number = Convert.ToInt32(Console.ReadLine());
                MySqlCommand command;
                MySqlDataReader reader;




                //qq = "CREATE TABLE dump_malisheva.`11statistics` (`id` INT NOT NULL AUTO_INCREMENT, `table_name` VARCHAR(450) NULL, `percent_bytes` VARCHAR(450) NULL, `packets` VARCHAR(450) NULL, PRIMARY KEY(`id`));";
                //command = new MySqlCommand(qq, conn);
                //  command.ExecuteNonQuery();
                //qq = "CREATE TABLE ipstream as SELECT* FROM dump_all where isIp = \"YES\";";
                //command = new MySqlCommand(qq, conn);
                //command.ExecuteNonQuery();
                // qq = "CREATE TABLE noipstream as SELECT* FROM dump_all where isIp = \"NO\";";
                // command = new MySqlCommand(qq, conn);
                //command.ExecuteNonQuery();
                //   switch (number)

                //Создание таблиц с уникальными парами ipSource - ipDestination
                //     case 1:
                {
                    //Получаем уникальные пары, формируем список
                    getUniquesPairIpSIpD(tableAndPacketsIpSIpD, conn, out command, out reader);


                    //Получаем всю таблицу и закидываем все в словарь
                    getAllPacketsAndCreateMapIpSIpD(tableAndPacketsIpSIpD, conn, out command, out reader);


                    //Создаем таблицы в бд
                    foreach (KeyValuePair<string, List<string>> entry in tableAndPacketsIpSIpD)
                    {
                        string tableName = entry.Key;
                        string sqlQueryCreateTable = getQueryCreateTable(tableName);
                        command = new MySqlCommand(sqlQueryCreateTable, conn);
                        command.ExecuteNonQuery();
                        foreach (string packetValues in entry.Value)
                        {
                            string sqlQueryInsert = getQueryInsertPacketToTable(tableName, packetValues);
                            command = new MySqlCommand(sqlQueryInsert, conn);
                            command.ExecuteNonQuery();

                        }
                    }


                    //Получение статистики
                    command = getStatisticsAndInsertToTable(tableAndPacketsIpSIpD, conn, command);
                    // break;
                }

                {

                    //Создание таблиц с уникальными парами ipSource - ipDestination - portSource
                    // case 2:
                    //Получаем уникальные пары, формируем список
                    getUniquesPairIpSIpDportS(tableAndPacketsIpSIpDportS, conn, out command, out reader);

                    //Получаем всю таблицу и закидываем все в словарь
                    getAllPacketsAndCreateMapIpSIpDportS(tableAndPacketsIpSIpDportS, conn, out command, out reader);

                    //Создаем таблицы в бд
                    foreach (KeyValuePair<string, List<string>> entry in tableAndPacketsIpSIpDportS)
                    {
                        string tableName = entry.Key;
                        string sqlQueryCreateTable = getQueryCreateTable(tableName);
                        command = new MySqlCommand(sqlQueryCreateTable, conn);
                        command.ExecuteNonQuery();
                        foreach (string packetValues in entry.Value)
                        {
                            string sqlQueryInsert = getQueryInsertPacketToTable(tableName, packetValues);
                            command = new MySqlCommand(sqlQueryInsert, conn);
                            command.ExecuteNonQuery();

                        }

                    }

                    //Получение статистики
                    command = getStatisticsAndInsertToTable(tableAndPacketsIpSIpDportS, conn, command);
                }
                //                        break;
                {
                    //Создание таблиц с уникальными парами ipSource - ipDestination - portDestination
                    //                  case 3:
                    //Получаем уникальные пары, формируем список
                    getUniquesPairIpSIpDportD(tableAndPacketsIpSIpDportD, conn, out command, out reader);

                    //Получаем всю таблицу и закидываем все в словарь
                    getAllPacketsAndCreateMapIpSIpDportD(tableAndPacketsIpSIpDportD, conn, out command, out reader);

                    //Создаем таблицы в бд
                    foreach (KeyValuePair<string, List<string>> entry in tableAndPacketsIpSIpDportD)
                    {
                        string tableName = entry.Key;
                        string sqlQueryCreateTable = getQueryCreateTable(tableName);
                        command = new MySqlCommand(sqlQueryCreateTable, conn);
                        command.ExecuteNonQuery();
                        foreach (string packetValues in entry.Value)
                        {
                            string sqlQueryInsert = getQueryInsertPacketToTable(tableName, packetValues);
                            command = new MySqlCommand(sqlQueryInsert, conn);
                            command.ExecuteNonQuery();

                        }

                    }

                    //Получение статистики
                    command = getStatisticsAndInsertToTable(tableAndPacketsIpSIpDportD, conn, command);
                }
                //                    break;
                //              default:
                //                break;
                //      }


                Console.WriteLine("End");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
            finally
            {
                conn.Close();
            }

            Console.Read();
        }




        //Методы для IpSource - IpDestination
        private static void getAllPacketsAndCreateMapIpSIpD(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryAllIpPacckets = "SELECT * FROM dump_malisheva.dump_all;"; //213213

            command = new MySqlCommand(sqlQueryAllIpPacckets, conn);

            reader = command.ExecuteReader();

            while (reader.Read())
            {
                string ipSource = reader[4].ToString();
                string ipDestination = reader[5].ToString();
                if (tableAndPackets.ContainsKey(formatNameIpSIpD(ipSource, ipDestination)))
                {
                    List<string> tempList = tableAndPackets.GetValueOrDefault(formatNameIpSIpD(ipSource, ipDestination));
                    String temp = reader[0].ToString() + ",'" + reader[1].ToString() + "','" + reader[2].ToString() + "','" + reader[3].ToString() + "','" + reader[4].ToString() + "','" + reader[5].ToString() + "','" + reader[6].ToString() + "','" + reader[7].ToString() + "','" + reader[8].ToString() + "'";
                    tempList.Add(temp);
                }
            }
            reader.Close();
        }

        private static void getUniquesPairIpSIpD(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryUniquesCouplesPortSource = "SELECT DISTINCT ip_source,ip_destination FROM dump_malisheva.ipstream;";

            command = new MySqlCommand(sqlQueryUniquesCouplesPortSource, conn);
            reader = command.ExecuteReader();
            while (reader.Read())
            {
                string ipSource = reader[0].ToString();
                string ipDestination = reader[1].ToString();
                //                    string portSource = reader[2].ToString();
                tableAndPackets.Add(formatNameIpSIpD(ipSource, ipDestination), new List<string>());
            }
            reader.Close();
        }

        static string formatNameIpSIpD(string ex1, string ex2)
        {
            return "1_ipS_" + ex1 + "_ipD_" + ex2;
        }

        //Методы для IpSource - IpDestination - portSource
        private static void getAllPacketsAndCreateMapIpSIpDportS(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryAllIpPacckets = "SELECT * FROM dump_malisheva.ipstream;";

            command = new MySqlCommand(sqlQueryAllIpPacckets, conn);

            reader = command.ExecuteReader();

            while (reader.Read())
            {
                string ipSource = reader[4].ToString();
                string ipDestination = reader[5].ToString();
                string portSource = reader[7].ToString();
                if (tableAndPackets.ContainsKey(formatNameIpSIpDportS(ipSource, ipDestination, portSource)))
                {
                    List<string> tempList = tableAndPackets.GetValueOrDefault(formatNameIpSIpDportS(ipSource, ipDestination, portSource));
                    String temp = reader[0].ToString() + ",'" + reader[1].ToString() + "','" + reader[2].ToString() + "','" + reader[3].ToString() + "','" + reader[4].ToString() + "','" + reader[5].ToString() + "','" + reader[6].ToString() + "','" + reader[7].ToString() + "','" + reader[8].ToString() + "'";
                    tempList.Add(temp);
                }
            }
            reader.Close();
        }

        private static void getUniquesPairIpSIpDportS(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryUniquesCouplesPortSource = "SELECT DISTINCT ip_source,ip_destination,port_source FROM dump_malisheva.ipstream;";

            command = new MySqlCommand(sqlQueryUniquesCouplesPortSource, conn);
            reader = command.ExecuteReader();
            while (reader.Read())
            {
                string ipSource = reader[0].ToString();
                string ipDestination = reader[1].ToString();
                string portSource = reader[2].ToString();
                tableAndPackets.Add(formatNameIpSIpDportS(ipSource, ipDestination, portSource), new List<string>());
            }
            reader.Close();
        }

        static string formatNameIpSIpDportS(string ex1, string ex2, string ex3)
        {
            return "2_ipS_" + ex1 + "_ipD_" + ex2 + "_portS_" + ex3;
        }

        //Методы для IpSource - IpDestination - portDestination
        private static void getAllPacketsAndCreateMapIpSIpDportD(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryAllIpPacckets = "SELECT * FROM dump_malisheva.ipstream;";

            command = new MySqlCommand(sqlQueryAllIpPacckets, conn);

            reader = command.ExecuteReader();

            while (reader.Read())
            {
                string ipSource = reader[4].ToString();
                string ipDestination = reader[5].ToString();
                string portDestination = reader[8].ToString();
                if (tableAndPackets.ContainsKey(formatNameIpSIpDportD(ipSource, ipDestination, portDestination)))
                {
                    List<string> tempList = tableAndPackets.GetValueOrDefault(formatNameIpSIpDportD(ipSource, ipDestination, portDestination));
                    String temp = reader[0].ToString() + ",'" + reader[1].ToString() + "','" + reader[2].ToString() + "','" + reader[3].ToString() + "','" + reader[4].ToString() + "','" + reader[5].ToString() + "','" + reader[6].ToString() + "','" + reader[7].ToString() + "','" + reader[8].ToString() + "'";
                    tempList.Add(temp);
                }
            }
            reader.Close();
        }

        private static void getUniquesPairIpSIpDportD(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, out MySqlCommand command, out MySqlDataReader reader)
        {
            string sqlQueryUniquesCouplesPortDestination = "SELECT DISTINCT ip_source,ip_destination,port_destination FROM dump_malisheva.ipstream;";

            command = new MySqlCommand(sqlQueryUniquesCouplesPortDestination, conn);
            reader = command.ExecuteReader();
            while (reader.Read())
            {
                string ipSource = reader[0].ToString();
                string ipDestination = reader[1].ToString();
                string portDestination = reader[2].ToString();
                tableAndPackets.Add(formatNameIpSIpDportD(ipSource, ipDestination, portDestination), new List<string>());
            }
            reader.Close();
        }

        private static string formatNameIpSIpDportD(string ex1, string ex2, string ex3)
        {
            return "3_ipS_" + ex1 + "_ipD_" + ex2 + "_portD_" + ex3;
        }

        //Общие методы
        private static string getQueryCreateTable(string tableName)
        {
            return "CREATE TABLE `" + tableName + "` (" +
    "`id` INT NOT NULL AUTO_INCREMENT," +
    "`time_stamp` VARCHAR(45) NULL," +
    "`packet_length` VARCHAR(45) NULL," +
    "`isIP` VARCHAR(45) NULL," +
    "`ip_source` VARCHAR(45) NULL," +
    "`ip_destination` VARCHAR(45) NULL," +
    "`protocol_type` VARCHAR(45) NULL," +
    "`port_source` VARCHAR(45) NULL," +
    "`port_destination` VARCHAR(45) NULL," +
    "PRIMARY KEY(`id`));";
        }

        private static string getQueryInsertPacketToTable(string tableName, string values)
        {
            return "INSERT INTO dump_malisheva.`" + tableName + "` (id, time_stamp, packet_length, isIP, ip_source, ip_destination, protocol_type, port_source, port_destination) VALUES (" + values + ");";
        }
        private static string getQueryInsertStatToTable(string tableName, string values)
        {
            return "INSERT INTO dump_malisheva.`" + tableName + "` (ip_source, ip_destination, port, duration, total_bytes, total_packets, table_name) VALUES (" + values + ");";
        }

        // Возвращаемые значения
        // 1 - ipSource
        // 2 - ipDestination
        // 3 - port
        private static List<string> convertName(String name)
        {
            List<string> result = new List<string>();
            int indexTemp = name.IndexOf("_ipS_") + 5;
            int indexIpD = name.IndexOf("_ipD_");
            result.Add(name.Substring(indexTemp, indexIpD - indexTemp));
            if (name.Contains("port"))
            {
                indexIpD += 5;
                indexTemp = name.Contains("_portS_") ? name.IndexOf("_portS_") : name.IndexOf("_portD_");
                result.Add(name.Substring(indexIpD, indexTemp - indexIpD));
                result.Add(name.Substring(indexTemp + 7));

            }
            else
            {
                result.Add(name.Substring(indexIpD + 5));
                result.Add("unknown");
            }
            return result;
        }

        private static int getPacketSize(string[] values)
        {
            string temp = values[2];
            temp = temp.Remove(0, 1);
            temp = temp.Remove(temp.Length - 1, 1);
            return Convert.ToInt32(temp);
        }

        private static DateTime getDate(string packetValues)
        {
            string[] values = packetValues.Split(',');
            string temp = values[1];
            temp = temp.Remove(0, 1);
            temp = temp.Remove(temp.Length - 1, 1);
            return DateTime.Parse(temp);
        }

        private static string createValuesStringForStatistic(string ipS, string ipD, string port, string duration, string totalBytes, string totalPack, string tableName)
        {
            return "'" + ipS + "','" + ipD + "','" + port + "','" + duration + "','" + totalBytes + "','" + totalPack + "','" + tableName + "'";
        }

        private static MySqlCommand getStatisticsAndInsertToTable(Dictionary<string, List<string>> tableAndPackets, MySqlConnection conn, MySqlCommand command)
        {
            string statisticTable = "11statistics";
            foreach (KeyValuePair<string, List<string>> entry in tableAndPackets)
            {
                string tableName = entry.Key;
                List<string> data = convertName(tableName);
                string ipSource = data[0];
                string ipDestination = data[1];
                string port = data[2];

                int totalPack = entry.Value.Count();
                int totalSize = 0;

                foreach (string packetValues in entry.Value)
                {
                    string[] values = packetValues.Split(',');
                    totalSize += getPacketSize(values);
                }

                DateTime first = getDate(entry.Value[0]);
                DateTime last = getDate(entry.Value[totalPack - 1]);
                string duration = (last - first).ToString();

                string sqlQueryInsertToStatistic = getQueryInsertStatToTable(statisticTable, createValuesStringForStatistic(ipSource, ipDestination, port, duration, totalSize.ToString(), totalPack.ToString(), tableName));
                command = new MySqlCommand(sqlQueryInsertToStatistic, conn);
                command.ExecuteNonQuery();
            }

            return command;
        }
    }

}