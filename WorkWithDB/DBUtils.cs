﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace SqlConn
{
    class DBUtils
    {
        public static MySqlConnection GetDBConnection()
        {
            string host = "localhost";
            int port = 3306;
            string database = "dump_malisheva";
            string username = "root";
            string password = "admin";

            return DBMySQLUtils.GetDBConnection(host, port, database, username, password);
        }

    }
}