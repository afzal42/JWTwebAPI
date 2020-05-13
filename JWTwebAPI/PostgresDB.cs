using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace JWTwebAPI
{
    public class PostgresDB
    {

        //####  private methods ###
        private string _conn = null;

        public PostgresDB()
        {
            string projectPath = AppDomain.CurrentDomain.BaseDirectory.Split(new String[] { @"bin\" }, StringSplitOptions.None)[0];
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(projectPath)
                .AddJsonFile("appsettings.json")
                .Build();
            string connectionString = configuration.GetConnectionString("DefaultConnection");
            _conn = connectionString;
        }

        private NpgsqlCommand GetCommand(string query, NpgsqlParameter[] npgsqlParameters, CommandType commandType)
        {
            NpgsqlConnection conn = new NpgsqlConnection(_conn);
            //conn.UseSslStream = false;
            conn.Open();

            query = query.ToLower();

            NpgsqlCommand command = new NpgsqlCommand(query, conn);
            command.CommandType = commandType;

            if (npgsqlParameters is NpgsqlParameter[])
            {
                command.Parameters.AddRange(npgsqlParameters);
            }

            return command;
        }

        //#### public methods ####

        public long ExecuteNonQuery(string query, NpgsqlParameter[] npgsqlParameters)
        {
            return ExecuteNonQuery(CommandType.StoredProcedure, query, npgsqlParameters);
        }

        public long ExecuteNonQuery(CommandType commandType, string query, NpgsqlParameter[] npgsqlParameters)
        {

            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters, commandType))
            {
                Int32 rowsaffected;

                try
                {
                    rowsaffected = command.ExecuteNonQuery();
                    return rowsaffected;
                }
                catch (Exception Ex)
                {
                    throw Ex;
                }

                finally
                {
                    command.Connection.Close();
                }
            }
        }

        public long ExecuteNonQuery(CommandType commandType, string query)
        {
            return ExecuteNonQuery(commandType, query, null);
        }

        public DataTable ExecuteScalar(string query, List<NpgsqlParameter> npgsqlParameters)
        {
            return ExecuteScalar(CommandType.StoredProcedure, query, npgsqlParameters);
        }

        public DataTable ExecuteScalar(CommandType commandType, string query, List<NpgsqlParameter> npgsqlParameters)
        {
            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters.ToArray(), commandType))
            {
                object result;

                try
                {
                    result = command.ExecuteReader();

                    var dt2 = new DataTable("OUTPARAM");
                    dt2.Columns.Add("KEY");
                    dt2.Columns.Add("VALUE");

                    foreach (NpgsqlParameter op in npgsqlParameters)
                    {
                        if (op.NpgsqlDbType != NpgsqlDbType.Refcursor && (op.Direction == ParameterDirection.InputOutput || op.Direction == ParameterDirection.Output))
                        {
                            DataRow dr2 = dt2.NewRow();
                            dr2["KEY"] = op.ParameterName;
                            dr2["VALUE"] = op.Value;
                            dt2.Rows.Add(dr2);
                        }
                    }

                    return dt2;
                }
                catch (Exception Ex)
                {
                    throw Ex;
                }
                finally
                {
                    command.Connection.Close();
                }
            }
        }

        public object ExecuteScalar(CommandType commandType, string query)
        {
            return ExecuteScalar(commandType, query, null);
        }

        public DataTable[] ExecuteDataTables(string query, NpgsqlParameter[] npgsqlParameters)
        {
            return ExecuteDataTables(CommandType.StoredProcedure, query, npgsqlParameters);

        }

        public DataTable[] ExecuteDataTables(CommandType commandType, string query, NpgsqlParameter[] npgsqlParameters)
        {
            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters, commandType))
            {
                try
                {
                    DataSet myDS = new DataSet();

                    NpgsqlTransaction t = command.Connection.BeginTransaction();

                    NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                    da.Fill(myDS);

                    t.Commit();

                    DataTable[] tables = new DataTable[myDS.Tables.Count];

                    myDS.Tables.CopyTo(tables, 0);

                    return tables;

                }
                catch (Exception Ex)
                {
                    throw Ex;
                }
                finally
                {
                    command.Connection.Close();
                }
            }
        }


        public DataSet ExecuteStoredProcedure(List<NpgsqlParameter> npgsqlParameters, string sp)
        {
            DataSet ds = new DataSet();
            NpgsqlConnection connection = null;
            NpgsqlTransaction transaction = null;
            NpgsqlCommand command = null;
            try
            {
                connection = new NpgsqlConnection(_conn);
                connection.Open();
                using (transaction = connection.BeginTransaction())
                {
                    command = new NpgsqlCommand();
                    command.Connection = connection;
                    command.CommandType = CommandType.Text;
                    command.CommandText = sp;
                    command.Transaction = transaction;

                    if (npgsqlParameters.ToArray() is NpgsqlParameter[])
                    {
                        command.Parameters.AddRange(npgsqlParameters.ToArray());
                    }
                    
                    object result = command.ExecuteNonQuery();

                    foreach (NpgsqlParameter op in npgsqlParameters)
                    {
                        if (op.NpgsqlDbType == NpgsqlDbType.Refcursor)
                        {
                            sp = $@"FETCH ALL IN ""{ op.Value.ToString()}""";

                            DataTable dt = new DataTable();
                            command = new NpgsqlCommand(sp, connection);
                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                            da.Fill(dt);
                            ds.Tables.Add(dt);
                        }
                    }

                    var dt2 = new DataTable("OUTPARAM");
                    dt2.Columns.Add("KEY");
                    dt2.Columns.Add("VALUE");

                    foreach (NpgsqlParameter op in npgsqlParameters)
                    {
                        if (op.NpgsqlDbType != NpgsqlDbType.Refcursor && (op.Direction == ParameterDirection.InputOutput || op.Direction == ParameterDirection.Output))
                        {
                            DataRow dr2 = dt2.NewRow();
                            dr2["KEY"] = op.ParameterName;
                            dr2["VALUE"] = op.Value;
                            dt2.Rows.Add(dr2);
                        }
                    }
                    ds.Tables.Add(dt2);
                    transaction.Commit();
                }
            }
            catch (Exception Ex)
            {
                if (transaction != null) transaction.Rollback();
            }
            finally
            {
                if (connection != null) connection.Close();
            }
            return ds;
        }


        public DataSet ExecuteStoredProcedureForReport(List<NpgsqlParameter> npgsqlParameters, string sp)
        {
            DataSet ds = new DataSet();
            NpgsqlConnection connection = null;
            NpgsqlTransaction transaction = null;
            NpgsqlCommand command = null;
            try
            {
                connection = new NpgsqlConnection(_conn);
                connection.Open();
                using (transaction = connection.BeginTransaction())
                {
                    command = new NpgsqlCommand();
                    command.Connection = connection;
                    command.CommandType = CommandType.Text;
                    command.CommandText = sp;
                    command.Transaction = transaction;

                    if (npgsqlParameters.ToArray() is NpgsqlParameter[])
                    {
                        command.Parameters.AddRange(npgsqlParameters.ToArray());
                    }

                    object result = command.ExecuteNonQuery();

                    foreach (NpgsqlParameter op in npgsqlParameters)
                    {
                        if (op.NpgsqlDbType == NpgsqlDbType.Refcursor)
                        {
                            sp = $@"FETCH ALL IN ""{ op.Value.ToString()}""";

                            DataTable dt = new DataTable();
                            command = new NpgsqlCommand(sp, connection);
                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                            da.Fill(dt);
                            ds.Tables.Add(dt);
                        }
                    }

                    transaction.Commit();
                }
            }
            catch (Exception Ex)
            {
                if (transaction != null) transaction.Rollback();
            }
            finally
            {
                if (connection != null) connection.Close();
            }
            return ds;
        }


        public DataSet ExecuteFunction(List<NpgsqlParameter> npgsqlParameters, string sp)
        {
            DataSet ds = new DataSet();
            NpgsqlConnection connection = null;
            NpgsqlTransaction transaction = null;
            NpgsqlCommand command = null;
            try
            {
                connection = new NpgsqlConnection(_conn);
                connection.Open();
                using (transaction = connection.BeginTransaction())
                {
                    command = new NpgsqlCommand();
                    command.Connection = connection;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = sp;
                    command.Transaction = transaction;

                    if (npgsqlParameters.ToArray() is NpgsqlParameter[])
                    {
                        command.Parameters.AddRange(npgsqlParameters.ToArray());
                    }

                    DataTable rdt = new DataTable();
                    NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                    da.Fill(rdt);

                    foreach (DataRow dr2 in rdt.Rows)
                    {
                        sp = $@"FETCH ALL IN ""{ dr2[0].ToString()}""";

                        DataTable dt = new DataTable();
                        command = new NpgsqlCommand(sp, connection);
                        da = new NpgsqlDataAdapter(command);
                        da.Fill(dt);
                        ds.Tables.Add(dt);
                    }

                    transaction.Commit();
                }
            }
            catch (Exception Ex)
            {
                if (transaction != null) transaction.Rollback();
            }
            finally
            {
                if (connection != null) connection.Close();
            }
            return ds;
        }

        public List<DataTable> GetRefCursorData(string sp, List<object> Parameters, out bool ErrorOccured)
        {
            string connectstring = _conn; //your connectstring here
            List<DataTable> dtRtn = new List<DataTable>();
            NpgsqlConnection connection = null;
            NpgsqlTransaction transaction = null;
            NpgsqlCommand command = null;
            try
            {
                connection = new NpgsqlConnection(connectstring);
                connection.Open();
                using (transaction = connection.BeginTransaction())
                {
                    //transaction = connection.BeginTransaction();
                    command = new NpgsqlCommand();
                    command.Connection = connection;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = sp;
                    command.Transaction = transaction;
                    //
                    if (Parameters != null)
                    {
                        foreach (object item in Parameters)
                        {
                            NpgsqlParameter parameter = new NpgsqlParameter();
                            parameter.Direction = ParameterDirection.Input;
                            parameter.Value = item;
                            command.Parameters.Add(parameter);
                        }
                    }
                    DataSet myDS = new DataSet();
                    NpgsqlDataAdapter da2 = new NpgsqlDataAdapter(command);
                    da2.Fill(myDS);

                    foreach (DataTable rdt in myDS.Tables)
                    {
                        foreach (DataRow dr2 in rdt.Rows)
                        {
                            sp = $@"FETCH ALL IN ""{ dr2[0].ToString()}""";

                            DataTable dt = new DataTable();
                            command = new NpgsqlCommand(sp, connection);
                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                            da.Fill(dt);
                            dtRtn.Add(dt);
                        }
                    }
                    ErrorOccured = false;
                    transaction.Commit();
                }
            }
            catch (Exception Ex)
            {
                ErrorOccured = true;
                if (transaction != null) transaction.Rollback();
            }
            finally
            {
                if (connection != null) connection.Close();
            }
            return dtRtn;
        }


        public DataTable[] ExecuteDataTables(CommandType commandType, string query)
        {
            return ExecuteDataTables(commandType, query, null);
        }
    }
}
