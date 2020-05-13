using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace JWTwebAPI.Controllers
{
    //[Authorize]
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        // GET api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public object Get(int id)
        {
            int i = 0;
            string sp = "call transfer7(:a,:pmsg)", jsonStr = "", j = "a";

            var db = new PostgresDB();

            //bool rtn = true;
            //var list = db.GetRefCursorData(sp, null, out rtn);  //Successfully Data Return

            //var ds = db.ExecuteStoredProcedure(new List<NpgsqlParameter>()
            //{
            //    new NpgsqlParameter("a", NpgsqlTypes.NpgsqlDbType.Integer) { Value = 0 },
            //    //new NpgsqlParameter("b", NpgsqlTypes.NpgsqlDbType.Refcursor){ Value=j},
            //}, sp);

            var ob = new CLASSNAME();
            //foreach(DataTable tab in ds.Tables)
            //foreach (DataRow dr in tab.Rows)
            //{
            //    ob.USER_ID = (dr["USER_ID"] == DBNull.Value) ? 0 : Convert.ToInt64(dr["USER_ID"]);
            //    ob.USERNAME = (dr["USERNAME"] == DBNull.Value) ? string.Empty : Convert.ToString(dr["USERNAME"]);
            //    ob.PASSWORD = (dr["password"] == DBNull.Value) ? string.Empty : Convert.ToString(dr["password"]);
            //    ob.EMAIL = (dr["email"] == DBNull.Value) ? string.Empty : Convert.ToString(dr["email"]);
            //    ob.CREATED_ON = (dr["created_on"] == DBNull.Value) ? string.Empty : Convert.ToString(dr["created_on"]);
            //    ob.LAST_LOGIN = (dr["last_login"] == DBNull.Value) ? string.Empty : Convert.ToString(dr["last_login"]);
            //}

            var ds = db.ExecuteFunction(new List<NpgsqlParameter>()
            {
                //new NpgsqlParameter("a", NpgsqlTypes.NpgsqlDbType.Integer){ Value=0,Direction=ParameterDirection.InputOutput},
                //new NpgsqlParameter<string>("pmsg", NpgsqlTypes.NpgsqlDbType.Varchar){ Value=j,Direction=ParameterDirection.InputOutput},
            }, "show_cities_multiple2");

            ds = db.ExecuteStoredProcedure(new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("a", NpgsqlTypes.NpgsqlDbType.Integer){ Value=0,Direction=ParameterDirection.InputOutput},
                new NpgsqlParameter<string>("pmsg", NpgsqlTypes.NpgsqlDbType.Varchar){ Value=j,Direction=ParameterDirection.InputOutput},
                new NpgsqlParameter("ref1", NpgsqlTypes.NpgsqlDbType.Refcursor){ Value=j,Direction=ParameterDirection.InputOutput},
                new NpgsqlParameter("ref2", NpgsqlTypes.NpgsqlDbType.Refcursor){ Value=j,Direction=ParameterDirection.InputOutput},
            }, sp);

            foreach (DataRow dr in ds.Tables["OUTPARAM"].Rows)
            {
                jsonStr += Convert.ToString('"') + dr["KEY"].ToString() + Convert.ToString('"') + ":" + Convert.ToString('"') + (dr["VALUE"].ToString().Replace(@"""", @"\""")) + Convert.ToString('"');
                if (i < ds.Tables["OUTPARAM"].Rows.Count)
                    jsonStr += ",";
                else
                    jsonStr += "}";
                i++;
            }

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(ob);
            return ("value " + jsonStr + json);
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody]string value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
