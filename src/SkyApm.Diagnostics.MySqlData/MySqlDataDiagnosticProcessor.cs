/*
 * Licensed to the OpenSkywalking under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


using MySql.Data.MySqlClient;
using SkyApm.Tracing;
using SkyApm.Tracing.Segments;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace SkyApm.Diagnostics.MySqlData
{
    public class MySqlDataDiagnosticProcessor : DefaultTraceListener, ITracingDiagnosticProcessor
    {
        private readonly ITracingContext _tracingContext;
        private readonly IExitSegmentContextAccessor _contextAccessor;
        private Dictionary<long, MySqlConnectionStringBuilder> _dbConn =
            new Dictionary<long, MySqlConnectionStringBuilder>();
        public MySqlDataDiagnosticProcessor(ITracingContext tracingContext,
            IExitSegmentContextAccessor contextAccessor)
        {
            _tracingContext = tracingContext;
            _contextAccessor = contextAccessor;
            MySqlTrace.Listeners.Clear();
            MySqlTrace.Listeners.Add(this);
            MySqlTrace.Switch.Level = SourceLevels.Information;
            MySqlTrace.QueryAnalysisEnabled = true;
        }

        private static string ResolveOperationName(MySqlDataTraceCommand sqlCommand)
        {
            var commandType = sqlCommand.SqlText?.Split(' ');
            return $"{MySqlDataDiagnosticStrings.MySqlDataPrefix}{commandType?.FirstOrDefault()}";
        }

        public string ListenerName { get; } = MySqlDataDiagnosticStrings.DiagnosticListenerName;

        public void BeforeExecuteCommand(MySqlDataTraceCommand sqlCommand)
        {
            var context = _tracingContext.CreateExitSegmentContext(ResolveOperationName(sqlCommand),
                sqlCommand.DbServer);
            context.Span.SpanLayer = Tracing.Segments.SpanLayer.DB;
            context.Span.Component = Common.Components.SQLCLIENT;
            context.Span.AddTag(Common.Tags.DB_TYPE, "MySql");
            context.Span.AddTag(Common.Tags.DB_INSTANCE, sqlCommand.Database);
            context.Span.AddTag(Common.Tags.DB_STATEMENT, sqlCommand.SqlText);
        }

        public void AfterExecuteCommand()
        {
            var context = _contextAccessor.Context;
            if (context != null)
            {
                _tracingContext.Release(context);
            }
        }

        public void ErrorExecuteCommand(Exception ex)
        {
            var context = _contextAccessor.Context;
            if (context != null)
            {
                context.Span?.ErrorOccurred(ex);
                context.Span?.AddLog(LogEvent.Event("error"), LogEvent.ErrorKind("MySqlException"), LogEvent.Message(ex.Message));
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="eventCache"></param>
        /// <param name="source"></param>
        /// <param name="eventType"></param>
        /// <param name="id"></param>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id,
            string format, params object[] args)
        {
            switch ((MySqlTraceEventType)id)
            {
                case MySqlTraceEventType.ConnectionOpened:
                    var driverId = (long)args[0];
                    var connStr = args[1].ToString();
                    _dbConn[driverId] = new MySqlConnectionStringBuilder(connStr);
                    break;
                case MySqlTraceEventType.ConnectionClosed:
                    //TODO
                    break;
                case MySqlTraceEventType.QueryOpened:
                    BeforeExecuteCommand(GetCommand(args[0], args[2]));
                    break;
                case MySqlTraceEventType.ResultOpened:
                    //TODO
                    break;
                case MySqlTraceEventType.ResultClosed:
                    //TODO
                    break;
                case MySqlTraceEventType.QueryClosed:
                    AfterExecuteCommand();
                    break;
                case MySqlTraceEventType.StatementPrepared:
                    //TODO
                    break;
                case MySqlTraceEventType.StatementExecuted:
                    //TODO
                    break;
                case MySqlTraceEventType.StatementClosed:
                    //TODO
                    break;
                case MySqlTraceEventType.NonQuery:
                    //TODO
                    break;
                case MySqlTraceEventType.UsageAdvisorWarning:
                    //TODO
                    break;
                case MySqlTraceEventType.Warning:
                    //TODO
                    break;
                case MySqlTraceEventType.Error:
                    ErrorExecuteCommand(GetMySqlErrorException(args[1], args[2]));
                    break;
                case MySqlTraceEventType.QueryNormalized:
                    //TODO
                    var a = 1;
                    break;
            }
        }


        private MySqlDataTraceCommand GetCommand(object driverIdObj, object cmd)
        {
            var command = new MySqlDataTraceCommand();
            if (_dbConn.TryGetValue((long)driverIdObj, out var database))
            {
                command.Database = database.Database;
                command.DbServer = database.Server;
            }

            command.SqlText = (cmd == null ? "" : cmd.ToString());
            return command;
        }

        private Exception GetMySqlErrorException(object errorCode, object errorMsg)
        {
            //TODO handle errorcode
            return new Exception($"{errorMsg}");
        }
    }
}
