using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using DataAccessLayer.Execution;
using Shared.IO;

namespace DataAccessLayer.Clean.Core;

/// <summary>
/// Sync surface for the clean DatabaseHelper.
/// </summary>
public sealed partial class CleanDatabaseHelper
{
    public DbExecutionResult Execute(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        RunExecution(
            nameof(Execute),
            request,
            command => new DbExecutionResult(ExecuteNonQueryWithFallback(command, cancellationToken), null, ExtractOutputs(command)),
            cancellationToken);

    public DbExecutionResult ExecuteScalar(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        RunExecution(
            nameof(ExecuteScalar),
            request,
            command => new DbExecutionResult(-1, ExecuteScalarWithFallback(command, cancellationToken), ExtractOutputs(command)),
            cancellationToken);

    public DbQueryResult<IReadOnlyList<T>> Query<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return RunQuery(nameof(Query), request, mapper, cancellationToken);
    }

    public IEnumerable<T> Stream<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ValidateRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        return StreamCore();

        IEnumerable<T> StreamCore()
        {
            var lease = ExecutePipeline(
                nameof(Stream),
                request,
                context =>
                {
                    var pipelineScope = context.CreateScope();
                    var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                    var reader = ExecuteReaderWithFallback(pipelineScope.Command, behavior, cancellationToken);
                    return new StreamingScope(pipelineScope, reader);
                },
                onCompleted: null,
                cancellationToken);

            using var streamingScope = lease;
            var yielded = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                bool hasRow;
                try
                {
                    hasRow = streamingScope.Reader.Read();
                }
                catch (Exception ex)
                {
                    streamingScope.MarkFailure(ex.Message);
                    throw;
                }

                if (!hasRow)
                {
                    break;
                }

                T item;
                try
                {
                    item = mapper(streamingScope.Reader);
                }
                catch (Exception ex)
                {
                    streamingScope.MarkFailure(ex.Message);
                    throw;
                }

                yielded++;
                yield return item;
            }

            streamingScope.RecordResult(yielded);
        }
    }

    private DbExecutionResult RunExecution(
        string operation,
        DbCommandRequest request,
        Func<DbCommand, DbExecutionResult> executor,
        CancellationToken cancellationToken) =>
        ExecutePipeline(
            operation,
            request,
            context => executor(context.Command),
            RecordResult,
            cancellationToken);

    private DbQueryResult<IReadOnlyList<T>> RunQuery<T>(
        string operation,
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken)
    {
        return ExecutePipeline(
            operation,
            request,
            context =>
            {
                var behavior = request.CommandBehavior == CommandBehavior.Default
                    ? CommandBehavior.Default
                    : request.CommandBehavior;

                using var reader = ExecuteReaderWithFallback(context.Command, behavior, cancellationToken);
                var items = new List<T>();
                while (reader.Read())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    items.Add(mapper(reader));
                }

                var execution = new DbExecutionResult(reader.RecordsAffected, null, ExtractOutputs(context.Command));
                return new DbQueryResult<IReadOnlyList<T>>(items, execution);
            },
            (activity, result) => _telemetry.RecordCommandResult(activity, result.Execution, result.Data.Count),
            cancellationToken);
    }
}
