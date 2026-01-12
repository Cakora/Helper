using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using DataAccessLayer.Execution;

namespace DataAccessLayer.Clean.Core;

/// <summary>
/// Sync surface for the clean DatabaseHelper.
/// </summary>
public sealed partial class CleanDatabaseHelper
{
    public DbExecutionResult Execute(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        ExecutePipeline(
            nameof(Execute),
            request,
            context =>
            {
                var rows = ExecuteNonQueryWithFallback(context.Command, cancellationToken);
                return new DbExecutionResult(rows, null, ExtractOutputs(context.Command));
            },
            RecordResult,
            cancellationToken);

    public DbExecutionResult ExecuteScalar(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        ExecutePipeline(
            nameof(ExecuteScalar),
            request,
            context =>
            {
                var scalar = ExecuteScalarWithFallback(context.Command, cancellationToken);
                return new DbExecutionResult(-1, scalar, ExtractOutputs(context.Command));
            },
            RecordResult,
            cancellationToken);

    public DbQueryResult<IReadOnlyList<T>> Query<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return ExecutePipeline(
            nameof(Query),
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
                    var pipelineLease = context.CreateLease();
                    var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                    var reader = ExecuteReaderWithFallback(pipelineLease.Command, behavior, cancellationToken);
                    return new StreamingLease(pipelineLease, reader);
                },
                onCompleted: null,
                cancellationToken);

            using var streamingLease = lease;
            var yielded = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                bool hasRow;
                try
                {
                    hasRow = streamingLease.Reader.Read();
                }
                catch (Exception ex)
                {
                    streamingLease.MarkFailure(ex.Message);
                    throw;
                }

                if (!hasRow)
                {
                    break;
                }

                T item;
                try
                {
                    item = mapper(streamingLease.Reader);
                }
                catch (Exception ex)
                {
                    streamingLease.MarkFailure(ex.Message);
                    throw;
                }

                yielded++;
                yield return item;
            }

            streamingLease.RecordResult(yielded);
        }
    }
}
