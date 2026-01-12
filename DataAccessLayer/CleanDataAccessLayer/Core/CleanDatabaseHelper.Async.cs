using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Execution;

namespace DataAccessLayer.Clean.Core;

/// <summary>
/// Async surface for the clean DatabaseHelper.
/// </summary>
public sealed partial class CleanDatabaseHelper
{
    public Task<DbExecutionResult> ExecuteAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        RunExecutionAsync(nameof(ExecuteAsync), request, (command, token) =>
            ExecuteNonQueryWithFallbackAsync(command, token)
                .ContinueWith(t => new DbExecutionResult(t.Result, null, ExtractOutputs(command)), cancellationToken, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default),
            cancellationToken);

    public Task<DbExecutionResult> ExecuteScalarAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default) =>
        RunExecutionAsync(nameof(ExecuteScalarAsync), request, async (command, token) =>
        {
            var scalar = await ExecuteScalarWithFallbackAsync(command, token).ConfigureAwait(false);
            return new DbExecutionResult(-1, scalar, ExtractOutputs(command));
        }, cancellationToken);

    public Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return RunQueryAsync(nameof(QueryAsync), request, mapper, cancellationToken);
    }

    public IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ValidateRequest(request);
        return StreamCoreAsync(request, mapper, cancellationToken);

        async IAsyncEnumerable<T> StreamCoreAsync(
            DbCommandRequest innerRequest,
            Func<DbDataReader, T> innerMapper,
            [EnumeratorCancellation] CancellationToken token)
        {
            var lease = await ExecutePipelineAsync(
                    nameof(StreamAsync),
                    innerRequest,
                    async (context, ct) =>
                    {
                        var pipelineLease = context.CreateLease();
                        var behavior = DbStreamUtilities.EnsureSequentialBehavior(innerRequest.CommandBehavior);
                        var reader = await ExecuteReaderWithFallbackAsync(pipelineLease.Command, behavior, ct)
                            .ConfigureAwait(false);
                        return new StreamingLease(pipelineLease, reader);
                    },
                    onCompleted: null,
                    token)
                .ConfigureAwait(false);

            await using var streamingLease = lease;
            var yielded = 0;

            while (true)
            {
                bool hasRow;
                try
                {
                    hasRow = await streamingLease.Reader.ReadAsync(token).ConfigureAwait(false);
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
                    item = innerMapper(streamingLease.Reader);
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

    private Task<DbExecutionResult> RunExecutionAsync(
        string operation,
        DbCommandRequest request,
        Func<DbCommand, CancellationToken, Task<DbExecutionResult>> executor,
        CancellationToken cancellationToken) =>
        ExecutePipelineAsync(
            operation,
            request,
            (context, token) => executor(context.Command, token),
            RecordResult,
            cancellationToken);

    private Task<DbQueryResult<IReadOnlyList<T>>> RunQueryAsync<T>(
        string operation,
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken)
    {
        return ExecutePipelineAsync(
            operation,
            request,
            async (context, token) =>
            {
                var behavior = request.CommandBehavior == CommandBehavior.Default
                    ? CommandBehavior.Default
                    : request.CommandBehavior;

                await using var reader = await ExecuteReaderWithFallbackAsync(context.Command, behavior, token)
                    .ConfigureAwait(false);
                var items = new List<T>();
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    items.Add(mapper(reader));
                }

                var execution = new DbExecutionResult(reader.RecordsAffected, null, ExtractOutputs(context.Command));
                return new DbQueryResult<IReadOnlyList<T>>(items, execution);
            },
            (activity, result) => _telemetry.RecordCommandResult(activity, result.Execution, result.Data.Count),
            cancellationToken);
    }
}
