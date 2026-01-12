using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Execution;

namespace DataAccessLayer.Clean.Abstractions;

/// <summary>
/// Minimal surface for the clean Database Helper façade.
/// </summary>
public interface ICleanDatabaseHelper
{
    Task<DbExecutionResult> ExecuteAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    Task<DbExecutionResult> ExecuteScalarAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);
}
