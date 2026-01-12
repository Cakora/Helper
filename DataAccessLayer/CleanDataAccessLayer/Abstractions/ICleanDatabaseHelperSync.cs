using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using DataAccessLayer.Execution;

namespace DataAccessLayer.Clean.Abstractions;

/// <summary>
/// Sync-only DAL helper surface.
/// </summary>
public interface ICleanDatabaseHelperSync
{
    DbExecutionResult Execute(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    DbExecutionResult ExecuteScalar(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    DbQueryResult<IReadOnlyList<T>> Query<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);

    IEnumerable<T> Stream<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);
}
