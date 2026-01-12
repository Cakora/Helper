using System;
using System.Threading;
using System.Threading.Tasks;

namespace DataAccessLayer.Clean.Abstractions;

/// <summary>
/// Runs a sequence of DAL operations inside a single transaction, committing only when every step succeeds.
/// </summary>
public interface ITransactionRunner
{
    void Execute(
        Action<ICleanDatabaseHelper, CancellationToken> work,
        CancellationToken cancellationToken = default);

    TResult Execute<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, TResult> work,
        CancellationToken cancellationToken = default);

    Task ExecuteAsync(
        Func<ICleanDatabaseHelper, CancellationToken, Task> work,
        CancellationToken cancellationToken = default);

    Task<TResult> ExecuteAsync<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, Task<TResult>> work,
        CancellationToken cancellationToken = default);
}
