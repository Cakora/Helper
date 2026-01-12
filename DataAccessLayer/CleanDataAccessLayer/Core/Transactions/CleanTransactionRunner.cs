using System;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Clean.Abstractions;
using DataAccessLayer.Exceptions;
using DataAccessLayer.Transactions;
using Microsoft.Extensions.Logging;

namespace DataAccessLayer.Clean.Core.Transactions;

/// <summary>
/// Executes a delegate inside a single DAL transaction. If the delegate throws, the transaction is rolled back.
/// </summary>
public sealed class CleanTransactionRunner : ITransactionRunner
{
    private readonly ITransactionManager _transactionManager;
    private readonly ICleanDatabaseHelper _helper;
    private readonly ILogger<CleanTransactionRunner> _logger;

    public CleanTransactionRunner(
        ITransactionManager transactionManager,
        ICleanDatabaseHelper helper,
        ILogger<CleanTransactionRunner> logger)
    {
        _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
        _helper = helper ?? throw new ArgumentNullException(nameof(helper));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public void Execute(
        Action<ICleanDatabaseHelper, CancellationToken> work,
        CancellationToken cancellationToken = default) =>
        ExecuteInternal(
            (helper, token) =>
            {
                work(helper, token);
                return true;
            },
            cancellationToken);

    public TResult Execute<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, TResult> work,
        CancellationToken cancellationToken = default) =>
        ExecuteInternal(work, cancellationToken);

    public Task ExecuteAsync(
        Func<ICleanDatabaseHelper, CancellationToken, Task> work,
        CancellationToken cancellationToken = default) =>
        ExecuteInternalAsync(
            async (helper, token) =>
            {
                await work(helper, token).ConfigureAwait(false);
                return true;
            },
            cancellationToken);

    public Task<TResult> ExecuteAsync<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, Task<TResult>> work,
        CancellationToken cancellationToken = default) =>
        ExecuteInternalAsync(work, cancellationToken);

    private TResult ExecuteInternal<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, TResult> work,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = _transactionManager.Begin();
        try
        {
            var result = work(_helper, cancellationToken);
            scope.Commit();
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Transaction runner detected failure. Rolling back.");
            scope.Rollback();
            throw new DataException("Transaction runner detected failure while executing the provided work delegate.", ex);
        }
    }

    private async Task<TResult> ExecuteInternalAsync<TResult>(
        Func<ICleanDatabaseHelper, CancellationToken, Task<TResult>> work,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        await using var scope = await _transactionManager.BeginAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        try
        {
            var result = await work(_helper, cancellationToken).ConfigureAwait(false);
            await scope.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Transaction runner detected failure. Rolling back.");
            await scope.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw new DataException("Transaction runner detected failure while executing the provided work delegate.", ex);
        }
    }
}
