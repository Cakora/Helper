
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Clean.Abstractions;
using DataAccessLayer.Clean.Core.Transactions;
using DataAccessLayer.Execution;
using DataAccessLayer.Exceptions;
using DataException = DataAccessLayer.Exceptions.DataException;
using DataAccessLayer.Transactions;
using Shared.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataAccessLayer.Tests.Transactions;

public sealed class CleanTransactionRunnerTests
{
    [Fact]
    public void Execute_CompletesWork_AndCommitsScope()
    {
        var manager = new StubTransactionManager();
        var helper = new StubCleanDatabaseHelper();
        var runner = new CleanTransactionRunner(manager, helper, NullLogger<CleanTransactionRunner>.Instance);

        var invoked = false;
        runner.Execute((h, token) =>
        {
            Assert.Same(helper, h);
            Assert.Equal(CancellationToken.None, token);
            invoked = true;
        });

        Assert.True(invoked);
        Assert.NotNull(manager.LastScope);
        Assert.True(manager.LastScope!.Committed);
        Assert.False(manager.LastScope!.RolledBack);
    }

    [Fact]
    public void Execute_WhenWorkThrows_RollsBack_AndWrapsException()
    {
        var manager = new StubTransactionManager();
        var helper = new StubCleanDatabaseHelper();
        var runner = new CleanTransactionRunner(manager, helper, NullLogger<CleanTransactionRunner>.Instance);

        Assert.Throws<DataException>(() =>
            runner.Execute((_, _) => throw new InvalidOperationException("boom")));

        Assert.NotNull(manager.LastScope);
        Assert.True(manager.LastScope!.RolledBack);
        Assert.False(manager.LastScope!.Committed);
    }

    private sealed class StubTransactionManager : ITransactionManager
    {
        public StubTransactionScope? LastScope { get; private set; }

        public ITransactionScope Begin(
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            DatabaseOptions? options = null,
            TransactionScopeOption scopeOption = TransactionScopeOption.Required)
        {
            LastScope = new StubTransactionScope();
            return LastScope;
        }

        public ValueTask<ITransactionScope> BeginAsync(
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            DatabaseOptions? options = null,
            TransactionScopeOption scopeOption = TransactionScopeOption.Required,
            CancellationToken cancellationToken = default) =>
            new(Begin(isolationLevel, options, scopeOption));
    }

    private sealed class StubTransactionScope : ITransactionScope
    {
        public bool Committed { get; private set; }
        public bool RolledBack { get; private set; }
        public DbConnection Connection { get; } = new StubConnection();
        public DbTransaction? Transaction => null;

        public void Commit() => Committed = true;

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            Commit();
            return Task.CompletedTask;
        }

        public void Rollback() => RolledBack = true;

        public Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            Rollback();
            return Task.CompletedTask;
        }

        public void BeginSavepoint(string name) { }
        public Task BeginSavepointAsync(string name, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void RollbackToSavepoint(string name) { }
        public Task RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void ReleaseSavepoint(string name) { }
        public Task ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class StubConnection : DbConnection
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => "Stub";
        public override string DataSource => "Stub";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class StubCleanDatabaseHelper : ICleanDatabaseHelper
    {
        public Task<DbExecutionResult> ExecuteAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<DbExecutionResult> ExecuteScalarAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
            DbCommandRequest request,
            Func<DbDataReader, T> mapper,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<T> StreamAsync<T>(
            DbCommandRequest request,
            Func<DbDataReader, T> mapper,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public DbExecutionResult Execute(DbCommandRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public DbExecutionResult ExecuteScalar(DbCommandRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public DbQueryResult<IReadOnlyList<T>> Query<T>(
            DbCommandRequest request,
            Func<DbDataReader, T> mapper,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IEnumerable<T> Stream<T>(
            DbCommandRequest request,
            Func<DbDataReader, T> mapper,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
