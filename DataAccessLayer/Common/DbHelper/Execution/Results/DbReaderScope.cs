using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace DataAccessLayer.Execution;

/// <summary>
/// Represents an owned reader/command/connection tuple that must be disposed by the caller.
/// Ensures commands are returned to the pool and connections are closed when the lease is disposed.
/// </summary>
/// <remarks>
/// Obtained exclusively via the top-level streaming APIs (<see cref="IDatabaseHelper.ExecuteReader"/> and
/// <see cref="IDatabaseHelper.ExecuteReaderAsync"/>). Disposing the lease (sync or async) returns the borrowed command to
/// <see cref="IDbCommandFactory"/> and releases the underlying connection scope.
/// </remarks>
public sealed class DbReaderScope : IAsyncDisposable, IDisposable
{
    private readonly IAsyncDisposable _connectionLease;
    private readonly IDbCommandFactory _commandFactory;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="DbReaderScope"/> class.
    /// </summary>
    /// <param name="reader">Active <see cref="DbDataReader"/>.</param>
    /// <param name="command">Command used to create the reader.</param>
    /// <param name="connectionLease">Connection lease controlling the underlying connection lifetime.</param>
    /// <param name="commandFactory">Factory used to return the command to its pool.</param>
    public DbReaderScope(
        DbDataReader reader,
        DbCommand command,
        IAsyncDisposable connectionLease,
        IDbCommandFactory commandFactory)
    {
        Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        Command = command ?? throw new ArgumentNullException(nameof(command));
        _connectionLease = connectionLease ?? throw new ArgumentNullException(nameof(connectionLease));
        _commandFactory = commandFactory ?? throw new ArgumentNullException(nameof(commandFactory));
    }

    /// <summary>
    /// Gets the active data reader. Caller should not dispose it directly; disposing the lease handles it.
    /// </summary>
    public DbDataReader Reader { get; }

    /// <summary>
    /// Gets the underlying command (for parameter inspection, etc.).
    /// </summary>
    public DbCommand Command { get; }

    /// <summary>
    /// Disposes the reader, command, and connection asynchronously.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            await Reader.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _commandFactory.ReturnCommand(Command);
            await _connectionLease.DisposeAsync().ConfigureAwait(false);
            _disposed = true;
        }
    }

    /// <summary>
    /// Disposes the reader, command, and connection synchronously.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        Reader.Dispose();
        _commandFactory.ReturnCommand(Command);
        _connectionLease.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _disposed = true;
    }
}
