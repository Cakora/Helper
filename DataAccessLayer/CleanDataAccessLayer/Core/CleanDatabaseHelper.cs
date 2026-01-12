using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Clean.Abstractions;
using DataAccessLayer.Common.DbHelper;
using DataAccessLayer.Configuration;
using DataAccessLayer.Execution;
using DataAccessLayer.Telemetry;
using DataException = DataAccessLayer.Exceptions.DataException;
using FluentValidation;
using Microsoft.Extensions.Logging;
using Polly;
using Shared.Configuration;

namespace DataAccessLayer.Clean.Core;

/// <summary>
/// Shared pipeline and helpers for the clean DatabaseHelper (sync + async partials live separately).
/// </summary>
public sealed partial class CleanDatabaseHelper : ICleanDatabaseHelper
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyOutputs =
        new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

    private readonly IConnectionScopeManager _connectionScopeManager;
    private readonly IDbCommandFactory _commandFactory;
    private readonly IResilienceStrategy _resilience;
    private readonly ILogger<CleanDatabaseHelper> _logger;
    private readonly IDataAccessTelemetry _telemetry;
    private readonly DalRuntimeOptions _runtimeOptions;
    private readonly IValidator<DbCommandRequest>[] _requestValidators;
    private readonly DatabaseOptions _defaultOptions;

    public CleanDatabaseHelper(
        IConnectionScopeManager connectionScopeManager,
        IDbCommandFactory commandFactory,
        IResilienceStrategy resilience,
        ILogger<CleanDatabaseHelper> logger,
        IDataAccessTelemetry telemetry,
        DalRuntimeOptions runtimeOptions,
        IEnumerable<IValidator<DbCommandRequest>> requestValidators,
        DatabaseOptions defaultOptions)
    {
        _connectionScopeManager = connectionScopeManager ?? throw new ArgumentNullException(nameof(connectionScopeManager));
        _commandFactory = commandFactory ?? throw new ArgumentNullException(nameof(commandFactory));
        _resilience = resilience ?? throw new ArgumentNullException(nameof(resilience));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        _requestValidators = (requestValidators?.ToArray()) ?? Array.Empty<IValidator<DbCommandRequest>>();
        _defaultOptions = defaultOptions ?? throw new ArgumentNullException(nameof(defaultOptions));
    }

    #region Pipeline

    private TResult ExecutePipeline<TResult>(
        string operation,
        DbCommandRequest request,
        Func<PipelineContext, TResult> executor,
        Action<Activity?, TResult>? onCompleted,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        var label = GetCommandLabel(request);
        var activity = _telemetry.StartCommandActivity(operation, request, _defaultOptions);
        var logScope = BeginLoggingScope(request);
        var stopwatch = Stopwatch.StartNew();
        ConnectionScope? scope = null;
        DbCommand? command = null;
        var context = new PipelineContext(this, activity, label, logScope, stopwatch);

        try
        {
            scope = _connectionScopeManager.Lease(request.OverrideOptions);
            cancellationToken.ThrowIfCancellationRequested();
            command = _commandFactory.GetCommand(scope.Connection, request);
            ApplyScopedTransaction(request, scope, command);
            context.Attach(scope, command);

            var result = executor(context);
            if (!context.LeaseIssued)
            {
                stopwatch.Stop();
                LogInformation("Executed command {Command} in {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
                onCompleted?.Invoke(activity, result);
                activity?.SetStatus(ActivityStatusCode.Ok);
            }

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Database command {Command} failed after {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw new DataException($"Database command '{label}' failed after {stopwatch.ElapsedMilliseconds} ms.", ex);
        }
        finally
        {
            if (!context.LeaseIssued)
            {
                if (command is not null)
                {
                    _commandFactory.ReturnCommand(command);
                }

                if (scope is not null)
                {
                    scope.Dispose();
                }

                logScope?.Dispose();
                activity?.Dispose();
            }
        }
    }

    private async Task<TResult> ExecutePipelineAsync<TResult>(
        string operation,
        DbCommandRequest request,
        Func<PipelineContext, CancellationToken, Task<TResult>> executor,
        Action<Activity?, TResult>? onCompleted,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request);
        var label = GetCommandLabel(request);
        var activity = _telemetry.StartCommandActivity(operation, request, _defaultOptions);
        var logScope = BeginLoggingScope(request);
        var stopwatch = Stopwatch.StartNew();
        ConnectionScope? scope = null;
        DbCommand? command = null;
        var context = new PipelineContext(this, activity, label, logScope, stopwatch);

        try
        {
            scope = await _connectionScopeManager.LeaseAsync(request.OverrideOptions, cancellationToken).ConfigureAwait(false);
            command = await _commandFactory.GetCommandAsync(scope.Connection, request, cancellationToken).ConfigureAwait(false);
            ApplyScopedTransaction(request, scope, command);
            context.Attach(scope, command);

            var result = await executor(context, cancellationToken).ConfigureAwait(false);
            if (!context.LeaseIssued)
            {
                stopwatch.Stop();
                LogInformation("Executed command {Command} in {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
                onCompleted?.Invoke(activity, result);
                activity?.SetStatus(ActivityStatusCode.Ok);
            }

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Database command {Command} failed after {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw new DataException($"Database command '{label}' failed after {stopwatch.ElapsedMilliseconds} ms.", ex);
        }
        finally
        {
            if (!context.LeaseIssued)
            {
                if (command is not null)
                {
                    _commandFactory.ReturnCommand(command);
                }

                if (scope is not null)
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }

                logScope?.Dispose();
                activity?.Dispose();
            }
        }
    }

    private sealed class PipelineContext
    {
        private readonly CleanDatabaseHelper _owner;
        private readonly Activity? _activity;
        private readonly string _label;
        private readonly IDisposable? _logScope;
        private readonly Stopwatch _stopwatch;

        public PipelineContext(
            CleanDatabaseHelper owner,
            Activity? activity,
            string label,
            IDisposable? logScope,
            Stopwatch stopwatch)
        {
            _owner = owner;
            _activity = activity;
            _label = label;
            _logScope = logScope;
            _stopwatch = stopwatch;
        }

        public DbCommand Command { get; private set; } = default!;
        public ConnectionScope Scope { get; private set; } = default!;
        public bool LeaseIssued { get; private set; }

        public void Attach(ConnectionScope scope, DbCommand command)
        {
            Scope = scope;
            Command = command;
        }

        public PipelineLease CreateLease()
        {
            if (LeaseIssued)
            {
                throw new InvalidOperationException("Pipeline lease already issued.");
            }

            LeaseIssued = true;
            return new PipelineLease(_owner, _activity, _label, _logScope, _stopwatch, Command, Scope);
        }
    }

    private sealed class PipelineLease : IAsyncDisposable, IDisposable
    {
        private readonly CleanDatabaseHelper _owner;
        private readonly Activity? _activity;
        private readonly string _label;
        private readonly IDisposable? _logScope;
        private readonly Stopwatch _stopwatch;
        private bool _disposed;

        public PipelineLease(
            CleanDatabaseHelper owner,
            Activity? activity,
            string label,
            IDisposable? logScope,
            Stopwatch stopwatch,
            DbCommand command,
            ConnectionScope scope)
        {
            _owner = owner;
            _activity = activity;
            _label = label;
            _logScope = logScope;
            _stopwatch = stopwatch;
            Command = command;
            Scope = scope;
        }

        public DbCommand Command { get; }
        public ConnectionScope Scope { get; }

        public void RecordResult(DbExecutionResult result, int? produced = null)
        {
            _owner._telemetry.RecordCommandResult(_activity, result, produced ?? result.RowsAffected);
            _activity?.SetStatus(ActivityStatusCode.Ok);
        }

        public void MarkFailure(string? message)
        {
            _activity?.SetStatus(ActivityStatusCode.Error, message);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            await Scope.DisposeAsync().ConfigureAwait(false);
            CompleteDispose();
            _disposed = true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Scope.Dispose();
            CompleteDispose();
            _disposed = true;
        }

        private void CompleteDispose()
        {
            if (_stopwatch.IsRunning)
            {
                _stopwatch.Stop();
            }

            _owner.LogInformation("Executed command {Command} in {Elapsed} ms.", _label, _stopwatch.ElapsedMilliseconds);
            _owner._commandFactory.ReturnCommand(Command);
            _logScope?.Dispose();
            _activity?.Dispose();
        }
    }

    private sealed class StreamingLease : IAsyncDisposable, IDisposable
    {
        private bool _disposed;
        private readonly PipelineLease _pipelineLease;

        public StreamingLease(PipelineLease lease, DbDataReader reader)
        {
            _pipelineLease = lease;
            Reader = reader;
        }

        public DbDataReader Reader { get; }

        public void RecordResult(int yielded)
        {
            var execution = new DbExecutionResult(Reader.RecordsAffected, null, EmptyOutputs);
            _pipelineLease.RecordResult(execution, yielded);
        }

        public void MarkFailure(string? message) => _pipelineLease.MarkFailure(message);

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            await Reader.DisposeAsync().ConfigureAwait(false);
            await _pipelineLease.DisposeAsync().ConfigureAwait(false);
            _disposed = true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Reader.Dispose();
            _pipelineLease.Dispose();
            _disposed = true;
        }
    }

    #endregion

    #region Helpers

    private void ValidateRequest(DbCommandRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.CommandText))
        {
            throw new ArgumentException("Command text must be provided.", nameof(request));
        }

        foreach (var validator in _requestValidators)
        {
            validator.ValidateAndThrow(request);
        }
    }

    private int ExecuteNonQueryWithFallback(DbCommand command, CancellationToken token) =>
        _resilience.CommandSyncPolicy.Execute(
            ctx =>
            {
                token.ThrowIfCancellationRequested();
                return command.ExecuteNonQuery();
            },
            new Context());

    private object? ExecuteScalarWithFallback(DbCommand command, CancellationToken token) =>
        _resilience.CommandSyncPolicy.Execute(
            ctx =>
            {
                token.ThrowIfCancellationRequested();
                return command.ExecuteScalar();
            },
            new Context());

    private DbDataReader ExecuteReaderWithFallback(
        DbCommand command,
        CommandBehavior behavior,
        CancellationToken token) =>
        _resilience.CommandSyncPolicy.Execute(
            ctx =>
            {
                token.ThrowIfCancellationRequested();
                return command.ExecuteReader(behavior);
            },
            new Context());

    private Task<int> ExecuteNonQueryWithFallbackAsync(DbCommand command, CancellationToken token) =>
        _resilience.CommandAsyncPolicy.ExecuteAsync(
            (_, ct) => command.ExecuteNonQueryAsync(ct),
            new Context(),
            token);

    private Task<object?> ExecuteScalarWithFallbackAsync(DbCommand command, CancellationToken token) =>
        _resilience.CommandAsyncPolicy.ExecuteAsync(
            (_, ct) => command.ExecuteScalarAsync(ct),
            new Context(),
            token);

    private Task<DbDataReader> ExecuteReaderWithFallbackAsync(
        DbCommand command,
        CommandBehavior behavior,
        CancellationToken token) =>
        _resilience.CommandAsyncPolicy.ExecuteAsync(
            (_, ct) => command.ExecuteReaderAsync(behavior, ct),
            new Context(),
            token);

    private IReadOnlyDictionary<string, object?> ExtractOutputs(DbCommand command)
    {
        if (command.Parameters.Count == 0)
        {
            return EmptyOutputs;
        }

        Dictionary<string, object?>? buffer = null;
        foreach (DbParameter parameter in command.Parameters)
        {
            if (parameter.Direction == ParameterDirection.Input)
            {
                continue;
            }

            buffer ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var value = parameter.Value is DBNull ? null : parameter.Value;
            buffer[parameter.ParameterName.TrimStart('@', ':', '?')] = value;
        }

        return buffer is null ? EmptyOutputs : new ReadOnlyDictionary<string, object?>(buffer);
    }

    private IDisposable? BeginLoggingScope(DbCommandRequest request) =>
        _runtimeOptions.EnableDetailedLogging && _logger.IsEnabled(LogLevel.Information)
            ? _logger.BeginScope("DatabaseCommand:{Name}", GetCommandLabel(request))
            : null;

    private string GetCommandLabel(DbCommandRequest request) =>
        request.TraceName ?? _telemetry.GetCommandDisplayName(request);

    private void ApplyScopedTransaction(DbCommandRequest request, ConnectionScope scope, DbCommand command)
    {
        if (scope.Transaction is not null)
        {
            command.Transaction = scope.Transaction;
        }
        else if (request.Transaction is not null)
        {
            command.Transaction = request.Transaction;
        }
    }

    private void LogInformation(string message, params object?[] args)
    {
        if (_runtimeOptions.EnableDetailedLogging && _logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(message, args);
        }
    }

    private void RecordResult(Activity? activity, DbExecutionResult result) =>
        _telemetry.RecordCommandResult(activity, result);

    #endregion
}
