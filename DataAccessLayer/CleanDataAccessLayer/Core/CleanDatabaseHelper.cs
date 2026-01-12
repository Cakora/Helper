using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Clean.Abstractions;
using DataAccessLayer.Common.DbHelper;
using DataAccessLayer.Execution;
using DataAccessLayer.Exceptions;
using DataAccessLayer.Telemetry;
using FluentValidation;
using Microsoft.Extensions.Logging;
using Polly;
using Shared.Configuration;
using DataException = DataAccessLayer.Exceptions.DataException;

namespace DataAccessLayer.Clean.Core;

/// <summary>
/// Clean, single-file database helper with clear sync + async flows. The happy path is:
/// validate request → open scope/command → execute (with resilience) → capture outputs/telemetry → return command to the pool.
/// </summary>
public sealed class CleanDatabaseHelper : ICleanDatabaseHelper
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyOutputs =
        new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

    private readonly IConnectionScopeManager _connectionScopes;
    private readonly IDbCommandFactory _commandFactory;
    private readonly IResilienceStrategy _resilience;
    private readonly IDataAccessTelemetry _telemetry;
    private readonly DatabaseOptions _defaultOptions;
    private readonly ILogger<CleanDatabaseHelper> _logger;
    private readonly IValidator<DbCommandRequest>[] _validators;

    public CleanDatabaseHelper(
        IConnectionScopeManager connectionScopes,
        IDbCommandFactory commandFactory,
        IResilienceStrategy resilience,
        IDataAccessTelemetry telemetry,
        DatabaseOptions defaultOptions,
        ILogger<CleanDatabaseHelper> logger,
        IEnumerable<IValidator<DbCommandRequest>> validators)
    {
        _connectionScopes = connectionScopes ?? throw new ArgumentNullException(nameof(connectionScopes));
        _commandFactory = commandFactory ?? throw new ArgumentNullException(nameof(commandFactory));
        _resilience = resilience ?? throw new ArgumentNullException(nameof(resilience));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _defaultOptions = defaultOptions ?? throw new ArgumentNullException(nameof(defaultOptions));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _validators = validators is null ? Array.Empty<IValidator<DbCommandRequest>>() : System.Linq.Enumerable.ToArray(validators);
    }

    #region Public API

    public Task<DbExecutionResult> ExecuteAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default)
    {
        if (TryBuildPostgresPlan(request, out var plan))
        {
            return ExecutePostgresPlanAsync(plan, expectScalar: false, cancellationToken);
        }

        return RunAsync(
            request,
            nameof(ExecuteAsync),
            async (context, token) =>
            {
                var rows = await ExecuteNonQueryAsync(request, context.Command, token).ConfigureAwait(false);
                return BuildExecution(context.Command, rows, null);
            },
            execution => execution,
            static _ => null,
            cancellationToken);
    }

    public DbExecutionResult Execute(
        DbCommandRequest request,
        CancellationToken cancellationToken = default)
    {
        if (TryBuildPostgresPlan(request, out var plan))
        {
            return ExecutePostgresPlan(plan, expectScalar: false, cancellationToken);
        }

        return Run(
            request,
            nameof(Execute),
            context =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var rows = context.Command.ExecuteNonQuery();
                return BuildExecution(context.Command, rows, null);
            },
            execution => execution,
            static _ => null,
            cancellationToken);
    }

    public Task<DbExecutionResult> ExecuteScalarAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default)
    {
        if (TryBuildPostgresPlan(request, out var plan))
        {
            return ExecutePostgresPlanAsync(plan, expectScalar: true, cancellationToken);
        }

        return RunAsync(
            request,
            nameof(ExecuteScalarAsync),
            async (context, token) =>
            {
                var scalar = await ExecuteScalarWithFallbackAsync(request, context.Command, token).ConfigureAwait(false);
                return BuildExecution(context.Command, -1, scalar);
            },
            execution => execution,
            static _ => null,
            cancellationToken);
    }

    public DbExecutionResult ExecuteScalar(
        DbCommandRequest request,
        CancellationToken cancellationToken = default)
    {
        if (TryBuildPostgresPlan(request, out var plan))
        {
            return ExecutePostgresPlan(plan, expectScalar: true, cancellationToken);
        }

        return Run(
            request,
            nameof(ExecuteScalar),
            context =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var scalar = context.Command.ExecuteScalar();
                return BuildExecution(context.Command, -1, scalar);
            },
            execution => execution,
            static _ => null,
            cancellationToken);
    }

    public Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);

        return RunAsync(
            request,
            nameof(QueryAsync),
            async (context, token) =>
            {
                var behavior = NormalizeReaderBehavior(request.CommandBehavior, forceSequential: false);
                await using var reader = await ExecuteReaderAsync(request, context.Command, behavior, token).ConfigureAwait(false);
                var buffer = new List<T>();
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    buffer.Add(mapper(reader));
                }

                var execution = BuildExecution(context.Command, reader.RecordsAffected, null);
                return new DbQueryResult<IReadOnlyList<T>>(buffer, execution);
            },
            result => result.Execution,
            result => result.Data.Count,
            cancellationToken);
    }

    public DbQueryResult<IReadOnlyList<T>> Query<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);

        return Run(
            request,
            nameof(Query),
            context =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var behavior = NormalizeReaderBehavior(request.CommandBehavior, forceSequential: false);
                using var reader = context.Command.ExecuteReader(behavior);
                var buffer = new List<T>();
                while (reader.Read())
                {
                    buffer.Add(mapper(reader));
                }

                var execution = BuildExecution(context.Command, reader.RecordsAffected, null);
                return new DbQueryResult<IReadOnlyList<T>>(buffer, execution);
            },
            result => result.Execution,
            result => result.Data.Count,
            cancellationToken);
    }

    public IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(mapper);
            ValidateRequest(request);

        return StreamCore(request, mapper, cancellationToken);

        async IAsyncEnumerable<T> StreamCore(
            DbCommandRequest innerRequest,
            Func<DbDataReader, T> innerMapper,
            [EnumeratorCancellation] CancellationToken innerToken)
        {
            innerToken.ThrowIfCancellationRequested();
            var activity = _telemetry.StartCommandActivity(nameof(StreamAsync), innerRequest, _defaultOptions);
            var context = await CreateContextAsync(innerRequest, innerToken).ConfigureAwait(false);
            DbDataReader? reader = null;
            var yielded = 0;
            Exception? failure = null;

            try
            {
                var behavior = NormalizeReaderBehavior(innerRequest.CommandBehavior, forceSequential: true);
                reader = await ExecuteReaderAsync(innerRequest, context.Command, behavior, innerToken).ConfigureAwait(false);
                while (true)
                {
                    bool hasRow;
                    try
                    {
                        hasRow = await reader.ReadAsync(innerToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        throw WrapException(innerRequest, ex);
                    }

                    if (!hasRow)
                    {
                        break;
                    }

                    yielded++;
                    T materialized;
                    try
                    {
                        materialized = innerMapper(reader);
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        throw WrapException(innerRequest, ex);
                    }

                    yield return materialized;
                }

                var execution = BuildExecution(context.Command, reader.RecordsAffected, null);
                _telemetry.RecordCommandResult(activity, execution, yielded);
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            finally
            {
                if (failure is not null)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, failure.Message);
                }

                if (reader is not null)
                {
                    await reader.DisposeAsync().ConfigureAwait(false);
                }

                await CleanupAsync(context).ConfigureAwait(false);
                activity?.Dispose();
            }
        }
    }

    public IEnumerable<T> Stream<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ValidateRequest(request);

        return StreamCore(request, mapper, cancellationToken);

        IEnumerable<T> StreamCore(
            DbCommandRequest innerRequest,
            Func<DbDataReader, T> innerMapper,
            CancellationToken innerToken)
        {
            innerToken.ThrowIfCancellationRequested();
            var activity = _telemetry.StartCommandActivity(nameof(Stream), innerRequest, _defaultOptions);
            var context = CreateContext(innerRequest);
            DbDataReader? reader = null;
            var yielded = 0;
            Exception? failure = null;

            try
            {
                var behavior = NormalizeReaderBehavior(innerRequest.CommandBehavior, forceSequential: true);
                reader = context.Command.ExecuteReader(behavior);
                while (true)
                {
                    bool hasRow;
                    try
                    {
                        hasRow = reader.Read();
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        throw WrapException(innerRequest, ex);
                    }

                    if (!hasRow)
                    {
                        break;
                    }

                    yielded++;
                    T materialized;
                    try
                    {
                        materialized = innerMapper(reader);
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        throw WrapException(innerRequest, ex);
                    }

                    yield return materialized;
                }

                var execution = BuildExecution(context.Command, reader.RecordsAffected, null);
                _telemetry.RecordCommandResult(activity, execution, yielded);
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            finally
            {
                if (failure is not null)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, failure.Message);
                }

                reader?.Dispose();
                Cleanup(context);
                activity?.Dispose();
            }
        }
    }

    #endregion

    #region Pipeline

    private Task<TResult> RunAsync<TResult>(
        DbCommandRequest request,
        string operation,
        Func<CommandContext, CancellationToken, Task<TResult>> action,
        Func<TResult, DbExecutionResult> executionSelector,
        Func<TResult, int?>? resultCountSelector,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        using var activity = _telemetry.StartCommandActivity(operation, request, _defaultOptions);

        return _resilience.CommandAsyncPolicy.ExecuteAsync(async (_, token) =>
        {
            var context = await CreateContextAsync(request, token).ConfigureAwait(false);
            try
            {
                var result = await action(context, token).ConfigureAwait(false);
                var execution = executionSelector(result);
                _telemetry.RecordCommandResult(activity, execution, resultCountSelector?.Invoke(result));
                activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                _logger.LogError(ex, "Clean DAL {Operation} failed for {Command}.", operation, request.TraceName ?? request.CommandText);
                throw WrapException(request, ex);
            }
            finally
            {
                await CleanupAsync(context).ConfigureAwait(false);
            }
        }, new Context(), cancellationToken);
    }

    private TResult Run<TResult>(
        DbCommandRequest request,
        string operation,
        Func<CommandContext, TResult> action,
        Func<TResult, DbExecutionResult> executionSelector,
        Func<TResult, int?>? resultCountSelector,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        using var activity = _telemetry.StartCommandActivity(operation, request, _defaultOptions);

        try
        {
            return _resilience.CommandSyncPolicy.Execute(() =>
            {
                var context = CreateContext(request);
                try
                {
                    var result = action(context);
                    var execution = executionSelector(result);
                    _telemetry.RecordCommandResult(activity, execution, resultCountSelector?.Invoke(result));
                    activity?.SetStatus(ActivityStatusCode.Ok);
                    return result;
                }
                catch (Exception ex)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    _logger.LogError(ex, "Clean DAL {Operation} failed for {Command}.", operation, request.TraceName ?? request.CommandText);
                    throw WrapException(request, ex);
                }
                finally
                {
                    Cleanup(context);
                }
            });
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    private async Task<CommandContext> CreateContextAsync(DbCommandRequest request, CancellationToken cancellationToken)
    {
        var scope = await AcquireScopeAsync(request, cancellationToken).ConfigureAwait(false);
        var command = await _commandFactory.GetCommandAsync(scope.Connection, request, cancellationToken).ConfigureAwait(false);
        ApplyTransaction(request, scope, command);
        return new CommandContext(scope, command);
    }

    private CommandContext CreateContext(DbCommandRequest request)
    {
        var scope = AcquireScope(request);
        var command = _commandFactory.GetCommand(scope.Connection, request);
        ApplyTransaction(request, scope, command);
        return new CommandContext(scope, command);
    }

    private async ValueTask CleanupAsync(CommandContext context)
    {
        _commandFactory.ReturnCommand(context.Command);
        await context.Scope.DisposeAsync().ConfigureAwait(false);
    }

    private void Cleanup(CommandContext context)
    {
        _commandFactory.ReturnCommand(context.Command);
        context.Scope.Dispose();
    }

    #endregion

    #region Provider helpers

    #pragma warning disable S6966 // Oracle provider uses sync paths; fallback is intentional.
    private async ValueTask<int> ExecuteNonQueryAsync(DbCommandRequest request, DbCommand command, CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return command.ExecuteNonQuery();
        }

        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<object?> ExecuteScalarWithFallbackAsync(DbCommandRequest request, DbCommand command, CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return command.ExecuteScalar();
        }

        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<DbDataReader> ExecuteReaderAsync(
        DbCommandRequest request,
        DbCommand command,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return command.ExecuteReader(behavior);
        }

        return await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
    }
    #pragma warning restore S6966

    private async ValueTask<ConnectionScope> AcquireScopeAsync(DbCommandRequest request, CancellationToken cancellationToken)
    {
        if (request.Connection is { } explicitConnection)
        {
            if (explicitConnection.State != ConnectionState.Open)
            {
                await explicitConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            return ConnectionScope.Wrap(explicitConnection, request.Transaction, request.CloseConnection);
        }

        return await _connectionScopes.LeaseAsync(request.OverrideOptions, cancellationToken).ConfigureAwait(false);
    }

    private ConnectionScope AcquireScope(DbCommandRequest request)
    {
        if (request.Connection is { } explicitConnection)
        {
            if (explicitConnection.State != ConnectionState.Open)
            {
                explicitConnection.Open();
            }

            return ConnectionScope.Wrap(explicitConnection, request.Transaction, request.CloseConnection);
        }

        return _connectionScopes.Lease(request.OverrideOptions);
    }

    private static void ApplyTransaction(
        DbCommandRequest request,
        ConnectionScope scope,
        DbCommand command)
    {
        var transaction = request.Transaction ?? scope.Transaction;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
    }

    private CommandBehavior NormalizeReaderBehavior(CommandBehavior behavior, bool forceSequential)
    {
        if (forceSequential)
        {
            return behavior == CommandBehavior.Default
                ? CommandBehavior.SequentialAccess
                : behavior | CommandBehavior.SequentialAccess;
        }

        return behavior;
    }

    private DbExecutionResult BuildExecution(DbCommand command, int rows, object? scalar) =>
        new(rows, scalar, ExtractOutputs(command));

    private static IReadOnlyDictionary<string, object?> ExtractOutputs(DbCommand command)
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
            var key = TrimPrefix(parameter.ParameterName);
            buffer[key] = parameter.Value is DBNull ? null : parameter.Value;
        }

        return buffer is null ? EmptyOutputs : new ReadOnlyDictionary<string, object?>(buffer);
    }

    private static string TrimPrefix(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return name;
        }

        return name[0] is '@' or ':' or '?' ? name[1..] : name;
    }

    private DatabaseProvider ResolveProvider(DbCommandRequest request) =>
        (request.OverrideOptions ?? _defaultOptions).Provider;

    private bool ShouldUseSynchronousProviderPath(DbCommandRequest request) =>
        ResolveProvider(request) == DatabaseProvider.Oracle;

    private bool ShouldWrapExceptions(DbCommandRequest request) =>
        (request.OverrideOptions?.WrapProviderExceptions) ?? _defaultOptions.WrapProviderExceptions;

    private Exception WrapException(DbCommandRequest request, Exception exception)
    {
        if (!ShouldWrapExceptions(request) || exception is DataException)
        {
            return exception;
        }

        return new DataException($"Database command '{request.TraceName ?? request.CommandText}' failed.", exception);
    }

    private void ValidateRequest(DbCommandRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.CommandText))
        {
            throw new ArgumentException("Command text must be provided.", nameof(request));
        }

        if (!request.Validate)
        {
            return;
        }

        foreach (var validator in _validators)
        {
            validator.ValidateAndThrow(request);
        }
    }

    #endregion

    #region PostgreSQL output emulation

    private bool TryBuildPostgresPlan(DbCommandRequest request, out PostgresOutputPlan plan)
    {
        plan = default;
        if (ResolveProvider(request) != DatabaseProvider.PostgreSql || request.Parameters.Count == 0)
        {
            return false;
        }

        var outputs = new List<string>();
        var rewrittenParameters = new List<DbParameterDefinition>(request.Parameters.Count);

        foreach (var parameter in request.Parameters)
        {
            switch (parameter.Direction)
            {
                case ParameterDirection.Input:
                    rewrittenParameters.Add(parameter);
                    break;
                case ParameterDirection.InputOutput:
                    outputs.Add(parameter.Name);
                    rewrittenParameters.Add(CloneWithDirection(parameter, ParameterDirection.Input));
                    break;
                case ParameterDirection.Output:
                case ParameterDirection.ReturnValue:
                    outputs.Add(parameter.Name);
                    break;
            }
        }

        if (outputs.Count == 0)
        {
            return false;
        }

        var commandText = BuildSelectStatement(request.CommandText, rewrittenParameters);
        var rewrittenRequest = new DbCommandRequest
        {
            CommandText = commandText,
            CommandType = CommandType.Text,
            Parameters = rewrittenParameters,
            CommandTimeoutSeconds = request.CommandTimeoutSeconds,
            PrepareCommand = request.PrepareCommand,
            Connection = request.Connection,
            CloseConnection = request.CloseConnection,
            Transaction = request.Transaction,
            OverrideOptions = request.OverrideOptions,
            CommandBehavior = CommandBehavior.SingleRow,
            TraceName = request.TraceName
        };

        var scalarColumn = outputs.Count == 1 ? outputs[0] : null;
        plan = new PostgresOutputPlan(rewrittenRequest, outputs.ToArray(), scalarColumn);
        return true;
    }

    private Task<DbExecutionResult> ExecutePostgresPlanAsync(
        PostgresOutputPlan plan,
        bool expectScalar,
        CancellationToken cancellationToken) =>
        RunAsync(
            plan.Request,
            nameof(ExecuteAsync),
            async (context, token) =>
            {
                await using var reader = await ExecuteReaderAsync(plan.Request, context.Command, CommandBehavior.SingleRow, token).ConfigureAwait(false);
                if (!await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    throw new ProviderFeatureException("PostgreSQL OUT parameter emulation expected a result row but none was returned.");
                }

                var outputs = plan.ReadOutputs(reader);
                var scalar = expectScalar ? plan.GetScalar(reader) : null;
                return new DbExecutionResult(reader.RecordsAffected, scalar, outputs);
            },
            execution => execution,
            static _ => null,
            cancellationToken);

    private DbExecutionResult ExecutePostgresPlan(
        PostgresOutputPlan plan,
        bool expectScalar,
        CancellationToken cancellationToken) =>
        Run(
            plan.Request,
            nameof(Execute),
            context =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var reader = context.Command.ExecuteReader(CommandBehavior.SingleRow);
                if (!reader.Read())
                {
                    throw new ProviderFeatureException("PostgreSQL OUT parameter emulation expected a result row but none was returned.");
                }

                var outputs = plan.ReadOutputs(reader);
                var scalar = expectScalar ? plan.GetScalar(reader) : null;
                return new DbExecutionResult(reader.RecordsAffected, scalar, outputs);
            },
            execution => execution,
            static _ => null,
            cancellationToken);

    private static DbParameterDefinition CloneWithDirection(DbParameterDefinition source, ParameterDirection direction) =>
        new()
        {
            Name = source.Name,
            Value = source.Value,
            DbType = source.DbType,
            Direction = direction,
            Size = source.Size,
            Precision = source.Precision,
            Scale = source.Scale,
            IsNullable = source.IsNullable,
            DefaultValue = source.DefaultValue,
            ProviderTypeName = source.ProviderTypeName,
            TreatAsList = source.TreatAsList,
            Values = source.Values,
            ValueConverter = source.ValueConverter
        };

    private static string BuildSelectStatement(string procedureName, IReadOnlyList<DbParameterDefinition> parameters)
    {
        var args = new List<string>(parameters.Count);
        foreach (var parameter in parameters)
        {
            args.Add("@" + parameter.Name);
        }

        var argumentList = string.Join(", ", args);
        return args.Count == 0
            ? $"select * from {procedureName}();"
            : $"select * from {procedureName}({argumentList});";
    }

    private readonly record struct PostgresOutputPlan(
        DbCommandRequest Request,
        string[] OutputColumns,
        string? ScalarColumn)
    {
        public IReadOnlyDictionary<string, object?> ReadOutputs(DbDataReader reader)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in OutputColumns)
            {
                var ordinal = GetOrdinal(reader, name);
                dict[name] = ordinal >= 0 && !reader.IsDBNull(ordinal)
                    ? reader.GetValue(ordinal)
                    : null;
            }

            return dict;
        }

        public object? GetScalar(DbDataReader reader)
        {
            if (ScalarColumn is not null)
            {
                var ordinal = GetOrdinal(reader, ScalarColumn);
                if (ordinal >= 0)
                {
                    return reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
                }
            }

            if (reader.FieldCount > 0)
            {
                return reader.IsDBNull(0) ? null : reader.GetValue(0);
            }

            return null;
        }

        private static int GetOrdinal(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }
    }

    #endregion

    private readonly record struct CommandContext(ConnectionScope Scope, DbCommand Command);
}
