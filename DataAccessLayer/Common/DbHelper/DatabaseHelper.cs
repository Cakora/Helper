using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shared.Configuration;
using DataAccessLayer.Execution;
using DataAccessLayer.Providers.Oracle;
using DataAccessLayer.Providers.Postgres;
using Polly;
using FluentValidation;
using System.Linq;
using DataAccessLayer.Configuration;
using DataAccessLayer.Mapping;
using DataAccessLayer.Telemetry;
using DataAccessLayer.Exceptions;
using DataException = DataAccessLayer.Exceptions.DataException;
using Shared.IO;

namespace DataAccessLayer.Common.DbHelper;

/// <summary>
/// Default implementation of <see cref="IDatabaseHelper"/> that normalizes provider-specific behaviors
/// (SQL Server, PostgreSQL, Oracle) and exposes a unified surface for executing commands, readers, and streams.
/// </summary>
public sealed class DatabaseHelper : IDatabaseHelper
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyOutputs =
        new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

    private readonly IConnectionScopeManager _connectionScopeManager;
    private readonly IDbCommandFactory _commandFactory;
    private readonly DatabaseOptions _defaultOptions;
    private readonly IResilienceStrategy _resilience;
    private readonly ILogger<DatabaseHelper> _logger;
    private readonly IValidator<DbCommandRequest>[] _requestValidators;
    private readonly IRowMapperFactory _rowMapperFactory;
    private readonly IDataAccessTelemetry _telemetry;
    private readonly DalRuntimeOptions _runtimeOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="DatabaseHelper"/> class.
    /// </summary>
    /// <param name="connectionScopeManager">Factory responsible for leasing provider connections and ambient scopes.</param>
    /// <param name="commandFactory">Reusable DbCommand factory/parameter binder.</param>
    /// <param name="options">Default database options (provider, connection string, diagnostics).</param>
    /// <param name="resilience">Resilience strategy (retry policies).</param>
    /// <param name="logger">Structured logger used for telemetry.</param>
    public DatabaseHelper(
        IConnectionScopeManager connectionScopeManager,
        IDbCommandFactory commandFactory,
        DatabaseOptions options,
        IResilienceStrategy resilience,
        ILogger<DatabaseHelper> logger,
        IDataAccessTelemetry telemetry,
        DalRuntimeOptions runtimeOptions,
        IEnumerable<IValidator<DbCommandRequest>> requestValidators,
        IRowMapperFactory rowMapperFactory)
    {
        _connectionScopeManager = connectionScopeManager ?? throw new ArgumentNullException(nameof(connectionScopeManager));
        _commandFactory = commandFactory ?? throw new ArgumentNullException(nameof(commandFactory));
        _defaultOptions = options ?? throw new ArgumentNullException(nameof(options));
        _resilience = resilience ?? throw new ArgumentNullException(nameof(resilience));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        _requestValidators = requestValidators?.ToArray() ?? Array.Empty<IValidator<DbCommandRequest>>();
        _rowMapperFactory = rowMapperFactory ?? throw new ArgumentNullException(nameof(rowMapperFactory));
    }

    private Activity? StartActivity(string operation, DbCommandRequest request) =>
        _telemetry.StartCommandActivity(operation, request, _defaultOptions);

    private void RecordActivityResult(Activity? activity, DbExecutionResult execution) =>
        _telemetry.RecordCommandResult(activity, execution);

    #region Public Operations

    /// <summary>
    /// Executes a non-query command asynchronously and returns rows affected plus output parameters.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbExecutionResult"/> containing rows affected and output parameter values.</returns>
    /// <remarks>
    /// This helper normalizes provider-specific behaviors (e.g., PostgreSQL OUT parameters) and automatically
    /// applies configured resilience/telemetry policies before invoking the underlying provider APIs.
    /// </remarks>
    public Task<DbExecutionResult> ExecuteAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
    ExecuteWithRequestActivityAsync(
        nameof(ExecuteAsync),
        request,
        async () =>
        {
            if (TryExecutePostgresOutParametersAsync(request, expectScalar: false, cancellationToken, out var task))
            {
                return await task.ConfigureAwait(false);
            }

            return await ExecuteWithCommandPolicyAsync(
                token => ExecuteScalarLikeAsync(
                    request,
                    async (command, innerToken) =>
                    {
                        var rows = await ExecuteNonQueryWithFallbackAsync(request, command, innerToken).ConfigureAwait(false);
                        return new CommandResult<object?>(null, rows);
                    },
                    token),
                cancellationToken).ConfigureAwait(false);
        },
        RecordActivityResult);

    /// <summary>
    /// Executes a non-query synchronously and returns rows affected plus output parameters.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <returns>A <see cref="DbExecutionResult"/> containing rows affected and output parameter values.</returns>
    public DbExecutionResult Execute(DbCommandRequest request) =>
        ExecuteWithRequestActivity(
            nameof(Execute),
            request,
            () =>
            {
                if (TryExecutePostgresOutParameters(request, expectScalar: false, out var result))
                {
                    return result;
                }

                return ExecuteWithCommandPolicy(() => ExecuteScalarLike(request, command =>
                {
                    var rows = command.ExecuteNonQuery();
                    return new CommandResult<object?>(null, rows);
                }));
            },
            RecordActivityResult);

    /// <summary>
    /// Executes a scalar query asynchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbExecutionResult"/> containing the scalar value and output parameter values.</returns>
    public Task<DbExecutionResult> ExecuteScalarAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
    ExecuteWithRequestActivityAsync(
        nameof(ExecuteScalarAsync),
        request,
        async () =>
        {
            if (TryExecutePostgresOutParametersAsync(request, expectScalar: true, cancellationToken, out var task))
            {
                return await task.ConfigureAwait(false);
            }

            return await ExecuteWithCommandPolicyAsync(
                token => ExecuteScalarLikeAsync(
                    request,
                    async (command, innerToken) =>
                    {
                        var scalar = await ExecuteScalarWithFallbackAsync(request, command, innerToken).ConfigureAwait(false);
                        return new CommandResult<object?>(scalar, -1);
                    },
                    token),
                cancellationToken).ConfigureAwait(false);
        },
        RecordActivityResult);

    /// <summary>
    /// Executes a scalar query synchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <returns>A <see cref="DbExecutionResult"/> containing the scalar value and output parameter values.</returns>
    public DbExecutionResult ExecuteScalar(DbCommandRequest request) =>
        ExecuteWithRequestActivity(
            nameof(ExecuteScalar),
            request,
            () =>
            {
                if (TryExecutePostgresOutParameters(request, expectScalar: true, out var result))
                {
                    return result;
                }

                return ExecuteWithCommandPolicy(() => ExecuteScalarLike(request, command =>
                {
                    var scalar = command.ExecuteScalar();
                    return new CommandResult<object?>(scalar, -1);
                }));
            },
            RecordActivityResult);

    /// <summary>
    /// Executes a data reader asynchronously and materializes each row with the supplied mapper.
    /// </summary>
    /// <typeparam name="T">Type produced by the mapper.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapper">Projection delegate invoked per row.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows and execution metadata.</returns>
    public async Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(DbCommandRequest request, Func<DbDataReader, T> mapper, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);

        return await ExecuteWithRequestActivityAsync(
            nameof(QueryAsync),
            request,
            async () =>
            {
                var (result, outputs) = await ExecuteWithCommandPolicyAsync(
                    token => ExecuteCoreAsync(request, async (command, innerToken) =>
                    {
                        var behavior = request.CommandBehavior == CommandBehavior.Default
                            ? CommandBehavior.Default
                            : request.CommandBehavior;

                        await using var reader = await ExecuteReaderWithFallbackAsync(request, command, behavior, innerToken).ConfigureAwait(false);
                        var buffer = new List<T>();
                        while (await reader.ReadAsync(innerToken).ConfigureAwait(false))
                        {
                            buffer.Add(mapper(reader));
                        }

                        var rows = reader.RecordsAffected;
                        return new CommandResult<IReadOnlyList<T>>(buffer, rows);
                    }, token),
                    cancellationToken).ConfigureAwait(false);

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<IReadOnlyList<T>>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Count);
            }).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a data reader asynchronously and materializes each row using the configured row-mapper pipeline.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapperRequest">Optional overrides (strategy, casing, column map).</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows and execution metadata.</returns>
    public Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        RowMapperRequest? mapperRequest = null,
        CancellationToken cancellationToken = default)
        where T : class =>
        QueryAsync(request, ResolveRowMapper<T>(mapperRequest), cancellationToken);

    /// <summary>
    /// Executes a data reader synchronously and materializes each row with the supplied mapper.
    /// </summary>
    /// <typeparam name="T">Type produced by the mapper.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapper">Projection delegate invoked per row.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows and execution metadata.</returns>
    public DbQueryResult<IReadOnlyList<T>> Query<T>(DbCommandRequest request, Func<DbDataReader, T> mapper)
    {
        ArgumentNullException.ThrowIfNull(mapper);

        return ExecuteWithRequestActivity(
            nameof(Query),
            request,
            () =>
            {
                var (result, outputs) = ExecuteWithCommandPolicy(() => ExecuteCore(request, command =>
                {
                    var behavior = request.CommandBehavior == CommandBehavior.Default
                        ? CommandBehavior.Default
                        : request.CommandBehavior;

                    using var reader = command.ExecuteReader(behavior);
                    var buffer = new List<T>();
                    while (reader.Read())
                    {
                        buffer.Add(mapper(reader));
                    }

                    var rows = reader.RecordsAffected;
                    return new CommandResult<IReadOnlyList<T>>(buffer, rows);
                }));

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<IReadOnlyList<T>>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Count);
            });
    }

    /// <summary>
    /// Executes a data reader synchronously using the configured row-mapper pipeline.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapperRequest">Optional overrides (strategy, casing, column map).</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows and execution metadata.</returns>
    public DbQueryResult<IReadOnlyList<T>> Query<T>(DbCommandRequest request, RowMapperRequest? mapperRequest = null)
        where T : class =>
        Query(request, ResolveRowMapper<T>(mapperRequest));

    /// <summary>
    /// Streams rows asynchronously without buffering the entire result set.
    /// </summary>
    /// <typeparam name="T">Type produced by the mapper.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapper">Projection delegate invoked per row.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>IAsyncEnumerable that lazily yields records.</returns>
    public IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ValidateRequest(request);
        return StreamAsyncCore(request, mapper, cancellationToken);

        async IAsyncEnumerable<T> StreamAsyncCore(
            DbCommandRequest innerRequest,
            Func<DbDataReader, T> innerMapper,
            [EnumeratorCancellation] CancellationToken innerToken)
        {
            await using var scope = await AcquireConnectionScopeAsync(innerRequest, innerToken).ConfigureAwait(false);
            var command = await _commandFactory.GetCommandAsync(scope.Connection, innerRequest, innerToken).ConfigureAwait(false);
            ApplyScopedTransaction(innerRequest, scope, command);
            var activity = StartActivity(nameof(StreamAsync), innerRequest);
            try
            {
                var behavior = DbStreamUtilities.EnsureSequentialBehavior(innerRequest.CommandBehavior);

                await using var reader = await ExecuteWithCommandPolicyAsync(
                    token => ExecuteReaderWithFallbackAsync(innerRequest, command, behavior, token).AsTask(),
                    innerToken).ConfigureAwait(false);
                var yielded = 0;
                try
                {
                    while (await reader.ReadAsync(innerToken).ConfigureAwait(false))
                    {
                        yielded++;
                        yield return innerMapper(reader);
                    }
                }
                finally
                {
                    var execution = new DbExecutionResult(reader.RecordsAffected, null, EmptyOutputs);
                    _telemetry.RecordCommandResult(activity, execution, yielded);
                    activity?.SetStatus(ActivityStatusCode.Ok);
                }
            }
            finally
            {
                _commandFactory.ReturnCommand(command);
                activity?.Dispose();
            }
        }
    }

    /// <summary>
    /// Streams rows asynchronously using the configured row mapper.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="mapperRequest">Optional overrides (strategy, casing, column map).</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>IAsyncEnumerable that lazily yields records.</returns>
    public IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        RowMapperRequest? mapperRequest = null,
        CancellationToken cancellationToken = default)
        where T : class =>
        StreamAsync(request, ResolveRowMapper<T>(mapperRequest), cancellationToken);

    /// <summary>
    /// Loads the results into a <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{DataTable}"/> containing the populated table.</returns>
    public Task<DbQueryResult<DataTable>> LoadDataTableAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
        ExecuteWithRequestActivityAsync(
            nameof(LoadDataTableAsync),
            request,
            async () =>
            {
                var (result, outputs) = await ExecuteWithCommandPolicyAsync(
                    token => ExecuteCoreAsync(request, async (command, innerToken) =>
                    {
                        await using var reader = await ExecuteReaderWithFallbackAsync(request, command, request.CommandBehavior, innerToken).ConfigureAwait(false);
                        var table = new DataTable();
                        table.Load(reader);
                        var rows = reader.RecordsAffected;
                        return new CommandResult<DataTable>(table, rows);
                    }, token),
                    cancellationToken).ConfigureAwait(false);

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<DataTable>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Rows.Count);
            });

    /// <summary>
    /// Loads the results into a <see cref="DataTable"/> synchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <returns>A <see cref="DbQueryResult{DataTable}"/> containing the populated table.</returns>
    public DbQueryResult<DataTable> LoadDataTable(DbCommandRequest request) =>
        ExecuteWithRequestActivity(
            nameof(LoadDataTable),
            request,
            () =>
            {
                var (result, outputs) = ExecuteWithCommandPolicy(() => ExecuteCore(request, command =>
                {
                    using var reader = command.ExecuteReader(request.CommandBehavior);
                    var table = new DataTable();
                    table.Load(reader);
                    var rows = reader.RecordsAffected;
                    return new CommandResult<DataTable>(table, rows);
                }));

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<DataTable>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Rows.Count);
            });

    /// <summary>
    /// Loads all result sets into a <see cref="DataSet"/> asynchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{DataSet}"/> containing tables and metadata.</returns>
    public Task<DbQueryResult<DataSet>> LoadDataSetAsync(DbCommandRequest request, CancellationToken cancellationToken = default) =>
        ExecuteWithRequestActivityAsync(
            nameof(LoadDataSetAsync),
            request,
            async () =>
            {
                var (result, outputs) = await ExecuteWithCommandPolicyAsync(
                    token => ExecuteCoreAsync(request, async (command, innerToken) =>
                    {
                        await using var reader = await ExecuteReaderWithFallbackAsync(request, command, request.CommandBehavior, innerToken).ConfigureAwait(false);
                        var dataSet = await ReadDataSetAsync(reader, innerToken).ConfigureAwait(false);
                        var rows = reader.RecordsAffected;
                        return new CommandResult<DataSet>(dataSet, rows);
                    }, token),
                    cancellationToken).ConfigureAwait(false);

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<DataSet>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Tables.Count);
            });

    /// <summary>
    /// Loads all result sets into a <see cref="DataSet"/> synchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <returns>A <see cref="DbQueryResult{DataSet}"/> containing tables and metadata.</returns>
    public DbQueryResult<DataSet> LoadDataSet(DbCommandRequest request) =>
        ExecuteWithRequestActivity(
            nameof(LoadDataSet),
            request,
            () =>
            {
                var (result, outputs) = ExecuteWithCommandPolicy(() => ExecuteCore(request, command =>
                {
                    using var reader = command.ExecuteReader(request.CommandBehavior);
                    var dataSet = ReadDataSet(reader);
                    var rows = reader.RecordsAffected;
                    return new CommandResult<DataSet>(dataSet, rows);
                }));

                var execution = new DbExecutionResult(result.RowsAffected, null, outputs);
                return new DbQueryResult<DataSet>(result.Data, execution);
            },
            (activity, queryResult) =>
            {
                _telemetry.RecordCommandResult(activity, queryResult.Execution, queryResult.Data.Tables.Count);
            });

    /// <summary>
    /// Executes a reader asynchronously and returns a lease that controls disposal.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbReaderScope"/> wrapping the reader/command/connection.</returns>
    public async Task<DbReaderScope> ExecuteReaderAsync(
            DbCommandRequest request,
            CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        try
        {
            var scope = await AcquireConnectionScopeAsync(request, cancellationToken).ConfigureAwait(false);
            var command = await _commandFactory.GetCommandAsync(scope.Connection, request, cancellationToken).ConfigureAwait(false);
            ApplyScopedTransaction(request, scope, command);
            var behavior = request.CommandBehavior == CommandBehavior.Default
                ? CommandBehavior.Default
                : request.CommandBehavior;
            var reader = await ExecuteReaderWithFallbackAsync(request, command, behavior, cancellationToken).ConfigureAwait(false);
            return new DbReaderScope(reader, command, scope, _commandFactory);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a reader synchronously and returns a lease that controls disposal.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <returns>A <see cref="DbReaderScope"/> wrapping the reader/command/connection.</returns>
    /// <summary>
    /// Executes a reader synchronously and returns a lease that tracks command and connection ownership.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <remarks>
    /// Callers are responsible for disposing the returned <see cref="DbReaderScope"/> to ensure the command,
    /// reader, and scoped connection are returned to their respective pools.
    /// </remarks>
    public DbReaderScope ExecuteReader(DbCommandRequest request) =>
        ExecuteWithRequestActivity(
            nameof(ExecuteReader),
            request,
            () =>
            {
                var scope = AcquireConnectionScope(request);
                var command = _commandFactory.GetCommand(scope.Connection, request);
                ApplyScopedTransaction(request, scope, command);
                var behavior = request.CommandBehavior == CommandBehavior.Default
                    ? CommandBehavior.Default
                    : request.CommandBehavior;
                var reader = command.ExecuteReader(behavior);
                return new DbReaderScope(reader, command, scope, _commandFactory);
            });

    /// <summary>
    /// Streams binary data from a single column into the specified stream asynchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="ordinal">Zero-based column ordinal.</param>
    /// <param name="destination">Destination stream receiving the bytes.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>Total bytes written.</returns>
    public Task<long> StreamColumnAsync(
        DbCommandRequest request,
        int ordinal,
        Stream destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        return WrapExecutionAsync(request, nameof(StreamColumnAsync), () =>
            ExecuteWithCommandPolicyAsync(
                token => StreamBinaryInternalAsync(request, ordinal, destination, token),
                cancellationToken),
            (activity, bytes) => activity?.SetTag("db.stream.bytes", bytes));
    }

    /// <summary>
    /// Streams binary data from a single column synchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="ordinal">Zero-based column ordinal.</param>
    /// <param name="destination">Destination stream receiving the bytes.</param>
    /// <returns>Total bytes written.</returns>
    public long StreamColumn(DbCommandRequest request, int ordinal, Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        return WrapExecution(request, nameof(StreamColumn), () => ExecuteWithCommandPolicy(() => StreamBinaryInternal(request, ordinal, destination)),
            (activity, bytes) => activity?.SetTag("db.stream.bytes", bytes));
    }

    /// <summary>
    /// Streams text data from a single column into the specified writer asynchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="ordinal">Zero-based column ordinal.</param>
    /// <param name="writer">Text writer receiving the characters.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>Total characters written.</returns>
    public Task<long> StreamTextAsync(
        DbCommandRequest request,
        int ordinal,
        TextWriter writer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return WrapExecutionAsync(request, nameof(StreamTextAsync), () =>
            ExecuteWithCommandPolicyAsync(
                token => StreamTextInternalAsync(request, ordinal, writer, token),
                cancellationToken),
            (activity, chars) => activity?.SetTag("db.stream.chars", chars));
    }

    /// <summary>
    /// Streams text data from a single column synchronously.
    /// </summary>
    /// <param name="request">Command request describing text, parameters, and transaction context.</param>
    /// <param name="ordinal">Zero-based column ordinal.</param>
    /// <param name="writer">Text writer receiving the characters.</param>
    /// <returns>Total characters written.</returns>
    public long StreamText(DbCommandRequest request, int ordinal, TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return WrapExecution(request, nameof(StreamText), () => ExecuteWithCommandPolicy(() => StreamTextInternal(request, ordinal, writer)),
            (activity, chars) => activity?.SetTag("db.stream.chars", chars));
    }

    /// <summary>
    /// Executes a stored procedure asynchronously.
    /// </summary>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbExecutionResult"/> describing the execution.</returns>
    public async Task<DbExecutionResult> ExecuteStoredProcedureAsync(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return await ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a stored procedure synchronously.
    /// </summary>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <returns>A <see cref="DbExecutionResult"/> describing the execution.</returns>
    public DbExecutionResult ExecuteStoredProcedure(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return Execute(request);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a stored procedure that returns result sets asynchronously.
    /// </summary>
    /// <typeparam name="T">Type produced by the mapper.</typeparam>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="mapper">Projection delegate invoked per row.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows.</returns>
    public async Task<DbQueryResult<IReadOnlyList<T>>> QueryStoredProcedureAsync<T>(
        string procedureName,
        Func<DbDataReader, T> mapper,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return await QueryAsync(request, mapper, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a stored procedure that returns result sets asynchronously and relies on the built-in row mapper.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="mapperRequest">Optional mapper overrides.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows.</returns>
    public Task<DbQueryResult<IReadOnlyList<T>>> QueryStoredProcedureAsync<T>(
        string procedureName,
        RowMapperRequest? mapperRequest = null,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        return QueryAsync<T>(request, mapperRequest, cancellationToken);
    }

    /// <summary>
    /// Executes a stored procedure that returns result sets synchronously.
    /// </summary>
    /// <typeparam name="T">Type produced by the mapper.</typeparam>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="mapper">Projection delegate invoked per row.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows.</returns>
    public DbQueryResult<IReadOnlyList<T>> QueryStoredProcedure<T>(
        string procedureName,
        Func<DbDataReader, T> mapper,
        IReadOnlyList<DbParameterDefinition>? parameters = null)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return Query(request, mapper);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a stored procedure synchronously using the built-in row mapper.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="mapperRequest">Optional mapper overrides.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <returns>A <see cref="DbQueryResult{T}"/> containing materialized rows.</returns>
    public DbQueryResult<IReadOnlyList<T>> QueryStoredProcedure<T>(
        string procedureName,
        RowMapperRequest? mapperRequest = null,
        IReadOnlyList<DbParameterDefinition>? parameters = null)
        where T : class
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        return Query<T>(request, mapperRequest);
    }

    /// <summary>
    /// Executes a stored procedure and returns a reader lease asynchronously.
    /// </summary>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbReaderScope"/> wrapping the reader.</returns>
    public async Task<DbReaderScope> ExecuteStoredProcedureReaderAsync(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return await ExecuteReaderAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes a stored procedure and returns a reader lease synchronously.
    /// </summary>
    /// <param name="procedureName">Name of the stored procedure.</param>
    /// <param name="parameters">Optional parameters collection.</param>
    /// <returns>A <see cref="DbReaderScope"/> wrapping the reader.</returns>
    public DbReaderScope ExecuteStoredProcedureReader(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null)
    {
        var request = CreateStoredProcedureRequest(procedureName, parameters);
        try
        {
            return ExecuteReader(request);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes an Oracle stored procedure that returns a REF CURSOR asynchronously.
    /// </summary>
    /// <param name="request">Command request describing the stored procedure.</param>
    /// <param name="cursorParameterName">Name of the REF CURSOR parameter (without provider prefix).</param>
    /// <param name="cancellationToken">Propagation token for the async operation.</param>
    /// <returns>A <see cref="DbReaderScope"/> that streams rows from the cursor.</returns>
    public async Task<DbReaderScope> ExecuteRefCursorAsync(
        DbCommandRequest request,
        string cursorParameterName,
        CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(cursorParameterName);
        EnsureOracleProvider(request);
        try
        {
            return await ExecuteRefCursorCoreAsync(request, cursorParameterName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    /// <summary>
    /// Executes an Oracle stored procedure that returns a REF CURSOR synchronously.
    /// </summary>
    /// <param name="request">Command request describing the stored procedure.</param>
    /// <param name="cursorParameterName">Name of the REF CURSOR parameter (without provider prefix).</param>
    /// <returns>A <see cref="DbReaderScope"/> that streams rows from the cursor.</returns>
    public DbReaderScope ExecuteRefCursor(
        DbCommandRequest request,
        string cursorParameterName)
    {
        ValidateRequest(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(cursorParameterName);
        EnsureOracleProvider(request);
        try
        {
            return ExecuteRefCursorCore(request, cursorParameterName);
        }
        catch (Exception ex)
        {
            throw WrapException(request, ex);
        }
    }

    #endregion

    #region Internal Helpers

    private async Task<DbExecutionResult> ExecuteScalarLikeAsync(
        DbCommandRequest request,
        Func<DbCommand, CancellationToken, Task<CommandResult<object?>>> executor,
        CancellationToken cancellationToken)
    {
        var (result, outputs) = await ExecuteCoreAsync(request, executor, cancellationToken).ConfigureAwait(false);
        return new DbExecutionResult(result.RowsAffected, result.Data, outputs);
    }

    private DbExecutionResult ExecuteScalarLike(
        DbCommandRequest request,
        Func<DbCommand, CommandResult<object?>> executor)
    {
        var (result, outputs) = ExecuteCore(request, executor);
        return new DbExecutionResult(result.RowsAffected, result.Data, outputs);
    }

    private async Task<TResult> ExecuteInstrumentedAsync<TResult>(
        DbCommandRequest request,
        Func<DbCommand, CancellationToken, Task<TResult>> executor,
        CancellationToken cancellationToken,
        string? labelOverride = null)
    {
        ValidateRequest(request);

        await using var scope = await AcquireConnectionScopeAsync(request, cancellationToken).ConfigureAwait(false);
        var command = await _commandFactory.GetCommandAsync(scope.Connection, request, cancellationToken).ConfigureAwait(false);
        ApplyScopedTransaction(request, scope, command);
        using var logScope = BeginLoggingScope(request);
        var stopwatch = Stopwatch.StartNew();
        var label = labelOverride ?? GetCommandLabel(request);

        try
        {
            var result = await executor(command, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            LogInformation("Executed command {Command} in {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Database command {Command} failed after {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            throw WrapException(request, ex);
        }
        finally
        {
            _commandFactory.ReturnCommand(command);
        }
    }

    private TResult ExecuteInstrumented<TResult>(
        DbCommandRequest request,
        Func<DbCommand, TResult> executor,
        string? labelOverride = null)
    {
        ValidateRequest(request);

        using var scope = AcquireConnectionScope(request);
        var command = _commandFactory.GetCommand(scope.Connection, request);
        ApplyScopedTransaction(request, scope, command);
        using var logScope = BeginLoggingScope(request);
        var stopwatch = Stopwatch.StartNew();
        var label = labelOverride ?? GetCommandLabel(request);

        try
        {
            var result = executor(command);
            stopwatch.Stop();
            LogInformation("Executed command {Command} in {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Database command {Command} failed after {Elapsed} ms.", label, stopwatch.ElapsedMilliseconds);
            throw WrapException(request, ex);
        }
        finally
        {
            _commandFactory.ReturnCommand(command);
        }
    }

    private Task<(CommandResult<T> Result, IReadOnlyDictionary<string, object?> Outputs)> ExecuteCoreAsync<T>(
        DbCommandRequest request,
        Func<DbCommand, CancellationToken, Task<CommandResult<T>>> executor,
        CancellationToken cancellationToken) =>
        ExecuteInstrumentedAsync(
            request,
            async (command, token) =>
            {
                var result = await executor(command, token).ConfigureAwait(false);
                var outputs = ExtractOutputs(command);
                return (result, outputs);
            },
            cancellationToken);

    private (CommandResult<T> Result, IReadOnlyDictionary<string, object?> Outputs) ExecuteCore<T>(
        DbCommandRequest request,
        Func<DbCommand, CommandResult<T>> executor) =>
        ExecuteInstrumented(
            request,
            command =>
            {
                var result = executor(command);
                var outputs = ExtractOutputs(command);
                return (result, outputs);
            });

    private static async Task<DataSet> ReadDataSetAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        var dataSet = new DataSet();
        var tableIndex = 0;
        do
        {
            var table = await MaterializeTableAsync(reader, $"Table{tableIndex++}", cancellationToken).ConfigureAwait(false);
            dataSet.Tables.Add(table);
        }
        while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

        return dataSet;
    }

    private static DataSet ReadDataSet(DbDataReader reader)
    {
        var dataSet = new DataSet();
        var tableIndex = 0;
        do
        {
            var table = MaterializeTable(reader, $"Table{tableIndex++}");
            dataSet.Tables.Add(table);
        }
        while (reader.NextResult());

        return dataSet;
    }

    private static async Task<DataTable> MaterializeTableAsync(DbDataReader reader, string tableName, CancellationToken cancellationToken)
    {
        var table = CreateSchemaTable(reader, tableName);
        var buffer = new object?[reader.FieldCount];

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            PopulateBuffer(reader, buffer);
            AddRow(table, buffer);
        }

        return table;
    }

    private static DataTable MaterializeTable(DbDataReader reader, string tableName)
    {
        var table = CreateSchemaTable(reader, tableName);
        var buffer = new object?[reader.FieldCount];

        while (reader.Read())
        {
            PopulateBuffer(reader, buffer);
            AddRow(table, buffer);
        }

        return table;
    }

    private static DataTable CreateSchemaTable(DbDataReader reader, string tableName)
    {
        var table = new DataTable(tableName);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var columnName = reader.GetName(i);
            if (table.Columns.Contains(columnName))
            {
                columnName = $"{columnName}_{i}";
            }

            var fieldType = reader.GetFieldType(i) ?? typeof(object);
            table.Columns.Add(columnName, fieldType);
        }

        return table;
    }

    private static void AddRow(DataTable table, object?[] values)
    {
        var row = table.NewRow();
        for (var i = 0; i < values.Length; i++)
        {
            row[i] = values[i] ?? DBNull.Value;
        }

        table.Rows.Add(row);
    }

    private static void PopulateBuffer(DbDataReader reader, object?[] buffer)
    {
        for (var i = 0; i < buffer.Length; i++)
        {
            buffer[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        }
    }

    private void ValidateRequest(DbCommandRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.CommandText))
        {
            throw new ArgumentException("Command text must be provided.", nameof(request));
        }

        if (request.SkipValidation)
        {
            return;
        }

        foreach (var validator in _requestValidators)
        {
            validator.ValidateAndThrow(request);
        }
    }

    private static DbCommandRequest CreateStoredProcedureRequest(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(procedureName);
        return new DbCommandRequest
        {
            CommandText = procedureName,
            CommandType = CommandType.StoredProcedure,
            Parameters = parameters ?? Array.Empty<DbParameterDefinition>()
        };
    }

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
            var value = parameter.Value is DBNull ? null : parameter.Value;
            buffer[key] = value;
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

    private ValueTask<int> ExecuteNonQueryWithFallbackAsync(DbCommandRequest request, DbCommand command, CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return ValueTask.FromResult(command.ExecuteNonQuery());
        }

        return new ValueTask<int>(command.ExecuteNonQueryAsync(cancellationToken));
    }

    private ValueTask<object?> ExecuteScalarWithFallbackAsync(DbCommandRequest request, DbCommand command, CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return ValueTask.FromResult(command.ExecuteScalar());
        }

        return new ValueTask<object?>(command.ExecuteScalarAsync(cancellationToken));
    }

    private ValueTask<DbDataReader> ExecuteReaderWithFallbackAsync(
        DbCommandRequest request,
        DbCommand command,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        if (ShouldUseSynchronousProviderPath(request))
        {
            return ValueTask.FromResult(command.ExecuteReader(behavior));
        }

        return new ValueTask<DbDataReader>(command.ExecuteReaderAsync(behavior, cancellationToken));
    }

    // Oracle's managed provider still executes async members synchronously. We short-circuit
    // to the sync path to avoid bogus async usage and reduce Task allocations.
    private bool ShouldUseSynchronousProviderPath(DbCommandRequest request) =>
        ResolveProvider(request) == DatabaseProvider.Oracle;

    private DatabaseProvider ResolveProvider(DbCommandRequest request) =>
        (request.OverrideOptions ?? _defaultOptions).Provider;

    private bool ShouldWrapExceptions(DbCommandRequest request) =>
        (request.OverrideOptions?.WrapProviderExceptions) ?? _defaultOptions.WrapProviderExceptions;

    private Exception WrapException(DbCommandRequest request, Exception exception)
    {
        if (!ShouldWrapExceptions(request) || exception is DataException)
        {
            return exception;
        }

        return new DataException($"Database command '{GetCommandLabel(request)}' failed.", exception);
    }

    private Task<T> WrapExecutionAsync<T>(
        DbCommandRequest request,
        string operation,
        Func<Task<T>> action,
        Action<Activity?, T>? onCompleted = null) =>
        ExecuteWithRequestActivityAsync(operation, request, action, onCompleted);

    private T WrapExecution<T>(
        DbCommandRequest request,
        string operation,
        Func<T> action,
        Action<Activity?, T>? onCompleted = null) =>
        ExecuteWithRequestActivity(operation, request, action, onCompleted);

    private async Task<T> ExecuteWithRequestActivityAsync<T>(
        string operation,
        DbCommandRequest request,
        Func<Task<T>> action,
        Action<Activity?, T>? onCompleted = null)
    {
        ValidateRequest(request);
        using var activity = StartActivity(operation, request);
        try
        {
            var result = await action().ConfigureAwait(false);
            onCompleted?.Invoke(activity, result);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw WrapException(request, ex);
        }
    }

    private T ExecuteWithRequestActivity<T>(
        string operation,
        DbCommandRequest request,
        Func<T> action,
        Action<Activity?, T>? onCompleted = null)
    {
        ValidateRequest(request);
        using var activity = StartActivity(operation, request);
        try
        {
            var result = action();
            onCompleted?.Invoke(activity, result);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw WrapException(request, ex);
        }
    }

    private bool TryExecutePostgresOutParametersAsync(
        DbCommandRequest request,
        bool expectScalar,
        CancellationToken cancellationToken,
        out Task<DbExecutionResult> task)
    {
        // PostgreSQL lacks native OUT parameters, so we rewrite the invocation into
        // a SELECT plan and hydrate outputs from the resulting single row.
        if (!PostgresOutParameterPlan.TryCreate(request, ResolveProvider(request), out var plan))
        {
            task = default!;
            return false;
        }

        task = ExecuteWithCommandPolicyAsync(
            token => ExecutePostgresPlanAsync(plan!, expectScalar, token),
            cancellationToken);
        return true;
    }

    private bool TryExecutePostgresOutParameters(
        DbCommandRequest request,
        bool expectScalar,
        out DbExecutionResult result)
    {
        if (!PostgresOutParameterPlan.TryCreate(request, ResolveProvider(request), out var plan))
        {
            result = default!;
            return false;
        }

        result = ExecuteWithCommandPolicy(() => ExecutePostgresPlan(plan!, expectScalar));
        return true;
    }

    private Task<DbExecutionResult> ExecutePostgresPlanAsync(
        PostgresOutParameterPlan plan,
        bool expectScalar,
        CancellationToken cancellationToken) =>
        ExecuteInstrumentedAsync(
            plan.Request,
            async (command, token) =>
            {
                await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, token).ConfigureAwait(false);
                if (!await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    throw new ProviderFeatureException("PostgreSQL OUT parameter emulation expected a result row but none was returned.");
                }

                var outputs = plan.ReadOutputs(reader);
                var data = expectScalar ? plan.GetScalar(reader) : null;
                return new DbExecutionResult(reader.RecordsAffected, data, outputs);
            },
            cancellationToken,
            plan.Request.TraceName ?? plan.Request.CommandText);

    private DbExecutionResult ExecutePostgresPlan(PostgresOutParameterPlan plan, bool expectScalar) =>
        ExecuteInstrumented(
            plan.Request,
            command =>
            {
                using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (!reader.Read())
                {
                    throw new ProviderFeatureException("PostgreSQL OUT parameter emulation expected a result row but none was returned.");
                }

                var outputs = plan.ReadOutputs(reader);
                var data = expectScalar ? plan.GetScalar(reader) : null;
                return new DbExecutionResult(reader.RecordsAffected, data, outputs);
            },
            plan.Request.TraceName ?? plan.Request.CommandText);

    // Oracle returns REF CURSORs via output parameters. We execute the procedure once, grab
    // the cursor parameter, and hand the underlying reader back to the caller inside a lease.
    private async Task<DbReaderScope> ExecuteRefCursorCoreAsync(
        DbCommandRequest request,
        string cursorParameterName,
        CancellationToken cancellationToken)
    {
        var scope = await AcquireConnectionScopeAsync(request, cancellationToken).ConfigureAwait(false);
        var command = await _commandFactory.GetCommandAsync(scope.Connection, request, cancellationToken).ConfigureAwait(false);
        ApplyScopedTransaction(request, scope, command);
        try
        {
            await ExecuteNonQueryWithFallbackAsync(request, command, cancellationToken).ConfigureAwait(false);
            var reader = GetOracleRefCursorReader(command, cursorParameterName);
            return new DbReaderScope(reader, command, scope, _commandFactory);
        }
        catch
        {
            _commandFactory.ReturnCommand(command);
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private DbReaderScope ExecuteRefCursorCore(
        DbCommandRequest request,
        string cursorParameterName)
    {
        var scope = AcquireConnectionScope(request);
        var command = _commandFactory.GetCommand(scope.Connection, request);
        ApplyScopedTransaction(request, scope, command);
        try
        {
            command.ExecuteNonQuery();
            var reader = GetOracleRefCursorReader(command, cursorParameterName);
            return new DbReaderScope(reader, command, scope, _commandFactory);
        }
        catch
        {
            _commandFactory.ReturnCommand(command);
            scope.Dispose();
            throw;
        }
    }

    private DbDataReader GetOracleRefCursorReader(DbCommand command, string cursorParameterName)
    {
        foreach (DbParameter parameter in command.Parameters)
        {
            var name = TrimPrefix(parameter.ParameterName);
            if (string.Equals(name, cursorParameterName, StringComparison.OrdinalIgnoreCase))
            {
                return OracleRefCursorReaderFactory.Create(parameter);
            }
        }

        throw new ProviderFeatureException($"Cursor parameter '{cursorParameterName}' was not found on the executed command.");
    }

    private void EnsureOracleProvider(DbCommandRequest request)
    {
        if (ResolveProvider(request) != DatabaseProvider.Oracle)
        {
            throw new ProviderNotSupportedException("REF CURSOR execution is only supported for Oracle providers.");
        }
    }

    private IDisposable? BeginLoggingScope(DbCommandRequest request) =>
        ShouldLogDetails ? _logger.BeginScope("DatabaseCommand:{Name}", GetCommandLabel(request)) : null;

    private readonly record struct CommandResult<T>(T Data, int RowsAffected);

    private Task<TResult> ExecuteWithCommandPolicyAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
        => _resilience.CommandAsyncPolicy.ExecuteAsync((_, token) => action(token), new Context(), cancellationToken);

    private TResult ExecuteWithCommandPolicy<TResult>(Func<TResult> action)
        => _resilience.CommandSyncPolicy.Execute(action);

    private Task<long> StreamBinaryInternalAsync(DbCommandRequest request, int ordinal, Stream destination, CancellationToken cancellationToken) =>
        ExecuteInstrumentedAsync(
            request,
            async (command, token) =>
            {
                var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                await using var reader = await ExecuteReaderWithFallbackAsync(request, command, behavior, token).ConfigureAwait(false);
                if (!await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    return 0;
                }

                await using var source = reader.GetStream(ordinal);
                var total = await DbStreamUtilities.CopyStreamAsync(source, destination, token).ConfigureAwait(false);
                LogInformation("Streamed {Bytes} bytes from {Command}.", total, GetCommandLabel(request));
                return total;
            },
            cancellationToken);

    private long StreamBinaryInternal(DbCommandRequest request, int ordinal, Stream destination) =>
        ExecuteInstrumented(
            request,
            command =>
            {
                var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                using var reader = command.ExecuteReader(behavior);
                if (!reader.Read())
                {
                    return 0;
                }

                using var source = reader.GetStream(ordinal);
                var total = DbStreamUtilities.CopyStream(source, destination);
                LogInformation("Streamed {Bytes} bytes from {Command}.", total, GetCommandLabel(request));
                return total;
            });

    private Task<long> StreamTextInternalAsync(DbCommandRequest request, int ordinal, TextWriter writer, CancellationToken cancellationToken) =>
        ExecuteInstrumentedAsync(
            request,
            async (command, token) =>
            {
                var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                await using var reader = await ExecuteReaderWithFallbackAsync(request, command, behavior, token).ConfigureAwait(false);
                if (!await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    return 0;
                }

                using var textReader = reader.GetTextReader(ordinal);
                var total = await DbStreamUtilities.CopyTextAsync(textReader, writer, token).ConfigureAwait(false);
                LogInformation("Streamed {Chars} chars from {Command}.", total, GetCommandLabel(request));
                return total;
            },
            cancellationToken);

    private long StreamTextInternal(DbCommandRequest request, int ordinal, TextWriter writer) =>
        ExecuteInstrumented(
            request,
            command =>
            {
                var behavior = DbStreamUtilities.EnsureSequentialBehavior(request.CommandBehavior);
                using var reader = command.ExecuteReader(behavior);
                if (!reader.Read())
                {
                    return 0;
                }

                using var textReader = reader.GetTextReader(ordinal);
                var total = DbStreamUtilities.CopyText(textReader, writer);
                LogInformation("Streamed {Chars} chars from {Command}.", total, GetCommandLabel(request));
                return total;
            });

    private string GetCommandLabel(DbCommandRequest request) =>
        _telemetry.GetCommandDisplayName(request);

    private bool ShouldLogDetails => _runtimeOptions.EnableDetailedLogging && _logger.IsEnabled(LogLevel.Information);

    private void LogInformation(string message, params object?[] args)
    {
        if (ShouldLogDetails)
        {
            _logger.LogInformation(message, args);
        }
    }

    private Func<DbDataReader, T> ResolveRowMapper<T>(RowMapperRequest? mapperRequest)
        where T : class
    {
        var mapper = _rowMapperFactory.Create<T>(mapperRequest);
        return mapper.Map;
    }

    private async ValueTask<ConnectionScope> AcquireConnectionScopeAsync(DbCommandRequest request, CancellationToken cancellationToken)
    {
        if (request.Connection is { } explicitConnection)
        {
            if (explicitConnection.State != ConnectionState.Open)
            {
                await explicitConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            return ConnectionScope.Wrap(explicitConnection, request.Transaction, request.CloseConnection);
        }

        return await _connectionScopeManager.LeaseAsync(request.OverrideOptions, cancellationToken).ConfigureAwait(false);
    }

    private ConnectionScope AcquireConnectionScope(DbCommandRequest request)
    {
        if (request.Connection is { } explicitConnection)
        {
            if (explicitConnection.State != ConnectionState.Open)
            {
                explicitConnection.Open();
            }

            return ConnectionScope.Wrap(explicitConnection, request.Transaction, request.CloseConnection);
        }

        return _connectionScopeManager.Lease(request.OverrideOptions);
    }

    private static void ApplyScopedTransaction(
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

    #endregion
}
