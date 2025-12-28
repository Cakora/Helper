using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Mapping;

namespace DataAccessLayer.Execution;

/// <summary>
/// Defines high-level database helper operations that expose both sync and async entry points.
/// </summary>
public interface IDatabaseHelper
{
    /// <summary>
    /// Executes a command that does not return a result set and captures rows affected plus OUT parameters.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>Execution metadata for the command.</returns>
    Task<DbExecutionResult> ExecuteAsync(DbCommandRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronous counterpart to <see cref="ExecuteAsync"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <returns>Execution metadata for the command.</returns>
    DbExecutionResult Execute(DbCommandRequest request);

    /// <summary>
    /// Executes a command and returns the first column of the first row plus OUT parameters.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>Execution metadata where <see cref="DbExecutionResult.Scalar"/> contains the value.</returns>
    Task<DbExecutionResult> ExecuteScalarAsync(DbCommandRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronous counterpart to <see cref="ExecuteScalarAsync"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <returns>Execution metadata where <see cref="DbExecutionResult.Scalar"/> contains the value.</returns>
    DbExecutionResult ExecuteScalar(DbCommandRequest request);

    /// <summary>
    /// Executes a query, materializes rows with the provided mapper, and captures OUT parameters.
    /// </summary>
    /// <typeparam name="T">Row materialization type.</typeparam>
    /// <param name="request">Execution request definition.</param>
    /// <param name="mapper">Delegate that maps the current <see cref="DbDataReader"/> row to <typeparamref name="T"/>.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>Materialized rows plus execution metadata.</returns>
    Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a query using the built-in mapper pipeline.
    /// </summary>
    Task<DbQueryResult<IReadOnlyList<T>>> QueryAsync<T>(
        DbCommandRequest request,
        RowMapperRequest? mapperRequest = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Synchronous counterpart to <see cref="QueryAsync{T}(DbCommandRequest, Func{DbDataReader, T}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">Row materialization type.</typeparam>
    /// <param name="request">Execution request definition.</param>
    /// <param name="mapper">Delegate that maps the current <see cref="DbDataReader"/> row to <typeparamref name="T"/>.</param>
    /// <returns>Materialized rows plus execution metadata.</returns>
    DbQueryResult<IReadOnlyList<T>> Query<T>(DbCommandRequest request, Func<DbDataReader, T> mapper);

    /// <summary>
    /// Synchronous counterpart to <see cref="QueryAsync{T}(DbCommandRequest, RowMapperRequest?, CancellationToken)"/>.
    /// </summary>
    DbQueryResult<IReadOnlyList<T>> Query<T>(DbCommandRequest request, RowMapperRequest? mapperRequest = null)
        where T : class;

    /// <summary>
    /// Streams rows using <see cref="IAsyncEnumerable{T}"/> to avoid buffering large result sets in memory.
    /// </summary>
    /// <typeparam name="T">Row materialization type.</typeparam>
    /// <param name="request">Execution request definition.</param>
    /// <param name="mapper">Delegate that maps the current <see cref="DbDataReader"/> row to <typeparamref name="T"/>.</param>
    /// <param name="cancellationToken">Token that cancels enumeration.</param>
    /// <remarks>Output parameters are not exposed for the streaming API to keep the pipeline allocation-free.</remarks>
    /// <returns>An async stream that yields rows as they are read.</returns>
    IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        Func<DbDataReader, T> mapper,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams rows using the built-in mapper pipeline.
    /// </summary>
    IAsyncEnumerable<T> StreamAsync<T>(
        DbCommandRequest request,
        RowMapperRequest? mapperRequest = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Loads the results of the command into a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>A populated <see cref="DataTable"/> plus execution metadata.</returns>
    Task<DbQueryResult<DataTable>> LoadDataTableAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronous counterpart to <see cref="LoadDataTableAsync"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <returns>A populated <see cref="DataTable"/> plus execution metadata.</returns>
    DbQueryResult<DataTable> LoadDataTable(DbCommandRequest request);

    /// <summary>
    /// Loads the results (potentially multiple result sets) into a <see cref="DataSet"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>A populated <see cref="DataSet"/> plus execution metadata.</returns>
    Task<DbQueryResult<DataSet>> LoadDataSetAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronous counterpart to <see cref="LoadDataSetAsync"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <returns>A populated <see cref="DataSet"/> plus execution metadata.</returns>
    DbQueryResult<DataSet> LoadDataSet(DbCommandRequest request);

    /// <summary>
    /// Executes the specified command and returns a lease exposing the raw <see cref="DbDataReader"/>.
    /// Caller must dispose the lease to release the underlying connection and command resources.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="cancellationToken">Token that cancels the underlying provider call.</param>
    /// <returns>A lease that owns the reader/command/connection.</returns>
    Task<DbReaderScope> ExecuteReaderAsync(
        DbCommandRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the specified command and returns a lease exposing the raw <see cref="DbDataReader"/>.
    /// Caller must dispose the lease to release the underlying connection and command resources.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <returns>A lease that owns the reader/command/connection.</returns>
    DbReaderScope ExecuteReader(DbCommandRequest request);

    /// <summary>
    /// Streams a binary column directly into the provided <see cref="Stream"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="ordinal">Ordinal of the column to stream.</param>
    /// <param name="destination">Destination stream that receives the bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Total bytes written.</returns>
    Task<long> StreamColumnAsync(
        DbCommandRequest request,
        int ordinal,
        Stream destination,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams a binary column directly into the provided <see cref="Stream"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="ordinal">Ordinal of the column to stream.</param>
    /// <param name="destination">Destination stream.</param>
    /// <returns>Total bytes written.</returns>
    long StreamColumn(DbCommandRequest request, int ordinal, Stream destination);

    /// <summary>
    /// Streams a text column into the provided <see cref="TextWriter"/>.
    /// </summary>
    /// <param name="request">Execution request definition.</param>
    /// <param name="ordinal">Ordinal of the column to stream.</param>
    /// <param name="writer">Destination writer.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Total characters written.</returns>
    Task<long> StreamTextAsync(
        DbCommandRequest request,
        int ordinal,
        TextWriter writer,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams a text column into the provided <see cref="TextWriter"/>.
    /// </summary>
    long StreamText(DbCommandRequest request, int ordinal, TextWriter writer);

    /// <summary>
    /// Executes a stored procedure that does not return a result set.
    /// </summary>
    /// <param name="procedureName">Stored procedure name.</param>
    /// <param name="parameters">Parameters supplied to the stored procedure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DbExecutionResult> ExecuteStoredProcedureAsync(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a stored procedure that does not return a result set.
    /// </summary>
    /// <param name="procedureName">Stored procedure name.</param>
    /// <param name="parameters">Parameters supplied to the stored procedure.</param>
    DbExecutionResult ExecuteStoredProcedure(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null);

    /// <summary>
    /// Executes a stored procedure and maps the result set.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="procedureName">Stored procedure name.</param>
    /// <param name="mapper">Row mapper.</param>
    /// <param name="parameters">Parameters supplied to the stored procedure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DbQueryResult<IReadOnlyList<T>>> QueryStoredProcedureAsync<T>(
        string procedureName,
        Func<DbDataReader, T> mapper,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a stored procedure using the built-in mapper pipeline.
    /// </summary>
    Task<DbQueryResult<IReadOnlyList<T>>> QueryStoredProcedureAsync<T>(
        string procedureName,
        RowMapperRequest? mapperRequest = null,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Synchronous counterpart to <see cref="QueryStoredProcedureAsync{T}(string, Func{DbDataReader, T}, IReadOnlyList{DbParameterDefinition}?, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">Destination type.</typeparam>
    /// <param name="procedureName">Stored procedure name.</param>
    /// <param name="mapper">Row mapper.</param>
    /// <param name="parameters">Parameters supplied to the stored procedure.</param>
    DbQueryResult<IReadOnlyList<T>> QueryStoredProcedure<T>(
        string procedureName,
        Func<DbDataReader, T> mapper,
        IReadOnlyList<DbParameterDefinition>? parameters = null);

    /// <summary>
    /// Executes a stored procedure using the built-in mapper pipeline (sync).
    /// </summary>
    DbQueryResult<IReadOnlyList<T>> QueryStoredProcedure<T>(
        string procedureName,
        RowMapperRequest? mapperRequest = null,
        IReadOnlyList<DbParameterDefinition>? parameters = null)
        where T : class;

    /// <summary>
    /// Executes a stored procedure and returns a raw reader lease.
    /// </summary>
    Task<DbReaderScope> ExecuteStoredProcedureReaderAsync(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a stored procedure and returns a raw reader lease.
    /// </summary>
    DbReaderScope ExecuteStoredProcedureReader(
        string procedureName,
        IReadOnlyList<DbParameterDefinition>? parameters = null);
}
