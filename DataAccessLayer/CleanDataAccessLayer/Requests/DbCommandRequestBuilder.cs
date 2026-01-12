using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using DataAccessLayer.Execution;
using Shared.Configuration;

namespace DataAccessLayer.Clean.Requests;

/// <summary>
/// Fluent helper for constructing <see cref="DbCommandRequest"/> instances without repetitive boilerplate.
/// </summary>
public sealed class DbCommandRequestBuilder
{
    private readonly List<DbParameterDefinition> _parameters = new();
    private string? _commandText;
    private CommandType _commandType = CommandType.Text;
    private CommandBehavior _behavior = CommandBehavior.Default;
    private int? _timeoutSeconds;
    private bool _prepare;
    private bool _validate = true;
    private DatabaseOptions? _overrideOptions;
    private DbConnection? _connection;
    private DbTransaction? _transaction;
    private bool _closeConnection;
    private string? _traceName;

    public static DbCommandRequestBuilder ForText(string sql) =>
        new DbCommandRequestBuilder().WithCommandText(sql);

    public static DbCommandRequestBuilder ForStoredProcedure(string procedureName) =>
        new DbCommandRequestBuilder().AsStoredProcedure(procedureName);

    public DbCommandRequestBuilder WithCommandText(string sql)
    {
        _commandText = sql;
        _commandType = CommandType.Text;
        return this;
    }

    public DbCommandRequestBuilder AsStoredProcedure(string procedureName)
    {
        _commandText = procedureName;
        _commandType = CommandType.StoredProcedure;
        return this;
    }

    public DbCommandRequestBuilder WithParameters(IEnumerable<DbParameterDefinition> parameters)
    {
        if (parameters is null)
        {
            throw new ArgumentNullException(nameof(parameters));
        }

        foreach (var parameter in parameters)
        {
            if (parameter is null)
            {
                continue;
            }

            _parameters.Add(parameter);
        }
        return this;
    }

    public DbCommandRequestBuilder WithParameter(DbParameterDefinition parameter)
    {
        if (parameter is null)
        {
            throw new ArgumentNullException(nameof(parameter));
        }

        _parameters.Add(parameter);
        return this;
    }

    public DbCommandRequestBuilder WithCommandBehavior(CommandBehavior behavior)
    {
        _behavior = behavior;
        return this;
    }

    public DbCommandRequestBuilder WithTimeout(int seconds)
    {
        if (seconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(seconds));
        }

        _timeoutSeconds = seconds;
        return this;
    }

    public DbCommandRequestBuilder Prepare(bool prepare = true)
    {
        _prepare = prepare;
        return this;
    }

    public DbCommandRequestBuilder WithOverrideOptions(DatabaseOptions options)
    {
        _overrideOptions = options ?? throw new ArgumentNullException(nameof(options));
        return this;
    }

    public DbCommandRequestBuilder WithConnection(DbConnection connection, bool closeWhenDone = false)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _closeConnection = closeWhenDone;
        return this;
    }

    public DbCommandRequestBuilder WithTransaction(DbTransaction transaction)
    {
        _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
        return this;
    }

    public DbCommandRequestBuilder TraceAs(string traceName)
    {
        _traceName = traceName;
        return this;
    }

    public DbCommandRequestBuilder SkipValidation(bool skip = true)
    {
        _validate = !skip;
        return this;
    }

    public DbCommandRequest Build()
    {
        if (string.IsNullOrWhiteSpace(_commandText))
        {
            throw new InvalidOperationException("Command text or stored procedure name must be provided.");
        }

        return new DbCommandRequest
        {
            CommandText = _commandText,
            CommandType = _commandType,
            Parameters = _parameters.Count == 0
                ? Array.Empty<DbParameterDefinition>()
                : _parameters.ToArray(),
            CommandBehavior = _behavior,
            CommandTimeoutSeconds = _timeoutSeconds,
            PrepareCommand = _prepare,
            OverrideOptions = _overrideOptions,
            Connection = _connection,
            CloseConnection = _closeConnection,
            Transaction = _transaction,
            TraceName = _traceName,
            Validate = _validate
        };
    }
}
