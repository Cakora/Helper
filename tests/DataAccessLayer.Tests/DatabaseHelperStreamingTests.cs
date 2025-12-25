using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Execution;
using DataAccessLayer.Common.DbHelper;
using DataAccessLayer.Configuration;
using DataAccessLayer.Mapping;
using DataAccessLayer.Telemetry;
using FluentValidation;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Configuration;
using Xunit;

namespace DataAccessLayer.Tests;

#nullable enable

    public sealed class DatabaseHelperStreamingTests
    {
        private static readonly DatabaseOptions Options = new()
        {
        Provider = DatabaseProvider.SqlServer,
        ConnectionString = "Server=.;Database=Fake;Trusted_Connection=True;",
        Resilience = new ResilienceOptions { EnableCommandRetries = false }
    };

    [Fact]
    public async Task StreamColumnAsync_WritesBytes()
    {
        var factory = new StreamingCommandFactory();
        var helper = CreateHelper(factory);
        var destination = new MemoryStream();

        var bytes = await helper.StreamColumnAsync(new DbCommandRequest { CommandText = "SELECT Blob" }, 0, destination);

        Assert.Equal(3, bytes);
        Assert.Equal("abc", System.Text.Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Fact]
    public void StreamText_WritesCharacters()
    {
        var factory = new StreamingCommandFactory();
        var helper = CreateHelper(factory);
        using var writer = new StringWriter();

        var count = helper.StreamText(new DbCommandRequest { CommandText = "SELECT Text" }, 1, writer);

        Assert.Equal(5, count);
        Assert.Equal("Hello", writer.ToString());
        }

        [Fact]
        public async Task StreamAsync_AppendsSequentialAccessFlagToCustomBehavior()
        {
            var factory = new StreamingCommandFactory();
            var helper = CreateHelper(factory);
            var request = new DbCommandRequest
            {
                CommandText = "SELECT Text",
                CommandBehavior = CommandBehavior.SingleResult
            };

            var enumerated = false;
            await foreach (var _ in helper.StreamAsync(request, reader => reader.GetString(reader.GetOrdinal("Text"))))
            {
                enumerated = true;
            }

            var command = Assert.IsType<FakeCommand>(factory.LastCommand);
            Assert.True(command.LastBehavior.HasFlag(CommandBehavior.SequentialAccess));
            Assert.True(command.LastBehavior.HasFlag(CommandBehavior.SingleResult));
            Assert.True(enumerated);
        }

        [Fact]
        public void StreamAsync_ThrowsWhenMapperIsNull()
        {
            var helper = CreateHelper(new StreamingCommandFactory());
            var request = new DbCommandRequest { CommandText = "SELECT Text" };

            Assert.Throws<ArgumentNullException>(() => helper.StreamAsync<string>(request, mapper: null!));
        }

        [Fact]
        public void StreamAsync_ThrowsWhenCommandTextMissing()
        {
            var helper = CreateHelper(new StreamingCommandFactory());
            var request = new DbCommandRequest { CommandText = "   " };

            Assert.Throws<ArgumentException>(() => helper.StreamAsync(request, reader => reader.GetString(reader.GetOrdinal("Text"))));
        }

        private static IDatabaseHelper CreateHelper(IDbCommandFactory commandFactory)
        {
            var connectionFactory = new FakeConnectionFactory();
            var scopeManager = new ConnectionScopeManager(connectionFactory, Options);
            var helperOptions = new DbHelperOptions();
        var telemetry = new DataAccessTelemetry(helperOptions);
        var rowMapperFactory = new RowMapperFactory(helperOptions);
        var features = DalFeatures.Default;
        return new DatabaseHelper(
            scopeManager,
            commandFactory,
            Options,
            new ResilienceStrategy(Options.Resilience, NullLogger<ResilienceStrategy>.Instance),
            NullLogger<DatabaseHelper>.Instance,
            telemetry,
            features,
            Array.Empty<IValidator<DbCommandRequest>>(),
            rowMapperFactory);
        }

        private sealed class StreamingCommandFactory : IDbCommandFactory
        {
            public FakeCommand? LastCommand { get; private set; }

            public DbCommand Rent(DbConnection connection, DbCommandRequest request)
            {
                var command = new FakeCommand();
                LastCommand = command;
                return command;
            }

            public Task<DbCommand> RentAsync(DbConnection connection, DbCommandRequest request, CancellationToken cancellationToken = default)
            {
                var command = new FakeCommand();
                LastCommand = command;
                return Task.FromResult<DbCommand>(command);
            }

            public void Return(DbCommand command) => command.Dispose();
        }

    private sealed class FakeConnectionFactory : IDbConnectionFactory
    {
        public DbConnection CreateConnection(DatabaseOptions options) => new FakeConnection();
    }

    private sealed class FakeConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => "Fake";
        public override string DataSource => "Fake";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => new FakeCommand();
    }

    private sealed class FakeCommand : DbCommand
    {
        private readonly FakeDataReader _reader = new();

        public CommandBehavior LastBehavior { get; private set; } = CommandBehavior.Default;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; } = 30;
        public override CommandType CommandType { get; set; } = CommandType.Text;
        public override UpdateRowSource UpdatedRowSource { get; set; }
            = UpdateRowSource.None;
        [System.Diagnostics.CodeAnalysis.AllowNull]
        protected override DbConnection? DbConnection { get; set; } = new FakeConnection();
        protected override DbParameterCollection DbParameterCollection { get; } = new FakeParameterCollection();
        protected override DbTransaction? DbTransaction { get; set; }
            = null;
        public override bool DesignTimeVisible { get; set; }
            = false;

        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object ExecuteScalar() => new object();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new FakeParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            LastBehavior = behavior;
            return _reader;
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            LastBehavior = behavior;
            return Task.FromResult<DbDataReader>(_reader);
        }
    }

    private sealed class FakeDataReader : DbDataReader
    {
        private bool _read;

        public override bool Read()
        {
            if (_read)
            {
                return false;
            }

            _read = true;
            return true;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override int FieldCount => 2;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override int Depth => 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override bool HasRows => true;
        public override bool GetBoolean(int ordinal) => false;
        public override byte GetByte(int ordinal) => 0;
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => '\0';
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => ordinal == 0 ? "varbinary" : "nvarchar";
        public override DateTime GetDateTime(int ordinal) => DateTime.MinValue;
        public override decimal GetDecimal(int ordinal) => 0;
        public override double GetDouble(int ordinal) => 0;
        public override Type GetFieldType(int ordinal) => typeof(object);
        public override float GetFloat(int ordinal) => 0;
        public override Guid GetGuid(int ordinal) => Guid.Empty;
        public override short GetInt16(int ordinal) => 0;
        public override int GetInt32(int ordinal) => 0;
        public override long GetInt64(int ordinal) => 0;
        public override string GetName(int ordinal) => ordinal == 0 ? "Blob" : "Text";
        public override int GetOrdinal(string name) => name == "Blob" ? 0 : 1;
        public override string GetString(int ordinal) => ordinal == 1 ? "Hello" : string.Empty;
        public override object GetValue(int ordinal) => ordinal == 1 ? "Hello" : new byte[] { 1, 2, 3 };
        public override int GetValues(object[] values)
        {
            values[0] = GetValue(0);
            values[1] = GetValue(1);
            return 2;
        }
        public override bool IsDBNull(int ordinal) => false;
        public override bool NextResult() => false;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();

        public override Stream GetStream(int ordinal)
        {
            if (ordinal != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            }

            return new MemoryStream(System.Text.Encoding.UTF8.GetBytes("abc"));
        }

        public override TextReader GetTextReader(int ordinal)
        {
            if (ordinal != 1)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            }

            return new StringReader("Hello");
        }
    }

    private sealed class FakeParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ParameterName { get; set; } = string.Empty;
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string SourceColumn { get; set; } = string.Empty;
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override object Value { get; set; } = null!;
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override void ResetDbType() { }
    }

    private sealed class FakeParameterCollection : DbParameterCollection
    {
        public override int Count => 0;
        public override object SyncRoot => this;
        public override int Add(object value) => 0;
        public override void AddRange(Array values) { }
        public override void Clear() { }
        public override bool Contains(object value) => false;
        public override bool Contains(string value) => false;
        public override void CopyTo(Array array, int index) { }
        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
        public override int IndexOf(object value) => -1;
        public override int IndexOf(string parameterName) => -1;
        public override void Insert(int index, object value) { }
        public override void Remove(object value) { }
        public override void RemoveAt(int index) { }
        public override void RemoveAt(string parameterName) { }
        protected override DbParameter GetParameter(int index) => new FakeParameter();
        protected override DbParameter GetParameter(string parameterName) => new FakeParameter();
        protected override void SetParameter(int index, DbParameter value) { }
        protected override void SetParameter(string parameterName, DbParameter value) { }
    }
}
