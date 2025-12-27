using System;
using System.Buffers;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Shared.IO;

/// <summary>
/// Provides reusable helpers for streaming database data to managed destinations.
/// </summary>
public static class DbStreamUtilities
{
    #region Binary Streaming

    /// <summary>
    /// Copies all bytes from <paramref name="source"/> to <paramref name="destination"/> asynchronously.
    /// </summary>
    /// <param name="source">The source stream exposed by a data reader.</param>
    /// <param name="destination">The destination stream that receives the bytes.</param>
    /// <param name="cancellationToken">Cancellation token for the asynchronous copy.</param>
    /// <returns>Total number of bytes written to the destination stream.</returns>
    public static async Task<long> CopyStreamAsync(
        Stream source,
        Stream destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);

        var buffer = ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            long total = 0;
            int read;
            while ((read = await source.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                total += read;
            }

            return total;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Copies all bytes from <paramref name="source"/> to <paramref name="destination"/> synchronously.
    /// </summary>
    /// <param name="source">The source stream exposed by a data reader.</param>
    /// <param name="destination">The destination stream that receives the bytes.</param>
    /// <returns>Total number of bytes written to the destination stream.</returns>
    public static long CopyStream(Stream source, Stream destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);

        var buffer = ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            long total = 0;
            int read;
            while ((read = source.Read(buffer, 0, buffer.Length)) > 0)
            {
                destination.Write(buffer, 0, read);
                total += read;
            }

            return total;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    #endregion

    #region Text Streaming

    /// <summary>
    /// Copies all characters from <paramref name="source"/> to <paramref name="destination"/> asynchronously.
    /// </summary>
    /// <param name="source">The text reader exposed by a data reader column.</param>
    /// <param name="destination">The text writer that receives the characters.</param>
    /// <param name="cancellationToken">Cancellation token for the asynchronous copy.</param>
    /// <returns>Total number of characters written to the destination writer.</returns>
    public static async Task<long> CopyTextAsync(
        TextReader source,
        TextWriter destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);

        var buffer = ArrayPool<char>.Shared.Rent(4096);
        try
        {
            long total = 0;
            int read;
            while ((read = await source.ReadAsync(buffer.AsMemory(0, buffer.Length)).ConfigureAwait(false)) > 0)
            {
                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                total += read;
            }

            return total;
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Copies all characters from <paramref name="source"/> to <paramref name="destination"/> synchronously.
    /// </summary>
    /// <param name="source">The text reader exposed by a data reader column.</param>
    /// <param name="destination">The text writer that receives the characters.</param>
    /// <returns>Total number of characters written to the destination writer.</returns>
    public static long CopyText(TextReader source, TextWriter destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);

        var buffer = ArrayPool<char>.Shared.Rent(4096);
        try
        {
            long total = 0;
            int read;
            while ((read = source.Read(buffer, 0, buffer.Length)) > 0)
            {
                destination.Write(buffer, 0, read);
                total += read;
            }

            return total;
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer);
        }
    }

    #endregion

    #region Command Behavior

    /// <summary>
    /// Ensures <see cref="CommandBehavior.SequentialAccess"/> is enabled for streaming operations without
    /// stripping any existing command behavior flags.
    /// </summary>
    /// <param name="behavior">The caller-specified behavior flags.</param>
    /// <returns>A behavior flag set that always includes <see cref="CommandBehavior.SequentialAccess"/>.</returns>
    public static CommandBehavior EnsureSequentialBehavior(CommandBehavior behavior)
    {
        if (behavior == CommandBehavior.Default)
        {
            return CommandBehavior.SequentialAccess;
        }

        return (behavior & CommandBehavior.SequentialAccess) != 0
            ? behavior
            : behavior | CommandBehavior.SequentialAccess;
    }

    #endregion
}
