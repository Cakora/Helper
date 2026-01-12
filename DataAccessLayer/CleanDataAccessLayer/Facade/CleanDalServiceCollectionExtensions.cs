using System;
using DataAccessLayer.Clean.Abstractions;
using DataAccessLayer.Clean.Core;
using DataAccessLayer.Clean.Core.Transactions;
using Microsoft.Extensions.DependencyInjection;

namespace DataAccessLayer.Clean.Facade;

/// <summary>
/// Central place to register Clean DAL abstractions and their implementations with the DI container.
/// </summary>
public static class CleanDalServiceCollectionExtensions
{
    /// <summary>
    /// Adds the clean data access layer services to the provided service collection.
    /// </summary>
    /// <param name="services">Application service collection.</param>
    /// <returns>The same collection to support fluent configuration.</returns>
    public static IServiceCollection AddCleanDataAccessLayer(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        RegisterCoreAbstractions(services);
        RegisterTransactionPipeline(services);

        return services;
    }

    #region Registration Helpers

    private static void RegisterCoreAbstractions(IServiceCollection services)
    {
        // ICleanDatabaseHelper -> CleanDatabaseHelper (scoped per request to reuse scoped connections/telemetry).
        services.AddScoped<ICleanDatabaseHelper, CleanDatabaseHelper>();
        services.AddScoped<ICleanDatabaseHelperAsync>(sp => (ICleanDatabaseHelperAsync)sp.GetRequiredService<ICleanDatabaseHelper>());
        services.AddScoped<ICleanDatabaseHelperSync>(sp => (ICleanDatabaseHelperSync)sp.GetRequiredService<ICleanDatabaseHelper>());
    }

    private static void RegisterTransactionPipeline(IServiceCollection services)
    {
        // ITransactionRunner -> CleanTransactionRunner (scoped to share the helper and transaction manager per request).
        services.AddScoped<ITransactionRunner, CleanTransactionRunner>();
    }

    #endregion
}
