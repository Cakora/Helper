using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DataAccessLayer.Execution;

namespace DataAccessLayer.Clean.Abstractions;

/// <summary>
/// Minimal surface for the clean Database Helper façade (combines sync + async).
/// </summary>
public interface ICleanDatabaseHelper : ICleanDatabaseHelperAsync, ICleanDatabaseHelperSync
{
}
