using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dan.EntityRegistryProxy.Models;
public class SyncState
{
    public DateTimeOffset LastUpdatedUnits { get; set; }
    public DateTimeOffset LastUpdatedSubUnits { get; set; }
}