using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dan.EntityRegistryUpdater.Models;

public class UnitType
{
    public string Name { get; set; }
    public string Url { get; set; } 
    public string StateBlob { get; set; }
    public bool IsSubUnit { get; set; }
}
