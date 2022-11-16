using System.Text.Json.Serialization;

namespace Dan.EntityRegistryUpdater.Models;

public partial class ChangeList
{
    [JsonPropertyName("_embedded")]
    public Embedded Embedded { get; set; }

    [JsonPropertyName("_links")]
    public ChangeListLinks Links { get; set; }

    [JsonPropertyName("page")]
    public Page Page { get; set; }
}

public partial class Embedded
{
    [JsonPropertyName("oppdaterteEnheter")]
    public List<OppdatertEnhet> OppdaterteEnheter { get; set; }

    [JsonPropertyName("oppdaterteUnderenheter")]
    public List<OppdatertEnhet> OppdaterteUnderenheter { get; set; }
}

public partial class OppdatertEnhet
{
    [JsonPropertyName("oppdateringsid")]
    public long Oppdateringsid { get; set; }

    [JsonPropertyName("dato")]
    public DateTimeOffset Dato { get; set; }

    [JsonPropertyName("organisasjonsnummer")]
    public string Organisasjonsnummer { get; set; }

    [JsonPropertyName("endringstype")]
    public string Endringstype { get; set; }

    [JsonPropertyName("_links")]
    public Links Links { get; set; }
}

public partial class Links
{
    [JsonPropertyName("enhet")]
    public Link Enhet { get; set; }

    [JsonPropertyName("underenhet")]
    public Link Underenhet { get; set; }
}

public partial class Link
{
    [JsonPropertyName("href")]
    public Uri Href { get; set; }
}

public partial class ChangeListLinks
{
    [JsonPropertyName("first")]
    public Link Link { get; set; }

    [JsonPropertyName("self")]
    public Link Self { get; set; }

    [JsonPropertyName("next")]
    public Link? Next { get; set; }

    [JsonPropertyName("last")]
    public Link Last { get; set; }
}

public partial class Page
{
    [JsonPropertyName("size")]
    public long Size { get; set; }

    [JsonPropertyName("totalElements")]
    public long TotalElements { get; set; }

    [JsonPropertyName("totalPages")]
    public long TotalPages { get; set; }

    [JsonPropertyName("number")]
    public long Number { get; set; }
}
