using System.Text.Json.Serialization;

namespace CO2EmissionsETL
{
	public class EmissionsIndicator
	{
        [JsonPropertyName("id")]
        public string Id { get; set; } = string.Empty;

        [JsonPropertyName("value")]
        public string Value { get; set; } = string.Empty;
    }
}

