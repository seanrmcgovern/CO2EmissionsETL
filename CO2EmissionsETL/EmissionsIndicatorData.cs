using System.Text.Json.Serialization;

namespace CO2EmissionsETL
{
	public class EmissionsIndicatorData
	{
        [JsonPropertyName("country")]
        public Country Country { get; set; } = new Country();

        [JsonPropertyName("countryiso3code")]
        public string Countryiso3code { get; set; } = string.Empty;

        [JsonPropertyName("date")]
        public string Date { get; set; } = string.Empty;

        [JsonPropertyName("indicator")]
        public EmissionsIndicator Indicator { get; set; } = new EmissionsIndicator();

        [JsonPropertyName("obs_status")]
        public string ObsStatus { get; set; } = string.Empty;

        [JsonPropertyName("unit")]
        public string Unit { get; set; } = string.Empty;

        [JsonPropertyName("value")]
        public decimal? Value { get; set; }
    }
}

