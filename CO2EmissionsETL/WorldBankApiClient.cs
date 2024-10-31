using System.Net.Http.Headers;
using System.Text.Json;

namespace CO2EmissionsETL
{
    public class WorldBankApiClient
	{
        // HttpClient is intended to be instantiated once and reused throughout the life of an application 
        private static readonly HttpClient client = new HttpClient();

        private static readonly string[] CountryCodes = { "BRA", "CHN", "FRA", "IND", "JPN", "USA" };

        public WorldBankApiClient()
        {
            // Clear default request headers and supply the HttpClient with updated parameters
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        public async Task<List<WorldBankEmissionsResponse>> FetchEmissionsDataForCountriesAsync()
        {
            var tasks = new List<Task<List<EmissionsIndicatorData>>>();

            // Fetch data for each country from the World Bank API, processing the data into a list
            foreach (var countryCode in CountryCodes)
            {
                tasks.Add(FetchDataForCountryAsync(countryCode));
            }

            // Wait for all tasks/calls to the World Bank API to finish
            List<EmissionsIndicatorData>[] results = await Task.WhenAll(tasks);

            // Convert the results to a list of WorldBankEmissionsResponse objects and return
            List<WorldBankEmissionsResponse> emissionsResponseList = new List<WorldBankEmissionsResponse>();
            foreach (List<EmissionsIndicatorData> indicatorDataList in results)
            {
                emissionsResponseList.Add(new WorldBankEmissionsResponse(indicatorDataList));
            }
            return emissionsResponseList;
        }

        public async Task<List<EmissionsIndicatorData>> FetchDataForCountryAsync(string countryCode)
        {
            try
            {
                // Construct the current url for the next country to fetch data for
                string url = $"https://api.worldbank.org/v2/country/{countryCode}/indicator/EN.GHG.ALL.MT.CE.AR5?format=json&per_page=100";

                HttpResponseMessage response = await client.GetAsync(url);
                response.EnsureSuccessStatusCode();

                string jsonResponse = await response.Content.ReadAsStringAsync();

                // Deserialize JSON, handling potential null response with a default empty list
                List<EmissionsIndicatorData> emissionsDataList = new List<EmissionsIndicatorData>();
                using (JsonDocument document = JsonDocument.Parse(jsonResponse))
                {
                    // Root of the json is an array
                    var root = document.RootElement;

                    // Second item of the root json element is the array of emissions data
                    var entries = JsonSerializer.Deserialize<List<EmissionsIndicatorData>>(root[1].GetRawText());
                    if (entries != null)
                    {
                        emissionsDataList = entries;
                    }
                }

                // Check if the response was properly deserialized and has the list has sufficient data
                if (emissionsDataList.Count > 1)
                {
                    return emissionsDataList;
                }
                else
                {
                    // If the expected data was not found, throw exception
                    throw new InvalidOperationException("Expected data was not returned by the World Bank API, data insufficient.");
                }
            }
            catch (Exception ex)
            {
                // If unexpected issue occurred, display a warning message and return null
                Console.WriteLine($"Error: {ex.Message}");
                return new List<EmissionsIndicatorData>();
            }
        }


    }
}

