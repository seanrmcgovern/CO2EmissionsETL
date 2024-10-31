
namespace CO2EmissionsETL;
class Program
{

    static async Task Main(string[] args)
    {
        bool success = false;
        Console.WriteLine("Fetching emissions data from the World Bank API.");
        var client = new WorldBankApiClient();
        List<WorldBankEmissionsResponse> emissionsData = await client.FetchEmissionsDataForCountriesAsync();

        // validate the emissions data returned
        bool validApiResponse = true;
        validApiResponse = emissionsData.Count > 0 && !emissionsData.Any(emissionsResponse => emissionsResponse.Data.Count == 0);

        // if valid data was fetched, proceed to save the data to the database
        if (validApiResponse)
        {
            Console.WriteLine("Successfully fetched emissions data.");
            Console.WriteLine("Proceeding to save returned emissions data to the SQLite database.");

            // Save the emissions data to the database
            bool dbInitializationValid = EmissionsDataManager.InitializeDatabase();
            if (dbInitializationValid)
            {
                success = EmissionsDataManager.InsertEmissionsData(emissionsData);
            }
        }
        else
        {
            Console.WriteLine("Invalid emissions data was returned.");
        }
        Console.WriteLine($"Emissions data was {(success ? string.Empty : "not ")}successfully saved.");
    }

}
