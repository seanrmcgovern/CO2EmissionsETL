namespace CO2EmissionsETL
{
	public class WorldBankEmissionsResponse
	{
        public List<EmissionsIndicatorData> Data { get; set; } = new List<EmissionsIndicatorData>();

        public WorldBankEmissionsResponse(List<EmissionsIndicatorData> data)
        {
            Data = data;
        }
    }
}

