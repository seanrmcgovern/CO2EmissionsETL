using System.Data.SQLite;

namespace CO2EmissionsETL
{
	public class EmissionsDataManager
	{
        private static readonly string connectionString = "Data Source=worldBankEmissions.db;Version=3;";

        /// <summary>
        /// Creates the Countries, EmissionsIndicators, and EmissionsData tables, if necessary
        /// </summary>
        public static bool InitializeDatabase()
        {
            try
            {
                using (var connection = new SQLiteConnection(connectionString))
                {
                    connection.Open();
                    string createTablesSql = @"
                    CREATE TABLE IF NOT EXISTS Countries (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        IsoCode TEXT UNIQUE NOT NULL,
                        Name TEXT NOT NULL,
                        Abbreviation TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS EmissionsIndicators (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Desc TEXT NOT NULL,
                        Code TEXT UNIQUE NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS EmissionsData (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        CountryId INTEGER,
                        IndicatorId INTEGER,
                        Year INTEGER,
                        Status TEXT,
                        Unit TEXT,
                        EmissionsValue NUMERIC(8, 4),
                        Date DATETIME,
                        Version INTEGER,
                        FOREIGN KEY (CountryId) REFERENCES Countries(Id),
                        FOREIGN KEY (IndicatorId) REFERENCES Indicators(Id)
                    );";
                    using (var command = new SQLiteCommand(createTablesSql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"InitializeDatabase(): Exception creating database tables: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Processes the emissions data, saving the country and indicator data if they are new,
        /// incrementing the version of the data pull, and saving to the EmissionsData table
        /// </summary>
        /// <param name="emissionsResponseList"></param>
        /// <returns>True if data was succesfully saved to db, false if not</returns>
        public static bool InsertEmissionsData(List<WorldBankEmissionsResponse> emissionsResponseList)
        {
            try
            {
                bool success = false;
                using (var connection = new SQLiteConnection(connectionString))
                {
                    connection.Open();

                    // Get the version of this data pull
                    int version = GetNextVersionOfData(connection);

                    foreach (var response in emissionsResponseList)
                    {
                        foreach (var (emissionsData, index) in response.Data.Select((item, index) => (item, index)))
                        {
                            // Insert the country and indicator data on the first iteration of this loop
                            // This data should be the same for each item in this list
                            if (index == 0)
                            {
                                // Insert country data
                                SaveCountry(emissionsData, connection);

                                // Insert indicator data
                                SaveIndicator(emissionsData, connection);
                            }

                            // Get primary keys of the matching country and indicator to use as the foreign keys when saving the EmissionsData
                            int countryKey = GetCountryByIsoCode(emissionsData.Countryiso3code, connection);
                            int indicatorKey = GetIndicatorByCode(emissionsData.Indicator.Id, connection);

                            // Insert emissions data
                            success = SaveEmissionsData(countryKey, indicatorKey, version, emissionsData, connection);
                        }
                    }
                }
                return success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"InsertEmissionsData(): Exception inserting emissions data: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Save the emissions data to the EmissionsData table
        /// </summary>
        /// <param name="connection"></param>
        /// <returns>True if data was succesfully saved to db, false if not</returns>
        public static bool SaveEmissionsData(int countryKey, int indicatorKey, int version, EmissionsIndicatorData emissionsData, SQLiteConnection connection)
        {
            try
            {
                int rowsInserted = 0;
                string insertEmissionsDataSql = "INSERT INTO EmissionsData (CountryId, IndicatorId, Year, Status, Unit, EmissionsValue, Date, Version) VALUES (@CountryId, @IndicatorId, @Year, @Status, @Unit, @EmissionsValue, @Date, @Version)";
                using (var emissionsCommand = new SQLiteCommand(insertEmissionsDataSql, connection))
                {
                    emissionsCommand.Parameters.AddWithValue("@CountryId", countryKey);
                    emissionsCommand.Parameters.AddWithValue("@IndicatorId", indicatorKey);
                    emissionsCommand.Parameters.AddWithValue("@Year", emissionsData.Date);
                    emissionsCommand.Parameters.AddWithValue("@Status", emissionsData.ObsStatus);
                    emissionsCommand.Parameters.AddWithValue("@Unit", emissionsData.Unit);
                    emissionsCommand.Parameters.AddWithValue("@EmissionsValue", emissionsData.Value);
                    emissionsCommand.Parameters.AddWithValue("@Date", DateTime.Now);
                    emissionsCommand.Parameters.AddWithValue("@Version", version);
                    rowsInserted = emissionsCommand.ExecuteNonQuery();
                }
                return rowsInserted > 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SaveEmissionsData(): Exception saving emissions data: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Save the country data to the Countries table, if the country does not yet exist
        /// </summary>
        /// <param name="emissionsData"></param>
        /// <param name="connection"></param>
        /// <returns>True if data was succesfully saved to db or if the country already existed, false if exception occurred</returns>
        public static bool SaveCountry(EmissionsIndicatorData emissionsData, SQLiteConnection connection)
        {
            try
            {
                string insertCountrySql = "INSERT OR IGNORE INTO Countries (IsoCode, Name, Abbreviation) VALUES (@IsoCode, @Name, @Abbreviation)";
                using (var countryCommand = new SQLiteCommand(insertCountrySql, connection))
                {
                    countryCommand.Parameters.AddWithValue("@IsoCode", emissionsData.Countryiso3code);
                    countryCommand.Parameters.AddWithValue("@Name", emissionsData.Country.Value);
                    countryCommand.Parameters.AddWithValue("@Abbreviation", emissionsData.Country.Id);
                    countryCommand.ExecuteNonQuery();
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SaveCountry(): Exception saving country data: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Save the indicator data to the EmissionsIndicators table, if the indicator does not yet exist
        /// </summary>
        /// <param name="emissionsData"></param>
        /// <param name="connection"></param>
        /// <returns>True if data was succesfully saved to db or if the indicator already existed, false if exception occurred</returns>
        public static bool SaveIndicator(EmissionsIndicatorData emissionsData, SQLiteConnection connection)
        {
            try
            {
                string insertIndicatorSql = "INSERT OR IGNORE INTO EmissionsIndicators (Desc, Code) VALUES (@Desc, @Code)";
                using (var indicatorCommand = new SQLiteCommand(insertIndicatorSql, connection))
                {
                    indicatorCommand.Parameters.AddWithValue("@Desc", emissionsData.Indicator.Value);
                    indicatorCommand.Parameters.AddWithValue("@Code", emissionsData.Indicator.Id);
                    indicatorCommand.ExecuteNonQuery();
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SaveIndicator(): Exception saving indicator data: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Fetch the id of a country by a unique isoCode
        /// </summary>
        /// <param name="isoCode"></param>
        /// <param name="connection"></param>
        /// <returns>The primary key/Id of the country with the matching isoCode, or if not found a default value of 0</returns>
        public static int GetCountryByIsoCode(string isoCode, SQLiteConnection connection)
        {
            string selectCountrySql = "SELECT * FROM Countries WHERE IsoCode = @IsoCode";
            using (var command = new SQLiteCommand(selectCountrySql, connection))
            {
                command.Parameters.AddWithValue("@IsoCode", isoCode);

                using (var dataReader = command.ExecuteReader())
                {
                    if (dataReader.Read())
                    {
                        return dataReader.GetInt32(dataReader.GetOrdinal("Id"));
                    }
                    else
                    {
                        return 0;
                    }
                }
            }
        }

        /// <summary>
        /// Fetch the id of an emissions indicator by a unique code
        /// </summary>
        /// <param name="code"></param>
        /// <param name="connection"></param>
        /// <returns>The primary key/Id of the emissions indicator with the matching code</returns>
        public static int GetIndicatorByCode(string code, SQLiteConnection connection)
        {
            string selectIndicatorSql = "SELECT * FROM EmissionsIndicators WHERE Code = @Code";
            using (var command = new SQLiteCommand(selectIndicatorSql, connection))
            {
                command.Parameters.AddWithValue("@Code", code);

                using (var dataReader = command.ExecuteReader())
                {
                    if (dataReader.Read())
                    {
                        return dataReader.GetInt32(dataReader.GetOrdinal("Id"));
                    }
                    else
                    {
                        return 0;
                    }
                }
            }
        }

        /// <summary>
        /// Get the next version of the ETL process from the World Bank API emissions data, defaulting to 1 if this is the first pull
        /// </summary>
        /// <param name="connection"></param>
        /// <returns>Integer representing the next version of the data pull to use for the Version column of the EmissionsData table</returns>
        public static int GetNextVersionOfData(SQLiteConnection connection)
        {
            // default next version to 1, to account for the first data pull
            int nextVersion = 1;
            string selectCurrentVersionSql = "SELECT MAX(Version) FROM EmissionsData";

            using (var command = new SQLiteCommand(selectCurrentVersionSql, connection))
            {
                var result = command.ExecuteScalar();

                // Check if result was found and increment the version
                if (result != DBNull.Value)
                {
                    nextVersion = Convert.ToInt32(result) + 1;
                } 
            }

            return nextVersion;
        }

    }
}

