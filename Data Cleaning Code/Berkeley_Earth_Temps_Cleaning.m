% --- Clean GlobalTemperatures.csv ---
fprintf('Cleaning GlobalTemperatures.csv...\n');

% Set options to read the date column 'dt' as a string
optsTemp = detectImportOptions('GlobalTemperatures.csv');
optsTemp = setvartype(optsTemp, 'dt', 'string');

optsTemp = setvartype(optsTemp, 'LandAndOceanAverageTemperature', 'double');
optsTemp = setvartype(optsTemp, 'LandAndOceanAverageTemperatureUncertainty', 'double');

% Read the table
T_Temp = readtable('GlobalTemperatures.csv', optsTemp);

% Pre-allocate a datetime column for efficiency
numRows = height(T_Temp);
dateColumn = NaT(numRows, 1); % NaT is "Not-a-Time"

% Find rows that use the 'M/d/yyyy' format (containing '/')
isSlashFormat = contains(T_Temp.dt, '/');

% Convert dates with 'yyyy-MM-dd' format
dateColumn(~isSlashFormat) = datetime(T_Temp.dt(~isSlashFormat), 'InputFormat', 'yyyy-MM-dd');

% Convert dates with 'M/d/yyyy' format
% Note: Using 'M/d/yyyy' is flexible for '1/1/1900' or '12/1/1900' etc.
dateColumn(isSlashFormat) = datetime(T_Temp.dt(isSlashFormat), 'InputFormat', 'M/d/yyyy');

% Replace the old string column with the new, correctly formatted datetime column
T_Temp.dt = dateColumn;

% 1. Filter data from 1900 and after
T_Temp_Clean = T_Temp(T_Temp.dt >= datetime('1900-01-01'), :);

% 2. Keep the date, temperature, and uncertainty columns
T_Temp_Clean = T_Temp_Clean(:, {'dt', 'LandAndOceanAverageTemperature', 'LandAndOceanAverageTemperatureUncertainty'});

% 3. Convert Temperature to Fahrenheit
% F = (C * 9/5) + 32
T_Temp_Clean.LandAndOceanAverageTemperature = (T_Temp_Clean.LandAndOceanAverageTemperature * (9/5)) + 32;

% 4. Rename the column to reflect the new unit
T_Temp_Clean = renamevars(T_Temp_Clean, 'LandAndOceanAverageTemperature', 'TemperatureFahrenheit');

% 5. Remove rows with missing (empty or NaN) data in any selected column
T_Temp_Clean = rmmissing(T_Temp_Clean);

% 6. Save the cleaned table to a new CSV file
writetable(T_Temp_Clean, 'cleaned_GlobalTemperatures.csv');

fprintf('Finished cleaning temperatures. Converted to Fahrenheit and included uncertainty. Saved to cleaned_GlobalTemperatures.csv\n');
