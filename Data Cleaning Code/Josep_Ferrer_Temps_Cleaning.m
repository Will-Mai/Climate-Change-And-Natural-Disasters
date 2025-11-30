% --- Clean "ddbb_surface_temperature_countries.csv" ---

fprintf('Cleaning ddbb_surface_temperature_countries.csv...\n');

% --- Load Data ---
opts = detectImportOptions('ddbb_surface_temperature_countries.csv');

% Ensure correct types are read
opts = setvartype(opts, 'Years', 'double');
opts = setvartype(opts, 'Month', 'double');
opts = setvartype(opts, 'Country', 'string'); % Read Country as string
opts = setvartype(opts, {'Temperature', 'Anomaly'}, 'double');

T = readtable('ddbb_surface_temperature_countries.csv', opts);

% 1. Filter data from 1900 and after
T_Clean = T(T.Years >= 1900, :);

% 2. NEW: Convert Temperature from Celsius to Fahrenheit
% F = (C * 9/5) + 32
T_Clean.Temperature = (T_Clean.Temperature * (9/5)) + 32;

% 3. Create a single Date column
% We use '1' for the day since this is monthly data
EventDate = datetime(T_Clean.Years, T_Clean.Month, 1);

% 4. Select the desired columns
% We keep the new date, country, and the converted temperature
T_Final = T_Clean(:, {'Country', 'Temperature', 'Anomaly'});
T_Final.EventDate = EventDate;

% Rename the 'Temperature' column to reflect the new unit
T_Final = renamevars(T_Final, 'Temperature', 'TemperatureFahrenheit');

% Reorder columns to be more logical
T_Final = T_Final(:, {'EventDate', 'Country', 'TemperatureFahrenheit', 'Anomaly'});

% 5. Remove any invalid entries
% This will remove rows with missing dates, country names, or temperatures
T_Final = rmmissing(T_Final);

% 6. Save the cleaned table to a new CSV file
writetable(T_Final, 'cleaned_surface_temperature_countries.csv');

fprintf('Finished cleaning. Temperatures converted to Fahrenheit. Saved to cleaned_surface_temperature_countries.csv\n');