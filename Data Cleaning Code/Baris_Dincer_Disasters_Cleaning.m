% --- Clean "1900_2021_DISASTERS.xlsx - emdat data.csv" ---

fprintf('Cleaning 1900_2021_DISASTERS.xlsx - emdat data.csv...\n');

% --- Load Data ---
opts = detectImportOptions('1900_2021_DISASTERS.xlsx - emdat data.csv');

% 1. Define the variables we want using their MATLAB-normalized names
% (Original names like 'Start Year' are converted to 'StartYear')
selectedVars = {'StartYear', 'StartMonth', 'StartDay', 'DisasterType', 'DisasterSubtype', 'DisasterSubsubtype', 'Region'};

% 2. Select ONLY those variables
opts.SelectedVariableNames = selectedVars;

% 3. Set the types for these selected variables
opts = setvartype(opts, 'StartYear', 'double');
opts = setvartype(opts, 'StartMonth', 'double');
opts = setvartype(opts, 'StartDay', 'double');
opts = setvartype(opts, 'DisasterType', 'string');
opts = setvartype(opts, 'DisasterSubtype', 'string');
opts = setvartype(opts, 'DisasterSubsubtype', 'string');
opts = setvartype(opts, 'Region', 'string');

% Read the table with these options
T = readtable('1900_2021_DISASTERS.xlsx - emdat data.csv', opts);

% 4. Filter data from 1900 and after
T_Clean = T(T.StartYear >= 1900, :);

% 5. Handle missing months or days by setting them to 1
T_Clean.StartMonth(isnan(T_Clean.StartMonth)) = 1;
T_Clean.StartDay(isnan(T_Clean.StartDay)) = 1;

% 6. Create the EventDate column
EventDate = datetime(T_Clean.StartYear, T_Clean.StartMonth, T_Clean.StartDay);

% 7. Create the final table with only the requested columns
T_Final = table(EventDate, ...
                T_Clean.Region, ...
                T_Clean.DisasterType, ...
                T_Clean.DisasterSubtype, ...
                T_Clean.DisasterSubsubtype);

% 8. Remove any rows with invalid/empty entries in any column
T_Final = rmmissing(T_Final);

% 9. Save the cleaned table to a new CSV file
writetable(T_Final, 'cleaned_Emdat_Disasters_Detailed.csv');

fprintf('Finished cleaning. Saved to cleaned_Emdat_Disasters_Detailed.csv\n');