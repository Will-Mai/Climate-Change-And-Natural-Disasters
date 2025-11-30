% --- Clean "Updated Data 1.csv" ---

fprintf('Cleaning Updated Data 1.csv...\n');

% --- Load Data ---
opts = detectImportOptions('Updated Data 1.csv');

% Read 'Title' and 'Category_title' as strings to catch empty text
% Read 'Date' as a string first to handle its format
opts = setvartype(opts, 'Title', 'string');
opts = setvartype(opts, 'Category_title', 'string');
opts = setvartype(opts, 'Date', 'string');

% Select only the columns we need to read
opts.SelectedVariableNames = {'Title', 'Category_title', 'Date'};

T = readtable('Updated Data 1.csv', opts);

% 1. Convert the 'Date' string column to datetime objects
% The format appears to be 'yyyy-MM-dd'
T.Date = datetime(T.Date, 'InputFormat', 'yyyy-MM-dd');

% 2. Keep the desired columns (already done by 'SelectedVariableNames')
% We just rename 'Category_title' to 'DisasterType' for consistency
T = renamevars(T, 'Category_title', 'DisasterType');

% 3. Remove any invalid or empty entries
% This removes rows where Title/DisasterType is missing
% or where Date is NaT (Not-a-Time)
T_Final = rmmissing(T);

% 4. Save the cleaned table to a new CSV file
writetable(T_Final, 'cleaned_NASA_Disasters.csv');

fprintf('Finished cleaning. Saved to cleaned_NASA_Disasters.csv\n');