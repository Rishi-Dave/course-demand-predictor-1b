import pandas as pd
from sqlalchemy import create_engine
import os
import ast # For safe_literal_eval

# --- Configuration ---
DATABASE_FILE = 'class_space.db'
engine = create_engine(f'sqlite:///{DATABASE_FILE}')
csv_folder_path = 'csv_data' # Make sure to update this path

# --- Helper Functions (from your original code) ---
def safe_literal_eval(val):
    try:
        if pd.isna(val) or val == '':
            return None
        return ast.literal_eval(val)
    except (ValueError, SyntaxError) as e:
        # If it's not a literal, return as string if not NaN, otherwise None
        return str(val) if not pd.isna(val) else None

def extract_meeting_details(meetings_list):
    """
    Extracts flattened meeting details from a list of meeting dictionaries.
    """
    if not meetings_list:
        return []
    processed_meetings = []
    for m in meetings_list:
        if isinstance(m, dict) and "meetingTime" in m and isinstance(m["meetingTime"], dict):
            meeting_time = m["meetingTime"]
            processed_meetings.append({
                "meetingBeginTime": meeting_time.get("beginTime"),
                "meetingEndTime": meeting_time.get("endTime"),
                "meetingBuildingDescription": meeting_time.get("buildingDescription"),
                "meetingRoom": meeting_time.get("room"),
                "meetingMonday": meeting_time.get("monday", False),
                "meetingTuesday": meeting_time.get("tuesday", False),
                "meetingWednesday": meeting_time.get("wednesday", False),
                "meetingThursday": meeting_time.get("thursday", False),
                "meetingFriday": meeting_time.get("friday", False),
                "meetingSaturday": meeting_time.get("saturday", False),
                "meetingSunday": meeting_time.get("sunday", False),
                "meetingStartDate": meeting_time.get("startDate"),
                "meetingEndDate": meeting_time.get("endDate"),
                "meetingTypeDescription": meeting_time.get("meetingTypeDescription")
            })
    return processed_meetings

# --- Main Data Ingestion Loop ---
all_csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
all_processed_dfs = [] # To store all processed dataframes before one final to_sql

selected_columns = [
    'subjectCourse',
    'courseDisplay',
    'courseNumber',
    'subject',
    'courseTitle',
    'creditHours',
    'faculty', # To be flattened
    'instructionalMethodDescription',
    'isSectionLinked',
    'maximumEnrollment',
    'enrollment',
    'seatsAvailable',
    'meetingsFaculty', # To be exploded and flattened
    'term',
    'termDesc',
    'waitAvailable',
    'waitCapacity',
    'waitCount',
    # 'prerequisites' - Disregarded as per your request
]

print("Starting data ingestion and transformation...")

for csv_file in sorted(all_csv_files):
    file_path = os.path.join(csv_folder_path, csv_file)
    print(f"Processing: {csv_file}")

    try:
        df = pd.read_csv(file_path)

        # --- Extract Year and Quarter from filename ---
        # Adjust this parsing based on your actual filename format
        # Example: '201810.csv' -> year=2018, quarter=1
        try:
            year = int(csv_file[:4])
            quarter_code = csv_file[4:6]
            quarter_map = {'10': 1, '20': 2, '30': 3, '40': 4}
            quarter = quarter_map.get(quarter_code)

            if quarter is None:
                print(f"Warning: Could not determine quarter from {csv_file}. Skipping file.")
                continue

            df['year'] = year
            df['quarter'] = quarter

        except ValueError:
            print(f"Could not parse year/quarter from filename: {csv_file}. Ensure consistent naming.")
            continue # Skip to the next file

        # --- Apply your original transformation logic ---
        # 1. Filter selected columns
        df_filtered = df[selected_columns].copy()

        # 2. Apply safe_literal_eval to 'faculty' and 'meetingsFaculty'
        for col in ['faculty', 'meetingsFaculty']:
            df_filtered[col] = df_filtered[col].apply(safe_literal_eval)

        # 3. Flatten 'faculty' details
        df_filtered['facultyDisplayName'] = df_filtered['faculty'].apply(
            lambda x: x[0].get("displayName") if isinstance(x, list) and x and isinstance(x[0], dict) else None
        )
        df_filtered['facultyEmailAddress'] = df_filtered['faculty'].apply(
            lambda x: x[0].get("emailAddress") if isinstance(x, list) and x and isinstance(x[0], dict) else None
        )
        df_filtered = df_filtered.drop(columns=['faculty'])

        # 4. Extract and explode 'meetingsFaculty' details
        df_filtered['meetingsFaculty'] = df_filtered['meetingsFaculty'].apply(extract_meeting_details)

        # Handle creditHours fillna before explode if it affects type
        df_filtered['creditHours'] = df_filtered['creditHours'].fillna("None")

        df_exploded = df_filtered.explode('meetingsFaculty').reset_index(drop=True)

        # Normalize the exploded meeting details
        df_exploded_final = pd.json_normalize(df_exploded['meetingsFaculty']).add_prefix('meeting_')

        # Drop the original nested 'meetingsFaculty' column after normalization
        if 'meetingsFaculty' in df_exploded.columns:
            df_exploded = df_exploded.drop(columns=['meetingsFaculty'])

        # Concatenate the flattened meeting details back to the main DataFrame
        df_final_processed = pd.concat([df_exploded, df_exploded_final], axis=1)

        # Replace pandas NaN with None for better SQL compatibility
        df_final_processed = df_final_processed.where(pd.notnull(df_final_processed), None)

        # Add year and quarter columns (extracted earlier) to the final DataFrame
        df_final_processed['year'] = year
        df_final_processed['quarter'] = quarter

        all_processed_dfs.append(df_final_processed)

    except pd.errors.EmptyDataError:
        print(f"Warning: {csv_file} is empty. Skipping.")
    except KeyError as e:
        print(f"Error: Missing expected column in {csv_file}: {e}. Please check CSV header.")
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")

# --- Combine all processed DataFrames and load into SQLite ---
if all_processed_dfs:
    final_combined_df = pd.concat(all_processed_dfs, ignore_index=True)

    # Define the table name
    table_name = 'course_data_flattened'

    # Create the table schema (optional, to ensure types or constraints)
    # You might want to infer dtypes first or define them explicitly
    # Example for creating table:
    # With so many dynamic columns from json_normalize, it's easier to let pandas infer
    # but for production, explicit schema definition is safer.
    # We'll rely on pandas to_sql to create the table if it doesn't exist.

    try:
        final_combined_df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"\nAll processed data loaded into '{table_name}' table in '{DATABASE_FILE}'.")
        print(f"Total rows inserted: {len(final_combined_df)}")
    except Exception as e:
        print(f"Error loading combined data to SQL: {e}")
else:
    print("\nNo data processed to load into the database.")

# Dispose of the engine connection
engine.dispose()
print("Database ingestion complete.")