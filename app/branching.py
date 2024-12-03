import psycopg2



def branching(table_name, conditions_dict):
    CONDITION_MAP = {
    "condition_01": "01",  
    "condition_02": "02",  
    }
    """
    Dynamically applies branching conditions to all records in a database table and assigns a 'branch' value.

    Args:
        table_name (str): Name of the table to process.
        conditions_dict (dict): A dictionary where keys are condition strings (e.g., "lead_score > 50") and
                                values are the corresponding branch actions ('01', '02', etc.).
    """
    host = "pg-26172c85-johnabram338-c3cf.e.aivencloud.com"
    dbname = "defaultdb"
    user = "avnadmin"
    password = "AVNS_PZlwOQ8SKHdu36tR8-R"
    port = "20784"

    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        # Check if the 'branch' column exists, if not, add it
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'branch';")
        column_exists = cursor.fetchone()

        if not column_exists:
            # Add the 'branch' column if it doesn't exist
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN branch VARCHAR(2);")
            print(f"'branch' column added to {table_name}.")
        
        # Clean the 'branch' column (remove existing values)
        cursor.execute(f"UPDATE {table_name} SET branch = NULL;")
        print(f"Existing 'branch' values cleared in {table_name}.")

        # Apply conditions directly via SQL query for all records
        for condition_str, branch_code in conditions_dict.items():
            # Update branch column based on condition directly in the SQL query
            cursor.execute(f"UPDATE {table_name} SET branch = %s WHERE {condition_str};", (branch_code,))
            print(f"Branch '{branch_code}' assigned to records where condition '{condition_str}' is met.")

        # Commit the updates to the database
        conn.commit()
        print(f"Branching based on conditions completed successfully for table '{table_name}'.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")


# Example usage
conditions_dict = {
    "lead_score > 50": "01",  
    "lead_score < 30": "02",  
}

branching("enriched_cold_leads", conditions_dict)
