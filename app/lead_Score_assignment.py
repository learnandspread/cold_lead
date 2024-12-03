import psycopg2

def lead_scores(table_name, conditions):
    print(conditions)
    """
    Dynamically updates the lead scores in the given table based on specified conditions.

    Args:
        table_name (str): Name of the table to update.
        conditions (list of tuples): A list where each tuple contains a condition (str) and score adjustment (int).
                                     Example: [("CAST(total_amount_spent AS NUMERIC) > 150", 50), 
                                               ("industry = 'Finance'", 20)]
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

        # Check if the 'lead_score' column exists
        cursor.execute(f"""
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND column_name = 'lead_score';
        """)
        column_exists = cursor.fetchone()

        if column_exists:
            # Reset 'lead_score' column to default value (0)
            cursor.execute(f"UPDATE {table_name} SET lead_score = 0;")
        else:
            # Add the 'lead_score' column if it doesn't exist
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN lead_score INTEGER DEFAULT 0;")
        
        conn.commit()


        
        # Apply each condition to update the lead scores
        for condition, score in conditions:
            query = f"""
            UPDATE {table_name}
            SET lead_score = lead_score + {score}
            WHERE {condition};
            """
            cursor.execute(query)
            conn.commit()

        print(f"Lead scores updated successfully in table '{table_name}' based on the provided conditions.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

# # Example Usage
# conditions = [
#     ["CAST(total_amount_spent AS NUMERIC) > 150", 50],
#     ["industry = 'Finance'", 20],
#     ["state = 'California'", 30]
# ]
# lead_scores("enriched_cold_leads", conditions)
