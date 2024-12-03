import psycopg2
import random

def enrichment(table_name):
    host = "pg-26172c85-johnabram338-c3cf.e.aivencloud.com"
    dbname = "defaultdb"
    user = "avnadmin"
    password = "AVNS_PZlwOQ8SKHdu36tR8-R"
    port = "20784"

    industry_pool = ["Technology", "Healthcare", "Finance"]
    state_pool = ["California", "Texas", "New York"]

    try:
        # Establish database connection
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        # Fetch data from the specified table
        cursor.execute(f"SELECT * FROM {table_name};")
        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # Get column names

        # Enrich records with random industry and state
        enriched_records = [
            record + (random.choice(industry_pool), random.choice(state_pool))
            for record in records
        ]

        # Add new columns for industry and state
        new_columns = columns + ["industry", "state"]

        # Create the name for the new table
        enriched_table_name = f"enriched_{table_name}"

        # Drop and recreate the enriched table
        cursor.execute(f"DROP TABLE IF EXISTS {enriched_table_name};")
        conn.commit()

        create_table_query = f"""
        CREATE TABLE {enriched_table_name} (
            {', '.join([f"{col} TEXT" for col in new_columns])}
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert enriched data into the new table
        insert_query = f"""
        INSERT INTO {enriched_table_name} ({', '.join(new_columns)})
        VALUES ({', '.join(['%s'] * len(new_columns))});
        """
        cursor.executemany(insert_query, enriched_records)
        conn.commit()

        print(f"Enriched data successfully saved in '{enriched_table_name}'.")

        
        cursor.close()
        conn.close()
        return enriched_table_name

    except Exception as e:
        print(f"Error: {e}")
# enrichment("cold_lead_results")