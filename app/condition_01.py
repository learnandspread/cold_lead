import psycopg2

def condition_01(msg_template):
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
    except Exception as e:
        print(f"Error: {e}")
        return

    cursor = conn.cursor()

    # Fetch required attributes where branch = '01'
    cursor.execute(
        """
        SELECT name, email, phone, industry
        FROM enriched_cold_leads
        WHERE branch = '01';
        """
    )

    # Fetch all records with branch = '01'
    leads = cursor.fetchall()

    # Iterate over the results and create a message for each lead using the provided template
    for lead in leads:
        name, email, phone, industry = lead

        # Format the message with the actual lead details
        msg = msg_template.format(name=name, email=email, phone=phone, industry=industry)

        # Print the formatted message (simulating sending the message)
        print("Slack to AE: " + msg)
        print("-------------------------------")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# # Example usage: Pass a custom message template
# msg_template = '''
#     {name}, is one of our most important cold lead, his email is {email}, and phone number is {phone}, belongs to {industry} industry
# '''

# # Call the function with the message template
# condition_01(msg_template)
