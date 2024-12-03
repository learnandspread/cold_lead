import psycopg2

def condition_02(email_template):
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

    # Fetch required attributes where branch = '02'
    cursor.execute(
        """
        SELECT name, email, industry
        FROM enriched_cold_leads
        WHERE branch = '02';
        """
    )

    # Fetch all records with branch = '02'
    leads = cursor.fetchall()

    # Iterate over the results and create and send an email for each lead
    for lead in leads:
        name, email, industry = lead

        # Customize the email message using the provided email template
        msg = email_template.format(name=name, email=email, industry=industry)

        # Simulate sending the email (replace with an actual email-sending service)
        print(f"Sending email to {email}:\n{msg}")
        print("-------------------------------")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# # Example email template
# email_template = """
# Subject: Exclusive Offer Just for You!

# Hello {name},

# We hope this message finds you well! As a valued potential customer in the {industry} industry, we're excited to offer you an exclusive opportunity.

# For a limited time, enjoy a **20% discount** on your first purchase. Let us help you take your business to the next level.

# To claim your offer, simply reply to this email or contact us at your convenience.

# We look forward to hearing from you!

# Best regards,  
# The Team
# """

# # Example usage
# condition_02(email_template)

