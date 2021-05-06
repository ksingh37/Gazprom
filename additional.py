import psycopg2

def dataView():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="Waheguruji@123",
                                      host="localhost",
                                      port="5432",
                                      database="reading_db1")
        cursor = connection.cursor()
        print("PostgreSQL server information")

        cursor.execute("SELECT Count(DISTINCT meter_number) FROM consu_tab2;")
        record1 = cursor.fetchall()
        print("Total meter count - ", record1, "\n")

        cursor.execute("SELECT * FROM consu_tab2 WHERE meter_number = '000000002';")
        record2 = cursor.fetchall()
        print("Data for meter 0000000002", record2, "\n")

        cursor.execute("SELECT Count(DISTINCT file_number) FROM header_tab2;")
        record3 = cursor.fetchall()
        print("Total files received - ", record3, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == '__main__':

    dataView()