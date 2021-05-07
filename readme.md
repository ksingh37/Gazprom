Setup

1. Install requirements.txt file for all dependencies by using below command
    pip install -r requirements.txt

2. Used PostgreSQL database to store consu and header records in two different
   tables.

3. Change the path and destination on line no
    19 and 22 of meterReading.py file as per your location.

3.  Update the username and password in meterReading.py on line 82 with your database credentials.
    engine = sqlalchemy.create_engine('postgresql://USERNAME:PASSWORD@123@localhost/imdb_DB1')

4.  Update same in meterReading.py file as well.

6.  Run the following commands for main and additional requirements on your terminal:
      cd Gazprom (Give path of project)
      python meterReading.py
      python additional.py
