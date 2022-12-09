import sqlite3
import pandas as pd

# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory
db_connect = sqlite3.connect('csc423-proj3.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

# String variable for passing queries to cursor
staff_query = """
    CREATE TABLE IF NOT EXISTS Staff (
    staffNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(32) NOT NULL,
    phone INT NOT NULL,
    DOB DATE,
    position VARCHAR(32),
    salary INT,
    clinicNo VARCHAR(10),
    FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo) ON DELETE CASCADE
    );
    """
 
clinic_query = """
    CREATE TABLE IF NOT EXISTS Clinic ( 
    clinicNo VARCHAR(10) NOT NULL PRIMARY KEY, 
    name VARCHAR(32) NOT NULL, 
    address VARCHAR(32) NOT NULL UNIQUE, 
    phone INT NOT NULL UNIQUE, 
    managerStaffNo VARCHAR(10),
    FOREIGN KEY (managerStaffNo) REFERENCES Staff(staffNo) ON DELETE SET NULL
    );
    """

owner_query = """
    CREATE TABLE IF NOT EXISTS Owner (
    ownerNo VARCHAR(10) NOT NULL PRIMARY KEY,
    name VARCHAR(32) NOT NULL,
    address VARCHAR(32),
    phone INT NOT NULL
    );
    """

pet_query = """
    CREATE TABLE IF NOT EXISTS Pet ( 
    petNo VARCHAR(10) NOT NULL PRIMARY KEY, 
    name VARCHAR(32) NOT NULL, 
    DOB DATE, 
    species VARCHAR(32) NOT NULL, 
    breed VARCHAR(32) NOT NULL, 
    color VARCHAR(32), 
    ownerNo VARCHAR(10) ,
    clinicNo VARCHAR(10),
    FOREIGN KEY (ownerNo) REFERENCES Owner(ownerNo) ON DELETE CASCADE,
    FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo) ON DELETE CASCADE
    );
    """

examination_query = """
    CREATE TABLE IF NOT EXISTS Examination (
    examNo VARCHAR(10) NOT NULL PRIMARY KEY,
    complaint VARCHAR(100) NOT NULL,
    description VARCHAR(100) NOT NULL,
    dateSeen DATE NOT NULL,
    actionsTaken VARCHAR(100),
    staffNo VARCHAR(10),
    petNo VARCHAR(10),
    FOREIGN KEY (staffNo) REFERENCES Staff(staffNo) ON DELETE CASCADE, 
    FOREIGN KEY (petNo) REFERENCES Pet(petNo) ON DELETE CASCADE
    );
    """

# Execute query, the result is stored in cursor
cursor.execute(staff_query)
cursor.execute(clinic_query)
cursor.execute(owner_query)
cursor.execute(pet_query)
cursor.execute(examination_query)

# Insert row into table
staff_insert_query = """
INSERT OR IGNORE INTO Staff
VALUES
('AL123', 'Alan', 8997799443, '14-NOV-82', 'Manager', 80000, 'WEL'),
('MA173', 'Mary', 8997731443, '10-AUG-85', 'Nurse', 70000, 'GEN'),
('JO314', 'John', 7867799123, '30-JAN-90', 'Assistant', 50000, 'SAJ'),
('HA987', 'Hailey', 8127790043, '25-MAY-92', 'Manager', 80000, 'WEL'),
('KY578', 'Kylian', 3057879443, '05-DEC-89', 'Doctor', 80000, 'CAL');
"""
clinic_insert_query = """
INSERT OR IGNORE INTO Clinic
VALUES
('GEN', 'General Examination National', '4820 Congress Ave', 6473483832, 'AL123'),
('MAJ', 'Mary-Jane', '1246 Ibis Ave', 6474644832, 'MA173'),
('WEL', 'Wellington', '8801 Military Trail', 5613483832, 'JO314'),
('SAJ', 'San Jose', '8990 Albenga Ave', 4933412232, 'KY578'),
('CAL', 'California', '7890 Stanford Ave', 1233483832, 'HA987');
"""
owner_insert_query = """
INSERT OR IGNORE INTO Owner
VALUES
('PA395', 'Pablo', '5776 Coconut Drive', 7857787909),
('MI305', 'Mike', '7890 5th Str', 7850799109),
('RA792', 'Rachel', '1102 Waterloo Ave', 1273784109),
('CH791', 'Christian', '1230 5th Str', 7400434901),
('IS341', 'Isabel', '1211 Walsh Ave', 3057038909);
"""
pet_insert_query = """
INSERT OR IGNORE INTO Pet
VALUES
('BO231', 'Bone', '12-OCT-2015', 'Dog', 'Golden Retriever', 'Gold', 'IS341', 'MAJ'),
('OR211', 'Orange', '30-JUL-2022', 'Fish', 'Goldfish', 'Orange', 'CH791', 'WEL'),
('NU731', 'Nugget', '12-AUG-2015', 'Chicken', 'Roster', 'Brown', 'PA395', 'CAL'),
('NI011', 'Nick', '16-MAY-2015', 'Dog', 'Poodle', 'Grey', 'IS341', 'MAJ'),
('EM231', 'Emmaaaa', '12-FEB-2021', 'Cat', 'Tabby', 'Brown', 'MI305', 'SAJ'),
('CR842', 'Cris', '10-SEP-2022', 'Turtle', 'Red-eared', 'Brown-red', 'RA792', 'GEN');
"""
examination_insert_query = """
INSERT OR IGNORE INTO Examination
VALUES
('DD123', 'Broken leg', 'Could not walk', '30-NOV-2022', 'Surgery', 'KY578', 'BO231'),
('ST903', 'Stomachache', 'Vomiting', '15-NOV-2022', 'Prescribe medicine', 'MA173', 'EM231'),
('LF523', 'Lost fin', 'Could not swim', '20-AUG-2022', 'Surgery', 'HA987', 'OR211'),
('SC383', 'Seasonal cold', 'Coughing', '09-SEP-2022', 'Prescribed Medicine', 'JO314', 'NI011'),
('BL023', 'Broken leg', 'Bleeding', '26-OCT-2022', 'Surgery', 'KY578', 'CR842');
"""

cursor.execute(staff_insert_query)
cursor.execute(clinic_insert_query)
cursor.execute(owner_insert_query)
cursor.execute(pet_insert_query)
cursor.execute(examination_insert_query)

# Select data
query1 = """
    SELECT *
    FROM Staff
    """
query2 = """
    SELECT *
    FROM Clinic
    """
query3 = """
    SELECT *
    FROM Owner
    """
query4 = """
    SELECT *
    FROM Pet
    """
query5 = """
    SELECT *
    FROM Examination
    """
queries = [query1, query2, query3, query4, query5]

print("DATABASE")

for query in queries:
    cursor.execute(query)
    # Extract column names from cursor
    column_names = [row[0] for row in cursor.description]
    # Fetch data and load into a pandas dataframe
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    print()
    # Examine dataframe
    print("---------------------------------------------------------------")
    print(df)
    print(df.columns)


# Example to extract a specific column
# print(df['name'])

# Develop 5 SQL queries using embedded SQL
prompt1 = "List all the pets whose owner's name is Isabel"
query1 = """
    SELECT p.*
    FROM Pet p, Owner o
    WHERE p.ownerNo = o.ownerNo AND o.name = 'Isabel'
    """

prompt2 = 'List the name and DOB of all the staff whose salary is greater than 70000'
query2 = """
    SELECT name, DOB, salary
    FROM Staff
    WHERE salary > 70000
    """

prompt3 = 'List the name and breed of pets and their owner\'s name whose complaint is broken leg'
query3 = """
SELECT p.name, p.breed, o.name
FROM Pet p, Owner o, Examination e
WHERE p.ownerNo = o.ownerNo AND e.petNo = p.petNo AND e.complaint = 'Broken leg'
"""

prompt4 = 'List the number of pets at each clinic'
query4 = """
    SELECT c.clinicNo, c.name, COUNT(petNo) AS num_pets
    FROM Clinic c, Pet p
    WHERE p.clinicNo = c.clinicNo
    GROUP BY c.clinicNo
    """

prompt5 = 'List all examinations that requires surgery'
query5 = """
    SELECT *
    FROM Examination
    WHERE actionsTaken = 'Surgery'
    """

queries = [(query1,prompt1), (query2,prompt2), (query3,prompt3), (query4,prompt4), (query5,prompt5)]
print()
print("--------------------------------------------------------------------------------")
print("QUERIES")

for (query,prompt) in queries:
    cursor.execute(query)
    column_names = [row[0] for row in cursor.description]
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    print()
    print("---------------------------------------------------------------")
    print(prompt)
    print(df)

print()
# Commit any changes to the database
db_connect.commit()

# Close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_connect.close()
