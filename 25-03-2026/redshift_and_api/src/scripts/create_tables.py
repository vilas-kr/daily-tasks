import logging

from src.service import redshift

logging.basicConfig(level=logging.INFO)

# Create Student dimension table

create_student = '''
CREATE TABLE IF NOT EXISTS student_dim (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(100),
    email VARCHAR(100),
    age INT,
    city VARCHAR(50)
)
'''

result = redshift.execute_query(create_student)
if result:
    logging.info("Student_dim table created successfully")
else:
    logging.info("Failed to create 'student_dim' tabel")
    
    
# create Course table

create_course = '''
CREATE TABLE IF NOT EXISTS course_dim (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    duration INT
);
'''
result = redshift.execute_query(create_course)
if result:
    logging.info("course_dim table created successfully")
else:
    logging.info("Failed to create 'course_dim' tabel")
    
# Create date tabel

create_date = '''
CREATE TABLE IF NOT EXISTS date_dim (
    date_id INT PRIMARY KEY,
    full_date DATE
);
'''
result = redshift.execute_query(create_date)
if result:
    logging.info("date_dim table created successfully")
else:
    logging.info("Failed to create 'date_dim' tabel")

# Create engagement table
create_engagement = '''
CREATE TABLE IF NOT EXISTS engagement_fact (
    engagement_id INT PRIMARY KEY,
    student_id INT,
    course_id INT,
    date_id INT,
    progress_percentage DECIMAL(5,2),
)
DISTSTYLE KEY
DISTKEY(course_id)
SORTKEY(date_id);
'''
result = redshift.execute_query(create_engagement)
if result:
    logging.info("engagement_fact table created successfully")
else:
    logging.info("Failed to create 'engagement_fact' tabel")
    
    

