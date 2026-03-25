import logging
import random
import faker

from src.service import redshift

logging.basicConfig(level=logging.INFO)

fake = faker.Faker()

# insert data into student_dim table
logging.info("Inserting data into student_dim table")

truncate_student = "TRUNCATE TABLE student_dim;"
redshift.execute_query(truncate_student)

for i in range(10):
    name = fake.name()
    email = fake.email()
    city = fake.city()
    age = random.randint(10, 45)
    id = i+1
    
    insert_student = '''
    INSERT INTO student_dim 
    VALUES ({}, '{}', '{}', {}, '{}')
    '''.format(id, name, email, age, city)
    
    result = redshift.execute_query(insert_student)
    if result:
        logging.info(f"Student data inserted successfully : {id}")
    else:
        logging.info(f"Failed to insert student: {id}")
        
logging.info("-" * 60)

# insert data into course_dim table
logging.info("Inserting data into course_dim table")

truncate_course = "TRUNCATE TABLE course_dim;"
redshift.execute_query(truncate_course)

for i in range(10):
    course_name = ["Python", "Java", "Datascience", "React", "HTML", "CSS", "Nodejs", "AWS", "GCP", "Spring"]
    duration = random.randint(30, 300)
    
    insert_course = '''
    INSERT INTO course_dim
    VALUES ({}, '{}', {});
    '''.format(i+1, course_name[i], duration)
    
    result = redshift.execute_query(insert_course)
    if result:
        logging.info(f"course data inserted successfully : {i+1}")
    else:
        logging.info(f"Failed to insert course: {i+1}")

logging.info("-" * 60)

#  insert data into time

logging.info("Inserting data into time_dim table")

truncate_time = "TRUNCATE TABLE time_dim;"
redshift.execute_query(truncate_time)

for i in range(10):
    date = fake.date() 
    
    insert_time = '''
    INSERT INTO date_dim
    VALUES ({}, '{}');
    '''.format(i+1, date)
    
    result = redshift.execute_query(insert_time)
    if result:
        logging.info(f"time data inserted successfully : {i+1}")
    else:
        logging.info(f"Failed to insert time: {i+1}")

logging.info("-" * 60)

# Insert data into engagement_fact table
logging.info("Inserting data into engagement_fact table")

truncate_engagement = "TRUNCATE TABLE engagement_fact;"
redshift.execute_query(truncate_engagement)

for i in range(100):
    id = i+1
    student_id = random.randint(1, 10)
    course_id = random.randint(1, 10)
    date_id = random.randint(1, 10)
    progress = random.randint(1, 100)
    
    insert_engagement = '''
    INSERT INTO engagement_fact 
    VALUES ({}, {}, {}, {}, {});
    '''.format(id, student_id, course_id, date_id, progress)
    
    result = redshift.execute_query(insert_engagement)
    if result:
        logging.info(f"engagement_fact data inserted successfully : {id}")
    else:
        logging.info(f"Failed to insert engagement_fact: {id}")
    
    
    
    




    
    
    