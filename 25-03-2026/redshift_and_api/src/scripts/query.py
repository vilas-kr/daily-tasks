import logging

from src.service import redshift

logging.basicConfig(level=logging.INFO)

# Completion rate per course
def course_completion_rate():
    course_completion = '''
        SELECT c.course_name,
            ( COUNT(CASE WHEN e.progress_percentage = 100 THEN 1 END)
            * 100.0 )
            / COUNT(DISTINCT e.student_id) AS completion_rate
        FROM engagement_fact e
        INNER JOIN course_dim c
        ON e.course_id = c.course_id
        GROUP BY c.course_name;
    '''

    result = redshift.execute_query(course_completion)

    data = []
    
    for row in result["Records"][1:]:
        data.append({
            "course_name": row[0].get("stringValue"),
            "completion_rate": float(row[1].get("stringValue"))
        })

    return data

# Daily active users
def daily_active_user():
    daily_active_usr = '''
        SELECT d.full_date, COUNT(DISTINCT e.student_id) AS daily_active_users
        FROM engagement_fact AS e 
        INNER JOIN date_dim AS d 
        ON e.date_id = d.date_id
        GROUP BY d.full_date
        ORDER BY d.full_date
    '''
    result = redshift.execute_query(daily_active_usr)
    
    data = []    
    for row in result["Records"][1:]:
        data.append({
            "date": row[0].get("stringValue"),
            "total_active_users": float(row[1].get("longValue"))
        })
        
    return data
    
    
if __name__ == "__main__":
    course_completion_rate()
    daily_active_user()
    