from fastapi import FastAPI

from src.scripts import query

app = FastAPI()

@app.get('/course/engagement')
def get_course_completion():
    return query.course_completion_rate()

@app.get('/course/daily_active_users')
def get_daily_active_user():
    return query.daily_active_user()