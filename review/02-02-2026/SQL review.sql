/*
1. Average score > overall average score
2. Total submissions > overall average submissions
3. Find users whose total activity count is greater than the average activity count of all users.
4. Find courses where the submission rate is less than 50% of enrolled users.
*/

USE lms_db;
GO

-- 1. Average score > overall average score on each course
WITH course_avg AS (
	SELECT a.course_id, AVG(ass.obtained_marks) AS avg_course_marks
	FROM lms.assessment AS a
	INNER JOIN lms.assessment_submit AS ass
		ON a.assessment_id = ass.assessment_id
	GROUP BY a.course_id

),
user_avg AS (
	SELECT a.course_id, ass.user_id, AVG(ass.obtained_marks) AS avg_user_marks
	FROM lms.assessment AS a
	INNER JOIN lms.assessment_submit AS ass
		ON a.assessment_id = ass.assessment_id
	GROUP BY a.course_id, ass.user_id
)
SELECT ca.course_id, ua.user_id, ua.avg_user_marks, ca.avg_course_marks
FROM course_avg AS ca
LEFT JOIN user_avg AS ua
	ON ca.course_id = ua.course_id
WHERE ua.avg_user_marks > ca.avg_course_marks;
GO

-- 2 Total submissions > overall average submissions each course
WITH course_submit AS (
	SELECT a.course_id, COUNT(*)/COUNT(DISTINCT ass.user_id) AS avg_course_submission
	FROM lms.assessment AS a
	INNER JOIN lms.assessment_submit AS ass
		ON a.assessment_id = ass.assessment_id
	GROUP BY a.course_id
),
user_submit AS (
	SELECT a.course_id, ass.user_id, COUNT(*) AS avg_user_submission
	FROM lms.assessment AS a
	INNER JOIN lms.assessment_submit AS ass
		ON a.assessment_id = ass.assessment_id
	GROUP BY a.course_id, ass.user_id
)
SELECT cs.course_id, us.user_id, cs.avg_course_submission, us.avg_user_submission
FROM course_submit AS cs
LEFT JOIN user_submit AS us
	ON cs.course_id = us.course_id
WHERE us.avg_user_submission > cs.avg_course_submission;
GO

-- 3 Find users whose total activity count is greater than the average activity count of all users.
SELECT user_id, COUNT(*) AS activity_count
FROM lms.assessment_submit
GROUP BY user_id
HAVING COUNT(*) > (
			SELECT COUNT(*)/COUNT(DISTINCT ass.user_id) AS avg_activity_count
			FROM lms.assessment_submit AS ass
			);
GO

-- 4 Find courses where the submission rate is less than 50% of enrolled users

SELECT e.course_id,
    (COUNT(DISTINCT s.user_id) * 1.0 / COUNT(DISTINCT e.user_id))*100 AS submission_rate
FROM lms.enrollment AS e
LEFT JOIN lms.assessment AS a
    ON e.course_id = a.course_id
LEFT JOIN lms.assessment_submit AS s
    ON a.assessment_id = s.assessment_id
   AND e.user_id = s.user_id
GROUP BY e.course_id
HAVING COUNT(DISTINCT s.user_id) * 1.0
       / COUNT(DISTINCT e.user_id) < 0.5;
GO

	


