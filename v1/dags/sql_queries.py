class SqlQueries:
  create_users_dim = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER PRIMARY KEY,
    name VARCHAR, 
    github_handle VARCHAR, 
    location VARCHAR, 
    email VARCHAR, 
    company VARCHAR
  )"""

  create_project_section_dim = """
  CREATE TABLE IF NOT EXISTS {} (
    project_id INTEGER,
    section_id VARCHAR, 
    project_name VARCHAR,
    section_name VARCHAR, 
    degree_name VARCHAR
  )"""

  create_video_log_dim = """
  CREATE TABLE IF NOT EXISTS {} (
    video_id INTEGER,
    video_name VARCHAR, 
    section_name VARCHAR, 
    degree_name VARCHAR
  )"""

  create_video_log = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER, 
    video_id VARCHAR, 
    first_view_date DATE,
    last_view_date DATE,
    views_per_user INTEGER, 
    PRIMARY KEY (user_id, video_id)
  )"""

  create_mentor_activity = """
  CREATE TABLE IF NOT EXISTS {} (
    post_date DATE, 
    answer_date DATE, 
    user_id INTEGER, 
    section_id INTEGER, 
    project_id INTEGER, 
    degree_id INTEGER, 
    prompt VARCHAR, 
    post_text VARCHAR, 
    post_score INTEGER, 
    answer_text VARCHAR, 
    answer_score INTEGER
  )"""

  create_section_feedback = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER PRIMARY KEY, 
    submit_date DATE,
    emoji_rating VARCHAR, 
    feedback VARCHAR
  )"""

  create_projects_feedback = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER PRIMARY KEY, 
    submit_date DATE,
    emoji_rating VARCHAR, 
    feedback VARCHAR
  )"""

  # fact queries

  avg_video_views_per_user = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT 
    video_id, 
    AVG(views_per_user) 
  FROM 
    {} 
  GROUP BY 
    video_id 
  ORDER BY 
    AVG(views_per_user) DESC
  """

  avg_video_view_date_range = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT 
    video_id, 
    AVG(last_view_date - first_view_date)
  FROM 
    {} 
  GROUP BY 
    video_id 
  ORDER BY 
    AVG(last_view_date - first_view_date) DESC
  """

  section_ratings = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT 
    section_id, 
    AVG(rating) 
  FROM 
    {}
  GROUP BY
  section_id
  ORDER BY 
    AVG(rating) ASC
  """

  project_ratings = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT 
    project_id, 
    AVG(rating) 
  FROM 
    {} 
  GROUP BY 
    project_id
  ORDER BY 
    AVG(rating) ASC
  """

  highest_mentor_activity_prompt_scores = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT post_date, 
    answer_date, 
    user_id, 
    section_id, 
    project_id, 
    degree_id, 
    prompt, 
    post_text, 
    post_score, 
    answer_text, 
    answer_score 
  FROM {}
  ORDER BY post_score DESC
  LIMIT 10
  """

  highest_mentor_activity_answer_scores = """
  DROP TABLE IF EXIST
  CREATE TABLE {} AS
  SELECT post_date, 
    answer_date, 
    user_id, 
    section_id, 
    project_id, 
    degree_id, 
    prompt, 
    post_text, 
    post_score, 
    answer_text, 
    answer_score 
  FROM {}
  ORDER BY answer_score DESC
  LIMIT 10
  """

  # update dimensional tables
  load_video_lessons = """
  INSERT INTO {} 
  SELECT DISTINCT
    video_id,
    video_name,
    section_name, 
    degree_name
  FROM {}
  """

  # spark sentiment analysis tables
  section_feebback = """
  INSERT INTO section_reviews (

  )"""