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

  create_project_dim = """
  CREATE TABLE IF NOT EXISTS {} (
    project_id INTEGER PRIMARY KEY,
    section_id INTEGER, 
    project_name VARCHAR,
    section_name VARCHAR, 
    degree_name VARCHAR
  )"""

  create_video_dim = """
  CREATE TABLE IF NOT EXISTS {} (
    video_id INTEGER PRIMARY KEY,
    video_name VARCHAR, 
    section_name VARCHAR, 
    degree_name VARCHAR
  )"""

  create_video_log = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER, 
    video_id INTEGER, 
    degree_id INTEGER,
    first_view_date TIMESTAMP,
    last_view_date TIMESTAMP,
    views_per_user INTEGER, 
    PRIMARY KEY (user_id, video_id, last_view_date)
  )"""

  create_mentor_activity = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER, 
    section_id INTEGER, 
    project_id INTEGER, 
    post_date DATE, 
    prompt VARCHAR,
    post_text VARCHAR, 
    post_score INTEGER, 
    answer_date DATE, 
    answer_text VARCHAR, 
    answer_score INTEGER,
    PRIMARY KEY (user_id, prompt, post_date)
  )"""

  create_section_feedback = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER, 
    section_id INTEGER,
    submit_date TIMESTAMP,
    text VARCHAR, 
    rating INTEGER,
    PRIMARY KEY (user_id, section_id)
  )"""

  create_projects_feedback = """
  CREATE TABLE IF NOT EXISTS {} (
    user_id INTEGER, 
    project_id INTEGER,
    submit_date TIMESTAMP,
    text VARCHAR, 
    rating INTEGER,
    PRIMARY KEY (user_id, project_id)
  )"""

  # fact queries

  avg_video_views_per_user = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT 
    vl.video_id, 
    vd.video_name,
    vd.section_name,
    CAST(AVG(vl.views_per_user) AS DECIMAL(10,1)) avg
  FROM 
    {table_1} vl
  JOIN videos_dim vd
    ON vl.video_id = vd.video_id
  GROUP BY 
    vl.video_id, vd.video_name, vd.section_name
  ORDER BY 
    avg DESC
  """

  avg_video_view_date_range = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT 
    vd.video_id, 
    vd.video_name,
    vd.section_name,
    AVG(DATEDIFF(hour, vl.first_view_date, vl.last_view_date)) avg
  FROM 
    {table_1} vl
  JOIN videos_dim vd
    ON vd.video_id = vl.video_id
  GROUP BY 
    vd.video_id, vd.video_name, vd.section_name
  ORDER BY 
    avg DESC
  """

  section_ratings = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT 
    sf.section_id,
    pd.section_name,
    CAST(AVG(sf.rating) AS DECIMAL(10,1)) avg
  FROM 
    {table_1} sf
  JOIN projects_dim pd
    ON sf.section_id = pd.section_id
  GROUP BY
    sf.section_id, pd.section_name
  ORDER BY 
    avg ASC
  """

  project_ratings = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT 
    pf.project_id, 
    pd.project_name,
    CAST(AVG(pf.rating) AS DECIMAL(10,1)) avg
  FROM 
    {table_1} pf
  JOIN projects_dim pd
    ON pf.project_id = pd.section_id
  GROUP BY
    pf.project_id, pd.project_name
  ORDER BY 
    avg ASC
  """

  highest_mentor_activity_prompt_scores = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT post_date, 
    answer_date, 
    user_id, 
    section_id, 
    project_id, 
    prompt, 
    post_text, 
    post_score, 
    answer_text, 
    answer_score 
  FROM {table_1}
  ORDER BY post_score DESC
  """

  highest_mentor_activity_answer_scores = """
  DROP TABLE IF EXISTS {};
  CREATE TABLE {} AS 
  SELECT post_date, 
    answer_date, 
    user_id, 
    section_id, 
    project_id, 
    prompt, 
    post_text, 
    post_score, 
    answer_text, 
    answer_score 
  FROM {table_1}
  ORDER BY answer_score DESC
  """