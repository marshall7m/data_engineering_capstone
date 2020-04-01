create_video_lessons = """
CREATE TABLE IF NOT EXISTS video_lessons (
    youtube_url           VARCHAR,
    youtube_section       VARCHAR,
    youtube_lesson        VARCHAR,
    youtube_nano_degree   VARCHAR,
    PRIMARY KEY (youtube_url)
)"""

create_video_log = """
CREATE TABLE IF NOT EXISTS video_user_logs (
    user_id               INT,
    youtube_url           VARCHAR,
    avg_view_per_viewer   FLOAT,
    avg_view_duration     FLOAT,
    PRIMARY KEY (user_id, youtube_url)
)"""

create_mentor_activity = """
CREATE TABLE IF NOT EXISTS mentor_activity (
    user_id              INT,
    mentor_prompt        VARCHAR,
    mentor_section       VARCHAR,
    mentor_project       VARCHAR,
    mentor_text          VARCHAR,
    mentor_nano_degree   VARCHAR,
    mentor_comments      VARCHAR
    PRIMARY KEY (user_id, mentor_prompt)
)"""

create_section_reviews = """
CREATE TABLE IF NOT EXISTS section_reviews (
    user_id        INT,
    emoji_rating   VARCHAR,
    review         VARCHAR,
    PRIMARY KEY (user_id)
)"""

create_projects_reviews = """
CREATE TABLE IF NOT EXISTS project_reviews (
    user_id        INT,
    emoji_rating   VARCHAR,
    review         VARCHAR,
    PRIMARY KEY (user_id)
)"""

create_table_dict = {
    'video_lessons': create_video_lessons,
    'video_log': create_video_log,
    'mentor_activity': create_mentor_activity,
    'section_reviews': create_section_reviews,
    'project_reviews': create_projects_reviews
}

insert_video_lessons = """
INSERT INTO video_lessons (

)"""

insert_video_log = """
INSERT INTO video_log (

)"""

insert_mentor_activity= """
INSERT INTO mentor_activity (

)"""

insert_section_reviews = """
INSERT INTO section_reviews (

)"""

insert_project_reviews = """
INSERT INTO project_reviews (

)"""