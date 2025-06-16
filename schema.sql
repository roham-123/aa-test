CREATE DATABASE IF NOT EXISTS aa_poll_demo;
USE aa_poll_demo;

CREATE TABLE processed_files (
  filename VARCHAR(255) PRIMARY KEY
);

CREATE TABLE surveys (
  survey_id VARCHAR(20) PRIMARY KEY,
  year INT NOT NULL,
  month INT NOT NULL,
  filename VARCHAR(255) NOT NULL
);

CREATE TABLE questions (
  question_id INT AUTO_INCREMENT PRIMARY KEY,
  survey_id VARCHAR(20) NOT NULL,
  question_number VARCHAR(10) NOT NULL,
  question_part INT DEFAULT 1,
  question_text TEXT NOT NULL,
  is_demographic BOOLEAN DEFAULT FALSE,
  base_description VARCHAR(255),
  FOREIGN KEY (survey_id) REFERENCES surveys(survey_id)
);

CREATE TABLE p1_responses (
  id INT AUTO_INCREMENT PRIMARY KEY,
  question_id INT NOT NULL,
  item_label VARCHAR(200),
  total_count INT,
  total_percent DECIMAL(5,2),
  male_count INT,
  male_percent DECIMAL(5,2),
  female_count INT,
  female_percent DECIMAL(5,2),
  age_18_24_count INT,
  age_18_24_percent DECIMAL(5,2),
  age_25_34_count INT,
  age_25_34_percent DECIMAL(5,2),
  age_35_44_count INT,
  age_35_44_percent DECIMAL(5,2),
  age_45_54_count INT,
  age_45_54_percent DECIMAL(5,2),
  age_55_64_count INT,
  age_55_64_percent DECIMAL(5,2),
  age_65_plus_count INT,
  age_65_plus_percent DECIMAL(5,2),
  region_scotland_count INT,
  region_scotland_percent DECIMAL(5,2),
  region_north_east_count INT,
  region_north_east_percent DECIMAL(5,2),
  region_north_west_count INT,
  region_north_west_percent DECIMAL(5,2),
  region_yorkshire_humberside_count INT,
  region_yorkshire_humberside_percent DECIMAL(5,2),
  region_west_midlands_count INT,
  region_west_midlands_percent DECIMAL(5,2),
  region_east_midlands_count INT,
  region_east_midlands_percent DECIMAL(5,2),
  region_wales_count INT,
  region_wales_percent DECIMAL(5,2),
  region_eastern_count INT,
  region_eastern_percent DECIMAL(5,2),
  region_london_count INT,
  region_london_percent DECIMAL(5,2),
  region_south_east_count INT,
  region_south_east_percent DECIMAL(5,2),
  region_south_west_count INT,
  region_south_west_percent DECIMAL(5,2),
  region_northern_ireland_count INT,
  region_northern_ireland_percent DECIMAL(5,2),
  seg_ab_count INT,
  seg_ab_percent DECIMAL(5,2),
  seg_c1_count INT,
  seg_c1_percent DECIMAL(5,2),
  seg_c2_count INT,
  seg_c2_percent DECIMAL(5,2),
  seg_de_count INT,
  seg_de_percent DECIMAL(5,2),
  FOREIGN KEY (question_id) REFERENCES questions(question_id)
);

-- Star schema for demographics
CREATE TABLE IF NOT EXISTS demographics (
    demo_id INT AUTO_INCREMENT PRIMARY KEY,
    demo_code VARCHAR(32) NOT NULL UNIQUE,
    demo_description VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS demographic_responses (
    id INT AUTO_INCREMENT PRIMARY KEY,
    question_id INT NOT NULL,
    survey_id VARCHAR(64) NOT NULL,
    demo_id INT NOT NULL,
    item_label VARCHAR(255),
    count INT,
    percent DECIMAL(5,2),
    FOREIGN KEY (question_id) REFERENCES questions(question_id),
    FOREIGN KEY (survey_id) REFERENCES surveys(survey_id),
    FOREIGN KEY (demo_id) REFERENCES demographics(demo_id)
);

-- New tables for narrow P1 facts
CREATE TABLE IF NOT EXISTS answer_options (
    option_id INT AUTO_INCREMENT PRIMARY KEY,
    question_id INT NOT NULL,
    option_text VARCHAR(255) NOT NULL,
    option_order INT DEFAULT NULL,
    UNIQUE KEY uq_question_option (question_id, option_text),
    CONSTRAINT fk_option_question FOREIGN KEY (question_id) REFERENCES questions(question_id)
);

-- Remove obsolete wide table if it exists
DROP TABLE IF EXISTS p1_responses;

CREATE TABLE IF NOT EXISTS p1_responses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    question_id INT NOT NULL,
    survey_id VARCHAR(20) NOT NULL,
    option_id INT NOT NULL,
    demo_id INT DEFAULT NULL,
    item_label VARCHAR(80) DEFAULT NULL,
    cnt INT DEFAULT NULL,
    pct DECIMAL(6,2) DEFAULT NULL,
    CONSTRAINT fk_p1_question FOREIGN KEY (question_id) REFERENCES questions(question_id),
    CONSTRAINT fk_p1_survey   FOREIGN KEY (survey_id)  REFERENCES surveys(survey_id),
    CONSTRAINT fk_p1_option   FOREIGN KEY (option_id)  REFERENCES answer_options(option_id),
    CONSTRAINT fk_p1_demo     FOREIGN KEY (demo_id)    REFERENCES demographics(demo_id)
);