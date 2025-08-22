CREATE DATABASE IF NOT EXISTS aa_poll_demo;
USE aa_poll_demo;

CREATE TABLE surveys (
  survey_id VARCHAR(20) PRIMARY KEY,
  year INT NOT NULL,
  month INT NOT NULL,
  filename VARCHAR(255) NOT NULL,
  processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE survey_questions (
  question_id INT AUTO_INCREMENT PRIMARY KEY,
  survey_id VARCHAR(20) NOT NULL,
  question_number VARCHAR(10) NOT NULL,
  question_part INT DEFAULT 1,
  question_text TEXT NOT NULL,
  is_demographic BOOLEAN DEFAULT FALSE,
  base_description VARCHAR(255) DEFAULT NULL,
  FOREIGN KEY (survey_id) REFERENCES surveys(survey_id)
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
    item_label VARCHAR(255) DEFAULT NULL,
    count INT DEFAULT NULL,
    percent DECIMAL(5,2) DEFAULT NULL,
    FOREIGN KEY (question_id) REFERENCES survey_questions(question_id),
    FOREIGN KEY (survey_id) REFERENCES surveys(survey_id),
    FOREIGN KEY (demo_id) REFERENCES demographics(demo_id)
);

-- All possible answer options for a question
CREATE TABLE IF NOT EXISTS answer_options (
    option_id INT AUTO_INCREMENT PRIMARY KEY,
    question_id INT NOT NULL,
    option_text VARCHAR(255) NOT NULL,
    option_order INT DEFAULT NULL,
    UNIQUE KEY uq_question_option (question_id, option_text),
    CONSTRAINT fk_option_question FOREIGN KEY (question_id) REFERENCES survey_questions(question_id)
);

-- Response counts of all demographics
CREATE TABLE IF NOT EXISTS p1_responses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    question_id INT NOT NULL,
    survey_id VARCHAR(20) NOT NULL,
    option_id INT NOT NULL,
    demo_id INT DEFAULT NULL,
    item_label VARCHAR(80) DEFAULT NULL,  
    cnt INT DEFAULT NULL,
    pct DECIMAL(6,2) DEFAULT NULL,
    CONSTRAINT fk_p1_question FOREIGN KEY (question_id) REFERENCES survey_questions(question_id),
    CONSTRAINT fk_p1_survey   FOREIGN KEY (survey_id)  REFERENCES surveys(survey_id),
    CONSTRAINT fk_p1_option   FOREIGN KEY (option_id)  REFERENCES answer_options(option_id),
    CONSTRAINT fk_p1_demo     FOREIGN KEY (demo_id)    REFERENCES demographics(demo_id)
);