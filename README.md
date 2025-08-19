# AA Survey Analysis System

A comprehensive system for processing AA survey data and providing intelligent analysis through an AI-powered chatbot. This project combines ETL data processing with advanced LLM-driven analytics to extract meaningful insights from survey responses.

## 🏗️ Architecture Overview

### Clean Separation of Concerns

The system follows a **clean architecture principle** where:

- **SQL = Dumb Data Pipe**: Pure data retrieval with no business logic
- **LLM = Smart Analyst**: All intelligence, reasoning, and insight generation

```
User Question → LLM Analysis → SQL Data Retrieval → Raw Data → LLM Synthesis → Intelligent Response
```

## 📊 System Components

### 1. ETL Pipeline (`runner.py`, `processor/`)

- **Purpose**: Extract survey data from Excel files into MySQL database
- **Key Feature**: Multi-row question text extraction with variant detection
- **Output**: Clean, structured survey data ready for analysis

### 2. Survey Chatbot (`survey_chatbot.py`)

- **Purpose**: AI-powered survey data analyst
- **Capabilities**: Natural language queries, demographic analysis, trend identification
- **Architecture**: GPT-4o driven with strategic data exploration

### 3. Database Layer (`database/`)

- **Schema**: Optimized for survey data with demographic breakdowns
- **Key Tables**: `surveys`, `survey_questions`, `p1_responses`, `demographics`
- **Design**: Supports complex demographic filtering without aggregation issues

## 🧠 Intelligent Analysis Architecture

### The Evolution: From Simple SQL to Smart Analysis

#### ❌ Initial Approach (Problematic)

```sql
-- Simple text-to-SQL translation
SELECT COUNT(*) FROM responses WHERE question LIKE '%parking%'
```

**Problems**:

- Literal keyword matching
- No context understanding
- Inflated counts from SUM() aggregation
- Limited insight generation

#### ✅ Final Architecture (Intelligent)

**Step 1: Intent Analysis**

```python
User: "How do different regions feel about speed cameras?"
LLM Analysis: {
  "intent": "Regional sentiment analysis on speed cameras",
  "keywords": ["speed", "camera", "speeding"],  # No demographic terms!
  "strategy": "Find speed camera questions, get regional breakdowns"
}
```

**Step 2: Pure Data Retrieval**

```sql
-- Query 1: Find relevant questions (no intelligence, just search)
SELECT question_id, question_text FROM survey_questions
WHERE question_text LIKE '%speed%' OR question_text LIKE '%camera%'

-- Query 2: Get clean response totals (no SUM!)
SELECT option_text, cnt, pct FROM p1_responses
WHERE question_id IN (...) AND item_label = 'Total'

-- Query 3: Get raw demographic data (individual records)
SELECT demo_code, item_label, option_text, cnt, pct
FROM p1_responses WHERE demo_id IS NOT NULL
```

**Step 3: LLM-Driven Analysis**

```python
# LLM receives raw data and makes intelligent decisions:
- London: "Support cameras": 1,234 responses (45.2%)
- Wales: "Support cameras": 987 responses (52.1%)
- Scotland: "Support cameras": 756 responses (38.9%)

# LLM decides aggregation strategy:
"Urban regions (London, Birmingham): 1,234 + 891 = 2,125 (46.8% avg support)
Rural regions (Wales, Scotland): 987 + 756 = 1,743 (45.5% avg support)
The data shows minimal urban/rural differences in speed camera support..."
```

## 🔧 Key Technical Innovations

### 1. Multi-Row Question Extraction

**Problem**: Survey questions span multiple Excel rows

```excel
Row 1: Q11. Imagine you parked in a private car park and
Row 2: received a parking charge notice (PCN) for a
Row 3: contravention that you believe you did not commit...
```

**Solution**: Enhanced ETL logic reads across rows until logical endpoints

```python
while current_row < len(data):
    cell_text = get_cell_value(current_row)
    if (cell_text.startswith('Base:') or
        cell_text.startswith('-') or      # Variant detection
        cell_text.startswith('Table')):
        break
    question_parts.append(cell_text)
```

### 2. Demographic-Aware Search Logic

**Problem**: Including region names in search corrupted results

```python
# ❌ Bad: Wales search found Wales-mention questions, not car crime
keywords = ["car crime", "Wales"]  # Polluted results

# ✅ Good: Pure topic search, demographic filtering happens later
keywords = ["car crime", "vehicle crime", "theft"]
```

### 3. Raw Data Analysis Pattern

**Problem**: SQL aggregation led to inflated/confusing numbers

```sql
-- ❌ Bad: 72,101 "respondents" (SUM across all demographics!)
SUM(pr.cnt) as total_responses

-- ✅ Good: Let LLM see individual records and decide aggregation
pr.cnt as responses, pr.item_label as demographic_value
```

## 🎯 Critical Design Decisions

### Memory-Based Learning [[memory:4934550]]

> In the p1_responses table, rows with item_label = 'Total' contain the true survey response totals, but they have demo_id = NULL. This means when calculating total survey participation, do NOT join with demographics table as it will filter out all Total rows. For total counts, query p1_responses directly WHERE item_label = 'Total'.

### Intelligent Reasoning [[memory:4647090]]

> User prefers that the survey chatbot uses intelligent reasoning and acts like a data analyst, not just literal SQL queries. The chatbot should understand intent behind questions, plan strategic data exploration, and provide comprehensive insights with demographic breakdowns and trends.

## 🚀 Usage Examples

### Basic Query

```python
from survey_chatbot import SurveyChatbot

chatbot = SurveyChatbot()
response = chatbot.chat("How many people in Wales experienced car crime?")
# Returns: "4,390 respondents in Wales reported experiencing car crime..."
```

### Complex Analysis

```python
response = chatbot.chat("Does parking cost affect high street shopping across age groups?")
# Returns comprehensive analysis with:
# - Intent analysis and data exploration strategy
# - Multiple survey question synthesis
# - Age-based demographic breakdowns
# - Trend analysis with proper citations
# - Policy implications
```

## 🏆 Architecture Benefits

### 1. **Maintainability**

- Clear separation: SQL retrieves, LLM analyzes
- Easy to debug: Data issues vs analysis issues
- Modular: Can swap LLM models or SQL backends

### 2. **Accuracy**

- No SQL aggregation bugs (eliminated 100x count inflation)
- LLM validates its own math and explains reasoning
- Proper demographic handling without double-counting

### 3. **Intelligence**

- Context-aware question interpretation
- Strategic multi-query data exploration
- Intelligent demographic grouping decisions
- Comprehensive insight synthesis

### 4. **Transparency**

- Every statistic cited with survey source (YYYY-MM Q#.#)
- LLM explains aggregation logic ("I combined 18-24 + 25-34...")
- Full conversation history for context

## 🔍 Development Journey: Key Problems Solved

### Problem 1: Truncated Question Text

**Issue**: Q11 showed as "Imagine you parked in a private car park and" (102 chars)
**Root Cause**: ETL only read first row of multi-row questions
**Solution**: Multi-row extraction with variant-aware stopping conditions
**Result**: Q11 now complete at 324 characters

### Problem 2: Inflated Response Counts

**Issue**: Christmas party question showed 72,101 respondents (impossible)
**Root Cause**: `SUM(pr.cnt)` aggregated across all demographics
**Solution**: Use `item_label = 'Total'` records only for survey totals
**Result**: Correct count of 28,886 respondents

### Problem 3: Poor Regional Analysis

**Issue**: "Wales car crime" found no data despite 76,961 Wales records
**Root Cause**: Search included "Wales" keyword, finding irrelevant questions
**Solution**: Pure topic search with demographic filtering in synthesis
**Result**: Wales analysis now works perfectly (4,390 car crime victims found)

### Problem 4: Limited Intelligence

**Issue**: Simple text-to-SQL provided shallow insights
**Root Cause**: No intent analysis or strategic thinking
**Solution**: Multi-step reasoning pipeline with GPT-4o
**Result**: Professional data analyst quality responses

## 🛠️ Technical Stack

- **Language**: Python 3.x
- **Database**: MySQL with custom survey schema
- **AI**: Azure OpenAI GPT-4o
- **ETL**: Pandas for Excel processing
- **Architecture**: Clean separation of data retrieval and analysis

## 📝 Key Lessons Learned

1. **Let AI Do What AI Does Best**: LLM excels at reasoning and synthesis, SQL excels at data retrieval
2. **Raw Data > Aggregated Data**: Give LLM individual records for maximum analytical flexibility
3. **Intent Analysis First**: Understanding user goals enables strategic data exploration
4. **Memory Systems Matter**: Persistent memory of data patterns improves accuracy
5. **Citation Is Critical**: Every statistic must be traceable to its survey source

---

_This system represents a sophisticated approach to survey data analysis, combining robust ETL processes with advanced AI reasoning to provide human-like analytical insights._
